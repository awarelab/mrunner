#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import json
import logging
import random
import re
import sys
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from copy import deepcopy

import click
from termcolor import colored
from path import Path

from mrunner.backends.k8s import KubernetesBackend
from mrunner.backends.slurm import SlurmBackend, SlurmNeptuneToken
from mrunner.backends.local import LocalBackend
from mrunner.cli.config import ConfigParser, context as context_cli
from mrunner.command import SimpleCommand, Command
#from mrunner.common import create_firestore_client
from mrunner.experiment import generate_experiments, get_experiments_spec_fn, merge_experiment_parameters, Experiment
from mrunner.utils.neptune import NeptuneWrapperCmd, NeptuneToken, NEPTUNE_LOCAL_VERSION

USE_FIRESTORE = False
LOGGER = logging.getLogger(__name__)


def get_default_config_path(ctx):
    default_config_file_name = 'config.yaml'

    app_name = Path(ctx.command_path).stem
    app_dir = Path(click.get_app_dir(app_name))
    return app_dir / default_config_file_name


@click.group()
@click.option('--debug/--no-debug', default=False, help='Enable debug messages')
@click.option('--config', default=None, type=click.Path(dir_okay=False),
              help='Path to mrunner yaml configuration')
@click.option('--context', default=None, help='Name of remote context to use '
                                              '(if not provided, "contexts.current" conf key will be used)')
@click.pass_context
def cli(click_ctx, debug, config, context):
    """Deploy experiments on computation cluster"""

    log_tags_to_suppress = ['pykwalify', 'docker', 'kubernetes', 'paramiko', 'requests.packages']
    logging.basicConfig(level=debug and logging.DEBUG or logging.INFO)
    for tag in log_tags_to_suppress:
        logging.getLogger(tag).setLevel(logging.ERROR)

    # read configuration
    config_path = Path(config or get_default_config_path(click_ctx))
    LOGGER.debug('Using {} as mrunner config'.format(config_path))
    config = ConfigParser(config_path).load()

    cmd_require_context = click_ctx.invoked_subcommand not in ['context']
    if cmd_require_context:
        context_name = context or config.current_context or None
        if not context_name:
            raise click.ClickException(
                'Provide context name (use CLI "--context" option or use "mrunner context set-active" command)')
        if context_name not in config.contexts:
            raise click.ClickException(
                'Could not find predefined context: "{}". Use context add command.'.format(context_name))

        try:
            context = config.contexts[context_name]
            for k in ['neptune', 'storage_dir', 'backend_type', 'context_name']:
                if k not in context:
                    raise AttributeError('Missing required "{}" context key'.format(k))
        except KeyError:
            raise click.ClickException('Unknown context {}'.format(context_name))
        except AttributeError as e:
            raise click.ClickException(e)

    click_ctx.obj.update({'config_path': config_path,
                          'config': config,
                          'context': context})


# INFO(maciek): copied from
# https://stackoverflow.com/questions/22693513/merging-hierarchy-of-dictionaries-in-python
def merge(a, b):
    if isinstance(b, dict) and isinstance(a, dict):
        a_and_b = a.keys() & b.keys()
        every_key = a.keys() | b.keys()
        return {k: merge(a[k], b[k]) if k in a_and_b else deepcopy(a[k] if k in a else b[k])
                for k in every_key}
    return deepcopy(b)


def overwrite_using_overwrite_dict(d1, d2):
    return merge(d1, d2)


@cli.command()
@click.option('--neptune', type=click.Path(), help="Path to neptune experiment config")
@click.option('--spec', default='spec', help="Name of function providing experiment specification")
@click.option('--cpu', default=None, help="-c in slurm")
@click.option('--tags', multiple=True, help='Additional tags')
@click.option('--requirements_file', type=click.Path(), help='Path to requirements file')
@click.option('--base_image', help='Base docker image used in experiment')
@click.option('--offline/--no-offline', default=False, help="Neptune offline option")
@click.option('--dry-run/--no-dry-run', default=False, help="Dry run")
@click.option('--shuffle/--no-shuffle', default=False, help="Do shuffle before limit?")
@click.option('--limit', default=None, type=int, help="")
@click.argument('script_path')
@click.argument('params', nargs=-1)
@click.pass_context
def run(click_ctx, neptune, spec, cpu, tags, requirements_file, base_image, offline, dry_run,
        shuffle, limit, script_path, params):
    """Run experiment"""

    context = click_ctx.obj['context']

    # validate options and arguments
    requirements = requirements_file and [req.strip() for req in Path(requirements_file).open('r')] or []
    if context['backend_type'] == 'kubernetes' and not base_image:
        raise click.ClickException('Provide docker base image')
    if context['backend_type'] == 'kubernetes' and not requirements_file:
        raise click.ClickException('Provide requirements.txt file')
    script_has_spec = get_experiments_spec_fn(script_path, spec) is not None
    neptune_support = context.get('neptune', None) or neptune
    if neptune_support and not neptune and not script_has_spec:
        raise click.ClickException('Neptune support is enabled in context '
                                   'but no neptune config or python experiment descriptor provided')
    if neptune and script_has_spec:
        raise click.ClickException('Provide only one of: neptune config or python experiment descriptor')

    if not neptune_support:
        # TODO: implement it if possible
        raise click.ClickException('Currentlu doesn\'t support experiments without neptune')

    neptune_dir = None
    try:
        # prepare neptune directory in case if neptune yamls shall be generated
        if neptune_support and not neptune:
            script_path = Path(script_path)
            neptune_dir = script_path.parent / 'neptune_{}'.format(script_path.stem)
            neptune_dir.makedirs_p()

        experiments = list(generate_experiments(script_path, neptune, context, spec_fn_name=spec,
                                      neptune_dir=neptune_dir, rest_argv=click_ctx.obj['rest_argv']))
        if shuffle:
            random.shuffle(experiments)

        if limit is not None:
            print(type(limit))
            experiments = experiments[:limit]

        print(colored(30 * '=' + (' Will run {} experiments '.format(len(experiments))) + 30 * '=', 'green', attrs=['bold']))

        # if len(l) == 1 and l[0][1]['backend_type'] == 'local':
        #     neptune_path, experiment = l[0]
        #     doit(base_image, context, cpu, dry_run, experiment, neptune_path, neptune_support, offline, params,
        #                      requirements, tags)
        # else:

        if len(experiments) == 1:
            experiment = experiments[0]
            doit(base_image, context, cpu, dry_run, experiment,
                                             neptune_support, offline, params,
                                             requirements, tags)
            # max_workers = 1
            # process_pool_class = ThreadPoolExecutor
        else:
            max_workers = 6
            process_pool_class = ProcessPoolExecutor

            with process_pool_class(max_workers=max_workers) as executor:
                futures = []
                for experiment in experiments:
                    future = executor.submit(doit, base_image, context, cpu, dry_run, experiment,
                                             neptune_support, offline, params,
                                             requirements, tags)
                futures.append(future)
                for future in futures:
                    print(future.result())

        print(colored(30 * '=' + (' Run {} experiments '.format(len(experiments))) + 30 * '=', 'green', attrs=['bold']))

    finally:
        if neptune_dir:
            neptune_dir.rmtree_p()


def experiment_to_some_dict(experiment: Experiment, context, **cli_kwargs):
    neptune_yaml_path, cli_kwargs_, parameters = experiment['neptune_yaml_path'], experiment['cli_params'], experiment['parameters']
    cli_kwargs_['name'] = re.sub(r'[ .,_-]+', '-', cli_kwargs_['name'].lower())
    cli_kwargs_['cwd'] = Path.getcwd()

    # neptune_config = load_neptune_config(neptune_path) if neptune_path else {}
    # del neptune_config['storage']

    neptune_config = {}
    cli_kwargs_.update(**cli_kwargs)
    experiment = merge_experiment_parameters(cli_kwargs_, neptune_config, context)
    experiment['neptune_yaml_path'] = neptune_yaml_path
    return experiment


def doit(base_image, context, cpu, dry_run, experiment, neptune_support, offline, params, requirements,
         tags, **cli_kwargs):
    experiment = experiment_to_some_dict(experiment, context, **cli_kwargs)

    neptune_yaml_path = experiment['neptune_yaml_path']
    experiment.update({'base_image': base_image, 'requirements': requirements})
    if neptune_support:
        script = experiment.pop('script')
        cmd = ' '.join([script] + list(params))
        # tags from neptune.yaml will be extracted by neptune
        additional_tags = context.get('tags', []) + list(tags)

        remote_neptune_token = None
        if NEPTUNE_LOCAL_VERSION.version[0] == 2:
            experiment['local_neptune_token'] = NeptuneToken()
            assert experiment['local_neptune_token'].path.expanduser().exists(), \
                'Login to neptune first with `neptune account login` command'

            remote_neptune_token = {
                'kubernetes': NeptuneToken,
                'slurm': lambda: SlurmNeptuneToken(experiment),
                'local': lambda: None
            }[experiment['backend_type']]()

        neptune_profile_name = remote_neptune_token.profile_name if remote_neptune_token else None


        experiment['neptune_cmd'] = NeptuneWrapperCmd(cmd=cmd, experiment_config_path=neptune_yaml_path,
                                              neptune_storage=context['storage_dir'],
                                              paths_to_dump=None,
                                              additional_tags=additional_tags,
                                              neptune_profile=neptune_profile_name,
                                              offline=offline)

        # INFO(maciek): this is a hack for setting this up with MPI,

        experiment['neptune_cmd_offline'] = NeptuneWrapperCmd(cmd=cmd, experiment_config_path=neptune_yaml_path,
                                              neptune_storage=context['storage_dir'],
                                              paths_to_dump=None,
                                              additional_tags=additional_tags,
                                              neptune_profile=neptune_profile_name,
                                              offline=True)
        experiment['env']['NEPTUNE_CMD'] = experiment['neptune_cmd'].command
        experiment['env']['NEPTUNE_CMD_OFFLINE'] = experiment['neptune_cmd_offline'].command
        experiment['env']['NEPTUNE_YAML_PATH'] = experiment['neptune_yaml_path']

        # INFO(lukasz): this is a hack for MPI
        if experiment['backend_type'] == 'slurm':
            experiment['resources'] = { 
                    'account': experiment['account'],
                    'partition': experiment['partition'],
                    'time': experiment['time'], 
                    'ntasks': experiment['overwrite_dict']['ntasks'],
                    'nodes': experiment['overwrite_dict']['num_nodes'],
                    'cpus-per-task': experiment['overwrite_dict']['resources']['cpu'],
                    'mem': experiment['overwrite_dict']['resources']['mem']}

        if experiment.get('entrypoint_path', None) is not None:
            experiment['cmd'] = SimpleCommand(cmd=experiment['entrypoint_path'])
        else:
            experiment['cmd'] = experiment['neptune_cmd']

        experiment.setdefault('paths_to_copy', [])
    else:
        # TODO: implement no neptune version
        # TODO: for sbatch set log path into something like os.path.join(resource_dir_path, "job_logs.txt")
        raise click.ClickException('Not implemented yet')

    experiment['neptune_yaml_path'] = neptune_yaml_path
    # TODO(maciek): this is a hack!
    experiment = overwrite_using_overwrite_dict(experiment, experiment.get('overwrite_dict', {}))
    experiment.pop('overwrite_dict')
    if cpu is not None:
        experiment['resources']['cpu'] = cpu


    #if USE_FIRESTORE:
    #    firestore_exp_id = str(uuid.uuid1())
    #    save_experiment_to_firestore(firestore_exp_id, experiment)
    #    experiment['env']['FIRESTORE_EXPERIMENT_ID'] = firestore_exp_id

    experiment['env'].update(experiment.get('env_update', {})) # This is bad!

    backend = {
        'kubernetes': KubernetesBackend,
        'slurm': SlurmBackend,
        'local': LocalBackend
    }[experiment['backend_type']]()
    backend.run(experiment=experiment, dry_run=dry_run)
    return 'OK'


#def save_experiment_to_firestore(firestore_exp_id, experiment):
#    db = create_firestore_client()
#    doc_ref = db.collection('experiments').document(firestore_exp_id)
#    doc_ref.set({
#        'hparams': str(experiment['hparams'])
#    })


cli.add_command(context_cli)


def main():
    argv = sys.argv
    my_argv, rest_argv = split_by_elem(argv, '--')
    cli.main(args=my_argv[1:], obj={'rest_argv': rest_argv})


def split_by_elem(argv, el):
    try:
        index = argv.index(el)
    except ValueError:
        index = None
    my_argv = argv[:index] if index is not None else argv
    rest = argv[index + 1:] if index is not None else []
    return my_argv, rest


if __name__ == '__main__':
    main()
