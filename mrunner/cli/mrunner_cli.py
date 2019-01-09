#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import json
import logging
import sys
import uuid
from copy import deepcopy

import click
from termcolor import colored
from path import Path

from mrunner.backends.k8s import KubernetesBackend
from mrunner.backends.slurm import SlurmBackend, SlurmNeptuneToken
from mrunner.backends.local import LocalBackend
from mrunner.cli.config import ConfigParser, context as context_cli
from mrunner.common import create_firestore_client
from mrunner.experiment import generate_experiments, get_experiments_spec_handle
from mrunner.utils.neptune import NeptuneWrapperCmd, NeptuneToken, NEPTUNE_LOCAL_VERSION

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

    click_ctx.obj = {'config_path': config_path,
                     'config': config,
                     'context': context}


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
@click.option('--limit', default=None, type=int, help="")
@click.argument('script')
@click.argument('params', nargs=-1)
@click.pass_context
def run(click_ctx, neptune, spec, cpu, tags, requirements_file, base_image, offline, dry_run,
        limit, script, params):
    """Run experiment"""

    context = click_ctx.obj['context']

    # validate options and arguments
    requirements = requirements_file and [req.strip() for req in Path(requirements_file).open('r')] or []
    if context['backend_type'] == 'kubernetes' and not base_image:
        raise click.ClickException('Provide docker base image')
    if context['backend_type'] == 'kubernetes' and not requirements_file:
        raise click.ClickException('Provide requirements.txt file')
    script_has_spec = get_experiments_spec_handle(script, spec) is not None
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
            script_path = Path(script)
            neptune_dir = script_path.parent / 'neptune_{}'.format(script_path.stem)
            neptune_dir.makedirs_p()

        l = list(generate_experiments(script, neptune, context, spec=spec,
                                                             neptune_dir=neptune_dir))
        if limit is not None:
            print(type(limit))
            l = l[:limit]

        print(colored(30 * '=' + (' Will run {} experiments '.format(len(l))) + 30 * '=', 'green', attrs=['bold']))

        for neptune_path, experiment in l:
            neptune_yaml_path = neptune_path
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
                experiment['cmd'] = NeptuneWrapperCmd(cmd=cmd, experiment_config_path=neptune_path,
                                                      neptune_storage=context['storage_dir'],
                                                      paths_to_dump=None,
                                                      additional_tags=additional_tags,
                                                      neptune_profile=neptune_profile_name,
                                                      offline=offline)
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

            firestore_exp_id = str(uuid.uuid1())
            save_experiment_to_firestore(firestore_exp_id, experiment)
            experiment['env']['FIRESTORE_EXPERIMENT_ID'] = firestore_exp_id

            backend = {
                'kubernetes': KubernetesBackend,
                'slurm': SlurmBackend,
                'local': LocalBackend
            }[experiment['backend_type']]()
            # TODO: add calling experiments in parallel
            backend.run(experiment=experiment, dry_run=dry_run)

        print(colored(30 * '=' + (' Run {} experiments '.format(len(l))) + 30 * '=', 'green', attrs=['bold']))

    finally:
        if neptune_dir:
            neptune_dir.rmtree_p()


def save_experiment_to_firestore(firestore_exp_id, experiment):
    db = create_firestore_client()
    doc_ref = db.collection('experiments').document(firestore_exp_id)
    doc_ref.set({
        'hparams': str(experiment['hparams'])
    })


cli.add_command(context_cli)

def main():
    argv = sys.argv

    my_argv, rest = split_by_elem(argv, '--')
    print(my_argv[1:], rest)


    cli.main(args=my_argv[1:])
    # cli()


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
