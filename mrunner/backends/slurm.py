# -*- coding: utf-8 -*-
import logging
import pprint
import socket
import tarfile
import tempfile
from time import sleep
from termcolor import colored

import attr
from fabric.api import run as fabric_run
from fabric.context_managers import cd
from fabric.contrib.project import rsync_project
from fabric.state import env
from paramiko.agent import Agent
from path import Path

from mrunner.experiment import COMMON_EXPERIMENT_MANDATORY_FIELDS, COMMON_EXPERIMENT_OPTIONAL_FIELDS
from mrunner.plgrid import PLGRID_USERNAME, PLGRID_HOST, PLGRID_TESTING_PARTITION
from mrunner.utils.namesgenerator import id_generator
from mrunner.utils.neptune import NeptuneToken
from mrunner.utils.utils import GeneratedTemplateFile, get_paths_to_copy, make_attr_class, filter_only_attr

LOGGER = logging.getLogger(__name__)
RECOMMENDED_CPUS_NUMBER = 4
DEFAULT_SCRATCH_SUBDIR = 'mrunner_scratch'
SCRATCH_DIR_RANDOM_SUFIX_SIZE = 10


def generate_experiment_scratch_dir(experiment):
    experiment_subdir = '{name}_{random_id}'.format(name=experiment.name,
                                                    random_id=id_generator(SCRATCH_DIR_RANDOM_SUFIX_SIZE))
    project_subdir = generate_project_scratch_dir(experiment)
    return project_subdir / experiment_subdir


def generate_project_scratch_dir(experiment):
    project_subdir = '{user_id}_{project}'.format(user_id=experiment.user_id, project=experiment.project)
    scratch_subdir = (experiment.scratch_subdir or DEFAULT_SCRATCH_SUBDIR)
    return Path(experiment._slurm_scratch_dir) / scratch_subdir / project_subdir


EXPERIMENT_MANDATORY_FIELDS = [
    ('venv', dict()),  # path to virtual environment
    ('user_id', dict()),  # used to generate project scratch dir
    ('_slurm_scratch_dir', dict())  # obtained from cluster $SCRATCH env
]

EXPERIMENT_OPTIONAL_FIELDS = [
    # by default use plgrid configuration
    ('slurm_url', dict(default='{}@{}'.format(PLGRID_USERNAME, PLGRID_HOST))),
    ('partition', dict(default=PLGRID_TESTING_PARTITION)),

    # scratch directory related
    ('scratch_subdir', dict(default='')),
    ('project_scratch_dir', dict(default=attr.Factory(generate_project_scratch_dir, takes_self=True))),
    ('experiment_scratch_dir', dict(default=attr.Factory(generate_experiment_scratch_dir, takes_self=True))),

    # run time related
    ('account', dict(default=None)),
    ('log_output_path', dict(default=None)),
    ('time', dict(default=None)),
    ('ntasks', dict(default=None)),
    ('num_nodes', dict(default=None)),
    ('modules_to_load', dict(default=attr.Factory(list), type=list)),
    ('after_module_load_cmd', dict(default='')),
    ('cmd_type', dict(default='srun')),
    ('requirements_file', dict(default=(None)))
]

EXPERIMENT_FIELDS = COMMON_EXPERIMENT_MANDATORY_FIELDS + EXPERIMENT_MANDATORY_FIELDS + \
                    COMMON_EXPERIMENT_OPTIONAL_FIELDS + EXPERIMENT_OPTIONAL_FIELDS

ExperimentRunOnSlurm = make_attr_class('ExperimentRunOnSlurm', EXPERIMENT_FIELDS, frozen=True)


class ExperimentScript(GeneratedTemplateFile):
    DEFAULT_SLURM_EXPERIMENT_SCRIPT_TEMPLATE = 'slurm_experiment.sh.jinja2'

    def __init__(self, experiment, template_filename=None):
        # merge env vars
        env = experiment.cmd.env.copy() if experiment.cmd else {}
        env.update(experiment.env)
        env = {k: str(v) for k, v in env.items()}

        template_filename = template_filename or self.DEFAULT_SLURM_EXPERIMENT_SCRIPT_TEMPLATE

        experiment = attr.evolve(experiment, env=env, experiment_scratch_dir=experiment.experiment_scratch_dir)

        super(ExperimentScript, self).__init__(template_filename=template_filename,
                                               experiment=experiment)
        self.experiment = experiment
        self.path.chmod('a+x')

    @property
    def script_name(self):
        e = self.experiment
        return '{}.sh'.format(e.experiment_scratch_dir.relpath(e.project_scratch_dir))


class SlurmWrappersCmd(object):

    def __init__(self, cmd_type, experiment, script_path):
        self._experiment = experiment
        print(100 * 'kurwa\n')
        print(type(self._experiment))
        print(self._experiment)
        self._script_path = script_path
        self._cmd_type = cmd_type

    @property
    def command(self):
        # see: https://slurm.schedmd.com/srun.html
        # see: https://slurm.schedmd.com/sbatch.html

        if not self._cmd_type:
            raise RuntimeError('Instantiate one of SlurmWrapperCmd subclasses')

        cmd_items = [self._cmd_type]

        def _extend_cmd_items(cmd_items, option, data_key, default=None):
            value = self._getattr(data_key)
            if value:
                cmd_items += [option, str(value)]
            elif default:
                cmd_items += [option, default]

        default_log_path = self._experiment.experiment_scratch_dir / 'slurm.log' if self._cmd_type == 'sbatch' else None
        _extend_cmd_items(cmd_items, '-A', 'account')
        _extend_cmd_items(cmd_items, '-o', 'log_output_path', default_log_path)  # output
        _extend_cmd_items(cmd_items, '-p', 'partition')
        _extend_cmd_items(cmd_items, '-t', 'time')

        cmd_items += self._resources_items()
        cmd_items += [self._script_path]

        return ' '.join(cmd_items)

    def _getattr(self, key):
        return getattr(self, key, getattr(self._experiment, key) or None)

    def _resources_items(self):
        """mapping from mrunner notation into slurm"""
        cmd_items = []
        mrunner_resources = self._getattr('resources')
        for resource_type, resource_qty in mrunner_resources.items():
            if resource_type == 'cpu':
                ntasks = int(self._getattr('ntasks') or 1)
                num_nodes = self._getattr('num_nodes')
                cores_per_task = resource_qty

                cmd_items += ['-c', str(cores_per_task)]

                if num_nodes is not None:
                    cmd_items += ['-N', str(num_nodes)]
                    LOGGER.debug('Running on {} nodes'.format(num_nodes))

                if ntasks:
                    cmd_items += ['-n', str(ntasks)]
                    LOGGER.debug('Running {} tasks'.format(ntasks))

            elif resource_type == 'gpu':
                cmd_items += ['--gres', 'gpu:{}'.format(resource_qty)]
                LOGGER.debug('Using {} gpu'.format(resource_qty))
            elif resource_type == 'mem':
                cmd_items += ['--mem', str(resource_qty)]
                LOGGER.debug('Using {} memory'.format(resource_qty))
            else:
                raise ValueError('Unsupported resource request: {}={}'.format(resource_type, resource_qty))

        return cmd_items

class SlurmNeptuneToken(NeptuneToken):

    def __init__(self, experiment):
        # TODO: need refactor other part of code - here expereiment can be dict or backend specific object
        user_id = experiment['user_id'] if isinstance(experiment, dict) else experiment.user_id
        profile_name = '{}-{}'.format(user_id, socket.gethostname())
        super(SlurmNeptuneToken, self).__init__(profile=profile_name)


class SlurmBackend(object):

    def run(self, experiment, dry_run=False):
        assert Agent().get_keys(), "Add your private key to ssh agent using 'ssh-add' command"
        pprint.pprint(experiment)
        # configure fabric

        # TODO(maciek): fast hack!
        plgrid_username = experiment.get('PLGRID_USERNAME', PLGRID_USERNAME)
        plgrid_host = experiment.get('PLGRID_HOST', PLGRID_HOST)

        slurm_url = experiment.pop('slurm_url', '{}@{}'.format(plgrid_username, plgrid_host))
        env['host_string'] = slurm_url



        slurm_scratch_dir = experiment.get('slurm_scratch_dir',
                                           Path(self._fabric_run('echo $SCRATCH')))

        # TODO(maciek): this is too misleading
        experiment = ExperimentRunOnSlurm(slurm_scratch_dir=slurm_scratch_dir, slurm_url=slurm_url,
                                          **filter_only_attr(ExperimentRunOnSlurm, experiment))
        LOGGER.debug('Configuration: {}'.format(experiment))

        if not dry_run:
            self.ensure_directories(experiment)
            self.deploy_neptune_token(experiment=experiment)
            script_path = self.deploy_code(experiment)

            cmd = SlurmWrappersCmd(experiment.cmd_type, experiment=experiment, script_path=script_path)
            self._fabric_run(cmd.command)
        else:
            print(colored(30 * '=' + ' dry_run = True, not executing!!! ' + 30 * '=', 'yellow', attrs=['bold']))

    def ensure_directories(self, experiment):
        self._ensure_dir(experiment.experiment_scratch_dir)
        self._ensure_dir(experiment.storage_dir)

    def deploy_code(self, experiment):
        paths_to_dump = get_paths_to_copy(exclude=experiment.exclude, paths_to_copy=experiment.paths_to_copy)
        with tempfile.NamedTemporaryFile(suffix='.tar.gz') as temp_file:
            # archive all files
            with tarfile.open(temp_file.name, 'w:gz') as tar_file:
                for p in paths_to_dump:
                    LOGGER.debug('Adding "{}" to deployment archive'.format(p.rel_remote_path))
                    tar_file.add(p.local_path, arcname=p.rel_remote_path)

            # upload archive to cluster and extract
            self._put(temp_file.name, experiment.experiment_scratch_dir)
            with cd(experiment.experiment_scratch_dir):
                archive_remote_path = experiment.experiment_scratch_dir / Path(temp_file.name).name
                self._fabric_run('tar xf {tar_filename} && rm {tar_filename}'.format(tar_filename=archive_remote_path))

        # create and upload experiment script
        script = ExperimentScript(experiment, template_filename='slurm_experiment_v2.sh.jinja2')

        remote_script_path = experiment.project_scratch_dir / script.script_name
        self._put(script.path, remote_script_path)

        return remote_script_path

    def deploy_neptune_token(self, experiment):
        if experiment.local_neptune_token:
            remote_token = SlurmNeptuneToken(experiment=experiment)
            self._ensure_dir(remote_token.path.parent)
            self._put(experiment.local_neptune_token.path, remote_token.path)

    @staticmethod
    def _put(local_path, remote_path, quiet=True):
        quiet_kwargs = {'ssh_opts': '-q', 'extra_opts': '-q'} if quiet else {}
        rsync_project(remote_path, local_path, **quiet_kwargs)

    @staticmethod
    def _ensure_dir(directory_path):
        SlurmBackend._fabric_run('mkdir -p {path}'.format(path=directory_path))

    @staticmethod
    def _fabric_run(cmd):
        return fabric_run(cmd)
