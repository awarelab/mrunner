import os
import random
import string
import subprocess
from pathlib import Path
from time import strftime

from termcolor import colored



class LocalBackend(object):
    def __int__(self):
        pass

    def run(self, experiment):

        def generate_exp_dir_path(experiment):
            random_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

            s = '{date}_{random_id}'.format(date=strftime('%Y_%m_%d_%H_%M'),
                                            random_id=random_id)
            exp_dir_path = Path(experiment['storage_dir']) / experiment['storage_dir'] / s

            return exp_dir_path

        env_update = {
            'NEPTUNE_YAML_PATH': experiment['neptune_yaml_path'],
            'EXP_DIR_PATH': str(generate_exp_dir_path(experiment))
        }

        # import ipdb; ipdb.set_trace()
        env = os.environ
        env.update(env_update)
        env.update(experiment['env'])

        print(colored(30 * '=' + ' Executing!!! ' + 30 * '=', 'green', attrs=['bold']))
        subprocess.call(experiment['cmd'].command, shell=True, env=env)



