import os
import subprocess
from termcolor import colored


class LocalBackend(object):
    def __int__(self):
        pass

    def run(self, experiment):
        env_update = {
            'NEPTUNE_YAML_PATH': experiment['neptune_yaml_path'],
        }

        env = os.environ
        env.update(env_update)
        env.update(experiment['env'])

        print(colored(30 * '=' + ' Executing!!! ' + 30 * '=', 'green', attrs=['bold']))
        subprocess.call(experiment['cmd'].command, shell=True, env=env)



