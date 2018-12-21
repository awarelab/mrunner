import subprocess


class LocalBackend(object):
    def __int__(self):
        pass

    def run(self, experiment):
        subprocess.check_call(experiment['cmd'].command, shell=True)

