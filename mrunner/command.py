class Command(object):
    '''This is because we don't want to change NeptuneWrapperCmd'''

    @property
    def command(self):
        raise NotImplementedError

    @property
    def env(self):
        raise NotImplementedError


class SimpleCommand(object):
    def __init__(self, cmd=None, env=None):
        self.cmd_ = cmd
        self.env_ = env or {}

    @property
    def command(self):
        return self.cmd_

    @property
    def env(self):
        return self.env_


