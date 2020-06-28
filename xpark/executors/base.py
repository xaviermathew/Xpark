class BaseExecutor(object):
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def execute(self):
        raise NotImplementedError