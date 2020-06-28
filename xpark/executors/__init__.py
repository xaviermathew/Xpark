class Executor(object):
    def __init__(self, ctx, backend):
        self.ctx = ctx
        self.backend = backend

    def execute(self, physical_plan):
        return self.backend.execute(physical_plan)
