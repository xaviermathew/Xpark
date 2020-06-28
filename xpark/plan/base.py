class BaseOp(object):
    is_start_op = False
    returns_data = True
    reads_data = True

    def __init__(self, ctx, stage_id, part_id=0):
        self.ctx = ctx
        self.stage_id = stage_id
        self.part_id = part_id

    def __repr__(self):
        return '<%s>' % self.task_id

    @property
    def task_id(self):
        return '%s.%s.%s.%s' % (self.ctx.job_id, self.stage_id, self.__class__.__name__, self.part_id)


class BasePlan(object):
    def __init__(self, nx_graph, start_node):
        self.nx_graph = nx_graph
        self.start_node = start_node
