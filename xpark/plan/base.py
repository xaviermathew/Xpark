class BaseOp(object):
    is_start_op = False
    returns_data = False
    reads_data = False

    def __init__(self, ctx, part_id=0):
        self.ctx = ctx
        self.stage_id = ctx.get_next_stage_id()
        self.part_id = part_id

    def __repr__(self):
        return '<%s>' % self.__class__.__name__

    @property
    def task_id(self):
        return '%s.%s.%s.%s' % (self.ctx.job_id, self.stage_id, self.__class__.__name__, self.part_id)


class BasePlan(object):
    def __init__(self, nx_graph, start_node):
        self.nx_graph = nx_graph
        self.start_node = start_node
