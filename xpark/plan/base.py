import networkx as nx


class BaseOp(object):
    reads_data = True
    returns_data = True

    def __init__(self, plan, part_id=0):
        self.plan = plan
        self.part_id = part_id

    @property
    def prev_op(self):
        nodes = list(self.plan.g.predecessors(self))
        if len(nodes) == 1:
            return nodes[0]
        else:
            raise RuntimeError('more than 1 predecessor')

    @property
    def next_op(self):
        nodes = list(self.plan.g.successors(self))
        if len(nodes) == 1:
            return nodes[0]
        else:
            raise RuntimeError('more than 1 successor')

    def add_op(self, op):
        self.plan.g.add_edge(self, op)


class BasePlan(object):
    start_node_class = None

    def __init__(self, ctx):
        self.ctx = ctx
        self.g = nx.DiGraph()
        self.start_node = self.start_node_class(self)
        self.g.add_node(self.start_node)

    def execute(self):
        raise NotImplementedError
