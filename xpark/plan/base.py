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
        return op.plan.add_op(self, op)

    def execute(self):
        return self.plan.execute()


class BasePlan(object):
    start_node_class = None

    def __init__(self, ctx, g=None, start_node=None):
        self.ctx = ctx
        if g is None:
            self.g = nx.DiGraph()
            self.start_node = self.start_node_class(self)
            self.g.add_node(self.start_node)
        else:
            self.g = g.copy()
            self.start_node = start_node

    def clone(self):
        new_plan = self.__class__(self.ctx, self.g, self.start_node)
        return new_plan

    def execute(self):
        raise NotImplementedError

    def add_op(self, from_op, to_op):
        new_plan = self.clone()
        new_plan.g.add_edge(from_op, to_op)
        to_op.plan = new_plan
        return new_plan
