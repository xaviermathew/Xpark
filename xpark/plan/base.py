import networkx as nx

from xpark.exceptions import MisconfiguredGraph


class BaseOp(object):
    reads_data = True
    returns_data = True
    is_terminal = False
    is_pure_compute = False
    returns_ops = False

    return_data_type_simple = 'simple'
    return_data_type_kv_store = 'kv_store'
    return_data_type_result_store = 'result_store'
    return_data_type_appendable_result_store = 'appendable_result_store'
    return_data_type_groupby_store = 'groupby_store'
    return_data_type = return_data_type_simple

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
        if self.is_terminal:
            raise MisconfiguredGraph('terminal ops cant add more ops')
        return op.plan.add_op(self, op)

    def execute(self):
        return self.plan.execute()

    @property
    def task_id(self):
        return '%s.%s.%s' % (self.plan.ctx.job_id, self.__class__.__name__, self.part_id)


class BasePlan(object):
    start_node_class = None

    def __init__(self, ctx, g=None, start_node=None, start_node_class=None):
        from xpark.utils.graph import DiGraph

        if start_node_class is None:
            start_node_class = self.start_node_class

        self.ctx = ctx
        if g is None:
            self.g = DiGraph()
            self.start_node = start_node_class(self)
            self.g.add_node(self.start_node)
        else:
            self.g = g.copy()
            self.start_node = start_node

    def clone_with_class(self, klass):
        new_plan = klass(self.ctx, self.g, self.start_node)
        return new_plan

    def clone(self):
        return self.clone_with_class(self.__class__)

    def execute(self):
        raise NotImplementedError

    def add_op(self, from_op, to_op):
        new_plan = self.clone()
        new_plan.g.add_edge(from_op, to_op)
        to_op.plan = new_plan
        return new_plan


class BaseLogicalPlan(BasePlan):
    physical_plan_class = None

    def to_physical_plan(self):
        pplan = self.physical_plan_class(self.ctx)
        prev_nodes = [pplan.start_node]
        for n1, n2 in nx.dfs_edges(self.g, source=self.start_node):
            n2g = n2.get_physical_plan(prev_nodes, pplan)
            pplan.g.update(n2g)
            prev_nodes = [x for x in n2g.nodes() if n2g.out_degree(x) == 0 and n2g.in_degree(x) == 1]
        return pplan

    def execute(self):
        return self.to_physical_plan().to_optimized_plan().execute()


class BasePhysicalPlan(BasePlan):
    optimized_plan_class = None

    def to_optimized_plan(self):
        return self.clone_with_class(self.optimized_plan_class)

    def execute(self):
        return self.to_optimized_plan().execute()


class BaseOptimizedPlan(BasePlan):
    def execute(self):
        return self.ctx.executor.execute(self)
