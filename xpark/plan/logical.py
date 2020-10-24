import networkx as nx

from xpark.plan.base import BaseOp, BasePlan
from xpark.plan.physical import ReadCSVChunkOp, ReadTextChunkOp, PhysicalStartOp, SerializeChunkOp, \
    DeserializeChunkOp, MapChunkOp, FilterChunkOp, GroupChunkByKeykOp, GroupByBarrierOp, \
    ReadParallelizedChunkOp, CollectOp as PhysicalCollectOp, PhysicalPlan, PostGroupByReadOp
from xpark.utils.iter import get_ranges_for_file, get_ranges_for_iterable


class LogicalPlanOp(BaseOp):
    def map(self, func):
        op = MapOp(self.plan, func)
        self.add_op(op)
        return op

    def filter(self, func):
        op = FilterOp(self.plan, func)
        self.add_op(op)
        return op

    def groupByKey(self):
        op = GroupByKeyOp(self.plan)
        self.add_op(op)
        return op

    def collect(self):
        op = CollectOp(self.plan)
        self.add_op(op)
        return op

    def get_physical_plan(self, prev_ops, pplan):
        raise NotImplementedError


class LogicalStartOp(LogicalPlanOp):
    def get_physical_plan(self, prev_ops, pplan):
        g = nx.DiGraph()
        op = PhysicalStartOp(pplan)
        for prev_op in prev_ops:
            g.add_edge(prev_op, op)
        return g


class BaseReadOp(LogicalPlanOp):
    physical_plan_op_class = None
    chunk_create_function = None

    def __init__(self, plan, ccf_kwargs):
        self.ccf_kwargs = ccf_kwargs
        self.chunks = self.chunk_create_function.__func__(
            num_executors=plan.ctx.num_executors,
            max_memory=plan.ctx.max_memory,
            **ccf_kwargs
        )
        super(__class__, self).__init__(plan)

    def get_physical_plan(self, prev_ops, pplan):
        g = nx.DiGraph()
        for i, (start, end) in enumerate(self.chunks):
            read_op = self.physical_plan_op_class(plan=pplan, start=start, end=end, part_id=i, **self.ccf_kwargs)
            for prev_op in prev_ops:
                g.add_edge(prev_op, read_op)
            ser_op = SerializeChunkOp(pplan, part_id=i)
            g.add_edge(read_op, ser_op)
        return g


class ReadTextOp(BaseReadOp):
    physical_plan_op_class = ReadTextChunkOp
    chunk_create_function = get_ranges_for_file

    def __init__(self, plan, fname):
        super(__class__, self).__init__(plan, ccf_kwargs={'fname': fname})


class ReadCSVOp(BaseReadOp):
    physical_plan_op_class = ReadCSVChunkOp
    chunk_create_function = get_ranges_for_file

    def __init__(self, plan, fname):
        super(__class__, self).__init__(plan, ccf_kwargs={'fname': fname})


class ReadParallelizedOp(BaseReadOp):
    physical_plan_op_class = ReadParallelizedChunkOp
    chunk_create_function = get_ranges_for_iterable

    def __init__(self, plan, iterable):
        super(__class__, self).__init__(plan, ccf_kwargs={'iterable': iterable})


class FunctionOp(LogicalPlanOp):
    physical_plan_op_class = None

    def __init__(self, plan, func):
        self.func = func
        super(__class__, self).__init__(plan)

    def get_physical_plan(self, prev_ops, pplan):
        g = nx.DiGraph()
        for i, prev_op in enumerate(prev_ops):
            deser_op = DeserializeChunkOp(pplan, part_id=i)
            g.add_edge(prev_op, deser_op)
            op = self.physical_plan_op_class(pplan, part_id=i, func=self.func)
            g.add_edge(deser_op, op)
            ser_op = SerializeChunkOp(pplan, part_id=i)
            g.add_edge(op, ser_op)
        return g


class MapOp(FunctionOp):
    physical_plan_op_class = MapChunkOp


class FilterOp(FunctionOp):
    physical_plan_op_class = FilterChunkOp


class GroupByKeyOp(LogicalPlanOp):
    def get_physical_plan(self, prev_ops, pplan):
        g = nx.DiGraph()
        barrier_op = GroupByBarrierOp(pplan)
        for i, prev_op in enumerate(prev_ops):
            deser_op = DeserializeChunkOp(pplan, part_id=i)
            g.add_edge(prev_op, deser_op)
            op = GroupChunkByKeykOp(pplan, part_id=i)
            g.add_edge(deser_op, op)
            g.add_edge(op, barrier_op)
            pgbr_op = PostGroupByReadOp(pplan, part_id=i)
            g.add_edge(barrier_op, pgbr_op)
            pgbr_deser_op = SerializeChunkOp(pplan, part_id=i)
            g.add_edge(pgbr_op, pgbr_deser_op)
        return g


class CollectOp(LogicalPlanOp):
    def get_physical_plan(self, prev_ops, pplan):
        g = nx.DiGraph()
        op = PhysicalCollectOp(pplan)
        for i, prev_op in enumerate(prev_ops):
            deser_op = DeserializeChunkOp(pplan, part_id=i)
            g.add_edge(prev_op, deser_op)
            g.add_edge(deser_op, op)
        return g


class LogicalPlan(BasePlan):
    start_node_class = LogicalStartOp

    def to_physical_plan(self):
        pplan = PhysicalPlan(self.ctx)
        prev_nodes = [pplan.start_node]
        for n1, n2 in nx.dfs_edges(self.g, source=self.start_node):
            n2g = n2.get_physical_plan(prev_nodes, pplan)
            pplan.g.update(n2g)
            prev_nodes = [x for x in n2g.nodes() if n2g.out_degree(x) == 0 and n2g.in_degree(x) == 1]
        return pplan

    def execute(self):
        return self.to_physical_plan().execute()
