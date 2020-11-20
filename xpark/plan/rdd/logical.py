import networkx as nx

from xpark.plan.base import BaseOp, BaseLogicalPlan
from xpark.plan.rdd.physical import PhysicalStartOp, SerializeChunkOp, \
    DeserializeChunkOp, MapChunkOp, FilterChunkOp, GroupChunkByKeykOp, GroupByBarrierOp, \
    CollectOp as PhysicalCollectOp, PostGroupByReadOp, PhysicalPlan, ReadDatasetChunkOp


class LogicalPlanOp(BaseOp):
    def __repr__(self):
        return '<RDD:%s>' % self.__class__.__name__

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


class LogicalStartOp(BaseOp):
    def get_physical_plan(self, prev_ops, pplan):
        g = nx.DiGraph()
        op = PhysicalStartOp(pplan)
        for prev_op in prev_ops:
            g.add_edge(prev_op, op)
        return g


class ReadDatasetOp(LogicalPlanOp):
    def __init__(self, plan, dataset):
        self.dataset = dataset
        super(__class__, self).__init__(plan)

    def get_physical_plan(self, prev_ops, pplan):
        g = nx.DiGraph()
        for i  in range(len(self.dataset.chunks)):
            read_op = ReadDatasetChunkOp(pplan, i, self.dataset)
            for prev_op in prev_ops:
                g.add_edge(prev_op, read_op)
            ser_op = SerializeChunkOp(pplan, part_id=i)
            g.add_edge(read_op, ser_op)
        return g


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


class LogicalPlan(BaseLogicalPlan):
    start_node_class = LogicalStartOp
    physical_plan_class = PhysicalPlan
