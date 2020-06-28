import networkx as nx

from xpark.plan.base import BaseOp, BasePlan
from xpark.plan.physical import ReadCSVChunkOp, ReadTextChunkOp, PhysicalStartOp, SerializeOp, \
    DeserializeOp, MapChunkOp, FilterChunkOp, GroupChunkByKeykOp, MergeGroupByResults, \
    ReadParallelizedChunkOp
from xpark.utils.iter import take_pairs, get_ranges_for_file, get_ranges_for_iterable


class LogicalPlan(BasePlan):
    @classmethod
    def from_pipeline(cls, pipeline):
        g = nx.DiGraph()
        for op1, op2 in take_pairs(pipeline.ops):
            g.add_edge(op1, op2)
        return cls(g, start_node=pipeline.ops[0])


class LogicalPlanOp(BaseOp):
    takes_input = False
    returns_input = False

    def get_physical_plan_ops(self, prev_stage):
        raise NotImplementedError


class LogicalStartOp(LogicalPlanOp):
    is_start_op = True

    def get_physical_plan_ops(self, prev_stage):
        return [
            (None, PhysicalStartOp(self.ctx))
        ]


class BaseReadOp(LogicalPlanOp):
    physical_plan_op_class = None

    def __init__(self, ctx, fname):
        self.fname = fname
        super(BaseReadOp, self).__init__(ctx=ctx)

    def get_physical_plan_ops(self, prev_stage):
        ranges = get_ranges_for_file(self.fname, self.ctx.num_workers, self.ctx.max_memory)
        for i, (start, end) in enumerate(ranges):
            yield (self.physical_plan_op_class(self.ctx, self.fname, start, end, part_id=i),
                   SerializeOp(self.ctx, part_id=i))


class ReadTextOp(BaseReadOp):
    physical_plan_op_class = ReadTextChunkOp


class ReadCSVOp(BaseReadOp):
    physical_plan_op_class = ReadCSVChunkOp


class ReadParallelizedOp(LogicalPlanOp):
    def __init__(self, ctx, iterable):
        self.iterable = iterable
        super(ReadParallelizedOp, self).__init__(ctx=ctx)

    def get_physical_plan_ops(self, prev_stage):
        ranges = get_ranges_for_iterable(self.iterable, self.ctx.num_workers, self.ctx.max_memory)
        for i, (start, end) in enumerate(ranges):
            yield (ReadParallelizedChunkOp(self.ctx, self.iterable, start, end, part_id=i),
                   SerializeOp(self.ctx, part_id=i))


class SimpleLogicalOp(LogicalPlanOp):
    physical_plan_op_class = None

    def get_physical_plan_ops(self, prev_stage):
        for op in prev_stage:
            yield (DeserializeOp(self.ctx, prev_task_id=op.task_id, part_id=op.part_id),
                   self.physical_plan_op_class(self.ctx, op.part_id),
                   SerializeOp(self.ctx, part_id=op.part_id))


class MapOp(SimpleLogicalOp):
    physical_plan_op_class = MapChunkOp


class FilterOp(SimpleLogicalOp):
    physical_plan_op_class = FilterChunkOp


class GroupByKeyOp(SimpleLogicalOp):
    def get_physical_plan_ops(self, prev_stage):
        group_chunk_task_ids = []
        for op in prev_stage:
            g_op = GroupChunkByKeykOp(self.ctx, op.part_id)
            group_chunk_task_ids.append(g_op.task_id)
            yield (DeserializeOp(self.ctx, prev_task_id=op.task_id, part_id=op.part_id),
                   g_op,
                   SerializeOp(self.ctx, part_id=op.part_id))
        yield MergeGroupByResults(self.ctx, group_chunk_task_ids)
