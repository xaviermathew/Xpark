import networkx as nx

from xpark.plan.base import BasePlan, BaseOp
from xpark.readers import read_csv, read_text


class PhysicalPlan(BasePlan):
    @classmethod
    def from_logical_plan(cls, lp):
        g = nx.DiGraph()
        start_node = lp.nx_graph.start_node
        prev_stage = None
        for op1, op2 in nx.dfs_edges(lp.nx_graph, start_node):
            if prev_stage is None:
                prev_stage = op1.get_physical_plan_ops(None)
            curr_stage = op2.get_edges(prev_stage)
            for edge in curr_stage:
                g.add_edge(*edge)
            prev_stage = curr_stage
        return cls(g, start_node=start_node)


class PhysicalPlanOp(BaseOp):
    is_start_op = False


class PhysicalStartOp(PhysicalPlanOp):
    is_start_op = True


class MapChunkOp(PhysicalPlanOp):
    def get_code(self):
        return map


class FilterChunkOp(PhysicalPlanOp):
    def get_code(self):
        return filter


class GroupChunkByKeykOp(PhysicalPlanOp):
    def get_code(self):
        def process_chunk(chunk):
            for key, d in chunk:
                self.ctx.groupby_store_backend.append(self.task_id, key, d)
        return process_chunk


class MergeGroupByResults(PhysicalPlanOp):
    def __init__(self, ctx, group_chunk_task_ids, **kwargs):
        self.group_chunk_task_ids = group_chunk_task_ids
        super(MergeGroupByResults, self).__init__(ctx=ctx, **kwargs)

    def get_code(self):
        def process_results():
            merged = self.ctx.groupby_store_backend
            for task_id in self.group_chunk_task_ids:
                for k, v_set in self.ctx.result_store[task_id].items():
                    merged.extend(k, v_set)
            return merged
        return process_results


class BasePhysicalReadOp(PhysicalPlanOp):
    def __init__(self, ctx, fname, start, end, **kwargs):
        self.fname = fname
        self.start = start
        self.end = end
        super(BasePhysicalReadOp, self).__init__(ctx=ctx, **kwargs)


class ReadCSVChunkOp(BasePhysicalReadOp):
    def get_code(self):
        def process():
            return read_csv(self.fname, self.start, self.end)
        return process


class ReadTextChunkOp(BasePhysicalReadOp):
    def get_code(self):
        def process():
            return read_text(self.fname, self.start, self.end)
        return process


class SerializeOp(PhysicalPlanOp):
    def __init__(self, ctx, **kwargs):
        super(SerializeOp, self).__init__(ctx=ctx, **kwargs)

    def get_code(self):
        def process(chunk):
            s = self.ctx.result_store.dumps(chunk)
            self.ctx.result_store[self.task_id] = s
        return process


class DeserializeOp(PhysicalPlanOp):
    def __init__(self, ctx, prev_task_id, **kwargs):
        self.prev_task_id = prev_task_id
        super(DeserializeOp, self).__init__(ctx=ctx, **kwargs)

    def get_code(self):
        def process():
            s = self.ctx.result_store[self.prev_task_id]
            return self.ctx.result_store.loads(s)
        return process
