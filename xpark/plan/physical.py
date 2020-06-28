import networkx as nx

from xpark.plan.base import BasePlan, BaseOp
from xpark.readers import read_csv, read_text, read_parallelized
from xpark.utils.iter import take_pairs


class PhysicalPlan(BasePlan):
    @classmethod
    def from_logical_plan(cls, lp):
        g = nx.DiGraph()
        start_node = lp.start_node
        prev_stage = None
        for op1, op2 in nx.dfs_edges(lp.nx_graph, start_node):
            if prev_stage is None:
                prev_stage = [list(edge) for edge in op1.get_physical_plan_ops(None)]
                start_node = prev_stage[0][1]
            curr_stage = list(op2.get_physical_plan_ops(prev_stage))
            if len(prev_stage) != len(curr_stage):
                if len(prev_stage) == 1:
                    chains = [(prev_stage[0][-1], *edge) for edge in curr_stage]
                else:
                    raise RuntimeError
            else:
                chains = [(prev_stage[i][-1], *edge) for i, edge in enumerate(curr_stage)]

            for chain in chains:
                for edge in take_pairs(chain):
                    g.add_edge(*edge)
            prev_stage = curr_stage
        return cls(lp.ctx, g, start_node=start_node)

    def execute(self):
        return self.ctx.executor.execute(self)


class PhysicalPlanOp(BaseOp):
    is_start_op = False

    def get_code(self):
        raise NotImplementedError


class PhysicalStartOp(PhysicalPlanOp):
    is_start_op = True

    def get_code(self):
        return lambda: None


class MapChunkOp(PhysicalPlanOp):
    def get_code(self):
        return map


class FilterChunkOp(PhysicalPlanOp):
    def get_code(self):
        return filter


class GroupChunkByKeykOp(PhysicalPlanOp):
    returns_data = False

    def get_code(self):
        def process_chunk(chunk):
            for key, d in chunk:
                self.ctx.groupby_store_backend.append(self.task_id, key, d)
        return process_chunk


class MergeGroupByResults(PhysicalPlanOp):
    reads_data = True

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
    reads_data = False

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


class ReadParallelizedChunkOp(PhysicalPlanOp):
    def __init__(self, ctx, iterable, start, end, **kwargs):
        self.iterable = iterable
        self.start = start
        self.end = end
        super(ReadParallelizedChunkOp, self).__init__(ctx, **kwargs)

    def get_code(self):
        def process():
            return read_parallelized(self.iterable, self.start, self.end)
        return process


class SerializeChunkOp(PhysicalPlanOp):
    reads_data = True
    returns_data = False

    def __init__(self, ctx, **kwargs):
        super(SerializeChunkOp, self).__init__(ctx=ctx, **kwargs)

    def get_code(self):
        def process(chunk):
            s = self.ctx.result_store.dumps(chunk)
            self.ctx.result_store[self.task_id] = s
        return process


class DeserializeChunkOp(PhysicalPlanOp):
    reads_data = False
    returns_data = True

    def __init__(self, ctx, prev_task_id, **kwargs):
        self.prev_task_id = prev_task_id
        super(DeserializeChunkOp, self).__init__(ctx=ctx, **kwargs)

    def get_code(self):
        def process():
            s = self.ctx.result_store[self.prev_task_id]
            return self.ctx.result_store.loads(s)
        return process
