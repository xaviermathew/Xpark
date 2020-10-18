from xpark.plan.base import BasePlan, BaseOp
from xpark.readers import read_csv, read_text, read_parallelized


class PhysicalPlanOp(BaseOp):
    def __repr__(self):
        return '<%s>' % self.task_id

    @property
    def task_id(self):
        return '%s.%s.%s' % (self.plan.ctx.job_id, self.__class__.__name__, self.part_id)

    def get_code(self):
        raise NotImplementedError


class PhysicalStartOp(PhysicalPlanOp):
    reads_data = False
    returns_data = False

    def get_code(self):
        return lambda: None


class FunctionChunkOp(PhysicalPlanOp):
    physical_plan_op_class = None

    def __init__(self, plan, func, part_id):
        self.func = func
        super(__class__, self).__init__(plan, part_id)


class MapChunkOp(FunctionChunkOp):
    def get_code(self):
        def process(chunk):
            return map(self.func, chunk[0])
        return process


class FilterChunkOp(FunctionChunkOp):
    def get_code(self):
        def process(chunk):
            return filter(self.func, chunk[0])
        return process


class CollectOp(PhysicalPlanOp):
    def get_code(self):
        def process(all_results):
            for result in all_results:
                yield from result
        return process


class GroupChunkByKeykOp(PhysicalPlanOp):
    returns_data = False

    def get_code(self):
        barrier_op = list(self.plan.g.successors(self))[0]
        def process(chunk):
            for key, d in chunk:
                self.plan.ctx.groupby_store_backend.append(barrier_op.task_id, key, d)
        return process


class GroupByBarrierOp(PhysicalPlanOp):
    def get_code(self):
        return lambda: None


class BasePhysicalReadOp(PhysicalPlanOp):
    reads_data = False

    def __init__(self, plan, fname, start, end, **kwargs):
        self.fname = fname
        self.start = start
        self.end = end
        super(BasePhysicalReadOp, self).__init__(plan, **kwargs)


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
    reads_data = False

    def __init__(self, plan, iterable, start, end, **kwargs):
        self.iterable = iterable
        self.start = start
        self.end = end
        super(ReadParallelizedChunkOp, self).__init__(plan, **kwargs)

    def get_code(self):
        def process():
            return read_parallelized(self.iterable, self.start, self.end)
        return process


class SerializeChunkOp(PhysicalPlanOp):
    returns_data = False

    def get_code(self):
        def process(chunk):
            self.plan.ctx.result_store.set(self.task_id, list(chunk[0]))
        return process


class DeserializeChunkOp(PhysicalPlanOp):
    reads_data = False

    def get_code(self):
        def process():
            return self.plan.ctx.result_store.get(self.prev_op.task_id)
        return process


class PhysicalPlan(BasePlan):
    start_node_class = PhysicalStartOp

    def execute(self):
        return self.ctx.executor.execute(self)
