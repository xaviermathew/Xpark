from xpark.plan.base import BasePlan, BaseOp
from xpark.readers import read_csv, read_text, read_parallelized
from xpark.utils.iter import get_ranges_for_iterable


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
            for key, d in chunk[0]:
                self.plan.ctx.groupby_store.append(barrier_op.task_id, key, d)
        return process


class GroupByBarrierOp(PhysicalPlanOp):
    reads_data = False
    returns_data = False

    def get_code(self):
        return lambda: None


class BasePhysicalReadOp(PhysicalPlanOp):
    reads_data = False
    chunk_reader_function = None

    def __init__(self, plan, part_id, start, end, crf_kwargs={}):
        self.start = start
        self.end = end
        self.crf_kwargs = crf_kwargs
        super(BasePhysicalReadOp, self).__init__(plan, part_id)

    def get_code(self):
        def process():
            return self.chunk_reader_function.__func__(start=self.start, end=self.end, **self.crf_kwargs)
        return process


class ReadCSVChunkOp(BasePhysicalReadOp):
    chunk_reader_function = read_csv

    def __init__(self, plan, part_id, start, end, fname):
        super(__class__, self).__init__(plan, part_id, start, end, crf_kwargs={'fname': fname})


class ReadTextChunkOp(BasePhysicalReadOp):
    chunk_reader_function = read_text

    def __init__(self, plan, part_id, start, end, fname):
        super(__class__, self).__init__(plan, part_id, start, end, crf_kwargs={'fname': fname})


class ReadParallelizedChunkOp(BasePhysicalReadOp):
    chunk_reader_function = read_parallelized

    def __init__(self, plan, part_id, start, end, iterable):
        super(__class__, self).__init__(plan, part_id, start, end, crf_kwargs={'iterable': iterable})


class PostGroupByReadOp(PhysicalPlanOp):
    reads_data = False

    def get_code(self):
        def process():
            # @todo: remove list() and instead add a groupby_store.get_items_for_part_id(self.part_id)
            iterable = list(self.plan.ctx.groupby_store.get_items(self.prev_op.task_id))
            ranges = list(get_ranges_for_iterable(iterable, self.plan.ctx.num_executors, self.plan.ctx.max_memory))
            if self.part_id < len(ranges):
                start, end = ranges[self.part_id]
                return read_parallelized(iterable, start, end)
            else:
                return []
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
