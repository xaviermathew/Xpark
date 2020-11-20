from xpark.plan.base import BaseOp, BasePhysicalPlan
from xpark.dataset.readers import read_parallelized
from xpark.utils.iter import get_ranges_for_iterable


class PhysicalPlanOp(BaseOp):
    def __init__(self, plan, schema, part_id=0):
        self.schema = schema
        super(__class__, self).__init__(plan, part_id)

    def __repr__(self):
        return '<%s>' % self.task_id

    @property
    def cols(self):
        return list(self.schema.keys())

    @property
    def task_id(self):
        return '%s.%s.%s' % (self.plan.ctx.job_id, self.__class__.__name__, self.part_id)

    def get_code(self):
        raise NotImplementedError


class PhysicalStartOp(BaseOp):
    reads_data = False
    returns_data = False

    def get_code(self):
        return lambda: None


# class MapChunkOp(PhysicalPlanOp):
#     def __init__(self, plan, dataset, func, part_id):
#         self.func = func
#         super(__class__, self).__init__(plan, dataset, part_id)
#
#     def get_code(self):
#         def process(chunk):
#             return map(self.func, chunk[0])
#         return process


class FunctionChunkOp(PhysicalPlanOp):
    chunk_op = None

    def __init__(self, plan, schema, part_id, **ac_kwargs):
        self.ac_kwargs = ac_kwargs
        super(__class__, self).__init__(plan, schema, part_id)

    def get_code(self):
        def process(chunk):
            return self.plan.ctx.expression_evaluator_backend.apply_chunk(chunk[0], self.chunk_op, **self.ac_kwargs)
        return process


class FilterChunkOp(FunctionChunkOp):
    chunk_op = 'filter'


class SelectChunkOp(FunctionChunkOp):
    chunk_op = 'select'


class AddColumnChunkOp(FunctionChunkOp):
    chunk_op = 'add_column'


class CountChunkOp(FunctionChunkOp):
    chunk_op = 'count'


class GroupByChunkOp(FunctionChunkOp):
    chunk_op = 'group_by'


class OrderByChunkOp(FunctionChunkOp):
    chunk_op = 'order_by'


class CollectOp(PhysicalPlanOp):
    def get_code(self):
        def process(all_chunks):
            col = list(self.cols)[0]
            for chunk in all_chunks:
                for i in range(len(chunk[col])):
                    yield {col: chunk[col][i] for col in self.cols}
        return process


class GroupByBarrierOp(PhysicalPlanOp):
    reads_data = False
    returns_data = False

    def get_code(self):
        return lambda: None


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


class OrderByBarrierOp(PhysicalPlanOp):
    reads_data = False
    returns_data = False

    def get_code(self):
        return lambda: None


class PostOrderByReadOp(PhysicalPlanOp):
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
            self.plan.ctx.result_store.set(self.task_id, chunk[0])
        return process


class DeserializeChunkOp(PhysicalPlanOp):
    reads_data = False

    def get_code(self):
        def process():
            return self.plan.ctx.result_store.get(self.prev_op.task_id)
        return process


class ReadDatasetOp(PhysicalPlanOp):
    reads_data = False

    def __init__(self, plan, schema, part_id, dataset):
        self.dataset = dataset
        super(__class__, self).__init__(plan, schema, part_id)

    def get_code(self):
        def process():
            return self.dataset.read_cols_chunk(self.part_id, self.schema.keys())
        return process


class PhysicalPlan(BasePhysicalPlan):
    start_node_class = PhysicalStartOp


class WriteChunkOp(PhysicalPlanOp):
    returns_data = False

    def __init__(self, plan, schema, part_id, dataset_writer):
        self.dataset_writer = dataset_writer
        super(__class__, self).__init__(plan, schema, part_id)

    def get_code(self):
        def process(chunk):
            return self.dataset_writer.write_chunk(chunk[0], self.part_id)
        return process
