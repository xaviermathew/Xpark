from collections import defaultdict
import heapq

from tqdm import tqdm

from xpark import settings
from xpark.plan.base import BaseOp, BasePhysicalPlan
from xpark.dataset.readers import read_parallelized
from xpark.plan.dataframe.optimized import OptimizedPlan
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
    is_pure_compute = True
    chunk_op = 'filter'


class SelectChunkOp(FunctionChunkOp):
    is_pure_compute = True
    chunk_op = 'select'


class AddColumnChunkOp(FunctionChunkOp):
    is_pure_compute = True
    chunk_op = 'add_column'


class CountChunkOp(FunctionChunkOp):
    is_pure_compute = True
    chunk_op = 'count'


class GroupByChunkOp(FunctionChunkOp):
    chunk_op = 'group_by'


class CollectOp(PhysicalPlanOp):
    is_pure_compute = True
    is_terminal = True
    returns_data = True

    def get_code(self):
        def process(all_chunks):
            from xpark.plan.dataframe.results import Result
            return Result.concat(all_chunks)
        return process


class GroupByBarrierOp(PhysicalPlanOp):
    reads_data = False
    returns_data = False

    def get_code(self):
        return lambda: None


class SumOp(PhysicalPlanOp):
    is_pure_compute = True
    is_terminal = True
    returns_data = False

    def get_code(self):
        return sum


class PostGroupByReadOp(PhysicalPlanOp):
    reads_data = False

    def get_code(self):
        def process():
            # @todo: remove list() and instead add a groupby_store.get_items_for_part_id(self.part_id)
            iterable = list(self.plan.ctx.groupby_store.get_items(self.prev_op.task_id))
            ranges = list(get_ranges_for_iterable(iterable, settings.NUM_EXECUTORS, settings.MAX_MEMORY))
            if self.part_id < len(ranges):
                start, end = ranges[self.part_id]
                return read_parallelized(iterable, start, end)
            else:
                return []
        return process


class SampleChunkOp(FunctionChunkOp):
    chunk_op = 'sample'
    is_pure_compute = True


class CalculateRangesOp(PhysicalPlanOp):
    def __init__(self, plan, schema, sort_cols):
        self.sort_cols = sort_cols
        super(__class__, self).__init__(plan, schema)

    def get_code(self):
        def process(all_samples):
            from xpark.utils.partitioning import get_ranges_from_samples
            return get_ranges_from_samples(all_samples)
        return process


class RangePartitionChunkOp(PhysicalPlanOp):
    chunk_op = 'range_partition'

    def __init__(self, plan, schema, part_id, sort_cols):
        self.sort_cols = sort_cols
        super(__class__, self).__init__(plan, schema, part_id)

    def get_code(self):
        def process(chunk_ranges_pair):
            from xpark.plan.dataframe.results import Result, ResultProxy

            chunk, ranges = chunk_ranges_pair
            if isinstance(ranges, (Result, ResultProxy)):
                chunk, ranges = ranges, chunk

            return self.plan.ctx.expression_evaluator_backend.apply_chunk(
                chunk, self.chunk_op, sort_cols=self.sort_cols, ranges=ranges
            )
        return process


class RangePartitionBarrierOp(PhysicalPlanOp):
    returns_ops = True

    def __init__(self, plan, schema, sort_cols):
        self.sort_cols = sort_cols
        super(__class__, self).__init__(plan, schema)

    def get_code(self):
        def process(all_partitioned_chunks):
            from xpark.utils.iter import get_ranges

            rp_results = defaultdict(list)
            for partitions in all_partitioned_chunks:
                for partition in partitions:
                    rp_results[partition.range_id].append(partition)

            new_pp = self.plan.clone()
            tasks = defaultdict(list)
            level = 0
            for range_id, partitions in rp_results.items():
                while True:
                    if level == 0:
                        prev_ops = [self] * len(partitions)
                    else:
                        prev_ops = tasks[level - 1]
                    k_prev_op_ranges = list(get_ranges(len(prev_ops), settings.EXTERNAL_SORT_MAX_INPUT_BUFFERS))
                    for i, (ll, ul) in enumerate(k_prev_op_ranges):
                        op = MergeSortedChunksOp(self.plan, self.schema, self.sort_cols, range_id, level, ll, ul)
                        k_prev_ops = prev_ops[ll:ul] if ll != ul else prev_ops[ll:]
                        for prev_op in k_prev_ops:
                            new_pp.g.add_edge(prev_op, op)
                        tasks[level].append(op)
                    if len(k_prev_op_ranges) == 1:
                        break
                    else:
                        level += 1
            return rp_results, new_pp
        return process


class MergeSortedChunksOp(PhysicalPlanOp):
    return_data_type = PhysicalPlanOp.return_data_type_appendable_result_store

    def __init__(self, plan, schema, sort_cols, range_id, level, ll, ul):
        self.sort_cols = sort_cols
        self.range_id = range_id
        self.level = level
        self.ll = ll
        self.ul = ul
        super(__class__, self).__init__(plan, schema)

    @property
    def task_id(self):
        parts = [self.plan.ctx.job_id, self.__class__.__name__, self.part_id,
                 ','.join(sorted(self.sort_cols)), self.range_id, self.level]
        return '.'.join(map(str, parts))

    def get_code(self):
        def process(partitions):
            partitions = partitions[0]
            if isinstance(partitions, dict):
                range_partitions = partitions[self.range_id]
                partitions = range_partitions[self.ll:self.ul] if self.ll != self.ul else range_partitions[self.ll:]
            else:
                pass
            merged = heapq.merge(*[p.data.to_dict('records') for p in partitions], key=lambda d: tuple([d[col] for col in self.sort_cols]))
            result_store = self.plan.ctx.appendable_result_store
            for item in tqdm(merged, desc='merging'):
                result_store.append(self.task_id, item)
        return process


class PostOrderByReadOp(PhysicalPlanOp):
    reads_data = False

    def get_code(self):
        def process():
            # @todo: remove list() and instead add a groupby_store.get_items_for_part_id(self.part_id)
            iterable = list(self.plan.ctx.groupby_store.get_items(self.prev_op.task_id))
            ranges = list(get_ranges_for_iterable(iterable, settings.NUM_EXECUTORS, settings.MAX_MEMORY))
            if self.part_id < len(ranges):
                start, end = ranges[self.part_id]
                return read_parallelized(iterable, start, end)
            else:
                return []
        return process


class SerializeChunkOp(PhysicalPlanOp):
    returns_data = True
    return_data_type = PhysicalPlanOp.return_data_type_result_store

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
    is_pure_compute = True

    def __init__(self, plan, schema, part_id, dataset):
        self.dataset = dataset
        super(__class__, self).__init__(plan, schema, part_id)

    def get_code(self):
        from xpark.dataset import Dataset

        def process():
            return self.dataset.read_cols_chunk(Dataset.DEST_FORMAT_DF, self.part_id, cols=self.schema.keys())
        return process


class ReadDatasetCountOp(PhysicalPlanOp):
    reads_data = False

    def __init__(self, plan, schema, part_id, dataset):
        self.dataset = dataset
        super(__class__, self).__init__(plan, schema, part_id)

    def get_code(self):
        from xpark.dataset import Dataset

        def process():
            return self.dataset.get_count(Dataset.DEST_FORMAT_DF, self.part_id)
        return process


class WriteChunkOp(PhysicalPlanOp):
    returns_data = False

    def __init__(self, plan, schema, part_id, dataset_writer):
        self.dataset_writer = dataset_writer
        super(__class__, self).__init__(plan, schema, part_id)

    def get_code(self):
        def process(chunk):
            return self.dataset_writer.write_chunk(chunk[0], self.part_id)
        return process


class ReadIndexFilterChunkOp(PhysicalPlanOp):
    reads_data = False  # might read data if the prev op is a ReadDatasetOp
    is_pure_compute = True

    def __init__(self, plan, schema, part_id, table, expr, augment_cols):
        self.table = table
        self.expr =  expr
        self.augment_cols = augment_cols
        super(__class__, self).__init__(plan, schema, part_id)

    def get_code(self):
        def process(chunk=None):
            return self.table.get_rids(self.part_id, self.expr, self.augment_cols, chunk)
        return process


class PostIndexFilterChunkOp(FunctionChunkOp):
    is_pure_compute = True
    chunk_op = 'filter'


class PhysicalPlan(BasePhysicalPlan):
    start_node_class = PhysicalStartOp
    optimized_plan_class = OptimizedPlan

    def to_optimized_plan(self):
        from xpark.plan.dataframe.optimized import OptimizationRule, OptimizedPlan
        oplan = self.clone_with_class(OptimizedPlan)
        oplan.g, oplan.stats = OptimizationRule.apply_all(self.g)
        return oplan
