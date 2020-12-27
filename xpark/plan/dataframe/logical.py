import copy

from xpark.plan.base import BaseOp, BaseLogicalPlan
from xpark.plan.dataframe.physical import (
    PhysicalStartOp, SerializeChunkOp, DeserializeChunkOp, FilterChunkOp, GroupByChunkOp,
    GroupByBarrierOp, CollectOp as PhysicalCollectOp, PostGroupByReadOp, ReadDatasetOp as PhysicalReadDatasetOp,
    SelectChunkOp, CountChunkOp, AddColumnChunkOp, OrderByChunkOp, PostOrderByReadOp,
    OrderByBarrierOp, PhysicalPlan, WriteChunkOp, SumOp
)
from xpark.plan.dataframe.expr import Expr, NumExpr, StrExpr


class Col(object):
    expr_class = Expr

    def __init__(self, df, name):
        self.df = df
        self.name = name

    def __repr__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.name)

    def toExpr(self):
        return self.expr_class(self.df.plan.ctx, self)

    def execute(self, chunk):
        from fastparquet.parquet_thrift.parquet.ttypes import RowGroup
        from xpark.plan.dataframe.results import Result
        from xpark.dataset.utils import RowGroupEvalRequest

        if callable(chunk):
            return chunk(self)
        elif isinstance(chunk, Result):
            return chunk[self.name]
        elif isinstance(chunk, RowGroup):
            return [col for col in chunk.columns if '.'.join(col.meta_data.path_in_schema) == self.name][0]
        elif isinstance(chunk, RowGroupEvalRequest):
            return chunk.to_rowgroupcolevalrequest(self.name)
        else:
            raise ValueError('Dont know how to execute type:%s' % type(chunk))


class NumCol(Col):
    expr_class = NumExpr


class StrCol(Col):
    expr_class = StrExpr


class LogicalPlanOp(BaseOp):
    data_type_map = {
        int: NumCol,
        float: NumCol,
        str: StrCol,
        None: Col
    }

    def __init__(self, plan, schema):
        self.plan = plan
        self.schema = copy.deepcopy(schema)
        self.col_cache = {}
        super(__class__, self).__init__(plan)

    def __repr__(self):
        return '<DataFrame:%s cols:%s>' % (self.__class__.__name__, self.cols)

    def __getitem__(self, name):
        if name not in self.schema:
            raise KeyError(name)

        if name not in self.col_cache:
            data_type = self.schema[name]
            self.col_cache[name] = self.data_type_map[data_type](self, name).toExpr()
        return self.col_cache[name]

    @property
    def cols(self):
        return list(self.schema.keys())

    def new(self, op_class, **kwargs):
        new_op = op_class(self.plan, self.schema, **kwargs)
        # new_plan.col_cache = copy.deepcopy(self.col_cache)
        self.add_op(new_op)
        return new_op

    def _add_column(self, name, value, data_type):
        self.schema[name] = data_type
        # self.col_cache[name] = value

    def count(self):
        return self.new(CountOp)

    def withColumn(self, name, expr, data_type):
        op = self.new(AddColumnOp, name=name, expr=expr)
        op._add_column(name, expr, data_type)
        return op

    def filter(self, expr):
        return self.new(FilterOp, expr=expr)

    def select(self, *cols):
        return self.new(SelectOp, cols=cols)

    # def agg(self):
    #     pass

    def groupBy(self, *expr_set):
        return self.new(GroupByOp, expr_set=expr_set)

    def distinct(self):
        return self.new(GroupByOp)

    # def foreach(self, func):
    #     op = self.new(MapOp, func=func)
    #     self.add_op(op)
    #     return op

    def join(self, rhs, on):
        return self.new(JoinOp, rhs=rhs, on=on)

    def limit(self, n):
        return self.new(LimitOp, n=n)

    def orderBy(self, *expr_set):
        return self.new(OrderByOp, expr_set=expr_set)

    def collect(self):
        return self.new(CollectOp)

    def toCSV(self, path):
        from xpark.dataset import CSVWriter
        return self.new(WriteOp, dataset_writer=CSVWriter(self.plan.ctx, path))

    def toText(self, path):
        from xpark.dataset import TextWriter
        return self.new(WriteOp, dataset_writer=TextWriter(self.plan.ctx, path))

    def toParquet(self, path):
        from xpark.dataset import ParquetWriter
        return self.new(WriteOp, dataset_writer=ParquetWriter(self.plan.ctx, path))

    def toTable(self, path, **options):
        from xpark.dataset.tables import TableWriterMixin
        return self.new(WriteOp, dataset_writer=TableWriterMixin.get_table_class()(self.plan.ctx, path, **options))

    def get_physical_plan(self, prev_ops, pplan):
        raise NotImplementedError


class LogicalStartOp(BaseOp):
    def get_physical_plan(self, prev_ops, pplan):
        from xpark.utils.graph import DiGraph

        g = DiGraph()
        op = PhysicalStartOp(pplan)
        for prev_op in prev_ops:
            g.add_edge(prev_op, op)
        return g


class ReadDatasetOp(LogicalPlanOp):
    def __init__(self, plan, schema, dataset):
        self.dataset = dataset
        super(__class__, self).__init__(plan, schema)

    def get_physical_plan(self, prev_ops, pplan):
        from xpark.utils.graph import DiGraph

        g = DiGraph()
        for i in range(len(self.dataset.chunks)):
            read_op = PhysicalReadDatasetOp(pplan, self.schema, i, self.dataset)
            for prev_op in prev_ops:
                g.add_edge(prev_op, read_op)
            ser_op = SerializeChunkOp(pplan, self.dataset, part_id=i)
            g.add_edge(read_op, ser_op)
        return g


class FunctionOp(LogicalPlanOp):
    physical_plan_op_class = None

    def __init__(self, plan, schema, **ppoc_kwargs):
        self.ppoc_kwargs = ppoc_kwargs
        super(__class__, self).__init__(plan, schema)

    def get_physical_plan(self, prev_ops, pplan):
        from xpark.utils.graph import DiGraph

        g = DiGraph()
        for i, prev_op in enumerate(prev_ops):
            deser_op = DeserializeChunkOp(pplan, self.schema, i)
            g.add_edge(prev_op, deser_op)
            op = self.physical_plan_op_class(pplan, self.schema, i, **self.ppoc_kwargs)
            g.add_edge(deser_op, op)
            ser_op = SerializeChunkOp(pplan, self.schema, i)
            g.add_edge(op, ser_op)
        return g


class AddColumnOp(FunctionOp):
    physical_plan_op_class = AddColumnChunkOp


class FilterOp(FunctionOp):
    physical_plan_op_class = FilterChunkOp


class SelectOp(FunctionOp):
    physical_plan_op_class = SelectChunkOp


class LimitOp(LogicalPlanOp):
    def __init__(self, plan, schema, limit):
        self.limit = limit
        super(__class__, self).__init__(plan, schema)


class JoinOp(LogicalPlanOp):
    def __init__(self, plan, schema, rhs, on):
        self.rhs = rhs
        self.on = on
        super(__class__, self).__init__(plan, schema)


class OrderByOp(LogicalPlanOp):
    def __init__(self, plan, schema, expr_set=None):
        self.expr_set = expr_set
        super(__class__, self).__init__(plan, schema)

    def get_physical_plan(self, prev_ops, pplan):
        from xpark.utils.graph import DiGraph

        g = DiGraph()
        barrier_op = OrderByBarrierOp(pplan, self.schema)
        for i, prev_op in enumerate(prev_ops):
            deser_op = DeserializeChunkOp(pplan, self.schema, i)
            g.add_edge(prev_op, deser_op)
            op = OrderByChunkOp(pplan, self.schema, i)
            g.add_edge(deser_op, op)
            g.add_edge(op, barrier_op)
            pgbr_op = PostOrderByReadOp(pplan, self.schema, i)
            g.add_edge(barrier_op, pgbr_op)
            pgbr_deser_op = SerializeChunkOp(pplan, self.schema, i)
            g.add_edge(pgbr_op, pgbr_deser_op)
        return g


class GroupByOp(LogicalPlanOp):
    def __init__(self, plan, dataset, expr_set=None):
        self.expr_set = expr_set
        super(__class__, self).__init__(plan, dataset)

    def get_physical_plan(self, prev_ops, pplan):
        from xpark.utils.graph import DiGraph

        g = DiGraph()
        barrier_op = GroupByBarrierOp(pplan, self.schema)
        for i, prev_op in enumerate(prev_ops):
            deser_op = DeserializeChunkOp(pplan, self.schema, i)
            g.add_edge(prev_op, deser_op)
            op = GroupByChunkOp(pplan, self.schema, i)
            g.add_edge(deser_op, op)
            g.add_edge(op, barrier_op)
            pgbr_op = PostGroupByReadOp(pplan, self.schema, i)
            g.add_edge(barrier_op, pgbr_op)
            pgbr_deser_op = SerializeChunkOp(pplan, self.schema, i)
            g.add_edge(pgbr_op, pgbr_deser_op)
        return g


class CollectOp(LogicalPlanOp):
    def get_physical_plan(self, prev_ops, pplan):
        from xpark.utils.graph import DiGraph

        g = DiGraph()
        op = PhysicalCollectOp(pplan, self.schema)
        for i, prev_op in enumerate(prev_ops):
            deser_op = DeserializeChunkOp(pplan, self.schema, i)
            g.add_edge(prev_op, deser_op)
            g.add_edge(deser_op, op)
        return g


class CountOp(LogicalPlanOp):
    def get_physical_plan(self, prev_ops, pplan):
        from xpark.utils.graph import DiGraph

        g = DiGraph()
        sum_op = SumOp(pplan, self.schema)
        for i, prev_op in enumerate(prev_ops):
            deser_op = DeserializeChunkOp(pplan, self.schema, i)
            g.add_edge(prev_op, deser_op)
            count_op = CountChunkOp(pplan, self.schema, i)
            g.add_edge(deser_op, count_op)
            g.add_edge(count_op, sum_op)
        return g


class LogicalPlan(BaseLogicalPlan):
    start_node_class = LogicalStartOp
    physical_plan_class = PhysicalPlan


class WriteOp(LogicalPlanOp):
    def __init__(self, plan, schema, dataset_writer):
        self.dataset_writer = dataset_writer
        super(__class__, self).__init__(plan, schema)

    def get_physical_plan(self, prev_ops, pplan):
        from xpark.utils.graph import DiGraph

        g = DiGraph()
        for i, prev_op in enumerate(prev_ops):
            deser_op = DeserializeChunkOp(pplan, self.schema, i)
            g.add_edge(prev_op, deser_op)
            op = WriteChunkOp(pplan, self.schema, i, self.dataset_writer)
            g.add_edge(deser_op, op)
        return g
