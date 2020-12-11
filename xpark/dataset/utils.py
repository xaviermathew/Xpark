import operator

from fastparquet import converted_types, encoding
from fastparquet.api import filter_val as doesnt_satisfy_filter
from fastparquet.parquet_thrift.parquet.ttypes import ColumnChunk
from fastparquet.util import ensure_bytes


def pq_filter_chunks(dataset, filters):
    from xpark.utils.context import Override

    for f in dataset.file_list.file_list:
        evaluator = ParquetRowGroupEvaluator(f.pf)
        for i, rg in enumerate(f.pf.row_groups):
            with Override(evaluator_backend=evaluator):
                if filters.execute(rg):
                    yield dataset.file_list.chunks.index(f.chunks[i])


class ParquetRowGroupEvaluator(object):
    expr_operator_map = {
        '&': operator.and_,
        '|': operator.or_,
    }

    def __init__(self, pf):
        self.pf = pf
        self.schema = pf.schema

    def apply_expr(self, lhs, operator_str, rhs):
        if isinstance(lhs, ColumnChunk):
            column = lhs
            val = rhs
        elif isinstance(rhs, ColumnChunk):
            column = rhs
            val = lhs
        elif isinstance(lhs, bool) and isinstance(rhs, bool):
            operator_func = self.expr_operator_map[operator_str]
            return operator_func(lhs, rhs)
        else:
            raise ValueError('Either lhs or rhs should be a column reference, not lhs:%s rhs:%s' % (lhs, rhs))

        name = '.'.join(column.meta_data.path_in_schema)
        se = self.schema.schema_element(name)
        vmax, vmin = None, None
        s = column.meta_data.statistics
        if s is not None:
            if s.max is not None:
                b = ensure_bytes(s.max)
                vmax = encoding.read_plain(b, column.meta_data.type, 1)
                if se.converted_type is not None:
                    vmax = converted_types.convert(vmax, se)
            if s.min is not None:
                b = ensure_bytes(s.min)
                vmin = encoding.read_plain(b, column.meta_data.type, 1)
                if se.converted_type is not None:
                    vmin = converted_types.convert(vmin, se)
            if doesnt_satisfy_filter(operator_str, val, vmin, vmax):
                return False
        return True
