import copy
import logging
import operator
from functools import cached_property

import numpy as np
import pandas as pd

from fastparquet import converted_types, encoding
from fastparquet.api import filter_val as doesnt_satisfy_filter
from fastparquet.parquet_thrift.parquet.ttypes import ColumnChunk
from fastparquet.util import ensure_bytes

from xpark import settings

_LOG = logging.getLogger(__name__)


def pq_prune_chunks_min_max(dataset, expr):
    from xpark.utils.context import Override

    for f in dataset.file_list.file_list:
        evaluator = ParquetRowGroupMinMaxEvaluator(f.pf)
        for i, rg in enumerate(f.pf.row_groups):
            with Override(evaluator_backend=evaluator):
                if expr.execute(rg):
                    yield dataset.file_list.chunks.index(f.chunks[i])


class RowGroupEvalRequest(object):
    def __init__(self, table, part_id, augment_cols, chunk):
        self.table = table
        self.part_id = part_id
        self.augment_cols = augment_cols or []
        self.chunk = chunk

    def to_rowgroupcolevalrequest(self, col):
        return RowGroupColEvalRequest(self.table, self.part_id, self.augment_cols, self.chunk, col)

    def __repr__(self):
        return '<RowGroupEvalRequest:%s:%s>' % (self.table, self.part_id)


class RowGroupColEvalRequest(RowGroupEvalRequest):
    def __init__(self, table, part_id, augment_cols, chunk, col):
        super(__class__, self).__init__(table, part_id, augment_cols, chunk)
        self.col = col
        self.index = table.indices[col][self.part_id]

    def __repr__(self):
        return '<RowGroupColEvalRequest:%s:%s:%s>' % (self.table, self.part_id, self.col)


class ColMask(object):
    def __init__(self, rids, augment_cols, col_map, chunk):
        self.rids = rids
        self.augment_cols = augment_cols
        self.col_map = col_map
        self.chunk = chunk
        self._chunk = {}
        self._col_map = {}

    def get_values(self, col):
        if self.chunk is not None and col in self.chunk:
            if col not in self._chunk:
                self._chunk[col] = self.chunk[col].sort_values('i').drop_index(reset=True)[col].values
            return self._chunk[col][self.rids]
        if self.col_map and col in self.col_map:
            if col not in self._col_map:
                self._col_map[col] = self.col_map[col][col].values
            return self._col_map[col][self.rids]
        else:
            raise ValueError('no way to get values for col:%s to augment RIDMap' % col)

    def get_value_at_idx(self, col, rid):
        if self.chunk and col in self.chunk:
            return self._chunk[col][rid]
        if self.col_map and col in self.col_map:
            return self._col_map[col][rid]
        else:
            raise ValueError('no way to get values for col:%s to augment RIDMap' % col)


class RIDMap(object):
    def __init__(self, idx_arr=None, ul=None, ll=None, rids=None,
                 augment_cols=None, chunk=None, col_map=None):
        self.idx_arr = idx_arr
        self.ul = ul
        self.ll = ll
        self.rids = rids if rids is not None else self._get_slice(self.idx_arr)
        self.augment_cols = augment_cols or []
        self.chunk = pd.DataFrame() if chunk is None else chunk
        self.col_map = col_map or {}
        self.col_mask = ColMask(self.rids, self.augment_cols, self.col_map, self.chunk)

    def __repr__(self):
        if self.rids is None:
            if self.ul is None:
                ul = len(self.chunk)
            else:
                ul = self.ul

            if self.ll is None:
                ll = 0
            else:
                ll = self.ll
            num_sliced = ul - ll
        else:
            num_sliced = len(self.rids)
        return '<RIDMap:%s/%s rows>' % (num_sliced, len(self.chunk))

    def _get_slice(self, arr):
        if self.ll is not None and self.ul is not None:
            return arr[self.ll:self.ul]
        elif self.ll is not None:
            return arr[self.ll:]
        elif self.ul is not None:
            return arr[:self.ul]

    def to_result(self):
        from xpark.plan.dataframe.results import Result

        _LOG.info('augment [%s] rids with cols:%s', len(self.rids), self.augment_cols)

        ridmap = {'i': self.rids}
        for col in self.augment_cols:
            ridmap[col] = self.col_mask.get_values(col)
        df = pd.DataFrame(ridmap)
        return Result.from_df(df)

    def _clone(self, rids, other):
        new_col_map = dict(self.col_map) #  shallow copy
        new_col_map.update(other.col_map)
        return self.__class__(
            rids=rids,
            chunk=self.chunk,
            augment_cols=self.augment_cols,
            col_map=new_col_map,
        )

    def __len__(self):
        return len(self.rids)

    def __and__(self, other):
        return self._clone(np.intersect1d(self.rids, other.rids), other)

    def __or__(self, other):
        return self._clone(np.union1d(self.rids, other.rids), other)


class ParquetRowGroupMinMaxEvaluator(object):
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


class ParquetRowGroupHighCardinalityIndexEvaluator(object):
    expr_operator_map = {
        '&': operator.and_,
        '|': operator.or_,
    }

    def __init__(self, table):
        self.table = table

    def apply_expr(self, lhs, operator_str, rhs):
        if isinstance(lhs, RIDMap) and isinstance(rhs, RIDMap):
            operator_func = self.expr_operator_map[operator_str]
            ridmap = operator_func(lhs, rhs)
            _LOG.info('eval expr - [%s] %s [%s] = [%s] rids', lhs, operator_str, rhs, len(ridmap))
            return ridmap

        if isinstance(lhs, RowGroupColEvalRequest):
            column = lhs
            val = rhs
        elif isinstance(rhs, RowGroupColEvalRequest):
            column = rhs
            val = lhs
        else:
            raise ValueError('Either lhs or rhs should be a column reference, not lhs:%s rhs:%s' % (lhs, rhs))

        ridmap = column.index.get_rids(operator_str, val, column.augment_cols, column.chunk)
        _LOG.info('eval expr - [%s] %s [%s] = [%s] rids', lhs, operator_str, rhs, len(ridmap))
        return ridmap


class ColExtractor(object):
    def __init__(self, table):
        self.table = table
        self.cols = []

    def apply_expr(self, lhs, operator_str, rhs):
        if lhs in self.table.cols:
            self.cols.append(lhs)
        if rhs in self.table.cols:
            self.cols.append(rhs)
