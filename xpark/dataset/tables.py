from collections import defaultdict
from functools import cached_property
import logging
import os

from fastparquet import ParquetFile
import pandas as pd

from xpark import FileDataset
from xpark.dataset import ParquetWriter, write_parquet
from xpark.dataset.files import FileList
from xpark.settings import settings

_LOG = logging.getLogger(__name__)


class TableUtil(object):
    def __init__(self, ctx):
        self.ctx = ctx

    def list(self):
        tables = {}
        for dir_name in os.listdir(settings.TABLE_STORAGE_PATH):
            dir_path = os.path.join(settings.TABLE_STORAGE_PATH, dir_name)
            if os.path.isdir(dir_path):
                t = Table(self.ctx, dir_name)
                tables[t.name] = t
        return tables

    def get(self, name):
        return self.list()[name]


class TableWriterMixin(object):
    PURPOSE_CATEGORICAL_BITMAP_INDEX = 'bitmap_index'
    PURPOSE_HIGH_CARDINALITY_SORTED_INDEX = 'sorted_index'

    def __init__(self, ctx, dir_name, partition_cols=None, categorical_cols=None):
        path = os.path.join(settings.TABLE_STORAGE_PATH, dir_name)
        super(__class__, self).__init__(ctx, path)
        self.partition_cols = partition_cols or []
        self.categorical_cols = categorical_cols or []

    @staticmethod
    def get_table_class():
        if settings.TABLE_STORAGE_FILE_TYPE == FileList.FILE_TYPE_PARQUET:
            return TableParquetWriter
        else:
            raise ValueError('Unknown file type')

    def add_high_cardinality_index(self, chunk, col, part_id, row_group_offsets):
        pass

    def add_categorical_index(self, chunk, col, part_id, row_group_offsets):
        pass

    def add_indices(self, chunk, part_id):
        pass


class TableParquetWriter(TableWriterMixin, ParquetWriter):
    def add_high_cardinality_index(self, chunk, col, part_id, row_group_offsets):
        _LOG.info('adding index on col:[%s] part_id:[%s] num_chunks:%s',
                  col, part_id, len(row_group_offsets))
        sorted_col = chunk[col].sort_values()
        sorted_col_chunk = pd.DataFrame({col: sorted_col.values, 'i': sorted_col.index})
        sorted_col_fname = self.get_fname(part_id, col=col, purpose=self.PURPOSE_HIGH_CARDINALITY_SORTED_INDEX)
        write_parquet(sorted_col_fname, sorted_col_chunk, row_group_offsets=row_group_offsets, write_index=True)

    def add_indices(self, chunk, part_id):
        chunk_fname = self.get_fname(part_id)
        pf = ParquetFile(chunk_fname)
        row_group_offsets = [0]
        for rg in pf.row_groups:
            row_group_offsets.append(row_group_offsets[-1] + rg.num_rows)
        row_group_offsets = row_group_offsets[:len(pf.row_groups)]

        high_cardinality_cols = set(chunk.cols) - set(self.categorical_cols) - set(self.partition_cols)
        for col in high_cardinality_cols:
            self.add_high_cardinality_index(chunk, col, part_id, row_group_offsets)

        for col in  self.categorical_cols:
            self.add_categorical_index(chunk, col, part_id, row_group_offsets)

    def write_chunk(self, chunk, part_id):
        super(TableParquetWriter, self).write_chunk(chunk, part_id)
        self.add_indices(chunk, part_id)


class BaseChunkIndex(object):
    has_values = False
    purpose = None

    def __init__(self, table, col, part_id, value=None):
        self.table = table
        self.col = col
        self.part_id = part_id
        self.value = value
        self.path = self._get_path()

    def __repr__(self):
        return '<%s ON %s(%s:%s)>' % (self.__class__.__name__, self.table, self.col, self.part_id)

    def _get_path(self):
        from xpark.dataset import DatasetWriter
        return DatasetWriter._get_fname(
            self.table.path, part_id=self.part_id, col=self.col,
            value=self.value, purpose=self.purpose
        )

    @cached_property
    def chunk(self):
        pf = ParquetFile(self.path)
        rg = pf.row_groups[0]
        df = pf.read_row_group(rg, pf.columns, categories=pf.categories, infile=pf.open(pf.fn))
        return df

    def get_rids(self, operator_str, value, augment_cols, chunk):
        raise NotImplementedError


class BitMapIndex(BaseChunkIndex):
    has_values = True
    purpose = TableWriterMixin.PURPOSE_CATEGORICAL_BITMAP_INDEX


class SortedIndex(BaseChunkIndex):
    purpose = TableWriterMixin.PURPOSE_HIGH_CARDINALITY_SORTED_INDEX

    def get_rids(self, operator_str, value, augment_cols, chunk=None):
        from xpark.dataset.utils import RIDMap

        index_chunk = self.chunk
        col_arr = index_chunk[self.col].values
        ll = None
        ul = None
        if operator_str == '<':
            ul = col_arr.searchsorted(value, side='right')
        elif operator_str == '<=':
            ul = col_arr.searchsorted(value, side='right')
        elif operator_str == '>':
            ll = col_arr.searchsorted(value)
        elif operator_str == '>=':
            ll = col_arr.searchsorted(value)
        elif operator_str == '==':
            ll = col_arr.searchsorted(value)
            if col_arr[ll] == value:
                if ll < len(col_arr) - 1 and col_arr[ll + 1] == value:
                    ul = col_arr.searchsorted(value, side='right')
                else:
                    ul = ll + 1
            else:
                ll = -1
                ul = -1
        else:
            raise ValueError('uknown operator:%s' % operator_str)

        ridmap = RIDMap(idx_arr=index_chunk.i.values, ul=ul, ll=ll,
                        rids=None, augment_cols=augment_cols,
                        chunk=chunk, col_map={self.col: index_chunk})
        # move augmenting logic to here. stop passing around so many vars
        return ridmap


class Table(FileDataset):
    index_backend_map = {
        TableWriterMixin.PURPOSE_HIGH_CARDINALITY_SORTED_INDEX: SortedIndex,
        TableWriterMixin.PURPOSE_CATEGORICAL_BITMAP_INDEX: BitMapIndex
    }

    def __init__(self, ctx, dir_name):
        path = os.path.join(settings.TABLE_STORAGE_PATH, dir_name)
        super(__class__, self).__init__(ctx, path, settings.TABLE_STORAGE_FILE_TYPE)
        self.indices = self.init_indices()

    def init_indices(self):
        indices = defaultdict(dict)
        for purpose_fname in os.listdir(self.path):
            purpose_path = os.path.join(self.path, purpose_fname)
            if os.path.isdir(purpose_path) and not purpose_fname.startswith('.'):
                index_class = self.index_backend_map[purpose_fname]
                for col_fname in os.listdir(purpose_path):
                    if not col_fname.startswith('.'):
                        col_path = os.path.join(purpose_path, col_fname)
                        if index_class.has_values:
                            pass
                        else:
                            for part_id_fname in os.listdir(col_path):
                                part_id_fname = int(part_id_fname)
                                indices[col_fname][part_id_fname] = index_class(self, col_fname, part_id_fname)
        return indices

    def has_index(self, col):
        return col in self.indices

    def extract_cols_from_expr(self, expr, schema):
        from xpark.dataset.utils import ColExtractor
        from xpark.utils.context import Override

        evaluator = ColExtractor(self)
        with Override(evaluator_backend=evaluator):
            expr.execute(lambda col: col.name)

        expr_cols = evaluator.cols
        indexed_cols = [col for col in expr_cols if self.has_index(col)]
        extra_cols = list(set(expr_cols) - set(indexed_cols))
        index_expr = expr
        extra_expr = None
        return indexed_cols, index_expr, extra_cols, extra_expr

    @property
    def name(self):
        return os.path.basename(self.path)

    def __repr__(self):
        return '<Table:%s>' % self.name

    def get_rids(self, part_id, expr, augment_cols, chunk=None):
        from xpark.dataset.utils import ParquetRowGroupHighCardinalityIndexEvaluator, RowGroupEvalRequest
        from xpark.utils.context import Override

        evaluator = ParquetRowGroupHighCardinalityIndexEvaluator(self)
        rg_idx = RowGroupEvalRequest(self, part_id, augment_cols, chunk)
        with Override(evaluator_backend=evaluator):
            ridmap = expr.execute(rg_idx)
            return ridmap.to_result()

    def get_rid_count(self, part_id, expr):
        from xpark.dataset.utils import ParquetRowGroupHighCardinalityIndexEvaluator, RowGroupEvalRequest
        from xpark.utils.context import Override

        evaluator = ParquetRowGroupHighCardinalityIndexEvaluator(self)
        rg_idx = RowGroupEvalRequest(self, part_id, augment_cols=[], chunk=None)
        with Override(evaluator_backend=evaluator):
            ridmap = expr.execute(rg_idx)
            return len(ridmap.rids)
