import csv
import os

from fastparquet import ParquetFile
from fastparquet.parquet_thrift.parquet.ttypes import Type
from xpark.dataset.readers import read_parallelized, read_text, read_csv, read_parquet
from xpark.dataset.writers import write_csv, write_parquet, write_text
from xpark.plan import dataframe, rdd
from xpark.utils.iter import get_ranges_for_iterable, get_ranges_for_file


class Dataset(object):
    def __init__(self, ctx, schema=None):
        self.ctx = ctx
        self.schema = schema

    def __repr__(self):
        return '<Dataset>'

    @property
    def cols(self):
        return list(self.schema.keys())

    def get_chunks(self):
        raise NotImplementedError

    def read_chunk(self, start, end):
        raise NotImplementedError

    def read_cols_chunk(self, start, end, cols=None):
        if cols is None:
            cols = self.cols
        chunk = {col: [] for col in cols}
        for d in self.read_chunk(start, end):
            for col in cols:
                chunk[col].append(d[col])
        return chunk

    def toDF(self):
        lp = dataframe.logical.LogicalPlan(self.ctx, start_node_class=dataframe.logical.LogicalStartOp)
        op = dataframe.logical.ReadDatasetOp(lp, self.schema, self)
        lp.start_node.add_op(op)
        return op

    def toRDD(self):
        lp = rdd.logical.LogicalPlan(self.ctx, start_node_class=rdd.logical.LogicalStartOp)
        op = rdd.logical.ReadDatasetOp(lp, self)
        lp.start_node.add_op(op)
        return op


class List(Dataset):
    def __init__(self, ctx, data, schema=None):
        self.data = data
        if schema is None:
            schema = {k: type(v) for k, v in data[0].items()}
        super(__class__, self).__init__(ctx, schema)

    def __repr__(self):
        data_repr = str(self.data)
        if len(data_repr) > 10:
            data_repr = data_repr[:10] + '...'
        return '<List:%s>' % data_repr

    def get_chunks(self):
        return get_ranges_for_iterable(self.data, self.ctx.num_executors, self.ctx.max_memory)

    def read_chunk(self, start, end):
        return read_parallelized(self.data, start, end)


class Text(Dataset):
    col_name = 'col_0'

    def __init__(self, ctx, fname):
        self.fname = fname
        super(__class__, self).__init__(ctx, {self.col_name: str})

    def get_chunks(self):
        return get_ranges_for_file(self.fname, self.ctx.num_executors, self.ctx.max_memory)

    def read_chunk(self, start, end):
        return read_text(self.fname, start, end)


class CSV(Dataset):
    def __init__(self, ctx, fname, schema=None):
        self.fname = fname
        if schema is None:
            with open(fname) as f:
                schema = {col: None for col in csv.DictReader(f).fieldnames}
        super(__class__, self).__init__(ctx, schema)

    def get_chunks(self):
        return get_ranges_for_file(self.fname, self.ctx.num_executors, self.ctx.max_memory)

    def read_chunk(self, start, end):
        return read_csv(self.fname, start, end)


class Parquet(Dataset):
    pq_to_python_type_map = {
        Type.BOOLEAN: bool,
        Type.BYTE_ARRAY: str,
        Type.DOUBLE: float,
        Type.INT32: int,
        Type.INT64: int,
        Type.INT96: int,
    }

    def __init__(self, ctx, fname, cols=None):
        self.fname = fname
        self.pf = ParquetFile(fname)
        if cols is None:
            cols = self.pf.columns
        schema_map = self.pf.schema.root.children
        schema = {col: self.pq_to_python_type_map[schema_map[col].type]
                  for col in cols}
        super(__class__, self).__init__(ctx, schema)

    def get_chunks(self):
        i = 0
        for rg in self.pf.row_groups:
            yield i, i + rg.num_rows
            i += i + rg.num_rows

    def read_chunk(self, start, end):
        df = read_parquet(self.fname, start, end)
        return df.to_dict('records')

    def read_cols_chunk(self, start, end, cols=None):
        if cols is None:
            cols = self.cols
        df = read_parquet(self.fname, start, end, cols)
        chunk = {col: list(series) for col, series in df.iteritems()}
        return chunk


class Table(Dataset):
    def __init__(self, ctx, fname, cols=None):
        self.fname = fname
        schema = {}
        super(__class__, self).__init__(ctx, schema)


class DatasetWriter(object):
    def __init__(self, ctx, path):
        self.ctx = ctx
        self.path = path
        os.makedirs(path, exist_ok=True)

    def get_fname(self, part_id):
        return os.path.join(self.path, str(part_id))

    def chunk_to_records(self, chunk):
        cols = list(chunk.keys())
        total = len(chunk[cols[0]])
        for i in range(total):
            yield {col: chunk[col][i] for col in cols}

    def write_chunk(self, chunk, part_id):
        raise NotImplementedError


class CSVWriter(DatasetWriter):
    def write_chunk(self, chunk, part_id):
        fname = self.get_fname(part_id)
        data = self.chunk_to_records(chunk)
        write_csv(fname, data, chunk.keys())


class TextWriter(DatasetWriter):
    def write_chunk(self, chunk, part_id):
        fname = self.get_fname(part_id)
        if len(chunk) != 1:
            raise ValueError('Writing to text needs data with just 1 column')

        lines = map(str, next(chunk.values()))
        write_text(fname, lines)


class ParquetWriter(DatasetWriter):
    def write_chunk(self, chunk, part_id):
        fname = self.get_fname(part_id)
        write_parquet(fname, chunk)


class TableWriter(DatasetWriter):
    pass
