import os

from xpark.dataset.readers import read_parallelized, read_text, read_csv, read_parquet, pd_read_parallelized
from xpark.dataset.files import FileList
from xpark.dataset.writers import write_csv, write_parquet, write_text
from xpark.plan import dataframe, rdd
from xpark.utils.iter import get_ranges_for_iterable


class Dataset(object):
    DEST_FORMAT_RDD = 'rdd'
    DEST_FORMAT_DF = 'df'

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

    def read_chunk(self, dest_format, i):
        raise NotImplementedError

    def read_cols_chunk(self, dest_format, i, cols=None):
        if cols is None:
            cols = self.cols
        chunk = {col: [] for col in cols}
        for d in self.read_chunk(dest_format, i):
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
    def __init__(self, ctx, data):
        from xpark.dataset.files import Chunk

        self.data = data
        self.chunks = []
        for start, end in get_ranges_for_iterable(data, ctx.num_executors, ctx.max_memory):
            self.chunks.append(Chunk(None, start, end))
        schema = {k: type(v) for k, v in data[0].items()}
        super(__class__, self).__init__(ctx, schema)

    def __repr__(self):
        data_repr = str(self.data)
        if len(data_repr) > 10:
            data_repr = data_repr[:10] + '...'
        return '<List:%s>' % data_repr

    def read_chunk(self, dest_format, i):
        from xpark.plan.dataframe.results import Result

        chunk = self.chunks[i]
        if dest_format == self.DEST_FORMAT_RDD:
            return read_parallelized(self.data, chunk.start, chunk.end)
        elif dest_format == self.DEST_FORMAT_DF:
            df = pd_read_parallelized(self.data, chunk.start, chunk.end)
            return Result.from_df(df)
        else:
            raise ValueError('Unknown dest_format')


class FileDataset(Dataset):
    def __init__(self, ctx, path, file_type):
        self.path = path
        self.file_list = FileList(self, path, file_type)
        super(__class__, self).__init__(ctx, schema=self.file_list.schema)

    @property
    def chunks(self):
        return self.file_list.chunks

    def read_chunk(self, dest_format, i):
        return self.file_list.read_chunk(dest_format, i)


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
