import os

from xpark import settings
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
        chunk = self.read_chunk(dest_format, i)
        if dest_format == self.DEST_FORMAT_RDD:
            new_chunk = [{k: v for k, v in d.items() if k in cols} for d in chunk]
        elif dest_format == self.DEST_FORMAT_DF:
            new_chunk = chunk.select(cols)
        else:
            raise ValueError('Unknown dest_format')
        return new_chunk

    def get_count(self, dest_format, i):
        raise NotImplementedError

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
        for start, end in get_ranges_for_iterable(data, settings.NUM_EXECUTORS, settings.MAX_MEMORY):
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

    def get_count(self, dest_format, i):
        return len(self.read_chunk(dest_format, i))


class FileDataset(Dataset):
    def __init__(self, ctx, path, file_type):
        self.ctx = ctx  # hack needed for File.__init__()
        self.path = path
        self.file_list = FileList(self, path, file_type)
        super(__class__, self).__init__(ctx, schema=self.file_list.schema)

    @property
    def chunks(self):
        return self.file_list.chunks

    def read_chunk(self, dest_format, i):
        return self.file_list.read_chunk(dest_format, i)

    def read_cols_chunk(self, dest_format, i, cols=None):
        return self.file_list.read_cols_chunk(dest_format, i, cols)

    def get_count(self, dest_format, i):
        return self.file_list.get_count(dest_format, i)


class DatasetWriter(object):
    def __init__(self, ctx, path):
        self.ctx = ctx
        self.path = path
        os.makedirs(path, exist_ok=True)

    @staticmethod
    def _get_fname(path, part_id, col=None, value=None, purpose=None):
        parts = [path]
        if purpose is not None:
            parts.append(purpose)
        if col is not None:
            parts.append(col)
        if value is not None:
            parts.append(str(value))
        parts.append(str(part_id))
        return os.path.join(*parts)

    def get_fname(self, part_id, col=None, value=None, purpose=None):
        return self._get_fname(self.path, part_id=part_id, col=col, value=value, purpose=purpose)

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
