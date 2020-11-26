import csv
import glob
import math
import os

from fastparquet import ParquetFile as PQFile
from fastparquet.parquet_thrift.parquet.ttypes import Type

from xpark.dataset.readers import (
    pd_read_csv, pd_read_text, pd_read_parquet, read_csv, read_text,
    read_parquet
)
from xpark.utils.iter import get_num_bytes_for_sample, _get_max_chunk_size_for_file, take_pairs


class Chunk(object):
    def __init__(self, file, start, end):
        self.file = file
        self.start = start
        self.end = end

    def __repr__(self):
        return '<Chunk:%s %s:%s>' % (self.file.fname if self.file else None, self.start, self.end)


class File(object):
    def __init__(self, file_list, fname, schema):
        self.file_list = file_list
        self.fname = fname
        self.schema = schema
        self.chunks = []
        self.num_rows = None
        self.num_bytes = os.stat(self.fname).st_size

    def __repr__(self):
        return '<File:%s>' % self.fname

    def get_stats(self):
        return {
            'num_chunks': len(self.chunks),
            'num_rows': self.num_rows,
            'num_bytes': self.num_bytes,
        }

    def read_chunk(self, dest_format, start, end):
        raise NotImplementedError

    def read_cols_chunk(self, dest_format, start, end, cols):
        if cols is None:
            cols = self.schema.keys()
        chunk = {col: [] for col in cols}
        for d in self.read_chunk(dest_format, start, end):
            for col in cols:
                chunk[col].append(d[col])
        return chunk


class ASCIIFile(File):
    def __init__(self, file_list, fname, schema):
        super(__class__, self).__init__(file_list, fname, schema)
        ctx = self.file_list.dataset.ctx
        self.sample_num_lines, self.sample_num_bytes = get_num_bytes_for_sample(fname)
        self.max_chunk_size = _get_max_chunk_size_for_file(ctx.max_memory,
                                                           self.sample_num_lines,
                                                           self.sample_num_bytes)
        self.num_rows = int(math.ceil((self.num_bytes / self.sample_num_bytes) * self.sample_num_lines))
        self.chunk_size = int(math.ceil(self.num_bytes / self.num_rows))
        for start, end in take_pairs(range(0, self.num_rows, self.chunk_size)):
            self.chunks.append(Chunk(self, start, end))

    def get_stats(self):
        stats = super(__class__, self).get_stats()
        stats['chunk_size'] = self.chunk_size
        return stats


class CSVFile(ASCIIFile):
    def __init__(self, file_list, fname, cols=None):
        if cols is None:
            with open(fname) as f:
                schema = {col: str for col in csv.DictReader(f).fieldnames}
        else:
            schema = {col: str for col in cols}
        super(__class__, self).__init__(file_list, fname, schema)

    def read_chunk(self, dest_format, start, end):
        from xpark.dataset import Dataset
        from xpark.plan.dataframe.results import Result

        if dest_format == Dataset.DEST_FORMAT_RDD:
            return read_csv(self.fname, start, end)
        elif dest_format == Dataset.DEST_FORMAT_DF:
            df = pd_read_csv(self.fname, start, end)
            return Result.from_df(df)
        else:
            raise ValueError('Unknown dest_format')


class TextFile(ASCIIFile):
    col_name = 'col_0'

    def __init__(self, file_list, fname):
        super(__class__, self).__init__(file_list, fname, schema={self.col_name: str})

    def read_chunk(self, dest_format, start, end):
        from xpark.dataset import Dataset
        from xpark.plan.dataframe.results import Result

        if dest_format == Dataset.DEST_FORMAT_RDD:
            return read_text(self.fname, start, end)
        elif dest_format == Dataset.DEST_FORMAT_DF:
            df = pd_read_text(self.fname, start, end)
            return Result.from_df(df)
        else:
            raise ValueError('Unknown dest_format')


class ParquetFile(File):
    pq_to_python_type_map = {
        Type.BOOLEAN: bool,
        Type.BYTE_ARRAY: str,
        Type.DOUBLE: float,
        Type.INT32: int,
        Type.INT64: int,
        Type.INT96: int,
    }

    def __init__(self, file_list, fname, cols=None):
        self.pf = PQFile(fname)
        if cols is None:
            cols = self.pf.columns
        schema_map = self.pf.schema.root.children
        schema = {col: self.pq_to_python_type_map[schema_map[col].type]
                  for col in cols}
        super(__class__, self).__init__(file_list, fname, schema)

        self.num_rows = self.pf.count
        start = 0
        for rg in self.pf.row_groups:
            end = start + rg.num_rows
            self.chunks.append(Chunk(self, start, end))
            start += end

    def read_chunk(self, dest_format, start, end):
        from xpark.dataset import Dataset
        from xpark.plan.dataframe.results import Result

        if dest_format == Dataset.DEST_FORMAT_RDD:
            return read_parquet(self.fname, start, end)
        elif dest_format == Dataset.DEST_FORMAT_DF:
            df = pd_read_parquet(self.fname, start, end)
            return Result.from_df(df)
        else:
            raise ValueError('Unknown dest_format')


class FileList(object):
    FILE_TYPE_TEXT = 'txt'
    FILE_TYPE_CSV = 'csv'
    FILE_TYPE_PARQUET = 'pq'
    file_type_map = {
        FILE_TYPE_CSV: CSVFile,
        FILE_TYPE_PARQUET: ParquetFile,
        FILE_TYPE_TEXT: TextFile
    }

    def __init__(self, dataset, path, file_type):
        self.dataset = dataset
        self.path = path
        if glob.has_magic(path):
            fnames = glob.glob(path)
        elif os.path.isdir(path):
            fnames = [os.path.join(path, fname) for fname in os.listdir(path)]
        else:
            fnames = [path]

        if not fnames:
            raise ValueError('No files under path:%s' % path)

        file_class = self.file_type_map[file_type]
        self.file_list = [file_class(self, f) for f in fnames]
        self.chunks = []
        for f in self.file_list:
            self.chunks.extend(f.chunks)

    @property
    def first(self):
        return sorted(self.file_list, key=lambda f: f.fname)[0]

    @property
    def schema(self):
        return self.first.schema

    def read_chunk(self, dest_format, i):
        chunk = self.chunks[i]
        return chunk.file.read_chunk(dest_format, chunk.start, chunk.end)
