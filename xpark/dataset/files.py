import csv
import glob
import logging
import math
import os

from fastparquet import ParquetFile as PQFile
from fastparquet.parquet_thrift.parquet.ttypes import Type

from xpark import settings
from xpark.dataset.readers import (
    pd_read_csv, pd_read_text, pd_read_parquet, read_csv, read_text,
    read_parquet
)
from xpark.utils.iter import get_num_bytes_for_sample, _get_max_chunk_size_for_file, get_chunk_info, \
    get_ranges

_LOG = logging.getLogger(__name__)


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

    def get_count(self, dest_format, start, end):
        return len(self.read_chunk(dest_format, start, end))


class ASCIIFile(File):
    def __init__(self, file_list, fname, schema):
        super(__class__, self).__init__(file_list, fname, schema)
        self.sample_num_lines, self.sample_num_bytes = get_num_bytes_for_sample(fname)
        self.avg_row_size = int(math.ceil(self.sample_num_bytes / self.sample_num_lines))
        self.max_chunk_size = _get_max_chunk_size_for_file(settings.MAX_MEMORY,
                                                           self.sample_num_lines,
                                                           self.sample_num_bytes)
        self.num_rows = int(math.ceil((self.num_bytes / self.sample_num_bytes) * self.sample_num_lines))
        self.num_chunks, self.chunk_size = get_chunk_info(self.num_rows,
                                                          settings.NUM_EXECUTORS,
                                                          self.max_chunk_size)
        pairs = list(get_ranges(self.num_rows, self.chunk_size))
        for i, (start, end) in enumerate(pairs):
            if len(pairs) == i + 1:
                end = None
            self.chunks.append(Chunk(self, start, end))

    def get_stats(self):
        stats = super(__class__, self).get_stats()
        stats['max_chunk_size'] = self.max_chunk_size
        stats['sample_num_lines'] = self.sample_num_lines
        stats['sample_num_bytes'] = self.sample_num_bytes
        stats['chunk_size'] = self.chunk_size
        stats['avg_row_size'] = self.avg_row_size
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
        rg_set = self.pf.row_groups
        for i, rg in enumerate(rg_set):
            if len(rg_set) == i + 1:
                end = None
            else:
                end = start + rg.num_rows
            self.chunks.append(Chunk(self, start, end))
            if end is not None:
                start += end

    def read_chunk(self, dest_format, start, end, cols=None):
        from xpark.dataset import Dataset
        from xpark.plan.dataframe.results import Result

        if dest_format == Dataset.DEST_FORMAT_RDD:
            return read_parquet(self.fname, start, end, cols)
        elif dest_format == Dataset.DEST_FORMAT_DF:
            df = pd_read_parquet(self.fname, start, end, cols)
            return Result.from_df(df)
        else:
            raise ValueError('Unknown dest_format')

    def read_cols_chunk(self, dest_format, start, end, cols=None):
        return self.read_chunk(dest_format, start, end, cols)

    def get_count(self, dest_format, start, end):
        for i, rg in enumerate(self.pf.row_groups):
            chunk = self.chunks[i]
            if chunk.start == start and chunk.end == end:
                return rg.num_rows
        raise ValueError('No chunk matches start:[%s] end:[%s]' % start, end)


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
            fnames = [os.path.join(path, fname)
                      for fname in os.listdir(path)
                      if not os.path.basename(fname).startswith('.')]
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
        _LOG.info('fname:[%s] start:[%s] end:[%s]', chunk.file.fname, chunk.start, chunk.end)
        return chunk.file.read_chunk(dest_format, chunk.start, chunk.end)

    def read_cols_chunk(self, dest_format, i, cols=None):
        chunk = self.chunks[i]
        _LOG.info('fname:[%s] start:[%s] end:[%s] cols:[%s]', chunk.file.fname, chunk.start, chunk.end, cols)
        return chunk.file.read_cols_chunk(dest_format, chunk.start, chunk.end, cols)

    def get_count(self, dest_format, i):
        chunk = self.chunks[i]
        return chunk.file.get_count(dest_format, chunk.start, chunk.end)
