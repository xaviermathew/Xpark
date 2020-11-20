import os

from xpark import FileDataset
from xpark.dataset import DatasetWriter


class TableUtil(object):
    def __init__(self, ctx):
        self.ctx = ctx

    def list(self):
        from xpark import FileList

        tables = {}
        for path in os.listdir(self.ctx.table_dir):
            t = Table(self.ctx, path, FileList.FILE_TYPE_PARQUET)
            tables[t.name] = t
        return tables

    def get(self, name):
        return self.list()[name]


class TableWriter(DatasetWriter):
    pass


class Table(FileDataset):
    @property
    def name(self):
        return os.path.basename(self.path)

    def __repr__(self):
        return '<Table:%s>' % self.name

    def get_chunks(self):
        raise NotImplementedError

    def read_chunk(self, i):
        raise NotImplementedError

    def read_cols_chunk(self, i, cols=None):
        raise NotImplementedError