import os

from xpark import FileDataset
from xpark.dataset import DatasetWriter, CSVWriter, ParquetWriter
from xpark.dataset.files import FileList
from xpark.settings import settings


class TableUtil(object):
    def __init__(self, ctx):
        self.ctx = ctx

    def list(self):
        tables = {}
        for path in os.listdir(self.ctx.table_dir):
            t = Table(self.ctx, path)
            tables[t.name] = t
        return tables

    def get(self, name):
        return self.list()[name]


class TableWriter(DatasetWriter):
    def __init__(self, ctx, fname):
        path = os.path.join(settings.TABLE_STORAGE_PATH, fname)
        super(__class__, self).__init__(ctx, path)
        if settings.TABLE_STORAGE_FILE_TYPE == FileList.FILE_TYPE_PARQUET:
            klass = ParquetWriter
        elif settings.TABLE_STORAGE_FILE_TYPE == FileList.FILE_TYPE_CSV:
            klass = CSVWriter
        self.__class__ = klass


class Table(FileDataset):
    def __init__(self, ctx, fname):
        path = os.path.join(settings.TABLE_STORAGE_PATH, fname)
        super(__class__, self).__init__(ctx, path, settings.TABLE_STORAGE_FILE_TYPE)

    @property
    def name(self):
        return os.path.basename(self.path)

    def __repr__(self):
        return '<Table:%s>' % self.name
