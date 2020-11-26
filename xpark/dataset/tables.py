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
        for dir_name in os.listdir(settings.TABLE_STORAGE_PATH):
            if not dir_name.startswith('.'):
                t = Table(self.ctx, dir_name)
                tables[t.name] = t
        return tables

    def get(self, name):
        return self.list()[name]


class TableWriter(DatasetWriter):
    def __init__(self, ctx, dir_name):
        path = os.path.join(settings.TABLE_STORAGE_PATH, dir_name)
        super(__class__, self).__init__(ctx, path)
        if settings.TABLE_STORAGE_FILE_TYPE == FileList.FILE_TYPE_PARQUET:
            klass = ParquetWriter
        elif settings.TABLE_STORAGE_FILE_TYPE == FileList.FILE_TYPE_CSV:
            klass = CSVWriter
        else:
            raise ValueError('Unknown file type')
        self.__class__ = klass


class Table(FileDataset):
    def __init__(self, ctx, dir_name):
        path = os.path.join(settings.TABLE_STORAGE_PATH, dir_name)
        super(__class__, self).__init__(ctx, path, settings.TABLE_STORAGE_FILE_TYPE)

    @property
    def name(self):
        return os.path.basename(self.path)

    def __repr__(self):
        return '<Table:%s>' % self.name
