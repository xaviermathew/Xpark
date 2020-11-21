from xpark.settings import settings
from xpark.dataset import List, FileDataset, FileList
from xpark.dataset.tables import TableUtil
from xpark.executors import Executor
from xpark.storage import KVStore, GroupByStore, ResultStore
from xpark.plan.dataframe.expr import SimpleEvaluator


class Context(object):
    def __init__(self):
        self.num_executors = settings.NUM_EXECUTORS
        self.max_memory = settings.MAX_MEMORY

        executor_backend = settings.EXECUTOR_BACKEND(self.num_executors, self.max_memory)
        self.executor = Executor(self, executor_backend)

        kv_store_backend = settings.KV_STORE_BACKEND()
        self.kv_store = KVStore(self, kv_store_backend)

        groupby_store_backend = settings.GROUPBY_STORE_BACKEND()
        self.groupby_store = GroupByStore(self, groupby_store_backend)

        result_store_backend = settings.RESULT_STORE_BACKEND()
        self.result_store = ResultStore(self, result_store_backend)

        self.expression_evaluator_backend = settings.EXPRESSION_EVALUATOR_BACKEND(self)

        self.job_id = 1

    def text(self, fname):
        return FileDataset(self, fname, FileList.FILE_TYPE_TEXT)

    def csv(self, fname):
        return FileDataset(self, fname, FileList.FILE_TYPE_TEXT)

    def parallelize(self, data):
        return List(self, data)

    def parquet(self, fname):
        return FileDataset(self, fname, FileList.FILE_TYPE_PARQUET)

    @property
    def tables(self):
        return TableUtil(self)
