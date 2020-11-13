import multiprocessing

from xpark.dataset import List, Text, CSV
from xpark.executors import Executor
from xpark.executors.backends import SimpleExecutor
from xpark.storage import KVStore, GroupByStore, ResultStore
from xpark.plan.dataframe.expr import SimpleEvaluator
from xpark.storage.backends import InMemoryKVBackend, InMemoryGroupByStoreBackend


class Context(object):
    def __init__(self, num_executors=None, max_memory=None, executor_backend=None,
                 kv_store_backend=None, groupby_store_backend=None, result_store_backend=None,
                 expression_evaluator_backend=None):
        if num_executors is None:
            num_executors = multiprocessing.cpu_count()
        self.num_executors = num_executors

        if max_memory is None:
            max_memory = 1024 * 1024
        self.max_memory = max_memory

        if executor_backend is None:
            executor_backend = SimpleExecutor(num_executors, max_memory)
        self.executor = Executor(self, executor_backend)

        if kv_store_backend is None:
            kv_store_backend = InMemoryKVBackend()
        self.kv_store = KVStore(self, kv_store_backend)

        if groupby_store_backend is None:
            groupby_store_backend = InMemoryGroupByStoreBackend()
        self.groupby_store = GroupByStore(self, groupby_store_backend)

        if result_store_backend is None:
            result_store_backend = InMemoryKVBackend()
        self.result_store = ResultStore(self, result_store_backend)

        if expression_evaluator_backend is None:
            expression_evaluator_backend = SimpleEvaluator(self)
        self.expression_evaluator_backend = expression_evaluator_backend

        self.job_id = 1

    def text(self, fname):
        return Text(self, fname)

    def csv(self, fname, schema=None):
        return CSV(self, fname, schema)

    def parallelize(self, data):
        return List(self, data)
