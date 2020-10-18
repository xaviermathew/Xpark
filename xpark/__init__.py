import multiprocessing

from xpark.executors import Executor
from xpark.executors.backends import SimpleExecutor
from xpark.storage import KVStore, GroupByStore, ResultStore
from xpark.plan.logical import ReadCSVOp, ReadTextOp, ReadParallelizedOp, LogicalStartOp, LogicalPlan
from xpark.storage.backends import InMemoryKVBackend, InMemoryGroupByStoreBackend


class Context(object):
    def __init__(self, num_executors=None, max_memory=None, executor_backend=None,
                 kv_store_backend=None, groupby_store_backend=None, result_store_backend=None):
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

        self.job_id = 1
        self.logical_plan = LogicalPlan(self)

    def text(self, fname):
        op = ReadTextOp(self.logical_plan, fname)
        self.logical_plan.start_node.add_op(op)
        return op

    def csv(self, fname):
        op = ReadCSVOp(self.logical_plan, fname)
        self.logical_plan.start_node.add_op(op)
        return op

    def parallelize(self, iterable):
        op = ReadParallelizedOp(self.logical_plan, iterable)
        self.logical_plan.start_node.add_op(op)
        return op

    def execute(self):
        return self.logical_plan.execute()
