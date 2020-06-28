import multiprocessing

from storage import KVStore, GroupByStore, ResultStore
from xpark.pipeline import Pipeline
from xpark.plan.logical import ReadCSVOp, ReadTextOp
from xpark.storage.backends import InMemoryKVBackend, InMemoryGroupByStoreBackend


class Context(object):
    def __init__(self, num_workers=None, max_memory=None, worker_backend=None,
                 kv_store_backend=None, groupby_store_backend=None, result_store_backend=None):
        if num_workers is None:
            num_workers = multiprocessing.cpu_count()
        self.num_workers = num_workers

        if max_memory is None:
            max_memory = 1024 * 1024
        self.max_memory = max_memory

        if worker_backend is None:
            worker_backend = None
        self.worker_backend = worker_backend

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
        self.stage_id_ctr = 0

    def get_next_stage_id(self):
        curr = self.stage_id_ctr
        self.stage_id_ctr += 1
        return curr

    def text(self, fname):
        op = ReadTextOp(self, fname)
        return Pipeline(self, ops=[op])

    def csv(self, fname):
        op = ReadCSVOp(self, fname)
        return Pipeline(self, ops=[op])
