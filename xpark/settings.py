import importlib
import logging
import multiprocessing


def get_object_from_python_path(python_path):
    parts = python_path.split('.')
    class_name = parts.pop(-1)
    mod_path = '.'.join(parts)
    mod = importlib.import_module(mod_path)
    return getattr(mod, class_name)


FILE_BYTES_TO_MEM_RATIO = 5
FILE_INSPECT_SAMPLE_SIZE = 100

TABLE_STORAGE_PATH = ''

NUM_EXECUTORS = multiprocessing.cpu_count()
MAX_MEMORY = 1024 * 1024
EXECUTOR_BACKEND = 'xpark.executors.backends.SimpleExecutor'
KV_STORE_BACKEND = 'xpark.storage.backends.InMemoryKVBackend'
GROUPBY_STORE_BACKEND = 'xpark.storage.backends.InMemoryGroupByStoreBackend'
RESULT_STORE_BACKEND = 'xpark.storage.backends.InMemoryKVBackend'
EXPRESSION_EVALUATOR_BACKEND = 'xpark.plan.dataframe.expr.SimpleEvaluator'
RESULT_CONTAINER = 'xpark.plan.dataframe.results.SimpleResult'

LOG_LEVEL = logging.DEBUG
logging.basicConfig(level=LOG_LEVEL)


class Settings(object):
    def __getattr__(self, item):
        v = globals()[item]
        if isinstance(v, str) and v.startswith('xpark.'):
            v = get_object_from_python_path(v)
        return v


settings = Settings()
