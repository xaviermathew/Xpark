import importlib
import logging
import multiprocessing
import warnings


def get_object_from_python_path(python_path):
    parts = python_path.split('.')
    class_name = parts.pop(-1)
    mod_path = '.'.join(parts)
    mod = importlib.import_module(mod_path)
    return getattr(mod, class_name)


FILE_INSPECT_SAMPLE_SIZE = 100

ROOT_PATH = '/tmp/xpark/'
TABLE_STORAGE_PATH = ROOT_PATH + 'tables/'
TMP_PATH = ROOT_PATH + 'tmp/'
TABLE_STORAGE_FILE_TYPE = 'pq'

NUM_EXECUTORS = multiprocessing.cpu_count()
MAX_MEMORY = 1024 * 1024
EXECUTOR_BACKEND = 'xpark.executors.backends.SimpleExecutor'
KV_STORE_BACKEND = 'xpark.storage.backends.InMemoryKVBackend'
GROUPBY_STORE_BACKEND = 'xpark.storage.backends.InMemoryGroupByStoreBackend'
RESULT_STORE_BACKEND = 'xpark.storage.backends.InMemoryKVBackend'
EXPRESSION_EVALUATOR_BACKEND = 'xpark.plan.dataframe.expr.SimpleEvaluator'
# RESULT_CONTAINER = 'xpark.plan.dataframe.results.SimpleResult'
RESULT_CONTAINER = 'xpark.plan.dataframe.results.PandasResult'
PARQUET_COMPRESSION = 'UNCOMPRESSED'

LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d:%(funcName)s():%(message)s'


class Settings(object):
    def __getattr__(self, item):
        v = globals()[item]
        if isinstance(v, str) and v.startswith('xpark.'):
            v = get_object_from_python_path(v)
        return v

try:
    from . import local
except ImportError:
    try:
        from . import production
    except ImportError:
        warnings.warn('local.py/production.py is missing')
    else:
        from .production import *
else:
    from .local import *


settings = Settings()
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
