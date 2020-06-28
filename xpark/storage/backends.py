from collections import defaultdict

IN_MEMORY_KV_DATA = {}
IN_MEMORY_GROUPBY_DATA = defaultdict(list)
IN_MEMORY_RESULT_DATA = {}

class BaseInMemoryBackend(object):
    @staticmethod
    def dumps(obj):
        return obj

    @staticmethod
    def loads(obj):
        return obj


class InMemoryKVBackend(BaseInMemoryBackend):
    data = IN_MEMORY_KV_DATA

    def get(self, key):
        return self.data[key]

    def set(self, key, value):
        self.data[key] = value


class InMemoryGroupByStoreBackend(BaseInMemoryBackend):
    data = IN_MEMORY_GROUPBY_DATA

    def get(self, key):
        return self.data[key]

    def append(self, key, value):
        return self.data[key].append(value)

    def extend(self, key, v_set):
        return self.data[key].extend(v_set)


class InMemoryResultBackend(InMemoryKVBackend):
    data = IN_MEMORY_RESULT_DATA
