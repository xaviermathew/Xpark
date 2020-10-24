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

    def _trim_key(self, prefix, key):
        # "+ 1" to remove the trailing ":"
        return key[len(prefix) + 1:]

    def get_keys(self, key, trim_key=True):
        for k in self.data.keys():
            if k.startswith(key):
                if trim_key:
                    k = self._trim_key(key, k)
                yield k

    def get_items(self, key):
        for k in self.get_keys(key, trim_key=False):
            yield self._trim_key(key, k), self.get(k)

    def append(self, key, value):
        return self.data[key].append(value)

    def extend(self, key, v_set):
        return self.data[key].extend(v_set)


class InMemoryResultBackend(InMemoryKVBackend):
    data = IN_MEMORY_RESULT_DATA
