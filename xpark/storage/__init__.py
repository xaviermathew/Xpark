class BaseStore(object):
    def __init__(self, ctx, backend):
        self.ctx = ctx
        self.backend = backend

    @staticmethod
    def get_key(task_id, key):
        return '%s:%s' % (task_id, key)


class KVStore(BaseStore):
    def get(self, task_id, key):
        key = self.get_key(task_id, key)
        return self.backend.get(key)

    def set(self, task_id, key, value):
        key = self.get_key(task_id, key)
        return self.backend.set(key, value)

    def clear(self):
        return self.backend.clear()


class GroupByStore(KVStore):
    def get(self, task_id, key):
        key = self.get_key(task_id, key)
        return self.backend.get(key)

    def append(self, task_id, key, value):
        key = self.get_key(task_id, key)
        return self.backend.append(key, value)

    def extend(self, task_id, key, v_set):
        key = self.get_key(task_id, key)
        return self.backend.extend(key, v_set)

    def clear(self):
        return self.backend.clear()


class ResultStore(BaseStore):
    def get(self, task_id, key):
        key = self.get_key(task_id, key)
        return self.backend.get(key)

    def set(self, task_id, key, value):
        key = self.get_key(task_id, key)
        return self.backend.set(key, value)

    def clear(self):
        return self.backend.clear()
