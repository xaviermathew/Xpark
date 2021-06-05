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

    def get_keys(self, task_id):
        return self.backend.get_keys(task_id)

    def get_items(self, task_id):
        return self.backend.get_items(task_id)

    def append(self, task_id, key, value):
        key = self.get_key(task_id, key)
        return self.backend.append(key, value)

    def extend(self, task_id, key, v_set):
        key = self.get_key(task_id, key)
        return self.backend.extend(key, v_set)

    def clear(self):
        return self.backend.clear()


class ResultStore(BaseStore):
    def get(self, task_id):
        return self.backend.get(task_id)

    def set(self, task_id, value):
        return self.backend.set(task_id, value)

    def clear(self):
        return self.backend.clear()


class AppendableResultStore(BaseStore):
    def get(self, task_id):
        return self.backend.get(task_id)

    def append(self, task_id, value):
        return self.backend.append(task_id, value)

    def clear(self):
        return self.backend.clear()
