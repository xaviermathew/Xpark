class Node(object):
    def __init__(self, op):
        self.op = op

    def __hash__(self):
        return hash(self.op.task_id)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __repr__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.op.task_id)