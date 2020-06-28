import networkx as nx


class BaseExecutor(object):
    def execute(self, physical_plan):
        raise NotImplementedError


class DummyExecutor(BaseExecutor):
    def __init__(self, num_executors, max_memory):
        self.num_executors = num_executors
        self.max_memory = max_memory

    def execute(self, physical_plan):
        for op in nx.topological_sort(physical_plan.nx_graph):
            print('executing op:%s' % op)
