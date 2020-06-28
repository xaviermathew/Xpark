import networkx as nx


class BaseExecutor(object):
    def execute(self, physical_plan):
        raise NotImplementedError


class DummyExecutor(BaseExecutor):
    def __init__(self, num_executors, max_memory):
        self.num_executors = num_executors
        self.max_memory = max_memory

    def execute(self, physical_plan):
        start_node = physical_plan.start_node
        for op1, op2, in nx.bfs_edges(physical_plan.nx_graph, start_node):
            print('executing op1:%s op2:%s' % (op1, op2))
