import logging
import networkx as nx

_LOG = logging.getLogger(__name__)


class MisconfiguredGraph(Exception):
    pass


class BaseExecutor(object):
    def execute(self, physical_plan):
        raise NotImplementedError


class DummyExecutor(BaseExecutor):
    def __init__(self, num_executors, max_memory):
        self.num_executors = num_executors
        self.max_memory = max_memory

    def execute(self, physical_plan):
        for op in nx.topological_sort(physical_plan.g):
            print('executing op:%s' % op)


class SimpleExecutor(BaseExecutor):
    def __init__(self, num_executors, max_memory):
        self.num_executors = num_executors
        self.max_memory = max_memory

    def execute(self, physical_plan):
        result_map = {}
        ppg = physical_plan.g
        for op in nx.topological_sort(ppg):
            _LOG.debug('executing op:%s' % op)
            fn = op.get_code()
            if op.reads_data:
                results = []
                for prev_op in ppg.predecessors(op):
                    if prev_op.returns_data:
                        results.append(result_map[prev_op])
                    else:
                        raise MisconfiguredGraph('op:[%s] reads data but prev op:[%s] does not return data' % (op, prev))
                result_map[op] = fn(results)
            else:
                result_map[op] = fn()
        return result_map[op]
