import logging
import networkx as nx

from xpark.exceptions import MisconfiguredGraph

_LOG = logging.getLogger(__name__)


class PlanTraversor(object):
    def __init__(self, physical_plan):
        self.prev_plans = []
        self.ops = []
        self.result_map = {}
        self.physical_plan = None
        self.update_physical_plan(physical_plan)

    def update_physical_plan(self, physical_plan):
        self.ops = list(nx.topological_sort(physical_plan.g))
        self.prev_plans.append(self.physical_plan)
        self.physical_plan = physical_plan

    def is_done(self):
        return len(self.result_map) == len(self.ops)

    def is_op_done(self, op):
        return op in self.result_map

    def is_op_schedulable(self, op):
        if self.is_op_done(op):
            return False

        for prev_op in self.physical_plan.g.predecessors(op):
            if not self.is_op_done(prev_op):
                return False

        return True

    def get_next_op(self):
        for op in self.ops:
            if self.is_op_schedulable(op):
                return op

    def mark_as_done(self, op, result):
        self.result_map[op] = result


class BaseExecutor(object):
    def __init__(self, num_executors, max_memory):
        self.num_executors = num_executors
        self.max_memory = max_memory

    def execute_op(self, op, fn, *args, **kwargs):
        raise NotImplementedError

    def execute(self, physical_plan):
        pt = PlanTraversor(physical_plan)
        op = None
        while not pt.is_done():
            op = pt.get_next_op()
            _LOG.info('executing op:%s' % op)
            fn = op.get_code()
            if op.reads_data:
                results = []
                for prev_op in physical_plan.g.predecessors(op):
                    if prev_op.returns_data:
                        results.append(pt.result_map[prev_op])
                    else:
                        raise MisconfiguredGraph('op:[%s] reads data but prev op:[%s] does not return data' % (op, prev_op))
                op_result = self.execute_op(op, fn, results)
            else:
                op_result = self.execute_op(op, fn)
            if op.mutates_graph:
                result, new_physical_plan = op_result
                pt.result_map[op] = result
                pt.update_physical_plan(new_physical_plan)
            else:
                pt.result_map[op] = op_result
        return pt.result_map[op]


class DummyExecutor(BaseExecutor):
    def execute_op(self, op, *args, **kwargs):
        print('executing op:%s args:%s kwargs:%s' % (op, args, kwargs))


class SimpleExecutor(BaseExecutor):
    def execute_op(self, op, fn, *args, **kwargs):
        return fn(*args, **kwargs)
