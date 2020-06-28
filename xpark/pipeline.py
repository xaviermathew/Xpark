#https://mallikarjuna_g.gitbooks.io/spark/spark-sql-catalyst-Optimizer.html

import copy

from xpark.plan.logical import StartOp, MapOp, FilterOp, GroupByKeyOp, LogicalPlan
from xpark.plan.physical import PhysicalPlan


class Pipeline(object):
    def __init__(self, ctx, ops):
        self.ctx = ctx
        if not ops[0].is_start_op:
            ops.insert(0, StartOp())
        self.ops = ops

    def clone(self):
        return Pipeline(self.ctx, copy.deepcopy(self.ops))

    def add_op(self, op_class, *args, **kwargs):
        p = self.clone()
        p.ops.append(op_class(*args, **kwargs))
        return p

    def map(self, func):
        return self.add_op(MapOp, ctx=self.ctx, func=func)

    def filter(self, func):
        return self.add_op(FilterOp, ctx=self.ctx, func=func)

    def group_by_key(self):
        return self.add_op(GroupByKeyOp, ctx=self.ctx)

    def to_graph(self):
        lp = LogicalPlan.from_pipeline(self)
        pp = PhysicalPlan.from_logical_plan(lp)
        return pp
