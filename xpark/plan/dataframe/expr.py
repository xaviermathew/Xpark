import logging
import math
import operator

import functools
import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)


class Expr(object):
    def __init__(self, ctx, s, rhs=None, operator_str=None):
        self.ctx = ctx
        self.s = s
        self.rhs = rhs
        self.operator_str = operator_str

    def __repr__(self):
        if self.rhs is None:
            return '%s:(%s)' % (self.__class__.__name__, self.s)
        else:
            return '%s:(%s %s %s)' % (self.__class__.__name__, self.s, self.operator_str, self.rhs)

    def copy(self):
        return self.__class__(ctx=self.ctx, s=self)

    def from_children(self, rhs, operator_str):
        if rhs is None:
            raise ValueError('rhs cant be none')
        if operator is None:
            raise ValueError('operator cant be none')
        new_op = self.copy()
        new_op.rhs = rhs
        new_op.operator_str = operator_str
        return new_op

    @classmethod
    def from_pair(cls, ctx, x, y, operator_str):
        if not isinstance(x, cls):
            x = cls(ctx, x)
        if not isinstance(y, cls):
            y = cls(ctx, y)
        return x.from_children(y, operator_str)

    @classmethod
    def from_unary(cls, ctx, x, operator_str):
        if not isinstance(x, cls):
            x = cls(ctx, x)
        return x.from_children(None, operator_str)

    def execute_binary_expression(self, lhs, operator_str, rhs):
        from xpark.utils.context import CURR_EVALUATOR_BACKEND

        if CURR_EVALUATOR_BACKEND is None:
            evaluator = self.ctx.expression_evaluator_backend
        else:
            evaluator = CURR_EVALUATOR_BACKEND
        return evaluator.apply_expr(lhs, operator_str, rhs)

    def execute_node(self, chunk):
        from xpark.plan.dataframe.logical import Col

        if isinstance(self.s, Col):
            return self.s.execute(chunk)
        elif isinstance(self.s, Expr):
            return self.s.execute(chunk)
        else:
            return self.s

    def execute(self, chunk):
        if self.rhs:
            results = [self.execute_node(chunk), self.operator_str, self.rhs.execute(chunk)]
            return self.execute_binary_expression(*results)
        else:
            return self.execute_node(chunk)

    def __and__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '&')

    def __or__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '|')

    def __add__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '+')

    def between(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, 'between')

    def isin(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, 'isin')

    def isNull(self):
        return Expr.from_unary(self.ctx, self, 'isNull')

    def map(self, func):
        return Expr.from_pair(self.ctx, self, func, 'map')

    def asc(self):
        return Expr.from_unary(self.ctx, self, 'asc')

    def desc(self):
        return Expr.from_unary(self.ctx, self, 'desc')


class NumExpr(Expr):
    def __sub__(self, rhs):
        return NumExpr.from_pair(self.ctx, self, rhs, '-')

    def __mul__(self, rhs):
        return NumExpr.from_pair(self.ctx, self, rhs, '*')

    def __floordiv__(self, rhs):
        return NumExpr.from_pair(self.ctx, self, rhs, '/')

    def __gt__(self, rhs):
        return NumExpr.from_pair(self.ctx, self, rhs, '>')

    def __ge__(self, rhs):
        return NumExpr.from_pair(self.ctx, self, rhs, '>=')

    def __lt__(self, rhs):
        return NumExpr.from_pair(self.ctx, self, rhs, '<')

    def __le__(self, rhs):
        return NumExpr.from_pair(self.ctx, self, rhs, '<=')


class StrExpr(Expr):
    def startswith(self, rhs):
        return StrExpr.from_pair(self.ctx, self, rhs, 'startswith')

    def endswith(self, rhs):
        return StrExpr.from_pair(self.ctx, self, rhs, 'endswith')

    def contains(self, rhs):
        return StrExpr.from_pair(self.ctx, self, rhs, 'contains')

    # def substr(self, start_idx, length):
    #     return Expr.from_pair(self.ctx, self, rhs, 'substr')


class SimpleEvaluator(object):
    expr_operator_map = {
        '+': operator.add,
        '-': operator.sub,
        '*': operator.mul,
        '/': operator.floordiv,
        '<': operator.lt,
        '<=': operator.le,
        '>': operator.gt,
        '>=': operator.ge,
        '&': operator.and_,
        '|': operator.or_,
        'between': lambda lhs, rhs: rhs[0] <= lhs <= rhs[1],
        'contains': lambda lhs, rhs: rhs in lhs,
        # 'substr': lambda lhs, rhs: rhs in lhs,
        # 'isNull': lambda lhs, rhs: rhs in lhs,
        'startswith': lambda lhs, rhs: lhs.startswith(rhs),
        'endswith': lambda lhs, rhs: lhs.endswith(rhs),
        'map': lambda lhs, func: map(func, lhs)
    }

    def __init__(self, ctx):
        self.ctx = ctx

    def apply_expr(self, lhs, operator_str, rhs):
        lhs_repr = 'Col:%s' % lhs.name if isinstance(lhs, pd.Series) else lhs
        rhs_repr = 'Col:%s' % rhs.name if isinstance(rhs, pd.Series) else rhs
        _LOG.info('apply_expr - (%s) %s (%s)', lhs_repr, operator_str, rhs_repr)
        operator_func = self.expr_operator_map[operator_str]
        is_lhs_iter = isinstance(lhs, (list, tuple))
        is_rhs_iter = isinstance(rhs, (list, tuple))
        if is_lhs_iter or is_rhs_iter:
            if is_lhs_iter and is_rhs_iter:
                pass
            elif is_lhs_iter and not is_rhs_iter:
                rhs = [rhs] * len(lhs)
            elif not is_lhs_iter and is_rhs_iter:
                lhs = [lhs] * len(rhs)

            result = []
            for i, l in enumerate(lhs):
                result.append(operator_func(l, rhs[i]))
        elif isinstance(lhs, pd.Series) or isinstance(rhs, pd.Series):
            result = operator_func(lhs, rhs)
        else:
            raise ValueError('unknown result format')
        return result

    def chunk_filter(self, chunk, expr):
        mask = expr.execute(chunk)
        return chunk.apply_mask(mask)

    def chunk_select(self, chunk, cols):
        return chunk.select(cols)

    def chunk_sample(self, chunk, cols, ratio):
        size = int(math.ceil(ratio * len(chunk)))
        results = [np.random.choice(chunk[col].values, size=size) for col in cols]
        return results

    def chunk_range_partition(self, chunk, sort_cols, ranges):
        from xpark.plan.dataframe.results import RangePartitionedResult

        masks = []
        for i, range_set in enumerate(ranges):
            mask = []
            for ll, ul in range_set:
                arr = chunk[sort_cols[i]]
                mask.append((ll <= arr) & (arr < ul))
            masks.append(mask)

        col_masks = [functools.reduce(operator.and_, [mask[i] for mask in masks]) for i in range(len(masks[0]))]
        results = []
        for i, mask in enumerate(col_masks):
            partition = chunk[mask].sort_values(by=list(sort_cols))
            results.append(RangePartitionedResult(partition, sort_cols, ranges, range_id=i))
        return results

    def chunk_add_column(self, chunk, name, expr):
        result = chunk.empty()
        for col in chunk.cols:
            result[col] = chunk[col]
        result[name] = expr.execute(chunk)
        return result

    def chunk_count(self, chunk):
        col = chunk.cols[0]
        return len(chunk[col])

    def chunk_group_by(self, chunk, cols):
        raise NotImplementedError

    def chunk_sort(self, chunk, cols):
        raise NotImplementedError

    def apply_chunk(self, chunk, operator_str, **kwargs):
        _LOG.info('apply_chunk - %s:%s op:%s(%s)', chunk.__class__.__name__, chunk.cols, operator_str, kwargs)
        operator_func = getattr(self, 'chunk_%s' % operator_str)
        return operator_func(chunk, **kwargs)
