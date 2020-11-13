import operator
import types


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

    @staticmethod
    def from_pair(ctx, x, y, operator_str):
        if not isinstance(x, Expr):
            x = Expr(ctx, x)
        if not isinstance(y, Expr):
            y = Expr(ctx, y)
        return x.from_children(y, operator_str)

    @staticmethod
    def from_unary(ctx, x, operator_str):
        if not isinstance(x, Expr):
            x = Expr(ctx, x)
        return x.from_children(None, operator_str)

    def execute_binary_expression(self, lhs, operator_str, rhs):
        return self.ctx.expression_evaluator_backend.apply_expr(lhs, operator_str, rhs)

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
        return Expr.from_pair(self.ctx, self, rhs, '-')

    def __mul__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '*')

    def __floordiv__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '/')

    def __gt__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '>')

    def __ge__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '>=')

    def __lt__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '<')

    def __le__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '<=')


class StrExpr(Expr):
    def startswith(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, 'startswith')

    def endswith(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, 'endswith')

    def contains(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, 'contains')

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
        is_lhs_gen = isinstance(lhs, (types.GeneratorType, list, tuple))
        is_rhs_gen = isinstance(rhs, (types.GeneratorType, list, tuple))
        if is_lhs_gen and is_rhs_gen:
            lhs = list(lhs)
            rhs = list(rhs)
        elif is_lhs_gen and not is_rhs_gen:
            lhs = list(lhs)
            rhs = [rhs] * len(lhs)
        elif not is_lhs_gen and is_rhs_gen:
            rhs = list(rhs)
            lhs = [lhs] * len(rhs)

        operator_func = self.expr_operator_map[operator_str]
        result = []
        for i, l in enumerate(lhs):
            result.append(operator_func(l, rhs[i]))
        return result

    def chunk_filter(self, chunk, expr):
        mask = expr.execute(chunk)
        cols = list(chunk.keys())
        total = len(chunk[cols[0]])
        result = {col: [] for col in cols}
        for i in range(total):
            if mask[i]:
                for col in cols:
                    result[col].append(chunk[col][i])
        return result

    def chunk_select(self, chunk, cols):
        result = {}
        for col in cols:
            result[col] = chunk[col]
        return result

    def chunk_add_column(self, chunk, name, expr):
        result = {}
        for col in chunk.keys():
            result[col] = chunk[col]
        result[name] = expr.execute(chunk)
        return result

    def chunk_count(self, chunk):
        col = next(chunk.keys())
        return len(chunk[col])

    def chunk_group_by(self, chunk, *expr_set):
        raise NotImplementedError

    def chunk_order_by(self, chunk, *expr_set):
        raise NotImplementedError

    def apply_chunk(self, df, operator_str, **kwargs):
        operator_func = getattr(self, 'chunk_%s' % operator_str)
        return operator_func(df, **kwargs)
