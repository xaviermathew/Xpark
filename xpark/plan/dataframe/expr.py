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
            return 'Expr:(%s)' % self.s
        else:
            return 'Expr:(%s %s %s)' % (self.s, self.operator_str, self.rhs)

    def copy(self):
        return Expr(ctx=self.ctx, s=self)

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

    def execute_binary_expression(self, lhs, operator_str, rhs):
        return self.ctx.df_expression_evaluator.apply(lhs, operator_str, rhs)

    def execute_node(self):
        from xpark.plan.dataframe.dataframe import Col

        if isinstance(self.s, Col):
            return self.s.execute()
        elif isinstance(self.s, Expr):
            return self.s.execute()
        else:
            return self.s

    def execute(self):
        if self.rhs:
            results = [self.execute_node(), self.operator_str, self.rhs.execute()]
            return self.execute_binary_expression(*results)
        else:
            return self.execute_node()

    def __add__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '+')

    def __sub__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '-')

    def __mul__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '*')

    def __div__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '/')

    def __and__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '&')

    def __or__(self, rhs):
        return Expr.from_pair(self.ctx, self, rhs, '|')


class SimpleEvaluator(object):
    operator_map = {
        '+': operator.add,
        '-': operator.sub,
        '*': operator.mul,
        '/': operator.floordiv,
        '&': operator.and_,
        '|': operator.or_,
    }

    def __init__(self, ctx):
        self.ctx = ctx

    def apply(self, lhs, operator_str, rhs):
        is_lhs_gen = isinstance(lhs, types.GeneratorType)
        is_rhs_gen = isinstance(rhs, types.GeneratorType)
        if is_lhs_gen and is_rhs_gen:
            lhs = list(lhs)
            rhs = list(rhs)
        elif is_lhs_gen and not is_rhs_gen:
            lhs = list(lhs)
            rhs = [rhs] * len(lhs)
        elif not is_lhs_gen and is_rhs_gen:
            rhs = list(rhs)
            lhs = [lhs] * len(rhs)

        operator_func = self.operator_map[operator_str]
        for i, l in enumerate(lhs):
            yield operator_func(l, rhs[i])
