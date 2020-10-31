import operator
import types


class Expr(object):
    def __init__(self, s, rhs=None, operator=None):
        self.s = s
        self.rhs = rhs
        self.operator = operator

    def __repr__(self):
        if self.rhs is None:
            return 'Expr:(%s)' % self.s
        else:
            return 'Expr:(%s %s %s)' % (self.s, self.operator, self.rhs)

    def copy(self):
        return Expr(s=self)

    def from_children(self, rhs, operator):
        if rhs is None:
            raise ValueError('rhs cant be none')
        if operator is None:
            raise ValueError('operator cant be none')
        new_op = self.copy()
        new_op.rhs = rhs
        new_op.operator = operator
        return new_op

    @staticmethod
    def from_pair(x, y, operator):
        if not isinstance(x, Expr):
            x = Expr(x)
        if not isinstance(y, Expr):
            y = Expr(y)
        return x.from_children(y, operator)

    @staticmethod
    def eval_binary_expression(lhs, operator, rhs):
        return SimpleEvaluator().apply(lhs, operator, rhs)

    def eval_node(self):
        from xpark.plan.dataframe.dataframe import Col

        if isinstance(self.s, Col):
            return self.s.eval()
        elif isinstance(self.s, Expr):
            return self.s.eval()
        else:
            return self.s

    def eval(self):
        if self.rhs:
            results = [self.eval_node(), self.operator, self.rhs.eval()]
            return self.eval_binary_expression(*results)
        else:
            return self.eval_node()

    def __add__(self, rhs):
        return Expr.from_pair(self, rhs, operator.add)

    def __sub__(self, rhs):
        return Expr.from_pair(self, rhs, operator.sub)

    def __mul__(self, rhs):
        return Expr.from_pair(self, rhs, operator.mul)

    def __div__(self, rhs):
        return Expr.from_pair(self, rhs, operator.floordiv)

    def __and__(self, rhs):
        return Expr.from_pair(self, rhs, operator.and_)

    def __or__(self, rhs):
        return Expr.from_pair(self, rhs, operator.or_)


class SimpleEvaluator(object):
    def apply(self, lhs, operator, rhs):
        is_lhs_gen = isinstance(lhs, types.GeneratorType)
        is_rhs_gen = isinstance(lhs, types.GeneratorType)
        if is_lhs_gen and is_rhs_gen:
            lhs = list(lhs)
            rhs = list(rhs)
        elif is_lhs_gen and not is_rhs_gen:
            lhs = list(lhs)
            rhs = [rhs] * len(lhs)
        elif not is_lhs_gen and is_rhs_gen:
            rhs = list(rhs)
            lhs = [lhs] * len(rhs)
        for i, l in enumerate(lhs):
            yield operator(l, rhs[i])
