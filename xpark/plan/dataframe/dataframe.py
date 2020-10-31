import itertools


class Col(object):
    def __init__(self, df, name):
        self.df = df
        self.name = name

    def __repr__(self):
        return '%s.<Col:%s>' % (self.df, self.name)

    def eval(self):
        return self.df.dataset.get_col(self.name)


class DataFrame(object):
    def __init__(self, dataset):
        self.dataset = dataset
        self.col_cache = {}

    def __repr__(self):
        return '<DataFrame:%s>' % self.dataset

    def __getitem__(self, name):
        from xpark.plan.dataframe.expr import Expr

        if name not in self.col_cache:
            self.col_cache[name] = Expr(Col(self, name))
        return self.col_cache[name]

    def __setitem__(self, name, value):
        self.col_cache[name] = value

    def eval(self):
        data = {}
        for name in self.dataset.cols:
            data[name] = self[name].eval()
        keys = data.keys()
        return (dict(zip(keys, row)) for row in itertools.zip_longest(*data.values()))
