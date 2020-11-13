import csv

from xpark.dataset.readers import read_parallelized, read_text, read_csv
from xpark.plan import dataframe, rdd
from xpark.utils.iter import get_ranges_for_iterable, get_ranges_for_file


class Dataset(object):
    def __init__(self, ctx, schema=None):
        self.ctx = ctx
        self.schema = schema

    def __repr__(self):
        return '<Dataset>'

    @property
    def cols(self):
        return list(self.schema.keys())

    def get_chunks(self):
        raise NotImplementedError

    def read_chunk(self, start, end):
        raise NotImplementedError

    def read_cols_chunk(self, start, end, cols=None):
        if cols is None:
            cols = self.cols
        chunk = {col: [] for col in cols}
        for d in self.read_chunk(start, end):
            for col in cols:
                chunk[col].append(d[col])
        return chunk

    def toDF(self):
        lp = dataframe.logical.LogicalPlan(self.ctx, start_node_class=dataframe.logical.LogicalStartOp)
        op = dataframe.logical.ReadDatasetOp(lp, self.schema, self)
        lp.start_node.add_op(op)
        return op

    def toRDD(self):
        lp = rdd.logical.LogicalPlan(self.ctx, start_node_class=rdd.logical.LogicalStartOp)
        op = rdd.logical.ReadDatasetOp(lp, self)
        lp.start_node.add_op(op)
        return op


class List(Dataset):
    def __init__(self, ctx, data, schema=None):
        self.data = data
        if schema is None:
            schema = {k: type(v) for k, v in data[0].items()}
        super(__class__, self).__init__(ctx, schema)

    def __repr__(self):
        data_repr = str(self.data)
        if len(data_repr) > 10:
            data_repr = data_repr[:10] + '...'
        return '<List:%s>' % data_repr

    def get_chunks(self):
        return get_ranges_for_iterable(self.data, self.ctx.num_executors, self.ctx.max_memory)

    def read_chunk(self, start, end):
        return read_parallelized(self.data, start, end)


class Text(Dataset):
    col_name = 'col_0'

    def __init__(self, ctx, fname):
        self.fname = fname
        super(__class__, self).__init__(ctx, {self.col_name: str})

    def get_chunks(self):
        return get_ranges_for_file(self.fname, self.ctx.num_executors, self.ctx.max_memory)

    def read_chunk(self, start, end):
        return read_text(self.fname, start, end)


class CSV(Dataset):
    def __init__(self, ctx, fname, schema=None):
        self.fname = fname
        if schema is None:
            with open(fname) as f:
                schema = {col: None for col in csv.DictReader(f).fieldnames}
        super(__class__, self).__init__(ctx, schema)

    def get_chunks(self):
        return get_ranges_for_file(self.fname, self.ctx.num_executors, self.ctx.max_memory)

    def read_chunk(self, start, end):
        return read_csv(self.fname, start, end)
