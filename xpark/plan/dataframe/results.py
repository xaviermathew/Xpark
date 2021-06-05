import pandas as pd

from xpark.settings import settings


class Result(object):
    def __init__(self, data):
        self.data = data

    @classmethod
    def from_df(cls, data):
        return settings.RESULT_CONTAINER.from_df(data)

    @classmethod
    def concat(cls, result_set):
        return settings.RESULT_CONTAINER.concat(result_set)

    def __repr__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.data)

    # def __getattr__(self, k):
    #     return getattr(self.data, k)
    #
    # def __setattr__(self, k, v):
    #     setattr(self.data, k, v)

    def __getitem__(self, k):
        return self.data[k]

    def __setitem__(self, k, v):
        self.data[k] = v

    def __delitem__(self, k, v):
        del self.data[k]

    def __len__(self):
        return len(self.data)

    @property
    def cols(self):
        raise NotImplementedError

    def empty(self):
        raise NotImplementedError

    @classmethod
    def empty_from_cols(cls, cols):
        raise NotImplementedError

    def select(self, cols):
        raise NotImplementedError

    def apply_mask(self, mask):
        raise NotImplementedError


class SimpleResult(Result):
    @classmethod
    def from_df(cls, df):
        return cls({k: v.tolist() for k, v in df.iteritems()})

    @classmethod
    def concat(cls, result_set):
        for result in result_set:
            yield from result.data

    @property
    def cols(self):
        return list(self.data.keys())

    @classmethod
    def empty(cls):
        return cls({})

    @classmethod
    def empty_from_cols(cls, cols):
        return cls({col: [] for col in cols})

    def select(self, cols):
        result = self.empty()
        for col in cols:
            result[col] = self[col]
        return self.__class__(result)

    def apply_mask(self, mask):
        cols = list(self.cols)
        total = len(self[cols[0]])
        result = self.empty_from_cols(cols)
        for i in range(total):
            if mask[i]:
                for col in cols:
                    result[col].append(self[col][i])
        return result


class PandasResult(Result):
    @classmethod
    def from_df(cls, df):
        return cls(df)

    @classmethod
    def concat(cls, result_set):
        return cls(pd.concat([result.data for result in result_set]))

    @property
    def cols(self):
        return self.data.columns.to_list()

    @classmethod
    def empty(cls):
        return cls(pd.DataFrame())

    @classmethod
    def empty_from_cols(cls, cols):
        return cls(pd.DataFrame(columns=cols))

    def select(self, cols):
        return self.__class__(self.data.filter(cols))

    def apply_mask(self, mask):
        return self.__class__(self.data[mask])


class RangePartitionedResult(object):
    def __init__(self, data, sort_cols, ranges, range_id):
        self.data = data
        self.sort_cols = sort_cols
        self.ranges = ranges
        self.range_id = range_id

    def __repr__(self):
        ranges_repr = ','.join(['%s[%s:%s]' % (self.sort_cols[i], *range_set[self.range_id])
                                for i, range_set in enumerate(self.ranges)])
        return '<%s [%s]>' % (self.__class__.__name__, ranges_repr)

    def __getitem__(self, k):
        return self.data[k]

    def __setitem__(self, k, v):
        self.data[k] = v

    def __delitem__(self, k, v):
        del self.data[k]

    def __len__(self):
        return len(self.data)


class ResultProxy(object):
    def __init__(self, op):
        self.op = op
        self.object_fn = self.get_object
        self._object = None

    def get_object(self):
        if self._object is None:
            ctx = self.op.plan.ctx
            self._object = getattr(ctx, self.op.return_data_type).get(self.op.task_id)
        return self._object

    def __getattr__(self, k):
        return getattr(self.__dict__['object_fn'](), k)

    def __getitem__(self, k):
        return self.__dict__['object_fn']()[k]

    def __setitem__(self, k, v):
        self.__dict__['object_fn']()[k] = v

    def __delitem__(self, k, v):
        del self.__dict__['object_fn']()[k]

    def __len__(self):
        return len(self.__dict__['object_fn']())
