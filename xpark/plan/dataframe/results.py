import pandas as pd

from xpark.settings import settings


class Result(object):
    def __init__(self, data):
        self.data = data

    @classmethod
    def from_df(cls, data):
        return settings.RESULT_CONTAINER.from_df(data)

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
