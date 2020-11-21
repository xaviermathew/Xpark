from xpark.settings import settings


class Result(object):
    def __init__(self, data):
        self.data = data

    @classmethod
    def from_df(cls, data):
        return settings.RESULT_CONTAINER.from_df(data)

    def __repr__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.data)

    def __getattr__(self, k):
        return getattr(self.data, k)

    def __setattr__(self, k, v):
        setattr(self.data, k, v)

    def __getitem__(self, k):
        return self.data[k]

    def __setitem__(self, k, v):
        self.data[k] = v

    def __len__(self):
        return len(self.data)


class SimpleResult(Result):
    @classmethod
    def from_df(cls, df):
        return cls({k: v.tolist() for k, v in df.iteritems()})


class PandasResult(Result):
    @classmethod
    def from_df(cls, df):
        return cls(df)
