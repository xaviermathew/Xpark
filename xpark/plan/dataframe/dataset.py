class Dataset(object):
    def __init__(self, cols):
        self.cols = cols

    def __repr__(self):
        return '<Dataset>'

    def get_col(self, name):
        raise NotImplementedError


class List(Dataset):
    def __init__(self, data):
        self.data = data
        super(__class__, self).__init__(cols=data[0].keys())

    def __repr__(self):
        data_repr = str(self.data)
        if len(data_repr) > 10:
            data_repr = data_repr[:10] + '...'
        return '<List:%s>' % data_repr

    def get_col(self, name):
        for d in self.data:
            yield d[name]
