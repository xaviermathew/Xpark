import csv
import functools
import itertools
import logging
import operator

from fastparquet import ParquetFile
import pandas as pd

_LOG = logging.getLogger(__name__)


def read_csv(fname, start, end):
    data = csv.DictReader(open(fname))
    return itertools.islice(data, start, end)


def pd_read_csv(fname, start, end, cols=None):
    nrows = None if end is None else end - start
    if start == 0:
        skiprows = None
    else:
        skiprows = range(1, start + 1)
    return pd.read_csv(fname, skiprows=skiprows, nrows=nrows,
                       usecols=cols, index_col=False)


def read_text(fname, start, end):
    data = open(fname)
    return itertools.islice(data, start, end)


def pd_read_text(fname, start, end):
    from xpark.dataset.files import TextFile
    nrows = None if end is None else end - start
    return pd.read_csv(fname, skiprows=start, nrows=nrows,
                       names=[TextFile.col_name], header=None, sep='\n')


def read_parallelized(iterable, start, end):
    return itertools.islice(iterable, start, end)


def pd_read_parallelized(iterable, start, end, cols=None):
    return pd.DataFrame.from_records(data=itertools.islice(iterable, start, end),
                                     columns=cols)


def _read_parquet(fname, start, end, cols=None):
    pf = ParquetFile(fname)
    if cols is None:
        cols = pf.columns
    i = 0
    df_set = []
    for rg in pf.row_groups:
        last_idx_in_rg = i + rg.num_rows - 1
        if start <= last_idx_in_rg:
            f = pf.open(pf.fn)
            df = pf.read_row_group(rg, cols, pf.categories, infile=f)
            filters = []
            if start > i:
                filters.append(df.index >= (start - i))
            if end is not None and end < last_idx_in_rg:
                filters.append(df.index < (end - i))
            if filters:
                _LOG.warning('unaligned chunk fname:[%s] start:[%s] end:[%s]',
                             fname, start, end)
                df = df[functools.reduce(operator.and_, filters)]
            df_set.append(df)
        i += rg.num_rows
        if end is not None and i >= end:
            break
    return df_set


def read_parquet(fname, start, end, cols=None):
    df_set = _read_parquet(fname, start, end, cols)
    for df in df_set:
        for row in df.iter:
            yield row


def pd_read_parquet(fname, start, end, cols=None):
    df_set = _read_parquet(fname, start, end, cols)
    return pd.concat(df_set, ignore_index=False)
