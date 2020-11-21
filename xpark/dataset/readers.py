import csv
import functools
import itertools
import operator

from fastparquet import ParquetFile
import pandas as pd


def read_csv(fname, start, end, header=None):
    options = {}
    if header:
        options['fieldnames'] = header

    options['f'] = open(fname)
    data = csv.DictReader(**options)
    return itertools.islice(data, start, end)


def pd_read_csv(fname, start, end, header=True, cols=None):
    return pd.read_csv(fname, skiprows=start, nrows=end-start,
                       names=header, usecols=cols)


def read_text(fname, start, end):
    data = open(fname)
    return itertools.islice(data, start, end)


def pd_read_text(fname, start, end):
    from xpark.dataset.files import TextFile

    return pd.read_csv(fname, skiprows=start, nrows=end - start,
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
            if end < last_idx_in_rg:
                filters.append(df.index < (end - i))
            if filters:
                df = df[functools.reduce(operator.and_, filters)]
            df_set.append(df)
        i += rg.num_rows
        if i >= end:
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
