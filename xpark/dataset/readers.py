import csv
import functools
import itertools
import operator

from fastparquet import ParquetFile
import pandas as pd


def read_csv(fname, start, end, header=True):
    options = {}
    if header:
        reader = csv.DictReader
        if isinstance(header, str):
            options['fieldnames'] = header
    else:
        reader = csv.reader

    options['file'] = open(fname)
    data = reader(**options)
    return itertools.islice(data, start, end)


def read_text(fname, start, end):
    data = open(fname).iterlines()
    return itertools.islice(data, start, end)


def read_parallelized(iterable, start, end):
    return itertools.islice(iterable, start, end)


def read_parquet(fname, start, end, cols=None):
    pf = ParquetFile(fname)
    if cols is None:
        cols = pf.columns
    i = 0
    df_set= []
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
    return pd.concat(df_set, ignore_index=False)
