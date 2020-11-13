import csv
import itertools


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
