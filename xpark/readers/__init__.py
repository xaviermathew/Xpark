import csv


def read_csv(fname, start, end, header=True):
    options = {}
    if header:
        reader = csv.DictReader
        if isinstance(header, str):
            options['fieldnames'] = header
    else:
        reader = csv.reader

    options['file'] = open(fname)
    return reader(**options)


def read_text(fname, start, end):
    return open(fname).iterlines()
