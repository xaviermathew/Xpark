import csv
import os

import fastparquet
import pandas as pd
from fastparquet.util import default_mkdirs
from tqdm import tqdm

from xpark import settings


def write_csv(fname, data, fieldnames):
    with open(fname, 'w') as f:
        writer = csv.DictWriter(f, fieldnames)
        writer.writeheader()
        for d in tqdm(data):
            writer.writerow(d)


def write_text(fname, lines):
    with open(fname, 'w') as f:
        for line in tqdm(lines):
            f.write(line)


def write_parquet(fname, chunk, **kwargs):
    if not isinstance(chunk, pd.DataFrame):
        chunk = pd.DataFrame(chunk)
    default_mkdirs(os.path.dirname(fname))
    fastparquet.write(fname, chunk, compression=settings.PARQUET_COMPRESSION, **kwargs)
