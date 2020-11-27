import csv

import fastparquet
import pandas as pd
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


def write_parquet(fname, chunk):
    df = pd.DataFrame(chunk)
    fastparquet.write(fname, df, compression=settings.PARQUET_COMPRESSION)
