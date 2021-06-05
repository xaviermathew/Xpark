import math

import numpy as np


def ceil_int(n):
    return int(math.ceil(n))


def histedges_equalN(x, nbin):
    npt = len(x)
    return np.interp(np.linspace(0, npt, nbin + 1),
                     np.arange(npt),
                     np.sort(x))


def get_ranges_from_samples(samples):
    from xpark.settings import settings
    from xpark.utils.iter import take_pairs

    all_samples = [[] for _ in samples[0]]
    for sample in samples:
        for i, col_values in enumerate(sample):
            all_samples[i].extend(col_values)
    all_samples = list(map(tuple, all_samples))

    total = int(len(all_samples[0]) / settings.RANGE_PARTITIONER_SAMPLE_RATIO)
    nbin_by_ratio = ceil_int(settings.RANGE_PARTITIONER_NUM_RANGES_RATIO * total)
    nbin_by_uniques = ceil_int(len(set(zip(*all_samples))) / 2.0)
    nbin = min(nbin_by_ratio, nbin_by_uniques)
    ranges = []
    for col_values in all_samples:
        values = histedges_equalN(col_values, nbin=nbin)
        col_range = []
        for ll, ul in take_pairs(values):
            entry = (ll, ul)
            if not col_range or col_range[-1] != entry:
                col_range.append(entry)
        ranges.append(col_range)
    return ranges
