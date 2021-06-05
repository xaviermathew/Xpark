import itertools
import math
import sys

from xpark.settings import settings


def take_pairs(iterable):
    iterable = iter(iterable)
    prev = None
    while True:
        try:
            curr = next(iterable)
        except StopIteration:
            break
        else:
            if prev is not None:
                yield prev, curr
            prev = curr


def get_ranges(n, chunk_size):
    iterable = iter(range(0, n, chunk_size))
    prev = None
    curr = 0
    while True:
        try:
            curr = next(iterable)
        except StopIteration:
            if curr < n:
                yield curr, n - 1
            break
        else:
            if prev is not None:
                yield prev, curr
            prev = curr


def get_line_count(fname):
    c = -1
    with open(fname) as f:
        for c, _line in enumerate(f):
            pass
    return c + 1


def get_chunk_info(line_count, min_chunks, max_chunk_size):
    simple_split_chunk_size = int(math.ceil(line_count / min_chunks))
    if simple_split_chunk_size <= max_chunk_size:
        return min_chunks, simple_split_chunk_size
    else:
        chunk_size = max_chunk_size
        num_chunks = line_count / chunk_size
        return int(math.ceil(num_chunks)), int(math.floor(chunk_size))


def get_num_bytes_for_sample(fname, sample_size=None):
    if sample_size is None:
        sample_size = settings.FILE_INSPECT_SAMPLE_SIZE
    lines = itertools.islice(open(fname), sample_size)
    num_bytes = 0
    num_lines = 0
    for line in lines:
        num_lines += 1
        num_bytes += len(line)
    return num_lines, num_bytes


def _get_max_chunk_size_for_file(max_memory, num_lines, num_bytes):
    return int(math.ceil((max_memory / num_bytes) * num_lines))


def get_max_chunk_size_for_file(fname, max_memory, sample_size=None):
    num_lines, num_bytes = get_num_bytes_for_sample(fname, sample_size)
    return _get_max_chunk_size_for_file(max_memory, num_lines, num_bytes)


def get_ranges_for_file(fname, num_executors, max_memory):
    num_rows = get_line_count(fname)
    max_chunk_size = get_max_chunk_size_for_file(fname, max_memory)
    num_chunks, chunk_size = get_chunk_info(num_rows, num_executors, max_chunk_size)
    return take_pairs(range(0, num_rows, chunk_size))


def get_max_chunk_size_for_iterable(iterable, max_memory, sample_size=100):
    items = itertools.islice(iterable, sample_size)
    num_bytes = 0
    num_items = 0
    avg_row_size = 0
    for item in items:
        num_items += 1
        num_bytes += sys.getsizeof(item)

        avg_row_size = int(math.ceil(num_bytes / num_items))
    chunk_size = int(math.ceil(max_memory / avg_row_size))
    return avg_row_size, chunk_size


def get_range_pairs(num_rows, chunk_size):
    i = 0
    start = i
    while i < num_rows:
        i += chunk_size
        yield start, min(num_rows, i)
        start = i


def get_ranges_for_iterable(iterable, num_executors, max_memory):
    num_rows = len(iterable)
    avg_row_size, max_chunk_size = get_max_chunk_size_for_iterable(iterable, max_memory)
    num_chunks, chunk_size = get_chunk_info(num_rows, num_executors, max_chunk_size)
    return get_range_pairs(num_rows, chunk_size)


def chunkify(iterable, n):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)
