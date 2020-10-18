import itertools
import math
import sys

FILE_BYTES_TO_MEM_RATIO = 5


def get_line_count(fname):
    c = -1
    with open(fname) as f:
        for c, _line in enumerate(f):
            pass
    return c + 1


def get_chunk_info(line_count, min_chunks, max_chunk_size, avg_row_size):
    simple_split_chunk_size = int(math.ceil(line_count / min_chunks))
    if (simple_split_chunk_size * avg_row_size) <= max_chunk_size:
        return min_chunks, simple_split_chunk_size
    else:
        chunk_size = max_chunk_size
        num_chunks = line_count / chunk_size
        return int(math.ceil(num_chunks)), int(math.floor(chunk_size))


def get_max_chunk_size_for_file(fname, max_memory, sample_size=100):
    lines = itertools.islice(open(fname), sample_size)
    num_bytes = 0
    num_lines = 0
    for line in lines:
        num_lines += 1
        num_bytes += len(line)

    in_memory_bytes = FILE_BYTES_TO_MEM_RATIO * num_bytes
    return (max_memory / in_memory_bytes) * num_lines


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
    num_chunks, chunk_size = get_chunk_info(num_rows, num_executors, max_chunk_size, avg_row_size)
    return get_range_pairs(num_rows, chunk_size)
