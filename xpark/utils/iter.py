import itertools

FILE_BYTES_TO_MEM_RATIO = 5


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


def get_line_count(fname):
    c = -1
    with open(fname) as f:
        for c, _line in enumerate(f):
            pass
    return c + 1


def get_chunk_info(line_count, min_chunks, max_chunk_size):
    if min_chunks * max_chunk_size > line_count:
        num_chunks = min_chunks
        chunk_size = line_count / num_chunks
    else:
        chunk_size = max_chunk_size
        num_chunks = line_count / chunk_size
    return num_chunks, chunk_size


def get_max_chunk_size(fname, max_memory, sample_size=100):
    lines = itertools.islice(open(fname), sample_size)
    num_bytes = 0
    num_lines = 0
    for line in lines:
        num_lines += 1
        num_bytes += len(line)

    in_memory_bytes = FILE_BYTES_TO_MEM_RATIO * num_bytes
    return (max_memory / in_memory_bytes) * num_lines


def get_ranges_for_file(fname, num_workers, max_memory):
    line_count = get_line_count(fname)
    max_chunk_size = get_max_chunk_size(fname, max_memory)
    num_chunks, chunk_size = get_chunk_info(line_count, num_workers, max_chunk_size)
    return take_pairs(range(0, line_count, chunk_size))
