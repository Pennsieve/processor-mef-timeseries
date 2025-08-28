import sys

import numpy as np

def infer_sampling_rate(timestamps):
    """
    Derives a sampling rate based on timestamps given in seconds.

    Assumes the first 10 (or all if < 10) timestamps represent contiguous samples
    """
    sampling_period = np.median(np.diff(timestamps[:10]))
    return 1 / sampling_period

def to_big_endian(data):
    if data.dtype.byteorder == '<' or (data.dtype.byteorder == '=' and sys.byteorder == 'little'):
        return data.byteswap(True).view(data.dtype.newbyteorder())
    else:
        return data
