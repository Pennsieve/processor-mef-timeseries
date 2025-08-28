# processor/single_channel_reader.py
import json
from pathlib import Path
from typing import List, Tuple, Optional
import numpy as np
from timeseries_channel import TimeSeriesChannel


class SingleChannelReader:
    """
    Per-channel staged reader for TimeSeriesChunkWriter.
    Loads only one channel's segments; timestamps cover only this channel.

    - sampling_rate (Hz)
    - timestamps (float seconds, absolute epoch)
    - channels: [TimeSeriesChannel] (index is the *global* channel index you pass in)
    - contiguous_chunks(): yields [start, end) index ranges (splits at gaps)
    - get_chunk(i, start, end): returns float64 samples for this channel in [start:end)

    NEW:
    - staged_dtype='auto' infers '<i4' if JSON unit == 'counts', else '<f8'.
    """

    def __init__(
        self,
        channel_json_path: str,
        staged_dtype: Optional[str] = "auto",
        global_index: int = 0,
    ):
        path = Path(channel_json_path)
        with path.open() as f:
            json_payload = json.load(f)

        self.name = json_payload["name"]
        self.rate_hz = float(json_payload["rate_hz"])
        self.period_microseconds = 1_000_000.0 / self.rate_hz
        self.unit = str(json_payload.get("unit", "")).lower()

        # Infer staged dtype if requested
        if staged_dtype == "auto" or staged_dtype is None:
            if self.unit == "counts":
                self.staged_dtype = "<i4"  # little-endian int32
            else:
                # Default to float64 (little-endian) if not explicitly counts
                self.staged_dtype = "<f8"
        else:
            self.staged_dtype = staged_dtype

        # segments == blocks of data between discontinuities
        segments = []
        for segment in sorted(json_payload["segments"], key=lambda s: int(s["start_us"])):
            start_microseconds = int(segment["start_us"])
            num_samples = int(segment["n_samples"])
            bin_path = str((path.parent / segment["data_path"]).resolve())
            segments.append((start_microseconds, num_samples, bin_path))
        self._segments: List[Tuple[int, int, str]] = segments

        # Build out timestamps for each segment
        timestamp_list = []
        for start_microseconds, num_samples, _ in segments:
            if num_samples <= 0:
                continue
            sample_indices = np.arange(num_samples, dtype=np.float64)  # [0..n-1]
            segment_ts = np.rint(
                start_microseconds + (sample_indices * self.period_microseconds)
            ).astype(np.int64)
            timestamp_list.append(segment_ts)

        if not timestamp_list:
            raise RuntimeError(
                f"Channel {self.name} has no segments. "
                f"JSON might be empty or malformed: {channel_json_path}"
            )

        self._timestamps_microseconds = np.concatenate(timestamp_list)
        self._timestamps = self._timestamps_microseconds / 1e6  # epoch seconds

        # Cumulative segment map for fast access
        self._segment_map = np.cumsum([0] + [n for _, n, _ in segments])

        # Single-channel metadata (global bounds in microseconds)
        start_global_microseconds = int(self._timestamps_microseconds[0])
        end_global_microseconds = int(self._timestamps_microseconds[-1])
        self._channels = [
            TimeSeriesChannel(
                index=global_index,
                name=self.name,
                rate=self.rate_hz,
                start=start_global_microseconds,
                end=end_global_microseconds,
                group="ElectrodeGroup",
            )
        ]

    @property
    def sampling_rate(self) -> float:
        return self.rate_hz

    @property
    def timestamps(self) -> np.ndarray:
        return self._timestamps

    @property
    def channels(self) -> List[TimeSeriesChannel]:
        return self._channels

    def contiguous_chunks(self):
        """Split at gaps: gap if Δt > 2 * period."""
        gap_threshold = (1.0 / self.rate_hz) * 2.0  # seconds
        ts = self._timestamps
        boundaries = np.concatenate(
            ([0], (np.diff(ts) > gap_threshold).nonzero()[0] + 1, [len(ts)])
        )
        for i in range(len(boundaries) - 1):
            yield int(boundaries[i]), int(boundaries[i + 1])

    def _find_segment_span(self, start: int, end: int):
        """
        Given [start:end) sample indices in the flat channel timeline,
        yield subspans that do not cross segment boundaries:
        returns iterator of (seg_idx, seg_local_lo, seg_local_hi, out_lo, out_hi)
        """
        # seg i contains global indices [cum[i], cum[i+1])
        i = int(np.searchsorted(self._segment_map, start + 1) - 1)
        g = start
        while g < end and i < len(self._segments):
            seg_glo = int(self._segment_map[i])
            seg_ghi = int(self._segment_map[i + 1])
            if g >= seg_ghi:
                i += 1
                continue
            h = min(end, seg_ghi)
            yield i, (g - seg_glo), (h - seg_glo), (g - start), (h - start)
            g = h
            i += 1

    def get_chunk(self, channel_index: int, start: int = None, end: int = None) -> np.ndarray:
        """
        Return float64 samples for this channel in [start:end).
        Reads only the needed slices from segment BINs; no full-file loads.
        """
        if start is None:
            start = 0
        if end is None:
            end = len(self._timestamps)
        size = end - start
        out = np.full(size, np.nan, dtype=np.float64)

        for seg_idx, seg_lo, seg_hi, out_lo, out_hi in self._find_segment_span(start, end):
            _, n, bin_path = self._segments[seg_idx]
            # memmap with staged dtype; slice; upcast to float64 for writer
            mm = np.memmap(bin_path, dtype=self.staged_dtype, mode="r", shape=(n,))
            out[out_lo:out_hi] = np.asarray(mm[seg_lo:seg_hi], dtype=np.float64)

        return out
