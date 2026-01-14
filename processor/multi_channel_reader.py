# processor/multi_channel_reader.py
"""
Multi-channel reader that aggregates all staged channels for NWB construction.
Memory-efficient: only loads metadata upfront, reads data on-demand.
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import numpy as np

log = logging.getLogger(__name__)


@dataclass
class ChannelInfo:
    """Metadata for a single channel, used for NWB electrode table."""
    index: int
    name: str
    rate: float
    group: str


@dataclass
class SegmentInfo:
    """Metadata for a segment without loading data."""
    index: int
    start_us: int
    end_us: int
    n_samples: int
    data_path: str


@dataclass
class ChannelMetadata:
    """Lightweight channel metadata loaded from JSON."""
    json_path: Path
    name: str
    rate_hz: float
    unit: str
    absolute_start_us: int
    absolute_end_us: int
    segments: List[SegmentInfo]
    total_samples: int


class MultiChannelReader:
    """
    Aggregates all staged channels for unified NWB construction.

    Memory-efficient design:
    - Only loads JSON metadata upfront (not binary data)
    - Computes timestamps on-the-fly when needed
    - Reads binary data in chunks during write phase
    """

    def __init__(self, channel_json_paths: List[Path]):
        """
        Initialize reader with all channel JSON paths.

        Args:
            channel_json_paths: List of paths to channel JSON manifest files
        """
        if not channel_json_paths:
            raise ValueError("No channel JSON paths provided")

        self._channel_json_paths = sorted(channel_json_paths, key=lambda p: p.stem)

        # Load metadata only (not binary data)
        self._channel_metadata: List[ChannelMetadata] = []
        for json_path in self._channel_json_paths:
            meta = self._load_channel_metadata(json_path)
            self._channel_metadata.append(meta)

        log.info("Loaded metadata for %d channels", len(self._channel_metadata))

        # Validate and compute unified properties
        self._validate_alignment()
        self._compute_session_start()
        self._detect_gaps()

    def _load_channel_metadata(self, json_path: Path) -> ChannelMetadata:
        """Load channel metadata from JSON without loading binary data."""
        with json_path.open() as f:
            data = json.load(f)

        segments = []
        total_samples = 0
        for seg in sorted(data["segments"], key=lambda s: int(s["start_us"])):
            seg_info = SegmentInfo(
                index=int(seg["index"]),
                start_us=int(seg["start_us"]),
                end_us=int(seg["end_us"]),
                n_samples=int(seg["n_samples"]),
                data_path=str((json_path.parent / seg["data_path"]).resolve()),
            )
            segments.append(seg_info)
            total_samples += seg_info.n_samples

        return ChannelMetadata(
            json_path=json_path,
            name=data["name"],
            rate_hz=float(data["rate_hz"]),
            unit=str(data.get("unit", "counts")),
            absolute_start_us=int(data.get("absolute_start_us", segments[0].start_us if segments else 0)),
            absolute_end_us=int(data.get("absolute_end_us", segments[-1].end_us if segments else 0)),
            segments=segments,
            total_samples=total_samples,
        )

    def _validate_alignment(self) -> None:
        """Validate that all channels have aligned sample counts and rates."""
        if len(self._channel_metadata) == 0:
            raise ValueError("No channels loaded")

        reference = self._channel_metadata[0]
        ref_samples = reference.total_samples
        ref_rate = reference.rate_hz

        for meta in self._channel_metadata[1:]:
            # Check sample count (allow small differences)
            if abs(meta.total_samples - ref_samples) > 100:
                log.warning(
                    "Channel '%s' has %d samples, expected ~%d (diff=%d)",
                    meta.name, meta.total_samples, ref_samples,
                    meta.total_samples - ref_samples
                )

            # Check sampling rate (within 2% tolerance)
            if abs(1 - (meta.rate_hz / ref_rate)) > 0.02:
                log.warning(
                    "Channel '%s' has rate %.2f Hz, expected ~%.2f Hz",
                    meta.name, meta.rate_hz, ref_rate
                )

    def _compute_session_start(self) -> None:
        """Compute unified session start time from earliest timestamp."""
        min_start_us = min(meta.absolute_start_us for meta in self._channel_metadata)
        self._session_start_us = min_start_us
        self._session_start_time = datetime.fromtimestamp(
            min_start_us / 1e6,
            tz=timezone.utc
        )
        log.info("Session start: %s (us=%d)",
                 self._session_start_time.isoformat(), min_start_us)

    def _detect_gaps(self) -> None:
        """Detect if any channel has gaps (discontinuities) based on segment boundaries."""
        reference = self._channel_metadata[0]
        gap_threshold_us = (1_000_000.0 / reference.rate_hz) * 2  # 2x sample period

        self._has_gaps = False
        self._contiguous_chunks = []

        if len(reference.segments) == 0:
            return

        current_start_sample = 0
        current_end_sample = 0

        for i, seg in enumerate(reference.segments):
            if i == 0:
                current_end_sample = seg.n_samples
                continue

            prev_seg = reference.segments[i - 1]
            gap_us = seg.start_us - prev_seg.end_us

            if gap_us > gap_threshold_us:
                # Gap detected - record the contiguous chunk
                self._contiguous_chunks.append((current_start_sample, current_end_sample))
                current_start_sample = current_end_sample
                self._has_gaps = True
                log.info("Gap detected: %d us between segment %d and %d",
                         gap_us, i - 1, i)

            current_end_sample += seg.n_samples

        # Add final chunk
        self._contiguous_chunks.append((current_start_sample, current_end_sample))

        if self._has_gaps:
            log.info("Detected %d contiguous segments (gaps present)",
                     len(self._contiguous_chunks))
        else:
            log.info("No gaps detected - continuous data")

    @property
    def num_channels(self) -> int:
        """Number of channels."""
        return len(self._channel_metadata)

    @property
    def num_samples(self) -> int:
        """Number of samples per channel (minimum across all channels)."""
        return min(meta.total_samples for meta in self._channel_metadata)

    @property
    def sampling_rate(self) -> float:
        """Sampling rate in Hz (from first channel)."""
        return self._channel_metadata[0].rate_hz

    @property
    def session_start_time(self) -> datetime:
        """Session start time as UTC datetime."""
        return self._session_start_time

    @property
    def session_start_us(self) -> int:
        """Session start time in microseconds since epoch."""
        return self._session_start_us

    @property
    def channels(self) -> List[ChannelInfo]:
        """List of channel metadata for NWB electrode table."""
        return [
            ChannelInfo(
                index=idx,
                name=meta.name,
                rate=meta.rate_hz,
                group="MEFElectrodeGroup"
            )
            for idx, meta in enumerate(self._channel_metadata)
        ]

    def has_gaps(self) -> bool:
        """Check if there are discontinuities in the data."""
        return self._has_gaps

    def get_contiguous_segments(self) -> List[tuple]:
        """Return list of (start_idx, end_idx) for contiguous segments."""
        return self._contiguous_chunks

    def get_timestamps_seconds(self) -> np.ndarray:
        """
        Get timestamps relative to session start, in seconds.
        Computed on-the-fly to save memory.

        Returns:
            1D array of timestamps in seconds from session start.
        """
        reference = self._channel_metadata[0]
        period_us = 1_000_000.0 / reference.rate_hz

        # Build timestamps for each segment
        timestamps_list = []
        for seg in reference.segments:
            if seg.n_samples <= 0:
                continue
            sample_indices = np.arange(seg.n_samples, dtype=np.float64)
            segment_ts_us = seg.start_us + (sample_indices * period_us)
            timestamps_list.append(segment_ts_us)

        if not timestamps_list:
            return np.array([], dtype=np.float64)

        # Concatenate and convert to relative seconds
        all_timestamps_us = np.concatenate(timestamps_list)
        relative_timestamps_us = all_timestamps_us - self._session_start_us
        return relative_timestamps_us[:self.num_samples] / 1e6

    def read_all_channels(self, start: int, end: int) -> np.ndarray:
        """
        Read samples [start:end) from all channels.

        Args:
            start: Start sample index (inclusive)
            end: End sample index (exclusive)

        Returns:
            Array of shape (end-start, num_channels) with float64 values.
        """
        num_samples = end - start
        result = np.empty((num_samples, self.num_channels), dtype=np.float64)

        for ch_idx, meta in enumerate(self._channel_metadata):
            chunk = self._read_channel_range(meta, start, end)
            result[:, ch_idx] = chunk

        return result

    def _read_channel_range(self, meta: ChannelMetadata, start: int, end: int) -> np.ndarray:
        """Read a range of samples from a single channel."""
        size = end - start
        out = np.full(size, np.nan, dtype=np.float64)

        # Determine dtype based on unit
        if meta.unit.lower() == "counts":
            dtype = "<i4"  # little-endian int32
        else:
            dtype = "<f8"  # little-endian float64

        # Build segment map for this channel
        segment_map = [0]
        for seg in meta.segments:
            segment_map.append(segment_map[-1] + seg.n_samples)

        # Find which segments overlap with [start, end)
        for seg_idx, seg in enumerate(meta.segments):
            seg_start_global = segment_map[seg_idx]
            seg_end_global = segment_map[seg_idx + 1]

            # Check if this segment overlaps with requested range
            if seg_end_global <= start or seg_start_global >= end:
                continue

            # Calculate overlap
            read_start_global = max(start, seg_start_global)
            read_end_global = min(end, seg_end_global)

            # Convert to segment-local indices
            seg_local_start = read_start_global - seg_start_global
            seg_local_end = read_end_global - seg_start_global

            # Convert to output indices
            out_start = read_start_global - start
            out_end = read_end_global - start

            # Read from file using memmap
            mm = np.memmap(seg.data_path, dtype=dtype, mode="r", shape=(seg.n_samples,))
            out[out_start:out_end] = np.asarray(mm[seg_local_start:seg_local_end], dtype=np.float64)
            del mm  # Release memmap

        return out

    def read_channel(self, channel_index: int, start: int, end: int) -> np.ndarray:
        """
        Read samples [start:end) from a single channel.

        Args:
            channel_index: Index of the channel to read
            start: Start sample index (inclusive)
            end: End sample index (exclusive)

        Returns:
            1D array of float64 values.
        """
        if channel_index < 0 or channel_index >= self.num_channels:
            raise IndexError(f"Channel index {channel_index} out of range [0, {self.num_channels})")

        return self._read_channel_range(self._channel_metadata[channel_index], start, end)

    def get_channel_names(self) -> List[str]:
        """Get list of channel names in order."""
        return [meta.name for meta in self._channel_metadata]
