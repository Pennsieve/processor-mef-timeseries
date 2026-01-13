# processor/multi_channel_reader.py
"""
Multi-channel reader that aggregates all staged channels for NWB construction.
Coordinates reading across all channels to produce aligned (samples, channels) data.
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import numpy as np

from processor.single_channel_reader import SingleChannelReader

log = logging.getLogger(__name__)


@dataclass
class ChannelInfo:
    """Metadata for a single channel, used for NWB electrode table."""
    index: int
    name: str
    rate: float
    group: str


class MultiChannelReader:
    """
    Aggregates all staged channels for unified NWB construction.

    Loads all channel JSON manifests, validates alignment, and provides
    methods to read sample ranges across all channels simultaneously.
    """

    def __init__(self, channel_json_paths: List[Path], alignment_tolerance_us: int = 1000):
        """
        Initialize reader with all channel JSON paths.

        Args:
            channel_json_paths: List of paths to channel JSON manifest files
            alignment_tolerance_us: Maximum allowed timestamp difference between
                                    channels at segment boundaries (microseconds)
        """
        if not channel_json_paths:
            raise ValueError("No channel JSON paths provided")

        self._channel_json_paths = sorted(channel_json_paths, key=lambda p: p.stem)
        self._alignment_tolerance_us = alignment_tolerance_us

        # Load all SingleChannelReaders
        self._readers: List[SingleChannelReader] = []
        for idx, json_path in enumerate(self._channel_json_paths):
            reader = SingleChannelReader(
                str(json_path),
                staged_dtype="auto",
                global_index=idx
            )
            self._readers.append(reader)

        log.info("Loaded %d channel readers", len(self._readers))

        # Validate and compute unified properties
        self._validate_alignment()
        self._compute_session_start()
        self._detect_gaps()

    def _validate_alignment(self) -> None:
        """Validate that all channels have aligned sample counts and rates."""
        if len(self._readers) == 0:
            raise ValueError("No channels loaded")

        reference = self._readers[0]
        ref_samples = len(reference.timestamps)
        ref_rate = reference.sampling_rate

        mismatches = []
        for reader in self._readers[1:]:
            samples = len(reader.timestamps)
            rate = reader.sampling_rate

            # Check sample count
            if samples != ref_samples:
                mismatches.append(
                    f"Channel '{reader.name}' has {samples} samples, "
                    f"expected {ref_samples}"
                )

            # Check sampling rate (within 2% tolerance)
            if abs(1 - (rate / ref_rate)) > 0.02:
                mismatches.append(
                    f"Channel '{reader.name}' has rate {rate} Hz, "
                    f"expected ~{ref_rate} Hz"
                )

        if mismatches:
            for msg in mismatches:
                log.warning(msg)
            # For now, log warnings but continue - truncate to minimum later
            log.warning("Channel alignment issues detected; will use minimum sample count")

    def _compute_session_start(self) -> None:
        """Compute unified session start time from earliest timestamp."""
        # Get minimum start timestamp across all channels
        min_start_us = min(
            reader.channels[0].start
            for reader in self._readers
        )
        self._session_start_us = min_start_us
        self._session_start_time = datetime.fromtimestamp(
            min_start_us / 1e6,
            tz=timezone.utc
        )
        log.info("Session start: %s (us=%d)",
                 self._session_start_time.isoformat(), min_start_us)

    def _detect_gaps(self) -> None:
        """Detect if any channel has gaps (discontinuities)."""
        # Use first channel's contiguous_chunks to detect gaps
        reference = self._readers[0]
        chunks = list(reference.contiguous_chunks())
        self._has_gaps = len(chunks) > 1
        self._contiguous_chunks = chunks

        if self._has_gaps:
            log.info("Detected %d contiguous segments (gaps present)", len(chunks))
        else:
            log.info("No gaps detected - continuous data")

    @property
    def num_channels(self) -> int:
        """Number of channels."""
        return len(self._readers)

    @property
    def num_samples(self) -> int:
        """Number of samples per channel (minimum across all channels)."""
        return min(len(r.timestamps) for r in self._readers)

    @property
    def sampling_rate(self) -> float:
        """Sampling rate in Hz (from first channel)."""
        return self._readers[0].sampling_rate

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
                name=reader.name,
                rate=reader.sampling_rate,
                group=reader.channels[0].group
            )
            for idx, reader in enumerate(self._readers)
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

        Returns:
            1D array of timestamps in seconds from session start.
        """
        # Use first channel's timestamps (all should be aligned)
        reference = self._readers[0]
        # Convert from absolute microseconds to relative seconds
        timestamps_us = reference._timestamps_microseconds[:self.num_samples]
        timestamps_relative_us = timestamps_us - self._session_start_us
        return timestamps_relative_us / 1e6

    def get_timestamps_absolute_us(self) -> np.ndarray:
        """
        Get absolute timestamps in microseconds.

        Returns:
            1D array of absolute timestamps in microseconds since epoch.
        """
        reference = self._readers[0]
        return reference._timestamps_microseconds[:self.num_samples].copy()

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

        for ch_idx, reader in enumerate(self._readers):
            # SingleChannelReader.get_chunk takes (channel_index, start, end)
            # but channel_index is ignored since it's a single-channel reader
            chunk = reader.get_chunk(0, start, end)
            result[:, ch_idx] = chunk

        return result

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

        return self._readers[channel_index].get_chunk(0, start, end)

    def get_channel_names(self) -> List[str]:
        """Get list of channel names in order."""
        return [r.name for r in self._readers]

    def get_channel_metadata_json(self, channel_index: int) -> dict:
        """Get the raw JSON metadata for a channel."""
        json_path = self._channel_json_paths[channel_index]
        with json_path.open() as f:
            return json.load(f)
