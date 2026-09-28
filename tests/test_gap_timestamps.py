"""
Tests for timestamps on gapped recordings.

A gap between segments forces explicit per-sample timestamps in the NWB. These
used to be built as one in-memory array, which for a multi-week recording is
tens of GB and got the container OOM-killed. They are now streamed in chunks,
and these tests pin that the streamed values match what the full array was.
"""
import json

import numpy as np
from pynwb import NWBHDF5IO

from processor.multi_channel_reader import MultiChannelReader
from processor.nwb_writer import NWBWriter

RATE_HZ = 100.0
PERIOD_US = 10_000


def _write_gapped_channel(directory, name, segments):
    """Stage one channel with several segments. segments is [(start_us, counts)]."""
    manifest_segments = []
    for i, (start_us, counts) in enumerate(segments):
        samples = np.asarray(counts, dtype="<i4")
        bin_path = directory / f"{name}_seg{i:03d}.bin"
        bin_path.write_bytes(samples.tobytes())
        manifest_segments.append({
            "index": i,
            "start_us": start_us,
            "end_us": start_us + len(samples) * PERIOD_US,
            "n_samples": len(samples),
            "data_path": bin_path.name,
        })

    manifest = {
        "name": name,
        "unit": "counts",
        "rate_hz": RATE_HZ,
        "voltage_conversion_factor": 1.0,
        "segments": manifest_segments,
    }
    (directory / f"{name}.json").write_text(json.dumps(manifest))


def _expected_timestamps(segments, session_start_us):
    parts = [
        (start_us - session_start_us + np.arange(len(counts)) * PERIOD_US) / 1e6
        for start_us, counts in segments
    ]
    return np.concatenate(parts)


# Three segments with a 1 s gap and a 5 s gap. Starts well past the epoch so
# precision loss from large absolute microsecond values would show up.
START_US = 946_750_012_886_516
SEGMENTS = [
    (START_US, list(range(7))),
    (START_US + 7 * PERIOD_US + 1_000_000, list(range(5))),
    (START_US + 12 * PERIOD_US + 6_000_000, list(range(9))),
]


def _reader(tmp_path):
    _write_gapped_channel(tmp_path, "LA01", SEGMENTS)
    _write_gapped_channel(tmp_path, "LA02", SEGMENTS)
    return MultiChannelReader.from_staged_dir(tmp_path)


class TestReaderTimestamps:

    def test_gaps_are_detected(self, tmp_path):
        assert _reader(tmp_path).has_gaps()

    def test_full_range_matches_expected(self, tmp_path):
        reader = _reader(tmp_path)
        np.testing.assert_allclose(
            reader.get_timestamps_seconds(0, reader.num_samples),
            _expected_timestamps(SEGMENTS, START_US),
            rtol=0, atol=1e-9,
        )

    def test_ranges_spanning_segment_boundaries_match(self, tmp_path):
        """Chunks rarely line up with segments; every split must agree."""
        reader = _reader(tmp_path)
        expected = _expected_timestamps(SEGMENTS, START_US)

        for start in range(reader.num_samples):
            for end in range(start + 1, reader.num_samples + 1):
                np.testing.assert_allclose(
                    reader.get_timestamps_seconds(start, end),
                    expected[start:end],
                    rtol=0, atol=1e-9,
                )


class TestNWBTimestamps:

    def test_streamed_timestamps_written_to_nwb(self, tmp_path):
        staged = tmp_path / "staged"
        staged.mkdir()
        reader = _reader(staged)
        out = tmp_path / "out.nwb"

        # Chunk size that doesn't divide the sample count or align with segments.
        NWBWriter(reader, out, chunk_samples=4).write()

        with NWBHDF5IO(str(out), mode="r") as io:
            series = io.read().acquisition["ElectricalSeries"]
            assert series.rate is None
            np.testing.assert_allclose(
                series.timestamps[:],
                _expected_timestamps(SEGMENTS, START_US),
                rtol=0, atol=1e-9,
            )
            assert series.data.shape == (21, 2)
