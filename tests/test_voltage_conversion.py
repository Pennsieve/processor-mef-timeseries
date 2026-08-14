"""
Tests for the voltage conversion factor path: CHANNEL_META -> manifest ->
reader -> NWB.

Samples are raw A/D counts. The MEF header's voltage conversion factor
(microvolts per count) is the only thing that makes them a physical
measurement. Previously it was dropped at every stage and the NWB was written
with conversion=1.0, which downstream read as volts and multiplied by 1e6 —
so displayed values were counts x 1e6 instead of counts x factor.
"""
import json

import numpy as np
import pytest
from hdmf.data_utils import GenericDataChunkIterator
from pynwb import NWBHDF5IO

from processor.mef_streamer import _ChannelState
from processor.multi_channel_reader import MultiChannelReader
from processor.nwb_writer import UV_TO_VOLTS, NWBWriter


def _write_channel(directory, name, counts, vcf, rate_hz=512.0, start_us=0):
    """Stage one channel the way mef_streamer does, then return its manifest path."""
    samples = np.asarray(counts, dtype="<i4")
    bin_path = directory / f"{name}_seg000.bin"
    bin_path.write_bytes(samples.tobytes())

    manifest = {
        "name": name,
        "type": "SEEG",
        "description": "test",
        "unit": "counts",
        "rate_hz": rate_hz,
        "low_cut_hz": -1.0,
        "high_cut_hz": -1.0,
        "absolute_start_us": start_us,
        "absolute_end_us": start_us + int(len(samples) / rate_hz * 1e6),
        "segments": [{
            "index": 0,
            "start_us": start_us,
            "end_us": start_us + int(len(samples) / rate_hz * 1e6),
            "n_samples": len(samples),
            "data_path": bin_path.name,
        }],
    }
    if vcf is not None:
        manifest["voltage_conversion_factor"] = vcf

    path = directory / f"{name}.json"
    path.write_text(json.dumps(manifest))
    return path


class TestStreamerManifest:
    """CHANNEL_META -> staged manifest."""

    def test_manifest_carries_conversion_factor_from_channel_meta(self, tmp_path):
        state = _ChannelState(tmp_path)
        state.begin_channel({"name": "LA01", "rate_hz": 512.0,
                             "voltage_conversion_factor": 0.2987})
        state.flush()

        manifest = json.loads((tmp_path / "LA01.json").read_text())
        assert manifest["voltage_conversion_factor"] == 0.2987

    def test_manifest_still_reports_counts_as_unit(self, tmp_path):
        state = _ChannelState(tmp_path)
        state.begin_channel({"name": "LA01", "rate_hz": 512.0,
                             "voltage_conversion_factor": 0.25})
        state.flush()

        manifest = json.loads((tmp_path / "LA01.json").read_text())
        assert manifest["unit"] == "counts"

    def test_missing_factor_defaults_to_one_and_warns(self, tmp_path, caplog):
        """An out-of-date jar omits the field; don't invent a factor silently."""
        state = _ChannelState(tmp_path)
        with caplog.at_level("WARNING"):
            state.begin_channel({"name": "LA01", "rate_hz": 512.0})
        state.flush()

        manifest = json.loads((tmp_path / "LA01.json").read_text())
        assert manifest["voltage_conversion_factor"] == 1.0
        assert "voltage_conversion_factor" in caplog.text

    def test_factor_does_not_leak_between_channels(self, tmp_path):
        """State is reused across channels; a stale factor would mis-scale."""
        state = _ChannelState(tmp_path)
        state.begin_channel({"name": "LA01", "rate_hz": 512.0,
                             "voltage_conversion_factor": 0.5})
        state.flush()
        state.begin_channel({"name": "LA02", "rate_hz": 512.0})
        state.flush()

        second = json.loads((tmp_path / "LA02.json").read_text())
        assert second["voltage_conversion_factor"] == 1.0


class TestReader:
    """Staged manifest -> MultiChannelReader."""

    def test_reader_exposes_per_channel_factors_in_channel_order(self, tmp_path):
        _write_channel(tmp_path, "LA01", [1, 2, 3], vcf=0.25)
        _write_channel(tmp_path, "LA02", [4, 5, 6], vcf=0.5)

        reader = MultiChannelReader.from_staged_dir(tmp_path)

        assert reader.channel_names == ["LA01", "LA02"]
        assert reader.voltage_conversion_factors == [0.25, 0.5]

    def test_reader_defaults_missing_factor_to_one(self, tmp_path, caplog):
        _write_channel(tmp_path, "LA01", [1, 2, 3], vcf=None)

        with caplog.at_level("WARNING"):
            reader = MultiChannelReader.from_staged_dir(tmp_path)

        assert reader.voltage_conversion_factors == [1.0]
        assert "voltage_conversion_factor" in caplog.text

    def test_reader_returns_unscaled_counts(self, tmp_path):
        """Scaling belongs in NWB metadata, not baked into the samples."""
        _write_channel(tmp_path, "LA01", [100, 200, 300], vcf=0.25)

        reader = MultiChannelReader.from_staged_dir(tmp_path)

        np.testing.assert_array_equal(
            reader.read_all_channels(0, 3)[:, 0], [100.0, 200.0, 300.0]
        )


class TestNWBOutput:
    """Reader -> NWB file. These pin the numbers a consumer actually sees."""

    def _write_nwb(self, tmp_path, channels):
        staged = tmp_path / "staged"
        staged.mkdir()
        for name, counts, vcf in channels:
            _write_channel(staged, name, counts, vcf)

        reader = MultiChannelReader.from_staged_dir(staged)
        out = tmp_path / "out.nwb"
        NWBWriter(reader, out, chunk_samples=10).write()
        return out

    def test_conversion_and_channel_conversion_are_written(self, tmp_path):
        out = self._write_nwb(tmp_path, [("LA01", [1, 2, 3], 0.25),
                                         ("LA02", [4, 5, 6], 0.5)])

        with NWBHDF5IO(str(out), mode="r") as io:
            series = io.read().acquisition["ElectricalSeries"]

            assert series.conversion == pytest.approx(UV_TO_VOLTS)
            np.testing.assert_allclose(series.channel_conversion[:], [0.25, 0.5])
            assert series.offset == 0.0

    def test_stored_data_remains_raw_counts(self, tmp_path):
        out = self._write_nwb(tmp_path, [("LA01", [100, 200, 300], 0.25)])

        with NWBHDF5IO(str(out), mode="r") as io:
            series = io.read().acquisition["ElectricalSeries"]
            np.testing.assert_array_equal(series.data[:, 0], [100.0, 200.0, 300.0])

    def test_downstream_scaling_yields_expected_microvolts(self, tmp_path):
        """
        The regression this change fixes, expressed the way the consumer computes
        it (processor-post-timeseries reader.py): counts * conversion *
        channel_conversion + offset gives volts, then x1e6 gives microvolts.

        200 counts at 0.25 uV/count must display as 50 uV. Before this change the
        same input produced 200 * 1.0 * 1e6 = 2e8 uV.
        """
        out = self._write_nwb(tmp_path, [("LA01", [200], 0.25), ("LA02", [200], 0.5)])

        with NWBHDF5IO(str(out), mode="r") as io:
            series = io.read().acquisition["ElectricalSeries"]

            volts = (series.data[:, :] * series.conversion
                     * np.array(series.channel_conversion[:]) + series.offset)
            microvolts = volts * 1e6

            np.testing.assert_allclose(microvolts[0], [50.0, 100.0])

    def test_negative_factor_inverts_polarity(self, tmp_path):
        out = self._write_nwb(tmp_path, [("LA01", [200], -0.25)])

        with NWBHDF5IO(str(out), mode="r") as io:
            series = io.read().acquisition["ElectricalSeries"]
            microvolts = (series.data[:, :] * series.conversion
                          * np.array(series.channel_conversion[:])) * 1e6

            np.testing.assert_allclose(microvolts[0], [-50.0])

    def test_channel_conversion_order_matches_electrode_table(self, tmp_path):
        """A mismatch here would scale each channel by another channel's factor."""
        out = self._write_nwb(tmp_path, [("LA01", [1], 0.25),
                                         ("LA02", [1], 0.5),
                                         ("LA03", [1], 0.75)])

        with NWBHDF5IO(str(out), mode="r") as io:
            nwb = io.read()
            series = nwb.acquisition["ElectricalSeries"]
            names = list(nwb.electrodes["channel_name"][:])

            assert names == ["LA01", "LA02", "LA03"]
            np.testing.assert_allclose(series.channel_conversion[:], [0.25, 0.5, 0.75])
