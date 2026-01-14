"""
NWB file writer for MEF timeseries data.

Produces NWB 2.x files with ElectricalSeries, compatible with
downstream processing pipelines (e.g., processor-post-timeseries).
"""
import logging
import uuid
from pathlib import Path
from typing import Tuple

import numpy as np
from hdmf.data_utils import GenericDataChunkIterator
from pynwb import NWBHDF5IO, NWBFile
from pynwb.ecephys import ElectricalSeries

from processor.multi_channel_reader import MultiChannelReader

log = logging.getLogger(__name__)


class _ChunkedDataIterator(GenericDataChunkIterator):
    """Streams channel data in chunks to avoid loading entire dataset into memory."""

    def __init__(self, reader: MultiChannelReader, chunk_samples: int):
        self._reader = reader
        self._shape = (reader.num_samples, reader.num_channels)
        buffer = (min(chunk_samples, reader.num_samples), reader.num_channels)

        super().__init__(buffer_shape=buffer, chunk_shape=buffer, display_progress=True)
        log.info(
            "Chunk iterator: %d samples x %d channels, chunk=%d",
            *self._shape, buffer[0],
        )

    def _get_data(self, selection: Tuple[slice, ...]) -> np.ndarray:
        return self._reader.read_all_channels(selection[0].start, selection[0].stop)

    def _get_maxshape(self) -> Tuple[int, int]:
        return self._shape

    def _get_dtype(self) -> np.dtype:
        return np.dtype("float64")


class NWBWriter:
    """
    Writes MEF channel data to NWB format.

    The output file contains:
      - Device and ElectrodeGroup metadata
      - Electrode table with channel names
      - ElectricalSeries with sample data (chunked for memory efficiency)
    """

    def __init__(
        self,
        reader: MultiChannelReader,
        output_path: Path,
        chunk_samples: int = 100_000,
    ):
        self._reader = reader
        self._output_path = Path(output_path)
        self._chunk_samples = chunk_samples

    def write(self) -> Path:
        log.info(
            "Writing NWB: %d channels, %d samples, %.2f Hz",
            self._reader.num_channels,
            self._reader.num_samples,
            self._reader.sampling_rate,
        )

        nwb = self._create_nwb_file()
        device = nwb.create_device(name="MEFDevice", description="MEF recording system")
        group = nwb.create_electrode_group(
            name="MEFElectrodes",
            description="Channels from MEF recording",
            location="Unknown",
            device=device,
        )

        nwb.add_electrode_column(name="channel_name", description="Original channel name")
        for name in self._reader.channel_names:
            nwb.add_electrode(
                x=0.0, y=0.0, z=0.0,
                imp=np.nan,
                location="Unknown",
                filtering="Unknown",
                group=group,
                channel_name=name,
            )

        electrodes = nwb.create_electrode_table_region(
            region=list(range(self._reader.num_channels)),
            description="All electrodes",
        )

        series = self._create_electrical_series(electrodes)
        nwb.add_acquisition(series)

        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        with NWBHDF5IO(str(self._output_path), mode="w") as io:
            io.write(nwb)

        size_mb = self._output_path.stat().st_size / (1024 * 1024)
        log.info("Wrote %s (%.1f MB)", self._output_path, size_mb)
        return self._output_path

    def _create_nwb_file(self) -> NWBFile:
        return NWBFile(
            session_description="MEF timeseries data",
            identifier=f"mef_{uuid.uuid4().hex[:8]}",
            session_start_time=self._reader.session_start_time,
            experiment_description="Converted from MEF format",
        )

    def _create_electrical_series(self, electrodes) -> ElectricalSeries:
        data = _ChunkedDataIterator(self._reader, self._chunk_samples)

        if self._reader.has_gaps():
            log.info("Using explicit timestamps (gaps detected)")
            return ElectricalSeries(
                name="ElectricalSeries",
                description="MEF timeseries data",
                data=data,
                electrodes=electrodes,
                timestamps=self._reader.get_timestamps_seconds(),
                conversion=1.0,
                offset=0.0,
            )
        else:
            log.info("Using constant rate (continuous)")
            return ElectricalSeries(
                name="ElectricalSeries",
                description="MEF timeseries data",
                data=data,
                electrodes=electrodes,
                rate=self._reader.sampling_rate,
                starting_time=0.0,
                conversion=1.0,
                offset=0.0,
            )
