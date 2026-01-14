# processor/nwb_writer.py
"""
NWB file writer for MEF data.
Constructs NWB files compatible with processor-post-timeseries.
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


class MEFDataChunkIterator(GenericDataChunkIterator):
    """
    Custom chunk iterator for MEF data that reads from MultiChannelReader.
    Provides memory-efficient streaming of data to NWB file.
    """

    def __init__(self, reader: MultiChannelReader, chunk_size: int = 100_000):
        self.reader = reader
        self._chunk_size = chunk_size
        self._num_samples = reader.num_samples
        self._num_channels = reader.num_channels

        # Calculate buffer shape (chunk of samples x all channels)
        buffer_samples = min(chunk_size, self._num_samples)

        super().__init__(
            buffer_shape=(buffer_samples, self._num_channels),
            chunk_shape=(buffer_samples, self._num_channels),
            display_progress=True,
        )

        log.info("MEFDataChunkIterator initialized:")
        log.info("  Total samples: %d", self._num_samples)
        log.info("  Channels: %d", self._num_channels)
        log.info("  Chunk size: %d samples", buffer_samples)

    def _get_data(self, selection: Tuple[slice, ...]) -> np.ndarray:
        """Read data for the given selection."""
        start = selection[0].start
        stop = selection[0].stop
        return self.reader.read_all_channels(start, stop).astype(np.float64)

    def _get_maxshape(self) -> Tuple[int, int]:
        """Return the maximum shape of the data."""
        return (self._num_samples, self._num_channels)

    def _get_dtype(self) -> np.dtype:
        """Return the data type."""
        return np.dtype('float64')


class MEFtoNWBWriter:
    """
    Writes MEF data to NWB format compatible with processor-post-timeseries.

    Creates an NWB file with:
    - ElectricalSeries in acquisition
    - Electrode table with channel_name column
    - Proper device and electrode group
    """

    def __init__(
        self,
        reader: MultiChannelReader,
        output_path: Path,
        chunk_size: int = 100_000,
    ):
        """
        Initialize the NWB writer.

        Args:
            reader: MultiChannelReader with loaded channel data
            output_path: Path to write the NWB file
            chunk_size: Number of samples to read/write at a time (for memory efficiency)
        """
        self.reader = reader
        self.output_path = Path(output_path)
        self.chunk_size = chunk_size

    def write(self) -> Path:
        """
        Write the NWB file.

        Returns:
            Path to the created NWB file.
        """
        log.info("Creating NWB file: %s", self.output_path)
        log.info("  Channels: %d", self.reader.num_channels)
        log.info("  Samples: %d", self.reader.num_samples)
        log.info("  Rate: %.2f Hz", self.reader.sampling_rate)
        log.info("  Has gaps: %s", self.reader.has_gaps())

        # Create NWB file structure
        nwbfile = self._create_nwb_file()

        # Create device and electrode group
        device = self._create_device(nwbfile)
        electrode_group = self._create_electrode_group(nwbfile, device)

        # Add electrodes to table
        self._add_electrodes(nwbfile, electrode_group)

        # Create electrode table region
        electrode_region = nwbfile.create_electrode_table_region(
            region=list(range(self.reader.num_channels)),
            description="All MEF electrodes"
        )

        # Create ElectricalSeries with data
        electrical_series = self._create_electrical_series(electrode_region)
        nwbfile.add_acquisition(electrical_series)

        # Write to file
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        log.info("Writing NWB file to: %s", self.output_path)

        with NWBHDF5IO(str(self.output_path), mode="w") as io:
            io.write(nwbfile)

        file_size_mb = self.output_path.stat().st_size / (1024 * 1024)
        log.info("NWB file written successfully: %.2f MB", file_size_mb)

        return self.output_path

    def _create_nwb_file(self) -> NWBFile:
        """Create the base NWB file with metadata."""
        identifier = f"mef_nwb_{uuid.uuid4().hex[:8]}"

        nwbfile = NWBFile(
            session_description="MEF converted timeseries data",
            identifier=identifier,
            session_start_time=self.reader.session_start_time,
            experimenter=None,
            lab=None,
            institution=None,
            experiment_description="Timeseries data converted from MEF format",
        )

        log.info("Created NWBFile: %s", identifier)
        log.info("  Session start: %s", self.reader.session_start_time.isoformat())

        return nwbfile

    def _create_device(self, nwbfile: NWBFile):
        """Create the recording device."""
        device = nwbfile.create_device(
            name="MEFDevice",
            description="MEF recording device",
            manufacturer="Unknown",
        )
        return device

    def _create_electrode_group(self, nwbfile: NWBFile, device):
        """Create the electrode group."""
        electrode_group = nwbfile.create_electrode_group(
            name="MEFElectrodeGroup",
            description="Electrodes from MEF recording",
            location="Unknown",
            device=device,
        )
        return electrode_group

    def _add_electrodes(self, nwbfile: NWBFile, electrode_group) -> None:
        """Add electrodes to the electrode table."""
        nwbfile.add_electrode_column(
            name="channel_name",
            description="Channel name from MEF recording"
        )

        for name in self.reader.channel_names:
            nwbfile.add_electrode(
                x=0.0, y=0.0, z=0.0,
                imp=np.nan,
                location="Unknown",
                filtering="Unknown",
                group=electrode_group,
                channel_name=name,
            )

        log.info("Added %d electrodes", self.reader.num_channels)

    def _create_electrical_series(self, electrode_region) -> ElectricalSeries:
        """Create the ElectricalSeries with data."""
        num_samples = self.reader.num_samples
        num_channels = self.reader.num_channels

        log.info("Creating ElectricalSeries with MEFDataChunkIterator")
        log.info("  Shape: (%d, %d)", num_samples, num_channels)
        log.info("  Chunk size: %d samples", self.chunk_size)

        # Create custom chunk iterator
        data_iterator = MEFDataChunkIterator(self.reader, self.chunk_size)

        # Decide whether to use rate or timestamps based on gaps
        if self.reader.has_gaps():
            log.info("Using explicit timestamps (gaps detected)")
            timestamps = self.reader.get_timestamps_seconds()

            electrical_series = ElectricalSeries(
                name="MEFElectricalSeries",
                description="Timeseries data converted from MEF format",
                data=data_iterator,
                electrodes=electrode_region,
                timestamps=timestamps,
                conversion=1.0,  # Data is in raw counts
                offset=0.0,
            )
        else:
            log.info("Using constant rate (no gaps)")

            electrical_series = ElectricalSeries(
                name="MEFElectricalSeries",
                description="Timeseries data converted from MEF format",
                data=data_iterator,
                electrodes=electrode_region,
                rate=self.reader.sampling_rate,
                conversion=1.0,  # Data is in raw counts
                offset=0.0,
                starting_time=0.0,
            )

        return electrical_series
