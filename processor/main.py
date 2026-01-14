#!/usr/bin/env python3
"""
MEF to NWB Converter

Converts Mayo Clinic MEF (Multiscale Electrophysiology Format) files to
NWB (Neurodata Without Borders) format for downstream analysis pipelines.
"""
import logging
import shlex
import subprocess
from pathlib import Path

from config import Config
from processor.mef_streamer import stage_from_stream
from processor.multi_channel_reader import MultiChannelReader
from processor.nwb_writer import NWBWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


def main():
    config = Config()

    input_dir = Path(config.INPUT_DIR).resolve()
    output_dir = Path(config.OUTPUT_DIR).resolve()
    staging_dir = output_dir / "staging"

    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    log.info("Input: %s", input_dir)
    log.info("Output: %s", output_dir)
    subprocess.run(["ls", "-lh", input_dir])

    if config.STREAM_FROM_JAR:
        stage_from_stream(shlex.split(config.JAVA_CMD), staging_dir)

    reader = MultiChannelReader.from_staged_dir(staging_dir)
    log.info(
        "Loaded %d channels, %d samples @ %.2f Hz",
        reader.num_channels,
        reader.num_samples,
        reader.sampling_rate,
    )

    output_path = output_dir / config.OUTPUT_FILENAME
    NWBWriter(reader, output_path).write()

    log.info("Conversion complete: %s", output_path)


if __name__ == "__main__":
    main()
