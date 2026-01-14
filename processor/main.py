import logging
import shlex
import subprocess
from pathlib import Path

from config import Config
from processor.mef_streamer import stage_from_stream
from processor.multi_channel_reader import MultiChannelReader
from processor.nwb_writer import MEFtoNWBWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("processor")


if __name__ == "__main__":
    config = Config()

    INPUT_DIR = Path(config.INPUT_DIR).resolve()
    OUTPUT_DIR = Path(config.OUTPUT_DIR).resolve()
    STAGING_DIR = OUTPUT_DIR / "staging"

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    log.info("INPUT_DIR=%s, OUTPUT_DIR=%s", INPUT_DIR, OUTPUT_DIR)
    subprocess.run(["ls", "-lh", INPUT_DIR])

    if config.STREAM_FROM_JAR:
        stage_from_stream(shlex.split(config.JAVA_CMD), STAGING_DIR)

    reader = MultiChannelReader.from_staged_dir(STAGING_DIR)
    log.info("Channels: %d, Samples: %d, Rate: %.2f Hz",
             reader.num_channels, reader.num_samples, reader.sampling_rate)
    log.info("Session start: %s", reader.session_start_time.isoformat())
    log.info("Has gaps: %s", reader.has_gaps())

    output_path = OUTPUT_DIR / config.OUTPUT_FILENAME
    writer = MEFtoNWBWriter(reader, output_path)
    writer.write()

    log.info("=" * 80)
    log.info("MEF to NWB conversion complete: %s", output_path)
    log.info("=" * 80)
