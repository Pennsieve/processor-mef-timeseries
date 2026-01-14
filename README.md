# MEF to NWB Converter

Converts Mayo Clinic MEF (Multiscale Electrophysiology Format) files to NWB (Neurodata Without Borders) format.

## Overview

This processor reads MEF 3.0 files via a Java streaming library and produces a single NWB 2.x file containing all channels as an `ElectricalSeries`. The output is compatible with downstream analysis pipelines such as `processor-post-timeseries`.

### Features

- Memory-efficient streaming: processes large recordings without loading entire datasets into RAM
- Gap detection: automatically uses explicit timestamps when discontinuities are present
- Multi-channel support: aggregates all channels into a single NWB file with proper electrode metadata

## Requirements

- Docker and Docker Compose
- MEF 3.0 input files

## Project Structure

```
processor-mef-timeseries/
├── processor/
│   ├── main.py                 # Entry point
│   ├── config.py               # Environment configuration
│   ├── mef_streamer.py         # Java subprocess streaming
│   ├── multi_channel_reader.py # Channel data access
│   └── nwb_writer.py           # NWB file generation
├── data/
│   ├── input/                  # Place MEF files here
│   └── output/                 # NWB output written here
├── Dockerfile
├── docker-compose.yml
├── dev.env
└── Makefile
```

## Data Setup

1. Create the input directory:
   ```bash
   mkdir -p data/input
   ```

2. Copy your MEF session folder into `data/input/`. The structure should look like:
   ```
   data/input/
   └── your_session.mefd/
       ├── your_session.timd/
       │   ├── channel1.tdat/
       │   ├── channel2.tdat/
       │   └── ...
       └── ...
   ```

3. Create the output directory:
   ```bash
   mkdir -p data/output
   ```

## Running

### Using Make (recommended)

```bash
make run
```

This will:
1. Build the Docker image (includes Java MEF streamer)
2. Mount `data/input` and `data/output` volumes
3. Run the conversion
4. Output the NWB file to `data/output/output.nwb`

### Using Docker Compose directly

```bash
docker-compose build
docker-compose up
```

### Clean output

```bash
make clean
```

## Configuration

Environment variables can be set in `dev.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUT_DIR` | `/data/input` | Directory containing MEF files |
| `OUTPUT_DIR` | `/data/output` | Directory for NWB output |
| `OUTPUT_FILENAME` | `output.nwb` | Name of the output file |
| `STREAM_FROM_JAR` | `true` | Whether to run the Java MEF streamer |
| `JAVA_CMD` | (see dev.env) | Command to launch MEF streamer |

## Output Format

The converter produces an NWB 2.x file with:

- **Device**: `MEFDevice`
- **ElectrodeGroup**: `MEFElectrodes` containing all channels
- **ElectricalSeries**: Sample data with either:
  - Constant `rate` (for continuous recordings)
  - Explicit `timestamps` (when gaps are detected)

Each electrode includes a `channel_name` column preserving the original MEF channel names.

## How It Works

1. **Streaming**: The Java `mefstreamer.jar` reads MEF files and emits a binary frame protocol to stdout
2. **Staging**: Python receives frames and writes intermediate `.bin` (samples) and `.json` (metadata) files
3. **Reading**: `MultiChannelReader` loads metadata and provides memory-mapped access to sample data
4. **Writing**: `NWBWriter` constructs the NWB file using chunked iteration to minimize memory usage

## Troubleshooting

### Out of memory errors

The processor is designed for large files, but if you encounter OOM errors:
- Ensure Docker has sufficient memory allocated (recommended: 8GB+)
- Check that no other memory-intensive processes are running

### No channels found

- Verify MEF files are in the correct location (`data/input/`)
- Check that the session folder has the `.mefd` extension
- Ensure the MEF files are valid MEF 3.0 format

### Java process hangs

- Check Docker logs for Java stderr output
- Verify the MEF files are not corrupted
- Try with a smaller test dataset first

## License

See LICENSE file for details.
