"""Runtime configuration from environment variables."""
import os


class Config:
    INPUT_DIR: str
    OUTPUT_DIR: str
    OUTPUT_FILENAME: str
    STREAM_FROM_JAR: bool
    JAVA_CMD: str

    def __init__(self):
        self.INPUT_DIR = os.getenv("INPUT_DIR", "/data/input")
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/data/output")
        self.OUTPUT_FILENAME = os.getenv("OUTPUT_FILENAME", "output.nwb")
        self.STREAM_FROM_JAR = os.getenv("STREAM_FROM_JAR", "true").lower() == "true"
        self.JAVA_CMD = os.getenv(
            "JAVA_CMD",
            f"/opt/java/openjdk/bin/java -jar /processor/mefstreamer.jar {self.INPUT_DIR}",
        )
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
