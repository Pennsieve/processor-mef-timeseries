import os


class Config:
    def __init__(self):
        self.ENVIRONMENT      = os.getenv("ENVIRONMENT", "local")

        self.OUTPUT_FILENAME  = os.getenv("OUTPUT_FILENAME", "output.nwb")
        self.INPUT_DIR        = os.getenv("INPUT_DIR")
        self.OUTPUT_DIR       = os.getenv("OUTPUT_DIR")
        if not os.path.exists(self.OUTPUT_DIR):
            os.makedirs(self.OUTPUT_DIR)

        self.STREAM_FROM_JAR  = os.getenv("STREAM_FROM_JAR", True)
        self.JAVA_CMD         = os.getenv("JAVA_CMD", f"/opt/java/openjdk/bin/java -jar /processor/mefstreamer.jar {self.INPUT_DIR}")
