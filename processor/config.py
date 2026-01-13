import os
import multiprocessing

class Config:
    def __init__(self):
        self.ENVIRONMENT          = os.getenv('ENVIRONMENT', 'local')
        self.STREAM_FROM_JAR      = os.getenv('STREAM_FROM_JAR', True)
        self.JAVA_CMD             = os.getenv('JAVA_CMD',f'/opt/java/openjdk/bin/java -jar /processor/mefstreamer.jar {os.getenv('INPUT_DIR')}')
        self.HEADER_SIZE          = 5  # MEF HEADER is 5 bytes
        self.NUM_WORKERS          = int(os.getenv('NUM_WORKERS', max(1, multiprocessing.cpu_count() // 2))) # default to half of available CPU while testing

        if self.ENVIRONMENT == 'local':
            self.INPUT_DIR            = os.getenv('INPUT_DIR')
            self.OUTPUT_DIR           = os.getenv('OUTPUT_DIR')
        else:
            self.INPUT_DIR            = os.getenv('INPUT_DIR')
            self.OUTPUT_DIR           = os.getenv('OUTPUT_DIR')
            if not os.path.exists(self.OUTPUT_DIR):
                os.makedirs(self.OUTPUT_DIR)

        # NWB output configuration
        self.NWB_OUTPUT_FILENAME  = os.getenv('NWB_OUTPUT_FILENAME', 'output.nwb')

def getboolenv(key, default=False):
    return os.getenv(key, str(default)).lower() in ('true', '1')
