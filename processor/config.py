import os
import uuid
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

        self.CHUNK_SIZE_MB        = int(os.getenv('CHUNK_SIZE_MB', '1'))

        # continue to use INTEGRATION_ID environment variable until runner
        # has been converted to use  a different variable to represent the workflow instance ID
        self.WORKFLOW_INSTANCE_ID = os.getenv('INTEGRATION_ID', str(uuid.uuid4()))

        self.API_KEY              = os.getenv('PENNSIEVE_API_KEY')
        self.API_SECRET           = os.getenv('PENNSIEVE_API_SECRET')
        self.API_HOST             = os.getenv('PENNSIEVE_API_HOST', 'https://api.pennsieve.net')
        self.API_HOST2            = os.getenv('PENNSIEVE_API_HOST2', 'https://api2.pennsieve.net')

        self.IMPORTER_ENABLED     = getboolenv("IMPORTER_ENABLED", self.ENVIRONMENT != 'local')

def getboolenv(key, default=False):
    return os.getenv(key, str(default)).lower() in ('true', '1')
