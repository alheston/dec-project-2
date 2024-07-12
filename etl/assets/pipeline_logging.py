import logging
import time
import uuid
import os

class PipelineLogging:
    def __init__(self, pipeline_name: str, log_folder_path: str):
        self.pipeline_name = pipeline_name
        self.log_folder_path = log_folder_path
        logger = logging.getLogger(pipeline_name)
        logger.setLevel(logging.INFO)
        # if not os.path.exists(self.log_folder_path):
        #     print(f"Log directory {self.log_folder_path} does not exist. Creating it.")
        #     os.makedirs(self.log_folder_path, exist_ok=True)
        # else:
        #     print(f"Log directory {self.log_folder_path} already exists.")
        
        unique_id = uuid.uuid4()
        self.file_path = (
            f"{self.log_folder_path}/{self.pipeline_name}_{time.time()}_{unique_id}.log"
        )
        print(f"Log file path: {self.file_path}")
        file_handler = logging.FileHandler(self.file_path)
        file_handler.setLevel(logging.INFO)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        self.logger = logger

    def get_logs(self) -> str:
        with open(self.file_path, "r") as file:
            return "".join(file.readlines())
    
    def log_error(self, message):
        self.logger.error(message)

    def log_exception(self, ex):
        ex_type, ex_value, ex_traceback = ex
        self.logger.error("Exception occurred", exc_info=(ex_type, ex_value, ex_traceback))
