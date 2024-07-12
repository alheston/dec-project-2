from jinja2 import Environment, FileSystemLoader
from etl.connectors.airbyte import AirbyteClient
from dotenv import load_dotenv
import os
from etl.assets.pipeline_logging import PipelineLogging
from sqlalchemy.exc import SQLAlchemyError
import requests
import subprocess

if __name__ == "__main__":
    load_dotenv()
    AIRBYTE_USERNAME = os.environ.get("AIRBYTE_USERNAME")
    AIRBYTE_PASSWORD = os.environ.get("AIRBYTE_PASSWORD")
    AIRBYTE_SERVER_NAME = os.environ.get("AIRBYTE_SERVER_NAME")
    AIRBYTE_CONNECTION_ID = os.environ.get("AIRBYTE_CONNECTION_ID")
    DBT_WD = os.environ.get("DBT_WD")

    check_env_vars = all([
        AIRBYTE_USERNAME,
        AIRBYTE_PASSWORD,
        AIRBYTE_SERVER_NAME,
        AIRBYTE_CONNECTION_ID
    ])

    pipeline_logging = PipelineLogging(
        pipeline_name="northwind", log_folder_path="../logs"
    )

    
    assert check_env_vars

    pipeline_logging.logger.info("Creating target client")
    airbyte_client = AirbyteClient(
            server_name=AIRBYTE_SERVER_NAME,
            username=AIRBYTE_USERNAME,
            password=AIRBYTE_PASSWORD,
    )
    pipeline_logging.logger.info("running airbyte client/connection")
    if airbyte_client.valid_connection():
        try:
            job_status = airbyte_client.trigger_sync(connection_id=AIRBYTE_CONNECTION_ID)
            if job_status == "succeeded":
                pipeline_logging.logger.info("airbyte sync completed")
                pipeline_logging.logger.info("starting to run dbt commands")
                try:
                    result = subprocess.run(["dbt", "run"], check=True, capture_output=True, text=True, cwd=DBT_WD)
                    pipeline_logging.logger.info(f"dbt wd: {os.environ.get(DBT_WD)}")
                    pipeline_logging.logger.info(f"dbt run output: {result.stdout}")
                    pipeline_logging.logger.info("dbt run completed successfully")

                except subprocess.CalledProcessError as e:
                    pipeline_logging.logger.error(f"dbt run failed: {e.stderr}")
            else:
                pipeline_logging.logger.error("Airbyte sync failed")
        except Exception as e:
            pipeline_logging.logger.error(f"Airbyte sync failed: {e}")
    else:
        pipeline_logging.logger.error("Invalid Airbyte connection")


                

        


