import os
from typing import Any, Dict, Literal, Optional, Union
import pandas_gbq
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from google.oauth2 import service_account
from google.cloud.bigquery.table import Table, TableReference
from google.cloud import bigquery
from google.cloud.bigquery.job.load import LoadJobConfig
import warnings

load_dotenv(find_dotenv())


class BigQueryClient:
    def __init__(self):
        self.info = {
            "type": os.getenv("GCP_TYPE"),
            "project_id": os.getenv("GCP_PROJECT_ID"),
            "private_key_id": os.getenv("GCP_PRIVATE_KEY_ID"),
            "private_key": os.getenv("GCP_PRIVATE_KEY"),
            "client_email": os.getenv("GCP_CLIENT_EMAIL"),
            "client_id": os.getenv("GCP_CLIENT_ID"),
            "auth_uri": os.getenv("GCP_AUTH_URI"),
            "token_uri": os.getenv("GCP_TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("GCP_AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("GCP_CLIENT_X509_CERT_URL"),
        }
        gcp_credentials = service_account.Credentials.from_service_account_info(
            self.info)
        self.gcp_credentials = gcp_credentials.with_scopes(
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )

    def load_gbq(self, query: str) -> pd.DataFrame:
        return pandas_gbq.read_gbq(query,
                                   project_id=self.gcp_credentials.project_id, credentials=self.gcp_credentials)

    def to_gbq(self,
               df: pd.DataFrame,
               destination: Union[Table, TableReference, str],
               if_exists: Literal['fail', 'append', 'replace'] = 'fail',
               project: Optional[str] = None,
               credentials: Optional[Any] = None,
               location: Optional[str] = 'EU',
               client_kwargs: Optional[Dict[str, Any]] = None,
               job_config: LoadJobConfig = None,
               job_kwargs: Optional[Dict[str, Any]] = None):

        _modes = {"fail": "WRITE_EMPTY",
                  "append": "WRITE_APPEND", "replace": "WRITE_TRUNCATE"}

        # Prepare default configurations
        default_credentials = self.gcp_credentials or None
        default_job_config = bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition=_modes.get(if_exists),
        )
        if if_exists == "append":
            default_job_config.schema_update_options = [
                "ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]

        # Create client with fallback to default credentials and upload dataframe
        client = bigquery.Client(
            credentials=credentials or default_credentials, **(client_kwargs or {}))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            job = client.load_table_from_dataframe(dataframe=df,
                                                   destination=destination,
                                                   project=project,
                                                   location=location,
                                                   job_config=job_config or default_job_config,
                                                   **(job_kwargs or {}))
            return job.result()
