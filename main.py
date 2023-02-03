from src.client import BigQueryClient
from src.helper import DBTHelper


def demo_normalization():
    _client = BigQueryClient()
    helper = DBTHelper(_client)
    project_id = "squeeze-359607"
    dataset = "raw"
    table = "_airbyte_raw_waitwhile_resources"
    DBTHelper.normalize(helper, project_id, dataset, table, "_airbyte_data")


def demo_unnest():
    _client = BigQueryClient()
    helper = DBTHelper(_client)
    project_id = "squeeze-359607"
    dataset = "aggregated"
    table = "mongodb_users_latest"
    DBTHelper.unnest(helper, project_id, dataset, table, "credits_log")


demo_unnest()
