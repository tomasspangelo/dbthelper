from src.client import BigQueryClient
from src.helper import DBTHelper
import sys
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--mode", help="Do the bar option")
parser.add_argument("--dataset", help="Do the bar option")
parser.add_argument("--table", help="Do the bar option")
parser.add_argument("--project", help="Do the bar option")
parser.add_argument("--argument", help="Do the bar option")

args = parser.parse_args()


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


def run():
    mode = args.mode
    client = BigQueryClient()
    helper = DBTHelper(client)
    project_id = args.project
    dataset = args.dataset
    table = args.table
    argument = args.argument

    if mode == "normalize":
        helper.normalize(project_id, dataset, table, argument)
    else:
        helper.unnest(project_id, dataset, table, argument)
