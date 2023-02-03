from .client import BigQueryClient
from .utils import get_fields


class DBTHelper:
    def __init__(self, client: BigQueryClient):
        self.client = client

    def normalize(self, project_id: str, dataset: str, table: str, json_field: str, prefix: str = ''):
        source = f"{project_id}.{dataset}.{table}"
        query = f"SELECT * FROM `{source}` LIMIT 1"
        data = self.client.load_gbq(query)

        fields = get_fields(data[json_field][0])

        name = table[len(prefix):]

        dbt_query = """WITH\n\tt AS (\n\t\tSELECT"""
        for i, field in enumerate(fields):
            dbt_query = dbt_query + \
                f"\n\t\t\tJSON_QUERY({json_field}, '$.{field}') AS {field}"
            if i < len(fields) - 1:
                dbt_query = dbt_query + ","
        dbt_query = dbt_query + "\n\t\tFROM {{ source(<name>, <name>) }})"
        dbt_query = dbt_query + "\nSELECT * FROM t"
        with open(f"{name}.sql", "w") as f:
            f.write(dbt_query)

    def unnest(self, project_id: str, dataset: str, table: str):
        schema_query = f"SELECT * FROM `{project_id}`.{dataset}.INFORMATION_SCHEMA.COLUMNS WHERE table_name='{table}'"
        schema = self.client.load_gbq(schema_query)
        print(schema)
