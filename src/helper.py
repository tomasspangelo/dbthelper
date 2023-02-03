from .client import BigQueryClient
from .utils import get_fields


class DBTHelper:
    def __init__(self, client: BigQueryClient):
        self.client = client

    def _get_fields(self, project_id: str, dataset: str, table: str, json_field: str):

        source = f"{project_id}.{dataset}.{table}"
        print(source)
        query = f"SELECT * FROM `{source}` LIMIT 1"
        data = self.client.load_gbq(query)

        return get_fields(data[json_field][0])

    def normalize(self, project_id: str, dataset: str, table: str, json_field: str, prefix: str = ''):
        fields = self._get_fields(project_id, dataset, table, json_field)

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

    def unnest(self, project_id: str, dataset: str, table: str, unnest_field: str, postfix: str = '_latest'):
        schema_query = f"SELECT * FROM `{project_id}`.{dataset}.INFORMATION_SCHEMA.COLUMNS WHERE table_name='{table}'"
        schema = self.client.load_gbq(schema_query)
        fields = set([record["column_name"] for record in schema.to_records(
        ) if record["column_name"] != unnest_field])
        print(schema)
        print(fields)

        dbt_query = """WITH\n\tt AS (\n\t\tSELECT"""
        for i, field in enumerate(fields):
            dbt_query = dbt_query + f"\n\t\t\t{field}"
            dbt_query = dbt_query + ","

        fields = self._get_fields(project_id, dataset, table, unnest_field)
        for i, field in enumerate(fields):
            dbt_query = dbt_query + \
                f"\n\t\t\tJSON_QUERY(credit, '$.{field}') AS {field}"
            if i < len(fields) - 1:
                dbt_query = dbt_query + ","

        dbt_query = dbt_query + \
            f"\n\t\tFROM {{{{ ref('{table}') }}}},\n\t\tUNNEST(JSON_EXTRACT_ARRAY({unnest_field})) as credit)"
        dbt_query = dbt_query + "\nSELECT * FROM t"

        name = table[:-len(postfix)] + "_unnested"
        with open(f"{name}.sql", "w") as f:
            f.write(dbt_query)

        print(dbt_query)
