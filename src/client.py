from dotenv import find_dotenv, load_dotenv
from dotenv import find_dotenv, load_dotenv

class BigQueryClient:
    def __init__(self, project_id, credentials=None):
        self.project_id = project_id
        self.credentials = credentials
        self._client = None