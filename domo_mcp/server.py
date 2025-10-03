import logging
import sys
from domo import Domo
from fastmcp import FastMCP, Context
import os
import requests

class DomoClient:
    def __init__(self, logger: logging.Logger):
        """Initialize the DomoClient with environment variables and constants."""
        self.DOMO_HOST = os.getenv("DOMO_HOST")
        self.DOMO_DEVELOPER_TOKEN = os.getenv("DOMO_DEVELOPER_TOKEN")
        self.DOMO_CLIENT_ID = os.getenv("DOMO_CLIENT_ID")
        self.DOMO_CLIENT_SECRET = os.getenv("DOMO_CLIENT_SECRET")
        self.DOMO_API_BASE = f"https://{self.DOMO_HOST}/api"
        self.logger = logger

        self.logger.info("DomoClient initialized with provided credentials.")
        self.logger.info(f"DOMO_HOST: {self.DOMO_HOST}")
        self.logger.info(f"DOMO_CLIENT_ID: {self.DOMO_CLIENT_ID}")
        self.logger.info(f"DOMO_CLIENT_SECRET: {self.DOMO_CLIENT_SECRET}")
        self.domo = Domo(client_id=self.DOMO_CLIENT_ID, client_secret=self.DOMO_CLIENT_SECRET, api_host='api.domo.com')

    async def make_request(
        self, url: str, method: str, data: dict = None
    ) -> dict[str, any] | None:
        """Make a request to the Domo API with proper error handling."""
        headers = {
            "X-DOMO-Developer-Token": self.DOMO_DEVELOPER_TOKEN,
            "Accept": "application/json",
        }

        full_url = f"{self.DOMO_API_BASE}{url}"

        try:
            if method.upper() == "GET":
                response = requests.get(full_url, headers=headers)
            elif method.upper() == "POST":
                response = requests.post(full_url, headers=headers, json=data)
            elif method.upper() == "DELETE":
                response = requests.delete(full_url, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP request failed: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return None
        
    async def list_datasets(self) -> str:
        """List all datasets in the Domo instance."""
        try:

            result = self.domo.ds_list().to_dict(orient='records')

            self.logger.info("Type of result: {}".format(type(result)))

            return result
        
        except Exception as e:
            self.logger.error(f"Error fetching dataset list: {str(e)}")
            return f"Error fetching dataset list: {str(e)}"
        
    async def get_dataset_metadata(self, dataset_id: str) -> str:
        """Get metadata for a Domo dataset."""
        try:
            url = f"/data/v3/datasources/{dataset_id}?part=core"
            data = await self.make_request(url, "GET")

            if not data:
                self.logger.warning("No data returned for dataset metadata.")
                return "Unable to fetch dataset metadata."

            return data
        except Exception as e:
            self.logger.error(f"Error fetching dataset metadata: {str(e)}")
            return f"Error fetching dataset metadata: {str(e)}"

    async def get_dataset_schema(self, dataset_id: str) -> str:
        """Get the schema of a Domo dataset."""
        try:
            url = f"/data/v2/datasources/{dataset_id}/schemas/latest"
            data = await self.make_request(url, "GET")

            if not data:
                self.logger.warning("No data returned for dataset schema.")
                return "Unable to fetch dataset schema."

            return data
        except Exception as e:
            self.logger.error(f"Error fetching dataset schema: {str(e)}")
            return f"Error fetching dataset schema: {str(e)}"

    async def query_dataset(self, dataset_id: str, sql: str) -> str:
        """Query a Domo dataset using SQL."""
        try:
            url = f"/query/v1/execute/{dataset_id}"
            data = await self.make_request(url, "POST", data={"sql": sql})

            if not data:
                self.logger.warning("No data returned for dataset query.")
                return "Unable to execute query on the dataset."

            return data
        except Exception as e:
            self.logger.error(f"Error executing query on dataset: {str(e)}")
            return f"Error executing query on dataset: {str(e)}"

    async def search_datasets(self, query: str) -> str:
        """Search for datasets in a Domo instance by name."""
        try:
            url = "/data/ui/v3/datasources/search"
            payload = {
                "entities": ["DATASET"],
                "filters": [
                    {
                        "field": "name_sort",
                        "filterType": "wildcard",
                        "query": f"*{query}*",
                    }
                ],
                "combineResults": True,
                "query": "*",
                "count": 1,
                "offset": 0,
                "sort": {
                    "isRelevance": False,
                    "fieldSorts": [{"field": "create_date", "sortOrder": "DESC"}],
                },
            }
            data = await self.make_request(url, "POST", data=payload)

            if not data:
                self.logger.warning("No data returned for dataset search.")
                return "Unable to search datasets."

            datasets = [
                {"id": ds["id"], "name": ds["name"]}
                for ds in data.get("dataSources", [])
            ]
            return datasets
        except Exception as e:
            self.logger.error(f"Error searching datasets: {str(e)}")
            return f"Error searching datasets: {str(e)}"

    async def list_roles(self) -> str:
        """List all roles in the Domo instance."""
        try:
            url = "/authorization/v1/roles"
            data = await self.make_request(url, "GET")

            if not data:
                self.logger.warning("No data returned for role list.")
                return "Unable to fetch role list."

            return data
        except Exception as e:
            self.logger.error(f"Error fetching role list: {str(e)}")
            return f"Error fetching role list: {str(e)}"

    async def create_role(self, role_data: dict) -> str:
        """Create a new role in the Domo instance."""
        try:
            url = "/authorization/v1/roles"
            data = await self.make_request(url, "POST", data=role_data)

            if not data:
                self.logger.warning("No data returned for role creation.")
                return "Unable to create role."

            return data
        except Exception as e:
            self.logger.error(f"Error creating role: {str(e)}")
            return f"Error creating role: {str(e)}"

    async def list_role_authorities(self, role_id: str) -> str:
        """List all authorities for a given role."""
        try:
            url = f"/authorization/v1/roles/{role_id}/authorities"
            data = await self.make_request(url, "GET")

            if not data:
                self.logger.warning("No data returned for role authorities.")
                return "Unable to fetch role authorities."

            return data
        except Exception as e:
            self.logger.error(f"Error fetching role authorities: {str(e)}")
            return f"Error fetching role authorities: {str(e)}"


def setup_logger():
    class Logger:
        def info(self, message):
            print(f"[INFO] {message}", file=sys.stderr)

        def warning(self, message):
            print(f"[WARNING] {message}", file=sys.stderr)

        def error(self, message):
            print(f"[ERROR] {message}", file=sys.stderr)

    return Logger()

logger = setup_logger()
domo_client = DomoClient(logger)
server = FastMCP("domo-mcp")


@server.tool()
async def GetDatasetSchema(dataset_id: str, ctx: Context):
    """Get a Domo dataset schema.
    Args: dataset_id: The ID of the dataset to get the schema for.
    """
    search_results = await domo_client.get_dataset_schema(dataset_id=dataset_id)
    logger.info("Schema fetched successfully.")
    await ctx.report_progress(progress=100, message="Schema fetched successfully")
    return search_results

@server.tool()
async def ListDatasets(ctx: Context):
    """List all datasets in the Domo instance."""
    datasets = await domo_client.list_datasets()
    logger.info("Datasets listed successfully.")
    await ctx.report_progress(progress=100, message="Datasets listed successfully")
    return {"datasets": datasets}

@server.tool()
async def GetDatasetMetadata(dataset_id: str, ctx: Context) -> str:
    """Get metadata for a Domo dataset.
    Args: dataset_id: The ID of the dataset to get metadata for.
    """
    metadata = await domo_client.get_dataset_metadata(dataset_id=dataset_id)
    logger.info("Metadata fetched successfully.")
    await ctx.report_progress(progress=100, message="Metadata fetched successfully")
    return metadata

@server.tool()
async def QueryDataset(dataset_id: str, sql: str, ctx: Context) -> str:
    """Query a Domo dataset using SQL.
    Args:
        dataset_id: The ID of the dataset to query.
        sql: The SQL query to execute on the dataset.
    """
    query_results = await domo_client.query_dataset(dataset_id=dataset_id, sql=sql)
    logger.info("Query executed successfully.")
    await ctx.report_progress(progress=100, message="Query executed successfully")
    return query_results

@server.tool()
async def SearchDatasets(query: str, ctx: Context) -> str:
    """Search for datasets in a Domo instance by name.
    Args: query: The search query to find datasets by name.
    """
    search_results = await domo_client.search_datasets(query=query)
    logger.info("Datasets searched successfully.")
    await ctx.report_progress(progress=100, message="Datasets searched successfully")
    return search_results

@server.tool()
async def ListRoles(ctx: Context):
    """List all roles in the Domo instance."""
    roles = await domo_client.list_roles()
    logger.info("Roles listed successfully.")
    await ctx.report_progress(progress=100, message="Roles listed successfully")
    return roles

@server.tool()
async def CreateRole(role_data: dict, ctx: Context) -> str:
    """Create a new role in the Domo instance.
    Args: role_data: The data for the role to create.
    """
    created_role = await domo_client.create_role(role_data=role_data)
    logger.info("Role created successfully.")
    await ctx.report_progress(progress=100, message="Role created successfully")
    return created_role

@server.tool()
async def ListRoleAuthorities(role_id: str, ctx: Context) -> str:
    """List authorities for a specific role in the Domo instance.
    Args: role_id: The ID of the role to list authorities for.
    """
    result = await domo_client.list_role_authorities(role_id=role_id)
    logger.info("Authorities listed successfully.")
    await ctx.report_progress(progress=100, message="Authorities listed successfully")
    return result



def main():
    """Run the server."""
    server.settings.host="0.0.0.0"
    server.settings.port=8001

    server.run()
    # asyncio.run(mcp.run_streamable_http_async(log_level="debug"))


if __name__ == "__main__":
    main()
