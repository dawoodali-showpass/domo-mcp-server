import logging
import sys
from time import sleep

from dotenv import load_dotenv
from pydomo import Domo
from fastmcp import FastMCP, Context
import os
import requests
import json

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
        self, url: str, method: str, data: dict = None, nojson: bool = False
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
            if nojson:
                return response.text
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP request failed: {e}\nReturned: {response.text}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return None
    
    # async def list_events(self): str | dict[str,any]:
    #     """List all events in the Domo dataset"""

    async def list_datasets(self) -> str:
        """List all datasets in the Domo instance."""
        try:

            result = self.domo.ds_list().to_dict(orient='records')

            self.logger.info("Sample result: {}".format(result[0]))

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

    async def query_dataset(self, prompt: str) -> str | dict[str,any]:
        """Query a Domo dataset using SQL."""
        TriggerId= "57e5928e-3488-4723-9075-2fc4dd1dd092"
        try:
            url = f"/workflow/v2/triggers/57e5928e-3488-4723-9075-2fc4dd1dd092/activate"
            trigger = await self.make_request(url, "POST", data={"prompt": prompt})

            instance_id = trigger['id']
            status = trigger['status']

            self.logger.info(f"the status of the trigger is {status}")

            timeout_seconds = 120

            while status=='IN_PROGRESS' and timeout_seconds>0:
                status = await self.make_request(f"/workflow/v1/instances/{instance_id}/status", "GET", nojson=True)
                self.logger.info(f"Workflow instance {instance_id} status: {status}")
                timeout_seconds -= 1
                sleep(1)

            results_url = f"/workflow/v1/instances/transactions?instanceId={instance_id}"

            messages = await self.make_request(results_url,"GET")


            # self.logger.info(f"Workflow instance {instance_id} messages: {messages}")

            result_obj = next((item for item in messages if item.get("id") == "result"), None)

            self.logger.info(f"Result object: {result_obj}")


            data = result_obj['value']

            # data = self.domo.ds_query(dataset_id=dataset_id,query=sql)

            # self.logger.info(data)

            if not data:
                self.logger.warning("No data returned for dataset query. Returned {}".format(data))
                return "Unable to execute query on the dataset."

            return json.loads(data)
        
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
load_dotenv()
logger.warning(f"Environment variables loaded: {os.environ}")
domo_client = DomoClient(logger)
server = FastMCP("domo-mcp")


# @server.tool()
# async def GetDatasetSchema(dataset_id: str, ctx: Context):
#     """Get a Domo dataset schema.
#     Args: dataset_id: The ID of the dataset to get the schema for.
#     """
#     search_results = await domo_client.get_dataset_schema(dataset_id=dataset_id)
#     logger.info("Schema fetched successfully.")
#     await ctx.report_progress(progress=100, message="Schema fetched successfully")
#     return search_results

# @server.tool()
# async def ListDatasets(ctx: Context):
#     """List all datasets in the Domo instance."""
    
#     datasets = await domo_client.list_datasets()
    
#     logger.info("Datasets listed successfully.")

#     filtered_datasets = []

#     count = 0

    # for dataset in datasets:

    #     metadata = await domo_client.get_dataset_metadata(dataset_id=dataset['id'])

    #     await ctx.report_progress(progress=count / len(datasets), message=f"Fetched metadata for dataset {dataset['id']}")
    #     count +=1

    #     if "transportType" in metadata.keys() and metadata["transportType"] in ["DATAFLOW", "CONNECTOR"]:
    #         domo_client.logger.info(f"Included dataset {dataset['id']} of type {metadata['transportType']}")
    #         filtered_datasets.append(dataset)

    # await ctx.report_progress(progress=100, message="Datasets listed successfully")

    # return {"datasets": datasets}

# @server.tool(description="Filter the datasets to only include those of type DATAFLOW or CONNECTOR which are datasets that can be queried, limited to 10 datasets per call")
# async def FilterDatasets(datasets_ids: list[str],ctx: Context):

#     count = 1
    
#     filtered_datasets = []

#     for dataset_id in datasets_ids:

#         metadata = await domo_client.get_dataset_metadata(dataset_id=dataset_id)

#         await ctx.report_progress(progress=count / len(datasets_ids), message=f"Fetched metadata for dataset {dataset_id}")

#         if "transportType" in metadata.keys() and metadata["transportType"] in ["DATAFLOW", "CONNECTOR"]:
#             domo_client.logger.info(f"Included dataset {dataset_id} of type {metadata['transportType']}")
#             filtered_datasets.append(dataset_id)
        
#         count +=1
        
#     return filtered_datasets

# @server.tool()
# async def GetDatasetMetadata(dataset_id: str, ctx: Context) -> str | dict[str,any]:
#     """Get metadata for a Domo dataset.
#     Args: dataset_id: The ID of the dataset to get metadata for.
#     """
#     metadata = await domo_client.get_dataset_metadata(dataset_id=dataset_id)
#     logger.info("Metadata fetched successfully.")
#     await ctx.report_progress(progress=100, message="Metadata fetched successfully")
#     return {"metadata": metadata}

# @server.tool()
# async def GetEventsInDataset(dataset_id:str, ctx: Context) -> str:
#     """Get all the event names in a Domo dataset.
#     Args: dataset_id: The ID of the dataset to get events for.
#     """
#     # Placeholder implementation

#     # results = await domo_client.
#     await ctx.report_progress(progress=100, message="Events fetched successfully")
#     return {"events": results}

@server.tool()
async def SearchDomo(prompt: str, ctx: Context) -> str | dict[str,any]:
    """Search Domo using Domo Agents and Prompts.
    Args:
        prompt: The prompt to pass to Domo. This 
    """
    query_results = await domo_client.query_dataset(prompt=prompt)

    # results = await domo_client.make_request("/04db5209-1c6c-45ed-a7c9-5cfd82d1487f","GET")
    logger.info("Query executed successfully.")
    await ctx.report_progress(progress=100, message="Query executed successfully")
    return {"data": query_results}


async def tix_domo(data: dict | None, TriggerId: str ):

    # TriggerId = '82833c7a-fcce-4987-88aa-96d54a086c74'

    venue_id = data.get("venueId",0) if data else 0


    try:
        url = f"/workflow/v2/triggers/{TriggerId}/activate"
        trigger = await domo_client.make_request(url, "POST", data={"venueId": venue_id})

        instance_id = trigger['id']
        status = trigger['status']

        domo_client.logger.info(f"the status of the trigger is {status}")

        timeout_seconds = 300

        while status=='IN_PROGRESS' and timeout_seconds>0:
            status = await domo_client.make_request(f"/workflow/v1/instances/{instance_id}/status", "GET", nojson=True)
            domo_client.logger.info(f"Workflow instance {instance_id} status: {status}")
            timeout_seconds -= 1
            sleep(1)

        results_url = f"/workflow/v1/instances/transactions?instanceId={instance_id}"

        messages = await domo_client.make_request(results_url,"GET")

        domo_client.logger.info(f"Workflow instance {instance_id} messages: {messages}")
        return {"status": status, "messages": messages}
    
    except Exception as e:
        domo_client.logger.error(f"Error executing tix agent: {str(e)}")
    return True



# @server.tool()
# async def SearchDatasets(query: str, ctx: Context) -> str:
#     """Search for datasets in a Domo instance by name.
#     Args: query: The search query to find datasets by name.
#     """
#     search_results = await domo_client.search_datasets(query=query)
#     logger.info("Datasets searched successfully.")
#     await ctx.report_progress(progress=100, message="Datasets searched successfully")
#     return search_results

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
