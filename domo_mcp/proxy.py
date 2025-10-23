from fastmcp import FastMCP
from fastmcp.server.proxy import ProxyClient
from starlette.responses import JSONResponse
from starlette.requests import Request
import logging
import os

from server import tix_domo 
def main():

    proxy = FastMCP.as_proxy(ProxyClient("./domo_mcp/server.py"))

    logging.info("RUNNING THE PROXY SERVER")

    # Add a custom route to the proxy's FastAPI app
    @proxy.custom_route("/health",methods=['GET'])
    async def health(request: Request) -> JSONResponse:

        logging.info(f"{dict(os.environ)}")
        return JSONResponse({"message": "Hello from custom route!"})
    
    @proxy.custom_route("/tix/aov",methods=['POST'])
    async def tix(request: Request) -> JSONResponse:
        data = await request.json()
        attempts = 5
        status = "uninitialized"

        while status !='COMPLETED' and attempts> 0:

            response = await tix_domo(data, TriggerId="82833c7a-fcce-4987-88aa-96d54a086c74")

            status = response.get('status','uninitialized')
            attempts -= 1

        if status == 'COMPLETED':
            filtered = [item for item in response['messages'] if item.get("id", "").startswith("result__")]

            return JSONResponse({"status": "completed", "data": filtered})

        logging.info(f"Received data: {data}")

        return JSONResponse({"status": response.get('status', 'unknown'), "data": response})
    
    @proxy.custom_route("/tix/rawdata",methods=['POST'])
    async def tix_rawdata(request: Request) -> JSONResponse:
        data = await request.json()
        attempts = 5
        status = "uninitialized"

        while status !='COMPLETED' and attempts> 0:

            response = await tix_domo(data, TriggerId="7e5718cc-40d5-423b-bc33-4eaacc308a4a")

            status = response.get('status','uninitialized')
            attempts -= 1

        if status == 'COMPLETED':
            return JSONResponse({"status": "completed", "data": response['messages']})

        logging.info(f"Received data: {data}")

        return JSONResponse({"status": response.get('status', 'unknown'), "data": response})
    
    proxy.settings.host= "0.0.0.0"
    proxy.settings.client_init_timeout=60
    
    proxy.run(transport='streamable-http',log_level='debug')

if __name__ == "__main__":
    main()