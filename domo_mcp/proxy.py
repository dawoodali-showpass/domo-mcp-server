from fastmcp import FastMCP
from fastmcp.server.proxy import ProxyClient
from starlette.responses import JSONResponse
from starlette.requests import Request
import logging
import os

from server import tix_domo

async def tix_workflow(request: Request, triggerId: str) -> JSONResponse:

    data = await request.json()
    attempts = 5
    status = "uninitialized"

    while status !='COMPLETED' and attempts> 0:

        response = await tix_domo(data, TriggerId=triggerId)

        status = response.get('status','uninitialized')
        attempts -= 1

    if status == 'COMPLETED':
        filtered = [item for item in response['messages'] if item.get("id", "").startswith("result__")]

        return JSONResponse({"status": "completed", "data": filtered})
    
    logging.info(f"Received data: {data}")
    
    return JSONResponse({"status": response.get('status', 'unknown'), "data": response})

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
        return await tix_workflow(request, triggerId="2942b06e-cf4a-4d84-9d57-46735dd5068e")
    
    
    @proxy.custom_route("/tix/rawdata",methods=['POST'])
    async def tix_rawdata(request: Request) -> JSONResponse:
        return await tix_workflow(request, triggerId="294469f5-1c4d-44ae-a96f-5f3d01b3a9c1")
    
    @proxy.custom_route("/tix/tsv",methods=['POST'])
    async def tix_rawdata(request: Request) -> JSONResponse:
        return await tix_workflow(request, triggerId="bf4556d4-2f7c-4cde-b951-83c9fa52a80e")
    
    @proxy.custom_route("/tix/rcf",methods=['POST'])
    async def tix_rcf(request: Request) -> JSONResponse:
        return await tix_workflow(request, triggerId="cf668421-0a21-4e60-bd64-e1fe608e33bf")

    
    proxy.settings.host= "0.0.0.0"
    proxy.settings.client_init_timeout=60
    
    proxy.run(transport='streamable-http',log_level='debug')

if __name__ == "__main__":
    main()