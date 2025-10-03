from fastmcp import FastMCP
from fastmcp.server.proxy import ProxyClient
from starlette.responses import JSONResponse
from starlette.requests import Request
import logging
import os 
def main():

    proxy = FastMCP.as_proxy(ProxyClient("./domo_mcp/server.py"))

    logging.info("RUNNING THE PROXY SERVER")

    # Add a custom route to the proxy's FastAPI app
    @proxy.custom_route("/health",methods=['GET'])
    async def health(request: Request) -> JSONResponse:

        logging.info(f"{dict(os.environ)}")
        return JSONResponse({"message": "Hello from custom route!"})
    
    proxy.settings.host= "0.0.0.0"
    proxy.settings.client_init_timeout=60
    
    proxy.run(transport='streamable-http',log_level='debug')

if __name__ == "__main__":
    main()