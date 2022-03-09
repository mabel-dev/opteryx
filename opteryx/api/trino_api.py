from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse
import os
import uvicorn

trino = FastAPI()


@trino.post("/v1/statement", response_class=ORJSONResponse)
async def read_main(request: Request):
    print(request)
    return {
        "id": 1,
        "stats": {"byes_read": 2000},
        "infoUri": "",
        "columns": [
            {"name": "fruit", "type": "varchar"},
            {"name": "color", "type": "varchar"},
        ],
        "data": [
            ["Apple", "green"],
            ["Banana", "yellow"],
            ["Orange", "orange"],
            ["Strawberry", "red"],
        ],
    }


# tell the server to start
if __name__ == "__main__":
    uvicorn.run(
        "trino_api:trino",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
        lifespan="on",
    )
