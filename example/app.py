import beanie.odm.utils.projection
from furiousapi.beanie.utils import get_projection


import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator


import uvicorn
from beanie import init_beanie
from fastapi import FastAPI
from furiousapi.api.exception_handling import furious_api_exception_handler, furious_db_exception_handler
from furiousapi.api.exceptions import FuriousAPIError
from furiousapi.db.exceptions import FuriousEntityError

from example.controllers import ItemController, ReviewController
from example.dependencies import client
from furiousapi.beanie.utils import gather_documents

documents = gather_documents("example.models")

beanie.odm.utils.projection.get_projection = get_projection


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    await init_beanie(database=client.db_name, document_models=documents)
    yield
    # Add any cleanup code here if needed


app = FastAPI(lifespan=lifespan)
app.add_exception_handler(FuriousEntityError, furious_db_exception_handler)
app.add_exception_handler(FuriousAPIError, furious_api_exception_handler)

app.include_router(ItemController.api_router)
app.include_router(ReviewController.api_router)

# Enable DEBUG logging for pymongo.command
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("pymongo.command")
logger.propagate = True
logger.setLevel(logging.DEBUG)

if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

if __name__ == "__main__":
    uvicorn.run(app, port=8081)
