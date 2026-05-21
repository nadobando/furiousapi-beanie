from contextlib import asynccontextmanager
from typing import AsyncGenerator

import beanie.odm.utils.projection
from beanie import init_beanie
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from furiousapi.api.exception_handling import furious_api_exception_handler, furious_db_exception_handler
from furiousapi.api.exceptions import FuriousAPIError
from furiousapi.db.exceptions import FuriousEntityError

from example.controllers import ItemController, ReviewController
from example.dependencies import client
from furiousapi.beanie.utils import gather_documents
from furiousapi.beanie.utils import get_projection

documents = gather_documents("example.models")

beanie.odm.utils.projection.get_projection = get_projection


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    await init_beanie(database=client.db_name, document_models=documents)
    yield


app = FastAPI(lifespan=lifespan)
app.add_exception_handler(FuriousEntityError, furious_db_exception_handler)
app.add_exception_handler(FuriousAPIError, furious_api_exception_handler)

app.include_router(ItemController.api_router)
app.include_router(ReviewController.api_router)
app.get("/", include_in_schema=False)(lambda: RedirectResponse("/redoc"))

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8083)
