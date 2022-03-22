import os
from typing import List
from services.lens_service import (
    SearchType, ResultType, get_app_ids, search_contents, get_trends, index_contents,
    MetadataSchema, search_profiles, search_publications
)
from fastapi import FastAPI, HTTPException, status
import logging
from services.es_search import es
from datetime import date
import time
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
import aioredis
import traceback


logger = logging.getLogger(__name__)

INDEXING_LIMIT = 100


def get_application() -> FastAPI:
    application = FastAPI(title="Lens Service", debug=True, version="1.0")

    @application.on_event("startup")
    async def startup_event():
        es.init_app()
        redis = await aioredis.create_redis_pool(f"redis://:{os.environ.get('REDIS_PASSWORD')}@{os.getenv('REDIS_HOST')}:{os.getenv('REDIS_PORT','6379')}/0", encoding="utf8")
        FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")

    @application.get("/contents", status_code=status.HTTP_200_OK)
    @cache(expire=10)
    async def search(
            text: str = "", search_type: SearchType = SearchType.all_words,
            description: str = None, profile_id: str = None, app_id: str = None,
            name: str = None, trait_type: str = None, attribute_value: int = 0, size: int = 10,
            page: int = 1, from_date: date = None, to_date: date = None,
            result_type: ResultType = ResultType.latest, retrying: bool = False):
        return search_contents(
            text, search_type, description, profile_id, app_id,
            name, trait_type, attribute_value, size, page, from_date, to_date,
            result_type, retrying)

    @application.get("/publications")
    @cache(expire=10)
    async def search_publications_endpoint(text: str = "", search_type: SearchType = SearchType.all_words, 
                                           min_collects: int = None, min_mirror: int = None, min_comments: int = None,
                                           app_id:str=None,from_date: date = None, to_date: date = None, 
                                           page: int = 1, size: int = 10):
        return search_publications(text, search_type, min_collects, min_mirror, min_comments, app_id, from_date, to_date, page, size)

    @application.get("/profiles")
    @cache(expire=10)
    async def search_profiles_endpoint(text: str = "", page: int = 1, size: int = 10, owned_by: str = None, 
                                       min_follower: int = None, min_posts: int = None, min_publications: int = None, min_comments: int = None):
        return search_profiles(text, owned_by, min_follower, min_posts, min_publications, min_comments, page, size)

    @application.post("/index", status_code=status.HTTP_201_CREATED)
    def index_lens_contents(contents: List[MetadataSchema]):
        if len(contents) > INDEXING_LIMIT:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Bulk insertion allowed for {INDEXING_LIMIT} entities, provided '{len(contents)}'"
            )
        try:
            contents = [MetadataSchema.dict(content) for content in contents]
            index_contents(contents=contents)
            return {
                'message': f'Indexed successfully at "{time.time()}".'
            }
        except:
            error = traceback.format_exc()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Indexing error at our end. Error '{error}'"
            )

    @application.get("/trends", status_code=status.HTTP_200_OK)
    @cache(expire=600)
    async def get_trends_api(size: int = 20):
        return get_trends(size, days_back=2)
    
    @application.get("/app_id/all")
    @cache(expire=600)
    async def get_app_ids_endpoint(size: int = 20):
        return get_app_ids(size)

    return application
