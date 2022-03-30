import os
from enum import Enum
from typing import List
from services.lens_service import (
    SearchType, ResultType, get_app_ids, get_publication_comments, get_trends, index_contents,
    MetadataSchema, search_nfts, search_profiles, search_publications
)
from fastapi import FastAPI, HTTPException, status
import logging
from services.es_search import es
from datetime import date
import time
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from fastapi.params import Query
import aioredis
import traceback
from arango import ArangoClient
from app.custom_http_client import CustomHTTPClient


logger = logging.getLogger(__name__)

INDEXING_LIMIT = 100

GRAPHDB_PASSWORD = os.environ.get('GRAPHDB_PASSWORD')
GRAPHDB_HOST = os.environ.get('GRAPHDB_HOST')

class DirectionEnum(str, Enum):
    any = "any"
    inbound = "inbound"
    outbound = "outbound"


def get_application() -> FastAPI:
    application = FastAPI(title="Lens Service", debug=True, version="1.0")
    client = ArangoClient(
        hosts=GRAPHDB_HOST, http_client=CustomHTTPClient())
    graph_db = client.db("lens", username="root", password=GRAPHDB_PASSWORD)

    @application.on_event("startup")
    async def startup_event():
        es.init_app()
        redis = await aioredis.create_redis_pool(f"redis://:{os.environ.get('REDIS_PASSWORD')}@{os.getenv('REDIS_HOST')}:{os.getenv('REDIS_PORT','6379')}/0", encoding="utf8")
        FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")

    @application.get("/publications")
    @cache(expire=10)
    async def search_publications_endpoint(text: str = "", bio: str = None, from_users: str = None, mention_users: str = None,
                                           search_type: SearchType = SearchType.any_words, result_type: ResultType = ResultType.top,
                                           min_collects: int = None, min_mirror: int = None, min_comments: int = None,
                                           min_profile_follower: int = None, min_profile_posts: int = None, app_id: str = None,
                                           from_date: date = None, to_date: date = None, page: int = 1, size: int = 10):
        return search_publications(text, bio, from_users, mention_users, search_type, result_type, min_collects, min_mirror, min_comments, min_profile_follower, min_profile_posts, app_id, from_date, to_date, page, size)
    
    @application.get("/comments")
    @cache(expire=10)
    async def get_publication_comments_endpoint(pub_id:str, page: int = 1, size: int = 10):
        return get_publication_comments(pub_id, page, size)

    @application.get("/profiles")
    @cache(expire=10)
    async def search_profiles_endpoint(text: str = "", bio: str = None, page: int = 1, size: int = 10, owned_by: str = None,
                                       min_follower: int = None, min_posts: int = None, min_publications: int = None, min_comments: int = None):
        return search_profiles(text, bio, owned_by, min_follower, min_posts, min_publications, min_comments, page, size)

    @application.get("/nfts")
    @cache(expire=10)
    async def search_nfts_endpoint(text: str = "", page: int = 1, size: int = 10, search_type: SearchType = SearchType.all_words,):
        return search_nfts(text, search_type, page, size)

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
    

    @application.get("/traverse")
    async def traverse(start: str = Query("start node id"),
                 max_depth: int = Query(2, description="max depth"),
                 direction: DirectionEnum = Query("any")):
        try:
            return graph_db.graph("profiles-graph").traverse(start_vertex=f"profiles/{start}", direction="any", max_depth=max_depth)
        except:
            return {"message": f"Data for {start} not found!"}

    return application
