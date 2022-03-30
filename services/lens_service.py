from functools import reduce
import operator
import os
from datetime import date, datetime, timedelta
from services.es_search import es
from enum import Enum
from elasticsearch import helpers
from pydantic import BaseModel
from typing import List, Optional


class SearchType(str, Enum):
    exact_phrase = "exact_phrase"
    all_words = "all_words"
    any_words = "any_words"
    hashtags = "hashtags"
    none_of_words = "none_of_words"


class QueryMatchType(str, Enum):
    should = "should"
    must = "must"
    must_not = "must_not"
    shoul_not = "shoul_not"

    def __str__(self):
        return str(self.value)


class ResultType(str, Enum):
    latest = "latest"
    links = "links"
    photo = "photo"
    video = "video"
    top = "top"


class AttributesSchema(BaseModel):
    traitType: str
    value: int


class MediaSchema(BaseModel):
    item: str
    mimeType: str


class MetadataSchema(BaseModel):
    version: str
    metadata_id: str
    description: Optional[str]
    contents: Optional[str]
    external_url: Optional[str]
    name: Optional[str]
    attributes: Optional[List[AttributesSchema]]
    image: Optional[str]
    imageMimeType: Optional[str]
    media: Optional[List[MediaSchema]]
    appId: str
    profileId: Optional[str]


INDEX = os.getenv("LENS_DATA_INDEX", "lens-test-data")
LENS_PROFILE_INDEX = os.getenv("LENS_PROFILE_INDEX", "lens-profile-csv-data")
LENS_PROFILE_INDEX = os.getenv("LENS_PROFILE_INDEX", "lens-final-profiles-data")
POSTS_INDEX = os.getenv("LENS_PROFILE_INDEX", "lens-final-posts-data")
NFTS_INDEX = os.getenv("LENS_NFTS_INDEX", "lens-nfts-test-data")

ES_RESPONSE_FIELDS = [
    "metadata_id",
    "description",
    "content",
    "external_url",
    "name",
    "attributes",
    "image",
    "imageMimeType",
    "media",
    "appId",
    "profileId",
    "ingested_at"
]

def get_match_query(search_type, text, field):
    if search_type == SearchType.hashtags:
        text = " ".join(
            hashtag if "#" in hashtag else f"#{hashtag}" for hashtag in text.split(" "))
    res = {"match": {field: {"query": text}}}
    if search_type == SearchType.exact_phrase:
        res = {"match_phrase": {field: {"query": text}}}
    if search_type == SearchType.all_words:
        res = {"match": {field: {"query": text, "operator": "and"}}}
    return res

def get_publication_comments(pub_id: str, page: int = 1, size: int = 10):
    query = {
        "query": {
            "bool": {"must": [
                {"match": {"mainPost.id.keyword": pub_id}}
            ]}
        },
        "size": size,
        "from": (page - 1 if page > 0 else 0) * size
    }
    res = es.search(index=POSTS_INDEX, body=query)
    data = list(map(lambda x: x["_source"], res["hits"]["hits"]))
    return {"page": page, "size": len(data), "total_count": res["hits"]["total"]["value"], "data": data}


def search_publications(text="", bio: str = None, from_users: str = None, mention_users: str = None, search_type=SearchType.any_words,
                        result_type: ResultType = ResultType.latest, min_collects: int = None,  min_mirror: int = None, min_comments: int = None,
                        min_profile_follower: int = None, min_profile_posts: int = None, app_id: str = None, from_date: date = None,
                        to_date: date = None, page: int = 1, size: int = 10, retrying: bool = False):

    results_map = {"links": {"metadata.content": "http https"}, "photo": {
        "metadata.media.original.mimeType": "image"}, "video": {"metadata.media.original.mimeType": "video"}}
    result_type_info = results_map.get(
        result_type.value, {}) if result_type else {}
    must_query_field_values = {
        "appId": app_id,
        "profile.handle": from_users,
        "profile.bio": bio,
        "metadata.description": mention_users
    }

    should_query_fields = set(["metadata.content", "metadata.description", "metadata.name", "profile.name", "profile.id",
                               "profile.bio", "profile.location", "profile.handle", "profile.twitterUrl", "profile.ownedBy"])
    should_query_field_values = {field: text for field in should_query_fields}

    gte_range_query_field_values = {
        "stats.totalAmountOfMirrors": min_mirror,
        "stats.totalAmountOfCollects": min_collects,
        "stats.totalAmountOfComments": min_comments,
        "profile.stats.totalFollowers": min_profile_follower,
        "profile.stats.totalPosts": min_profile_posts
    }
    sort_by = {"createdAt": "desc"} if result_type != ResultType.top else None
    res = search(POSTS_INDEX, must_query_field_values, should_query_field_values, search_type, gte_range_query_field_values,
                 prefix_field_values=result_type_info, page=page, size=size, sort_by=sort_by)

    if len(res["hits"]["hits"]) == 0 and not retrying:
        suggestion_res = get_search_suggestion(res, False)
        bio = suggestion_res.get("profile.bio", [bio])[0] if bio else None
        # from_users = suggestion_res.get("profile.handle", from_users)[
        #     0] if from_users else None
        # do not consider suggestion for must fields (handle and bio) again in should fields
        if bio:
            suggestion_res.pop("profile.bio", None)
        if from_users:
            suggestion_res.pop("profile.handle", None)
        should_suggestions = [suggestion_res.get(
            field) for field in should_query_fields if field in suggestion_res]
        should_suggestions = list(
            reduce(operator.concat, should_suggestions, []))
        suggestion = should_suggestions[0] if should_suggestions else ""
        if SearchType.any_words == search_type:
            suggestion = " ".join(should_suggestions)
        if suggestion:
            text = suggestion
        return search_publications(text, bio, from_users, mention_users, search_type, result_type, min_collects,
                                   min_mirror, min_comments, min_profile_follower, min_profile_posts, app_id,
                                   from_date, to_date, page, size, retrying=True)

    data = list(map(lambda x: x["_source"], res["hits"]["hits"]))
    return {
        "page": page, "size": len(data), "total_count": res["hits"]["total"]["value"], "data": data,
        "query": {
            "text": text,
            "min_collects": min_collects,
            "min_comments": min_comments,
            "min_mirror": min_mirror
        }}


def search_profiles(text: str, bio: str, owned_by: str, min_follower: int, min_posts: int, min_publications: int, min_comments: int,
                    page: int = 1, size: int = 10, retrying=False):

    must_query_field_values = {
        "ownedBy": owned_by,
        "bio": bio
    }

    should_query_fields = ["name", "bio", "location", "handle", "twitterUrl"]
    should_query_field_values = {field: text for field in should_query_fields}

    gte_range_query_field_values = {
        "stats.totalFollowers": min_follower,
        "stats.totalPublications": min_publications,
        "stats.totalComments": min_comments,
        "stats.totalPosts": min_posts
    }
    res = search(LENS_PROFILE_INDEX, must_query_field_values, should_query_field_values,
                 SearchType.any_words, gte_range_query_field_values, page=page, size=size)

    if len(res["hits"]["hits"]) == 0 and not retrying:
        suggestion_res = get_search_suggestion(res)
        bio = suggestion_res.get("bio", bio) if bio else None
        if bio:
            suggestion_res.pop("bio", None)
        suggestion = " ".join(suggestion_res.values())
        if suggestion:
            text = suggestion
        return search_profiles(text, bio, owned_by, min_follower, min_posts, min_publications, min_comments, page, size, retrying=True)

    data = list(map(lambda x: x["_source"], res["hits"]["hits"]))
    return {
        "page": page, "size": len(data), "total_count": res["hits"]["total"]["value"], "data": data,
        "query": {
            "text": text,
            "owned_by": owned_by,
            "min_follower": min_follower,
            "min_posts": min_posts,
            "min_publications": min_publications,
            "min_comments": min_comments
        }}


def search_nfts(text="", search_type=SearchType.any_words, page: int = 1, size: int = 10, retrying: bool = False):

    query = {
        "query": {
            "bool": {
                "must_not": [],
                "must": [],
                "should": [],
            }
        },
        "size": size,
        "from": (page - 1 if page > 0 else 0) * size
    }

    if text:
        text_query_fields = ["contractName", "contractAddress", "symbol", "tokenId", "owners.address", "ercType",
                             "name", "description", "contentURI", "originalContent.uri", "collectionName"]
        query_field_values = [(field, text) for field in text_query_fields]
        add_match_query_multi(query_field_values, query,
                              QueryMatchType.should, search_type)
        query["query"]["bool"]["minimum_should_match"] = 1
        add_query_suggestions(text, query, text_query_fields)
    res = es.search(index=NFTS_INDEX, body=query)

    if len(res["hits"]["hits"]) == 0 and not retrying:
        suggestions = list(get_search_suggestion(res).values())
        suggestion = suggestions[0] if suggestions else ""
        if SearchType.any_words == search_type:
            suggestion = " ".join(suggestions)
        if suggestion:
            return search_nfts(suggestion, search_type, page, size, retrying=True)

    data = list(map(lambda x: x["_source"], res["hits"]["hits"]))
    return {
        "page": page, "size": len(data), "total_count": res["hits"]["total"]["value"], "data": data,
        "query": {"text": text}
    }


def search(es_index: str, must_query_field_values, should_query_field_values, search_type, gte_range_query_field_values, prefix_field_values: dict = {}, sort_by: str = None,  page: int = 1, size: int = 10):
    query = {
        "query": {
            "bool": {
                "must_not": [],
                "must": [],
                "should": [],
            }
        },
        "size": size,
        "from": (page - 1 if page > 0 else 0) * size,
        "suggest": {}
    }
    if sort_by:
        query["sort"] = sort_by
    add_match_query_multi(should_query_field_values.items(),
                          query, QueryMatchType.should, search_type)
    add_match_query_multi(must_query_field_values.items(),
                          query, QueryMatchType.must, search_type)
    add_query_suggestions(query, should_query_field_values.items())
    add_query_suggestions(query, must_query_field_values.items())
    add_range_query_multi(gte_range_query_field_values.items(), query)
    add_prefix_query_multi(prefix_field_values.items(), query)
    if query["query"]["bool"]["should"]:
        query["query"]["bool"]["minimum_should_match"] = 1
    res = es.search(index=es_index, body=query)
    return res


def add_query_suggestions(query, field_values):
    for field, q in field_values:
        add_query_suggestion(q, field, query)


def add_query_suggestion(q, field, query):
    if q:
        query["suggest"].update({
            field: {
                "text": q,
                "term": {
                    "field": field
                }
            }
        })


def get_search_suggestion(res, flatten=True):
    out = {}
    if not "suggest" in res:
        return out
    suggest_items = res["suggest"].items()
    for key, suggestion in suggest_items:
        if not suggestion or not suggestion[0]["options"]:
            continue
        suggest_options = suggestion[0]["options"]
        if not suggest_options:
            continue
        suggest_text = [sug["text"] for sug in suggest_options[:2]]
        if flatten:
            suggest_text = " ".join(suggest_text)
        if suggest_text:
            out[key] = suggest_text
    return out


def get_trends(size: int = 10, days_back: int = 1):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "ingested_at": {
                                "gte": (datetime.now()-timedelta(days=days_back)).isoformat(),
                                "lte": "now"
                            }
                        }
                    }
                ]
            }
        },
        "size": 0,
        "aggs": {
            "trends": {
                "significant_text": {
                    "field": "count",
                    "min_doc_count": 3,
                    "size": size
                }
            }
        }
    }
    return [keyword for keyword in es.search(index=INDEX, body=query)['aggregations']['trends']['buckets'] if keyword['key'][0].isalpha()]


def add_ingested_date(post, ingested_at):
    post['ingested_at'] = ingested_at
    return post


def index_contents(contents):
    ingested_at = datetime.now().isoformat()
    actions = [
        {
            "_index": INDEX,
            "_id": content.get("metadata_id"),
            "_source": add_ingested_date(content, ingested_at),
        } for content in contents
    ]
    helpers.bulk(es, actions)


def add_match_query(field, value, query, match_type: QueryMatchType, search_type: SearchType = SearchType.any_words):
    if value:
        res = get_match_query(search_type, value, field)
        query["query"]["bool"][str(match_type)].append(res)


def add_prefix_query(field, value, query, match_type: QueryMatchType = QueryMatchType.must):
    if value:
        query["query"]["bool"][match_type].append(
            {"prefix": {field: {"value": value}}})


def add_prefix_query_multi(field_values, query, match_type: QueryMatchType = QueryMatchType.must):
    for field, value in field_values:
        add_prefix_query(field, value, query, match_type)


def add_match_query_multi(field_values, query, match_type: QueryMatchType = QueryMatchType.should, search_type: SearchType = SearchType.any_words):
    for field, value in field_values:
        add_match_query(field, value, query, match_type, search_type)


def add_range_query(field, value, query, match_type: QueryMatchType = QueryMatchType.must, range_cmp="gte"):
    if value and value > 0:
        query["query"]["bool"][str(match_type)].append(
            {"range": {field: {range_cmp: value}}})


def add_range_query_multi(field_values, query, match_type: QueryMatchType = QueryMatchType.must, range_cmp="gte"):
    for field, value in field_values:
        add_range_query(field, value, query,
                        match_type=match_type, range_cmp=range_cmp)


def get_app_ids(size: int = 10):
    query = {
        "aggs": {
            "app-ids": {
                "terms": {
                    "field": "appId.keyword",
                    "size": size
                }
            }
        },
        "size": 0
    }
    return [keyword for keyword in es.search(index=POSTS_INDEX, body=query)['aggregations']['app-ids']['buckets']]
