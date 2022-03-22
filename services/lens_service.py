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
LENS_PROFILE_INDEX = os.getenv("LENS_PROFILE_INDEX", "lens-profile-data")
POSTS_INDEX = os.getenv("LENS_PROFILE_INDEX", "lens-posts-data")

def get_match_query(search_type, text, field):
    if search_type == SearchType.hashtags:
        text = " ".join(hashtag if "#" in hashtag else f"#{hashtag}" for hashtag in text.split(" "))
    res = {"match": {field: {"query": text}}}
    if search_type == SearchType.exact_phrase:
        res = {"match_phrase": {field: {"query": text}}}
    if search_type == SearchType.all_words:
        res = {"match": {field: {"query": text, "operator": "and"}}}
    return res

def search_contents(text = None, search_type = SearchType.any_words, \
        description = None, profile_id = None, app_id = None, name: str = None, \
        trait_type: str = None, attribute_value: int = 0, size: int = 10, \
        page: int = 1, from_date: date = None, to_date: date = None, \
        result_type: ResultType = ResultType.latest, retrying: bool = False,):
    
    sort_by = {"ingested_at": "desc"}
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
        "sort": sort_by
    }

    if profile_id:
        q = {"match": {"profileId": profile_id}}
        query["query"]["bool"]["must"].append(q)
        query["suggest"] = { "profile_id": { "text": profile_id, "term": { "field": "profileId" }}}
    
    if app_id:
        q = {"match": {"appId": app_id}}
        query["query"]["bool"]["must"].append(q)
        query["suggest"] = { "app_id": { "text": app_id, "term": { "field": "appId" }}}
    
    if trait_type:
        q = {"match": {"attributes.traitType": trait_type}}
        query["query"]["bool"]["must"].append(q)
    
    if attribute_value:
        q = {"match": {"attributes.value": attribute_value}}
        query["query"]["bool"]["must"].append(q)
    
    if name:
        q = {"match": {"name": name}}
        query["query"]["bool"]["must"].append(q)
        query["suggest"] = { "name": { "text": name, "term": { "field": "name" }}}
    
    if text:
        if search_type == SearchType.none_of_words:
            query["query"]["bool"]["must_not"].append(
                {"match": {"content": text}})
        else:
            query["query"]["bool"]["must"].append(get_match_query(search_type, text, "content"))
            query["suggest"] = { "text": { "text": text, "term": { "field": "content" }}}
    
    if description:
        q = {"match": {"description": {
            "query": description, "operator": "and"}}}
        query["query"]["bool"]["must"].append(q)

    ingested_at_q = {}
    if from_date:
        ingested_at_q["gte"] = from_date

    if to_date and to_date != date.today():
        ingested_at_q["lte"] = to_date

    if ingested_at_q:
        q = {"range": {"ingested_at": ingested_at_q}}
        query["query"]["bool"]["must"].append(q)

    if result_type == ResultType.links:
        q = {"match": {"content": {"query": "http https"}}}
        query["query"]["bool"]["must"].append(q)

    if result_type == ResultType.photo:
        q = {"match": {"media.mimeType": {"query": "image"}}}
        query["query"]["bool"]["must"].append(q)

    if result_type == ResultType.video:
        q = {"match": {"media.mimeType": {"query": "video"}}}
        query["query"]["bool"]["must"].append(q)   


    res = es.search(index=INDEX, body=query)

    if len(res["hits"]["hits"]) == 0 and not retrying:
        suggestion = get_search_suggestion(res)
        if suggestion:
            text = suggestion.get("text", text)
            profile_id = suggestion.get("profile_id", profile_id)
            app_id = suggestion.get("app_id", app_id)

            return search_contents(
                text, SearchType.any_words, description, profile_id, app_id, \
                name, trait_type, attribute_value, size, page, from_date, to_date, \
                result_type, retrying=True
            )

    data = res["hits"]["hits"]
    return {
        "page": page, "size": len(data), "total_count": res["hits"]["total"]["value"], "data": data,
        "query": {
            "text": text, 
            "profile_id": profile_id, 
            "app_id": app_id, 
            "description": description, 
            "trait_type": trait_type,
            "search_type": search_type 
        }}
    

def search_publications(text = "", search_type = SearchType.any_words, min_collects:int = None, min_mirror:int = None, 
                        min_comments:int = None, app_id:str = None, from_date:date=None, to_date:date=None,
                        page:int = 1, size:int = 10, retrying:bool=False):
    
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
        "sort": {"createdAt": "desc"}
    }
    add_match_query("appId", app_id, query, QueryMatchType.must)
    
    if text:
        text_query_fields = ["metadata.content", "metadata.description", "metadata.name", "profile.name", "profile.id",
                             "profile.bio", "profile.location", "profile.handle", "profile.twitterUrl", "profile.ownedBy"]
        query_field_values = [(field, text) for field in text_query_fields]
        add_match_query_multi(query_field_values, query, QueryMatchType.should, search_type)
        query["query"]["bool"]["minimum_should_match"] = 1
        add_query_suggestion(text, query, text_query_fields)
    
    number_query_fields = [("stats.totalAmountOfMirrors", min_mirror), 
                           ("stats.totalAmountOfCollects", min_collects),
                           ("stats.totalAmountOfComments", min_comments),
                           ]
    add_range_query_multi(number_query_fields, query)
    res = es.search(index=POSTS_INDEX, body=query)

    if len(res["hits"]["hits"]) == 0 and not retrying:
        suggestions = list(get_search_suggestion(res).values())
        suggestion = suggestions[0] if suggestions else ""
        if SearchType.any_words == search_type:
            suggestion = " ".join(suggestions)
        if suggestion:
            return search_publications(suggestion, search_type, min_collects, min_mirror, min_comments, from_date, to_date, page, size, retrying=True)

    data = list(map(lambda x:x["_source"], res["hits"]["hits"]))
    return {
        "page": page, "size": len(data), "total_count": res["hits"]["total"]["value"], "data": data,
        "query": {
            "text": text,
            "min_collects": min_collects, 
            "min_comments": min_comments, 
            "min_mirror": min_mirror
        }}
    
def search_profiles(text:str, owned_by:str, min_follower:int, min_posts:int, min_publications:int, min_comments:int, 
                    page: int = 1, size: int = 10, retrying=False):
    
    query = {
        "query": {
            "bool": {
                "must_not": [],
                "must": []
            }
        },
        "size": size,
        "from": (page - 1 if page > 0 else 0) * size
    }
    add_match_query("ownedBy", owned_by, query, QueryMatchType.must)
    
    if text:
        text_query_fields = ["name", "bio", "location", "handle", "twitterUrl"]
        query_field_values = [(field, text) for field in text_query_fields]
        add_match_query_multi(query_field_values, query, "should")
        query["query"]["bool"]["minimum_should_match"] = 1
        add_query_suggestion(text, query, text_query_fields)
    
    number_query_fields = [("stats.totalFollowers", min_follower), 
                           ("stats.totalPublications", min_publications),
                           ("stats.totalPosts", min_posts),
                           ("stats.totalComments", min_comments)
                           ]
    add_range_query_multi(number_query_fields, query)
    res = es.search(index=LENS_PROFILE_INDEX, body=query)

    if len(res["hits"]["hits"]) == 0 and not retrying:
        suggestion = get_search_suggestion(res)
        suggestion = " ".join(suggestion.values())
        if suggestion:
            return search_profiles(suggestion, owned_by, min_follower, min_posts, min_publications, min_comments, page, size, retrying=True)

    data = list(map(lambda x:x["_source"], res["hits"]["hits"]))
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

def add_query_suggestion(q, query, fields):
    query["suggest"] = {
        field: {
            "text": q,
            "term": {
                "field": field
            }
        } for field in fields
    }

def get_search_suggestion(res):
    out = {}
    if not "suggest" in res:
        return out
    for key, suggestion in res["suggest"].items():
        if not suggestion or not suggestion[0]["options"]:
            continue
        suggest_options = suggestion[0]["options"]
        if not suggest_options:
            continue
        suggest_text = ""
        for sug in suggest_options[:2]:
            suggest_text += sug['text'] + " "
        if suggest_text:
            out[key] = suggest_text.strip()
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
    

def add_match_query(field, value, query, match_type:QueryMatchType, search_type:SearchType=SearchType.any_words):
    if value:
        res = get_match_query(search_type, value, field)
        query["query"]["bool"][str(match_type)].append(res)
  
def add_match_query_multi(field_values, query, match_type:QueryMatchType = QueryMatchType.should, search_type:SearchType = SearchType.any_words):
    for field, value in field_values:
        add_match_query(field, value, query, match_type, search_type)

def add_range_query(field, value, query, match_type:QueryMatchType = QueryMatchType.must, range_cmp="gte"):
    if value and value > 0:
        query["query"]["bool"][str(match_type)].append({"range": {field: {range_cmp: value}}})

def add_range_query_multi(field_values, query, match_type:QueryMatchType = QueryMatchType.must, range_cmp="gte"):
    for field, value in field_values:
        add_range_query(field, value, query, match_type=match_type, range_cmp=range_cmp)
        
def get_app_ids(size: int = 10):
    query = {
        "aggs": {
            "app-ids": {
                "terms": {
                    "field": "appId.keyword",
                    "size": size
                }
            }
        }
    }
    return [keyword for keyword in es.search(index=POSTS_INDEX, body=query)['aggregations']['app-ids']['buckets']]
   

