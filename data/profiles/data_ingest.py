from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from queries import get_profiles_query, get_followers_query
import time
from pymongo import MongoClient
from pymongo.operations import UpdateOne
import os

MONGO_HOST = os.environ.get('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.environ.get('MONGO_PORT', 27017))
MONGO_USER = os.environ.get('MONGO_USER', '')
MONGO_PASS = os.environ.get('MONGO_PASSWORD', '')
MONGO_DB = os.environ.get('MONGO_DB', '')

mongo_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, username=MONGO_USER, password=MONGO_PASS)

db = mongo_client[MONGO_DB]
collection = db['lens-profiles']


# build the request framework
transport = RequestsHTTPTransport(url="https://api-mumbai.lens.dev/", use_json=True)

# create the client
client = Client(transport=transport, fetch_schema_from_transport=True)

# define a query
query = gql("""{query: ping}""")
response = client.execute(query)

if response['query']=='pong':
    print("Connection established!!!")

count = 1

total_user_ids = set()
total_user_addresses = set()

def get_user_profiles(user_ids):
    query = gql(get_profiles_query(user_ids))
    response = client.execute(query)
    return response.get("profiles", []).get("items", [])

def add_followers_info(entries):
    profiles_data = []
    for entry in entries:
        profile_owned_by = entry['ownedBy']
        profile_id = entry['id']
        followers = []
        total_user_ids.add(profile_id)
        total_user_addresses.add(profile_owned_by)
        follower_query = gql(get_followers_query(profile_id))
        follower_response = client.execute(follower_query)
        for elm in follower_response.get('followers', {}).get('items', []):
            wallet = elm.get('wallet')
            if wallet and wallet.get('defaultProfile'):
                if wallet.get('defaultProfile', {}).get('id'):
                     followers.append(wallet.get('defaultProfile', {}).get('id'))   
        entry['followers'] = followers
        profiles_data.append(UpdateOne({"_id": profile_id},{"$set": entry}, upsert=True))
    if profiles_data:
        result = collection.bulk_write(profiles_data, ordered=False)
    return result

def get_unordered_user_ids():
    docs = list(collection.find({}).limit(50))
    user_profiles = set()
    user_follower_profiles = set()
    for doc in docs:
        user_id = doc['_id']
        user_profiles.add(user_id)
        for follower in doc['followers']:
            user_follower_profiles.add(follower)
    return list(user_follower_profiles - user_profiles)

while True:
    user_ids = [f"0x0{i}" if len(str(i))%2!=0 else f"0x{i}"\
         for i in range(count, count+50)]
    entries = get_user_profiles(user_ids)
    if len(entries)==0:
        time.sleep(60)
        count = 1
        continue
    result = add_followers_info(entries)
    unordered_user_ids = get_unordered_user_ids()
    entries = get_user_profiles(unordered_user_ids)
    if entries:
        result = add_followers_info(entries)
    count+=50
    time.sleep(1)
