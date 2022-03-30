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
    print("add_followers_info called..!!")
    profiles_data = []
    for entry in entries:
        profile_owned_by = entry['ownedBy']
        profile_id = entry['id']
        # print(f"For profile_id: {profile_id}")
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
        profiles_data.append(UpdateOne({"_id": int(profile_id, base=16)},{"$set": entry}, upsert=True))
    if profiles_data:
        result = collection.bulk_write(profiles_data, ordered=False)
    return result

term_count = 0

while True:
    user_ids = []
    for i in range(count, count+50):
        user_id = hex(i)
        if len(user_id)%2!=0:
            user_ids.append(user_id.replace("0x", "0x0"))
            continue
        user_ids.append(user_id)
    entries = get_user_profiles(user_ids)
    print(f"Number of users found {len(entries)}")
    if len(entries)==0:
        term_count+=1
        if term_count == 50:
            print("Processing complete....\n\n\n")
            time.sleep(60)
            count = 1
            term_count = 0
            continue
        count+=50
        time.sleep(1)
        continue
    term_count = 0
    result = add_followers_info(entries)
    count+=50
    time.sleep(1)

