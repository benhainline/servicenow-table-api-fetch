import json
import pymongo
import requests
import yaml

# Load the configuration from the YAML file
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Set the ServiceNow API endpoint URL
url = config['servicenow']['url']
if 'sysparm_query' in config['servicenow']:
    url += f'?sysparm_query={config["servicenow"]["sysparm_query"]}'
if 'sysparm_offset' in config['servicenow']:
    url += f'&sysparm_offset={config["servicenow"]["sysparm_offset"]}'

# Set the ServiceNow API credentials
username = config['servicenow']['username']
password = config['servicenow']['password']
auth = (username, password)

# Set the MongoDB connection parameters
if 'mongodb' in config:
    if 'url' in config['mongodb']:
        mongo_url = config['mongodb']['url']
    if 'database' in config['mongodb']:
        mongo_db = config['mongodb']['database']
    if 'collection' in config['mongodb']:
        mongo_collection = config['mongodb']['collection']

    # Connect to the MongoDB database and collection
    client = pymongo.MongoClient(mongo_url)
    db = client[mongo_db]
    collection = db[mongo_collection]

# Function to store an incident in the MongoDB collection
def store_incident_in_mongodb(incident):
    # Insert or update the incident record in the collection based on its sys_id
    collection.replace_one({'sys_id': incident['sys_id']}, incident, upsert=True)

# Function to fetch incidents with pagination and write to MongoDB if specified
def fetch_incidents_with_pagination(url, headers, auth, limit=10, write_to_mongodb=False):
    # Set the initial query parameters
    params = {
        'sysparm_limit': limit,
        'sysparm_count': "true",
        'sysparm_offset': 0
    }

    # Send the initial API request to fetch the first set of records
    response = requests.get(url, headers=headers, auth=auth, params=params)

    # Check the status code of the response
    if response.status_code == 200:
        # Process the first set of records
        data = response.json()
        for incident in data['result']:
            # Process each incident record as needed
            print(incident)
            if write_to_mongodb:
                store_incident_in_mongodb(incident)

        # Set the total number of records and the initial offset
        total_records = data['total_count']
        offset = limit

        # Loop through the remaining records using pagination
        while offset < total_records:
            # Set the query parameters for the next set of records
            params['sysparm_offset'] = offset

            # Send the API request to fetch the next set of records
            response = requests.get(url, headers=headers, auth=auth, params=params)

            # Check the status code of the response
            if response.status_code == 200:
                # Process the next set of records
                data = response.json()
                for incident in data['result']:
                    # Process each incident record as needed
                    print(incident)
                    if write_to_mongodb:
                        store_incident_in_mongodb(incident)

                # Update the offset for the next set of records
                offset += limit
            else:
                # Print the error message
                print('Error fetching records: ', response.content)
                break
    else:
        # Print the error message
        print('Error fetching records: ', response.content)

# Fetch incidents and write to MongoDB
fetch_incidents_with_pagination(url, {'Accept': 'application/json'}, auth)
