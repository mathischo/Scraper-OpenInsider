import requests
import os
import pandas as pd
import time
from ratelimit import limits, sleep_and_retry

# API settings
api_key = '4c16d3051c57139afb34626b686e6419b0f9f21ec38a2e205a868d2761654486'
endpoint = 'https://api.sec-api.io/insider-trading'
headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Maastricht University m.schomacher@student.maastrichtuniversity.nl',
    'Accept-Encoding': 'gzip, deflate',
    'Authorization': api_key
}

# Rate limits
ONE_SECOND = 1
MAX_CALLS_PER_SECOND = 10
REQUEST_BATCH_SIZE = 10
SLEEP_AFTER_BATCH = 0.5

@sleep_and_retry
@limits(calls=MAX_CALLS_PER_SECOND, period=ONE_SECOND)
def send_request(payload):
    # Send the request and return the response
    response = requests.post(endpoint, headers=headers, json=payload)
    return response

# Directory and file settings
directory = "/Users/mathisschomacher/Documents/Thesis"
file_name = "form_4_submissions.csv"
path = os.path.join(directory, file_name)

# Ensure the directory exists
if not os.path.exists(directory):
    os.makedirs(directory)

# Initialize parameters for pagination and timing
from_param = 0
size = 50
has_more_data = True
start_time = time.time()  # Capture start time
request_counter = 0  # Initialize request counter

# Initialize an empty DataFrame to hold all data
all_transactions = pd.DataFrame()

# Flatten the JSON structure
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

# Loop until there are no more records to fetch or time limit reached
while has_more_data and (time.time() - start_time) <= 100:
    # Define the request payload with the current 'from' value
    payload = {
        "query": "documentType:4",
        "from": from_param,
        "size": size,
        "sort": [{"filedAt": {"order": "desc"}}]
    }

    # Make the POST request with rate limiting
    response = send_request(payload)
    request_counter += 1  # Increment request counter

    # Check if the request counter has reached the batch size
    if request_counter >= REQUEST_BATCH_SIZE:
        time.sleep(SLEEP_AFTER_BATCH)  # Pause for 0.5 seconds after each batch of 10 requests
        request_counter = 0  # Reset request counter

    # Check the response status
    if response.status_code == 200:
        # Get data from the response
        data = response.json()
        transactions = data.get('transactions', [])

        # Flatten each transaction and append to the DataFrame
        for trans in transactions:
            flat_trans = flatten_json(trans)
            all_transactions = all_transactions.append(flat_trans, ignore_index=True)

        # Check if the returned data is less than the requested size (end of data)
        if len(transactions) < size:
            has_more_data = False
        else:
            # Increment 'from' for the next batch of data
            from_param += size

    else:
        print(f"Failed to fetch data: {response.text}")
        break

# Save all transactions to a CSV file
all_transactions.to_csv(path, index=False)
print(f"Data fetching and saving complete. File saved at {path}")