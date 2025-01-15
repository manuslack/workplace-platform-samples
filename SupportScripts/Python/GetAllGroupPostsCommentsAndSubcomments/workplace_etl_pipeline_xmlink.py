# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
# 
# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

import requests
import os
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
import time
import logging
import json

# for current working directory use below function instead
#load_dotenv()
dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(dotenv_path=dotenv_path)

# Workplace graph API token
access_token = os.getenv('WP_ACCESS_TOKEN')
# Other ENV Variables
data_for_how_many_days = int(os.getenv('DATA_FOR_HOW_MANY_DAYS'))
logfilepath=os.getenv('LOGFILEPATH')
logfilename="workplace_etl_pipeline_production"+'_'+str("{:%Y_%m_%d_%H_%M_%S}".format(datetime.now()))+'.log'
logfilepathandname=logfilepath+logfilename



#other variables
end_time = datetime.now()
start_time = end_time - timedelta(days=data_for_how_many_days)

# for ad-hoc run
# start_time = '2024-03-04 10:00:02.076580'
# end_time = '2024-03-06 10:00:02.076580'

# logging default configurations
logging.basicConfig(
    filename=logfilepathandname,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %I:%M:%S%p",
)

# Function to fetch all group IDs, handling pagination
def get_all_groups():
    # Initial URL for fetching the first page of group data
    url = "https://graph.facebook.com/community/groups?limit=100"
    headers = {"Authorization": f"Bearer {access_token}"}
    # List to store all group data
    all_group = []
    
    logging.info('starting phase 1 of data extraction to fetch all group ids and associated metadata')
    
    # Counter to track the number of pages fetched
    page_number_groups = 0
    
    # Loop through pages while there is a URL to fetch
    while url:
        page_number_groups = page_number_groups+1
        # Log progress every 5 pages
        if (page_number_groups%5==0):
            logging.info('on page number:'+str(page_number_groups))
        
        # Initialize retry counter for handling errors
        number_of_retries = 0
        
        # Make an HTTP GET request to fetch the current page
        response = https_get_call_handling_groups(url, headers, page_number_groups, number_of_retries)
        status = response.status_code
        
        # Attempt to parse the response as JSON
        try: 
            json_output = response.json()
        except Exception:
            # Log an error if JSON parsing fails
            logging.error('something wrong with parsing json',exc_info=True)
        
        # Handle successful response
        if status == 200:
            # Extract group data from the response
            groups = json_output.get("data", [])
            all_group.extend(groups)
            
            # Check for pagination info and update the URL for the next request
            pagination = json_output.get("paging", {})
            next_url = pagination.get("next")
            url = next_url if next_url else None
        
        # Handle rate-limiting (status 429)
        elif status == 429:
            logging.info('sleeping for 30 seconds due to rate limit exceeding')
            time.sleep(30)
            # Retry the same request after sleeping
            response = https_get_call_handling_groups(url, headers, page_number_groups, number_of_retries)
            try: 
                json_output = response.json()
            except Exception:
                logging.error('something wrong with parsing json',exc_info=True)
            groups = json_output.get("data", [])
            all_group.extend(groups)
            
            # Check for pagination info and update the URL for the next request
            pagination = json_output.get("paging", {})
            next_url = pagination.get("next")
            url = next_url if next_url else None
        
        # Exit the loop for any other status
        else: 
            logging.warn(str(status)+'exiting the loop early'+page_number_groups)
            break
    
    # Return the complete list of group data
    return all_group

# Creates arrays of arrays containing 50 ids each (can be reused for group ids or any ids)
def create_batches(ids):
    # Initialize an empty list to hold batches of IDs
    batches = []
    batch_size = 50
    # Iterate over the list of IDs, stepping by batch_size to create chunks
    for i in range(0, len(ids), batch_size):
        # Append a slice of the list containing up to batch_size elements to the batches list
        batches.append(ids[i:i+batch_size])
    # Return the list of batches, each containing up to 50 IDs
    return batches

def get_all_group_ids(all_group):
    group_ids = []
    for i in range(0, len(all_group)):
        group_ids.append(all_group[i]['id'])
    return group_ids

def make_batch_request(batch_payload, number_of_retries):
    # Tries to send a POST request to the Facebook Graph API with a batch payload and access token
    try:
        response = requests.post(f'https://graph.facebook.com/?batch={batch_payload}&include_headers=false&access_token={access_token}', timeout=60)
        # Raises an exception if the HTTP request returned an unsuccessful status code
        response.raise_for_status()
        # Returns the response if the request is successful
        return response
    except requests.exceptions.HTTPError as e:
        # Increments the retry count and retries up to 3 times if an HTTP error occurs
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            logging.warning(f'sleeping for 60 seconds and then retry making batch request - HTTP Error: {e}')
            time.sleep(60)
            response = make_batch_request(batch_payload, number_of_retries)
            return response
        else:
            # Logs the issue and error details if retries are exhausted
            logging.info('issue with batch request')
            logging.error("Http Error:", exc_info=True)
    except requests.exceptions.ConnectionError:
        # Handles connection errors similarly, with up to 3 retries
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            logging.warning(f'sleeping for 60 seconds and then retry making batch request - Connection Error')
            time.sleep(60)
            response = make_batch_request(batch_payload, number_of_retries)
            return response
        else:
            logging.info('issue with batch request')
            logging.error("Connection Error:", exc_info=True)
    except requests.exceptions.Timeout:
        # Handles timeout errors with retry logic
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            logging.warning(f'sleeping for 60 seconds and then retry making batch request - Timeout')
            time.sleep(60)
            response = make_batch_request(batch_payload, number_of_retries)
            return response
        else:
            logging.info('issue with batch request')
            logging.error("Timeout:", exc_info=True)
    except requests.exceptions.RequestException:
        # Handles any other request-related exceptions with retries
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            logging.warning(f'sleeping for 60 seconds and then retry making batch request - Request Exception')
            time.sleep(60)
            response = make_batch_request(batch_payload, number_of_retries)
            return response
        else:
            logging.info('issue with batch request')
            logging.error("Request Exception:", exc_info=True)

def get_all_subgroups(all_group):
    # Logs the beginning of the second phase of data extraction
    logging.info('starting phase 2 of data extraction to fetch all sub-group ids and associated metadata')
    
    # Create batches of group IDs from the provided all_group data for batch processing
    batches = create_batches(get_all_group_ids(all_group))
    sub_groups = []  # List to store all the sub-group data

    for batch in batches:
        # Prepare a batch payload for a batch API request to fetch sub-groups for each group ID
        batch_payload = requests.utils.quote(
            json.dumps(
                [
                    {
                        "method": "GET",
                        "relative_url": f"{group_id}/groups?limit=100"
                    }
                    for group_id in batch
                ]
            )
        )
        # Make a batch request to get sub-group data for the current batch
        response = make_batch_request(batch_payload, 0)
        logging.info(str(response))

        # Iterate through the responses for each group ID in the batch
        for group_num_in_batch, individual_response in enumerate(response.json()):
            # Retry logic for individual requests if the status code is not 200
            # We need to retry individual responses within the batch of responses if the status code is not 200
            individual_response_retries = 0
            while individual_response.get('code') != 200:
                individual_response_retries = individual_response_retries + 1
                logging.warning('retrying individual response for group id: ' + str(batch[group_num_in_batch]))
                
                # Stop retrying after 3 attempts
                if individual_response_retries > 3:
                    logging.error('max retries reached for group id : ' + str(batch[group_num_in_batch]))
                    break

                # Prepare a single request payload for the individual retry
                batch_payload = requests.utils.quote(
                    json.dumps(
                        [
                            {
                                "method": "GET",
                                "relative_url": f"{batch[group_num_in_batch]}/groups?limit=100"
                            }
                        ]
                    )
                )
                # Retry the individual request
                individual_response = make_batch_request(batch_payload, 0).json()[0]

            # Parse the response body for the current group
            body = json.loads(individual_response.get('body'))
            logging.info('response for group id ' + str(batch[group_num_in_batch]) + ': ' + str(body))

            # Append the fetched sub-groups to the list
            for item in body.get('data'):
                sub_groups.append(item)

            # Handle paging if more data is available for the current group
            if 'paging' in body and 'next' in body['paging']:
                logging.info('Paging for group id ' + str(batch[group_num_in_batch]))
                url = body['paging']['next']
                page_number_groups = 1
                while url:
                    #response = requests.get(url).json()

                    # Handle additional pages of sub-group data
                    page_number_groups = page_number_groups + 1
                    response = https_get_call_handling_groups(url, {"Authorization": f"Bearer {access_token}"}, page_number_groups, 0).json()
                    logging.info('Paging response: ' + str(response))

                    # Append more sub-group data from additional pages
                    for item in response.get('data'):
                        sub_groups.append(item)

                    # Check if further paging is available
                    if 'paging' in response and 'next' in response['paging']:
                        url = response['paging']['next']

                    else:
                        url = None

    # Log the total count of sub-groups found
    logging.info('sub groups count: ' + str(len(sub_groups)))                    
    
    # Return the collected sub-groups
    return sub_groups

def https_get_call_handling_groups(url, headers, page_number, number_of_retries):
    # Attempt to make an HTTPS GET request to fetch group data with the provided URL and headers
    try:
        response = requests.get(url, headers=headers, timeout=60)
        # Raise an HTTPError if the response contains a 4xx or 5xx status code
        response.raise_for_status()
        return response

    # Handle specific HTTP errors
    except requests.exceptions.HTTPError:
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            # Retry after a 1-minute wait if the number of retries is within the limit
            logging.warning('sleeping for 1 minute and get call to fetch group ids (HTTP error)')
            time.sleep(60)
            # Recursive retry of the same request
            return https_get_call_handling_groups(url, headers, page_number, number_of_retries)
        else:
            # Log information and error details if maximum retries are exceeded
            logging.info('page_number:'+str(page_number))
            logging.error("Http Error:", exc_info=True)  
    
    # Handle connection-related errors
    except requests.exceptions.ConnectionError:
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            logging.warning('sleeping for 1 minute and get call to fetch group ids (connection error)')
            time.sleep(60)
            return https_get_call_handling_groups(url, headers, page_number, number_of_retries)
        else:
            logging.info('page_number:'+str(page_number))
            logging.error("Error Connecting:",exc_info=True)
    
    # Handle timeout errors
    except requests.exceptions.Timeout:
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            logging.warning('sleeping for 1 minute and get call to fetch group ids (timeout)')
            time.sleep(60)
            return https_get_call_handling_groups(url, headers, page_number, number_of_retries)
        else:
            logging.info('page_number:'+str(page_number))
            logging.error("Timeout Error:",exc_info=True)
    
    # Handle other request-related errors
    except requests.exceptions.RequestException:
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            logging.warning('sleeping for 1 minute and get call to fetch group ids (request exception)')
            time.sleep(60)
            return https_get_call_handling_groups(url, headers, page_number, number_of_retries)
        else:
            logging.info('page_number:'+str(page_number))
            logging.error("Something Other API error:",exc_info=True)

def https_get_call_handling_per_group(url, headers, group_id, number_of_retries):
    # Attempt to make an HTTPS GET request to fetch group-level data
    try:
        response = requests.get(url, headers=headers, timeout=60)
        status = response.status_code
        # Raise an HTTPError if the response contains a 4xx or 5xx status code
        response.raise_for_status()
        return response, status

    # Handle specific HTTP errors
    except requests.exceptions.HTTPError:
        # Check if the group no longer exists and handle this specific error
        if response.status_code == 400 and "does not exist" in response.json().get("error", {})['message']:
            logging.warning(response.json().get("error", {})['message'])
            logging.info('group doesn''t exist anymore, so skipping group number'+str(group_id))
            response = {"data": []}
            #giving randon number not related to https requests
            # Set a non-HTTP status to indicate this condition
            status = None
            return  response, status
        # Handle a specific unknown server error
        elif response.status_code == 500 and "An unknown error has occurred" in response.json().get("error", {})['message']:
            logging.warning(response.json().get("error", {})['message'])
            logging.info('one of the graph end point has issues, so skipping group number:'+str(group_id))
            response = {"data": []}
            #giving randon number not related to https requests
            # Set a non-HTTP status to indicate this condition
            status = None
            return  response, status
        else: 
            # Retry for other HTTP errors
            number_of_retries = number_of_retries+1
            if number_of_retries <= 3:
                logging.warning('sleeping for 1 minute and get call to fetch group level information - HTTP Error')
                time.sleep(60)
                response,status = https_get_call_handling_per_group(url, headers, group_id, number_of_retries)
                status = response.status_code
                return response, status
            else:
                logging.info('error occured at group number:'+str(group_id))
                logging.error("Http Error:", exc_info=True)  

    # Handle connection-related errors
    except requests.exceptions.ConnectionError:
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            logging.warning('sleeping for 1 minute and get call to fetch group level information - Connection Error')
            time.sleep(60)
            response, status = https_get_call_handling_per_group(url, headers, group_id, number_of_retries)
            status = response.status_code
            return response, status
        else:
            logging.info('error occured at group number:'+str(group_id))
            logging.error("Error Connecting:",exc_info=True)

    # Handle timeout errors
    except requests.exceptions.Timeout:
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            logging.warning('sleeping for 1 minute and get call to fetch group level information - Timeout')
            time.sleep(60)
            response, status = https_get_call_handling_per_group(url, headers, group_id, number_of_retries)
            status = response.status_code
            return response, status
        else:
            logging.info('error occured at group number:'+str(group_id))
            logging.error("Timeout Error:",exc_info=True)

    # Handle other general request-related errors
    except requests.exceptions.RequestException:
        number_of_retries = number_of_retries+1
        if number_of_retries <= 3:
            logging.warning('sleeping for 1 minute and get call to fetch group level information - Request Exception')
            time.sleep(60)
            response, status = https_get_call_handling_per_group(url, headers, group_id, number_of_retries)
            status = response.status_code
            return response, status
        else:
            logging.info('error occured at group number:'+str(group_id))
            logging.error("Something Other API error:",exc_info=True)

'''
In this function, a batch of 50 group objects is the input. A batch of requests is made to Meta to get the post feeds.
Another key is added to each group object which is a array to store the posts for each group.
This enhance group object is then sent into the load_group function, which loads each post for the group to the project
'''
def get_group_batch_data(batch, group_type):
    # Constructs a batch payload for making a batch request to the API
    batch_payload = requests.utils.quote(
        json.dumps(
            [
                {
                    # For each group in the batch, prepare a GET request to retrieve feed data
                    "method": "GET",
                    "relative_url": f"{group['id']}/feed?fields=id,from{{email, title, name, id, primary_address, department, locale, link}},message,permalink_url, created_time,comments{{id,from{{email, title, name, id, primary_address, department, locale, link}},message,comments{{id,from{{email, title, name, id, primary_address, department, locale, link}},message, permalink_url, created_time}} ,permalink_url, created_time}}&since={start_time}&until={end_time}"
                }
                for group in batch
            ]
        )
    )
    # Sends the batch request using the provided payload
    response = make_batch_request(batch_payload, 0)
    logging.info('batch request response: ' + str(response.status_code))
    
    # Iterates over individual responses within the batch
    for group_num_in_batch, individual_response in enumerate(response.json()):
        # We need to retry individual responses within the batch of responses if the status code is not 200
        # Retries individual group requests up to 3 times if the status code is not 200
        individual_response_retries = 0
        while individual_response.get('code') != 200:
            individual_response_retries = individual_response_retries + 1
            logging.warning('retrying individual response for group id: ' + str(batch[group_num_in_batch]['id']))
            if individual_response_retries > 3:
                logging.error('max retries reached for group id : ' + str(batch[group_num_in_batch]['id']))
                break

            # Reconstructs the payload for the individual group retry
            batch_payload = requests.utils.quote(
                json.dumps(
                    [
                        {
                            "method": "GET",
                            "relative_url": f"{batch[group_num_in_batch]['id']}/feed?fields=id,from{{email, title, name, id, primary_address, department, locale, link}},message,permalink_url, created_time,comments{{id,from{{email, title, name, id, primary_address, department, locale, link}},message,comments{{id,from{{email, title, name, id, primary_address, department, locale, link}},message, permalink_url, created_time}} ,permalink_url, created_time}}&since={start_time}&until={end_time}"
                        }
                    ]
                )
            )
            # Sends the retry request and updates the individual response
            individual_response = make_batch_request(batch_payload, 0).json()[0]
        
        # Parses the response body
        body = json.loads(individual_response.get('body'))
        logging.info('response for group id ' + str(batch[group_num_in_batch]['id']) + ': ' + str(body))
        # Initializes the 'posts' field for storing group feed data
        batch[group_num_in_batch]['posts'] = []
        for item in body.get('data'):
            batch[group_num_in_batch]['posts'].append(item)

        # Handles pagination if more data is available
        if 'paging' in body and 'next' in body['paging']:
            logging.info('Paging for group id ' + str(batch[group_num_in_batch]['id']))
            url = body['paging']['next']
            while url:
                #response = requests.get(url).json()

                # Uses a helper method to make paginated requests with error handling
                response_json, status = https_get_call_handling_per_group(url, {"Authorization": f"Bearer {access_token}"}, batch[group_num_in_batch]['id'], 0)
                response = response_json.json()
                logging.info('Paging response: ' + str(response))
                # Appends additional paginated data to the posts list
                for item in response.get('data'):
                    batch[group_num_in_batch]['posts'].append(item)

                # Checks for further pagination or exits the loop
                if 'paging' in response and 'next' in response['paging']:
                    url = response['paging']['next']

                else:
                    url = None
    
    # Processes and extracts messages for each group
    batch_messages_processed = 0
    batch_messages_extracted = 0
    for group in batch:
        group_messages_processed, group_messages_extracted = load_group(group, group_type)
        batch_messages_processed = batch_messages_processed + group_messages_processed
        batch_messages_extracted = batch_messages_extracted + group_messages_extracted
    
    # Returns the total number of messages processed and extracted
    return batch_messages_processed, batch_messages_extracted

# Write data directly to a CSV file
def exporting_from_workplace_to_project(all_group, group_type):
    # Logs the beginning of data extraction for the specified group type
    logging.info(f'starting next phase of data extraction, to fetch information from each {group_type}')
    # Logs the total number of groups to be processed
    logging.info('total_groups:'+str(len(all_group)))
    # Splits all_group into smaller batches to process them in chunks
    group_batches = create_batches(all_group)
    # Initializes counters for messages and groups
    total_count_messages_extracted=0
    total_count_messages_processed=0
    total_count_groups_processed=0
    
    # Iterates over each batch of groups
    for batch in group_batches:
        # Retrieves and processes data for each batch
        batch_messages_processed, batch_messages_extracted = get_group_batch_data(batch, group_type)
        # Updates the total counts with values returned from processing the current batch
        total_count_messages_extracted = total_count_messages_extracted + batch_messages_extracted
        total_count_messages_processed = total_count_messages_processed + batch_messages_processed
        total_count_groups_processed = total_count_groups_processed + len(batch)
        # Logs progress after processing a batch
        logging.info('batch complete, total groups processed: ' + str(total_count_groups_processed))

    # Logs the final total count of messages extracted and processed
    logging.info('total_count_messages_extracted:'+str(total_count_messages_extracted))
    logging.info('total_count_messages_processed:'+str(total_count_messages_processed))


def elt_main():
    # Logs the date range for which data extraction is being performed
    logging.info('Date Range: ' + str(start_time) + ' to ' + str(end_time))
    
    # Fetches all groups using the get_all_groups function
    all_group = get_all_groups()
    
    # Use this if you don't want to get all groups via the API and instead read from the file
    # Optional: Uncomment this block to read group data from a file instead of fetching from the API
    '''
    with open("all_group.json", "r") as file:
        all_group = json.load(file)
    '''
    
    # Writes the fetched group data to a JSON file named "all_group.json"
    with open("all_group.json", "w") as outfile: 
        json.dump(all_group, outfile)
    
    # Fetches all sub-groups using the get_all_subgroups function with the previously fetched group data
    all_sub_groups = get_all_subgroups(all_group)
    
    # Use this if you don't want to get all sub-groups via the API and instead read from the file
    # Optional: Uncomment this block to read sub-group data from a file instead of fetching from the API
    '''
    with open("all_sub_groups.json", "r") as file:
        all_sub_groups = json.load(file)
    '''
    
    # Writes the fetched sub-group data to a JSON file named "all_sub_groups.json"
    with open("all_sub_groups.json", "w") as outfile:
        json.dump(all_sub_groups, outfile)
    
    # Exports group-level data to the project, specifying "group" as the group type
    exporting_from_workplace_to_project(all_group, "group")
    
    # Exports sub-group-level data to the project, specifying "sub-group" as the group type
    exporting_from_workplace_to_project(all_sub_groups, "sub-group")

try:
    elt_main()
except Exception:
    logging.error('something went wrong with the script',exc_info=True)