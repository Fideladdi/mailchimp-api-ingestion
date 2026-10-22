"""
MAILCHIMP CONTACT API INGESTION

Read the saved watermark, request contacts updated during that time window,
and write the raw API contact records to page files. After all pages are
written, create a _SUCCESS marker for the data engineer's copy activity and
advance the watermark. Keeping the raw records supports downstream ELT
transformations and reprocessing without another API request.
"""

import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime,timezone
import time
from logs import logging

# Resume from the last completed extraction so each run can request changes
# instead of downloading the full contact history again.
with open("watermark.json", "r") as file:
    watermark = json.load(file)

start_date = watermark['start_date']

# Capture this run's upper bound before extraction begins. Any later updates
# will fall into a future run rather than being skipped.
end_date = datetime.now(timezone.utc).isoformat(timespec='seconds')

updated_watermark = {}

# ELT landing date: keep each run's extracted contact pages together so they
# can be copied and transformed downstream without changing the source records.
timestamp = datetime.now().strftime('%Y-%m-%d')

os.makedirs(f'Contacts/{timestamp}', exist_ok=True)


# Each page is written separately; the _SUCCESS marker is created only after
# pagination finishes, so a downstream copy activity can wait for a complete
# extraction rather than reading files while they are still being written.
output_folder = f'Contacts\\{timestamp}\\mailchimp_contacts_page'
success = f'Contacts\\{timestamp}'

load_dotenv()

api_key = os.getenv('MAILCHIMP_API_KEY')

audience_id = os.getenv('audience_id')


params = {
    'count':1000,
    # Ask the API only for contacts changed since the previous watermark,
    # through the timestamp captured above.
    'updated_since':start_date,
    'updated_before':end_date
}


url  = f'https://api.mailchimp.com/3.0/audiences/{audience_id}/contacts/'
header = {'Authorization': f"Bearer {api_key}"}
page = 1

while True:
    #adding retry incase the request fails
    logging.info("Connection to server")
    for attempt in range(1,4):
        try:
            response = requests.get(
                url=url,
                timeout=30,
                headers=header,
                params=params
            )
            status_code = response.status_code
            response.raise_for_status()
            logging.info("Connected succesfully")
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            print('The request timed out or a network connection issues.')
            logging.warning(f"Page {page} timed out on attempt {attempt}")
            if attempt ==3:
                raise
            time.sleep(3)
            

        except requests.exceptions.HTTPError as e:
            # print(' Mailchimp returned an HTTP error')
            logging.warning(f"Page {page} timed out on attempt {attempt} with {status_code} error code")
            logging.warning(f"{str(e)}")
            if status_code not in [429,500, 502,503,504]:
                raise
            if attempt ==3:
                raise

            retry_after = response.headers.get('Retry-After')
            if retry_after is not None and retry_after.isdigit():  
                time.sleep(int(retry_after))
            else:
                time.sleep(3)
    
    data = response.json()
    contacts = data['contacts']
    next_page = data.get('next_cursor')
    # Land the contact objects as returned by the API. Defer cleaning and
    # reshaping to later ELT steps, preserving the source data for reprocessing.
    with open(f'{output_folder}{page}.json','w', encoding='utf-8') as file:
        json.dump(contacts,file, indent=4)
    logging.info(f"page{page} extration completed")
    params['cursor'] = next_page
    if next_page is None:
        break
    print(f'Done with page {page}')
    print(f'{len(contacts)} contacts extracted')
    page+=1
    

# This empty marker signals extraction completion to downstream orchestration;
# it is not a transformed data file.
with open(f'{success}/_SUCCESS','w', encoding='utf-8') as file:
        pass

# Advance the watermark only after every page has been written and the
# completion marker exists, so a failed run can be retried from the old value.
updated_watermark['start_date'] = end_date

# Persist the upper bound for the next run's incremental extraction window.
with open(f'watermark.json','w', encoding='utf-8') as file:
        json.dump(updated_watermark,file, indent=4)


logging.info("Extration completed")
