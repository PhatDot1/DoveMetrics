import os
import requests
import json
import time
import csv
import re
from datetime import datetime, timedelta

# Load API tokens from environment variables
API_TOKEN = os.getenv('WEB_SCRAPER_API_TOKEN')  # Web Scraper API token
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')  # Airtable API token

# Web Scraper API details
SITEMAP_ID = '1046174'
MODIFY_URL = f'https://api.webscraper.io/api/v1/sitemap/{SITEMAP_ID}?api_token={API_TOKEN}'
SCRAPING_JOB_URL = f'https://api.webscraper.io/api/v1/scraping-job?api_token={API_TOKEN}'
HEADERS = {'Content-Type': 'application/json'}

# Airtable API details
BASE_ID = 'appmaoZN0UuSrbWaW'
TABLE_ID = 'tblfZgkeDf4IC4OEv'

# Calculate the required dates
today = datetime.today()
start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
end_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')

# Modified JSON data for the sitemap
modified_json = {
    "_id": "cypto-fundraising",
    "startUrl": [f"https://crypto-fundraising.info/deal-flow/?start_date={start_date}&end_date={end_date}&sortby=date&sort=desc"],
    "selectors": [
        {"id": "Pagination", "paginationType": "auto", "parentSelectors": ["_root", "Pagination"], "selector": "a.next", "type": "SelectorPagination"},
        {"id": "Project", "linkType": "linkFromAttributes", "multiple": True, "parentSelectors": ["Pagination"], "selector": "a.t-project-link", "type": "SelectorLink"},
        {"id": "Description", "multiple": False, "parentSelectors": ["Project"], "regex": "", "selector": ".dt-only div.project-description", "type": "SelectorText"},
        {"id": "Raised Amount", "multiple": False, "parentSelectors": ["Project"], "regex": "", "selector": "span.abbrusd", "type": "SelectorText"},
        {"id": "Date", "multiple": False, "parentSelectors": ["Project"], "regex": "", "selector": "div.raisedin", "type": "SelectorText"},
        {"id": "Round", "multiple": False, "parentSelectors": ["Project"], "regex": "", "selector": "div.roundtype", "type": "SelectorText"},
        {"extractAttribute": "title", "id": "Investors", "parentSelectors": ["Project"], "selector": "a.investlogo-newrised", "type": "SelectorGroup"},
        {"id": "Website", "linkType": "linkFromHref", "multiple": False, "parentSelectors": ["Project"], "selector": "div.sidewebsites a:not([href*='github']):not([href*='linkedin']):not([href*='discord']):not([href*='twitter']):not([href*='medium']):not([href*='blog']):not([href*='docs']):not([href*='reddit'])", "type": "SelectorLink"},
        {"id": "Twitter", "linkType": "linkFromHref", "multiple": False, "parentSelectors": ["Project"], "selector": ".community a[href*='twitter']", "type": "SelectorLink"},
        {"id": "blog", "linkType": "linkFromHref", "multiple": False, "parentSelectors": ["Project"], "selector": "a[href*='blog']", "type": "SelectorLink"},
        {"id": "docs", "linkType": "linkFromHref", "multiple": False, "parentSelectors": ["Project"], "selector": "a[href*='docs']", "type": "SelectorLink"},
        {"id": "medium", "linkType": "linkFromHref", "multiple": False, "parentSelectors": ["Project"], "selector": "a[href*='medium']", "type": "SelectorLink"},
        {"id": "reddit", "linkType": "linkFromHref", "multiple": False, "parentSelectors": ["Project"], "selector": "a[href*='reddit']", "type": "SelectorLink"},
        {"id": "discord", "linkType": "linkFromHref", "multiple": False, "parentSelectors": ["Project"], "selector": ".community a[href*='discord']", "type": "SelectorLink"},
        {"id": "github", "linkType": "linkFromHref", "multiple": False, "parentSelectors": ["Project"], "selector": "a[href*='github']", "type": "SelectorLink"},
        {"id": "Whitepaper", "linkType": "linkFromHref", "multiple": True, "parentSelectors": ["Project"], "selector": "a.linkwithicon[href*='docsend'], a.linkwithicon[href*='docs'], a.linkwithicon[href*='pdf']", "type": "SelectorLink"}
    ]
}

# Modify the sitemap
response = requests.put(MODIFY_URL, headers=HEADERS, data=json.dumps(modified_json))

if response.status_code == 200:
    print('Sitemap modified successfully.')

    # Create a scraping job
    scraping_job_data = {
        "sitemap_id": SITEMAP_ID,
        "driver": "fast",  # You can change this to "fulljs" if needed
        "page_load_delay": 2000,
        "request_interval": 2000,
        "proxy": 0,  # Change this if you need to use a proxy
    }

    trigger_response = requests.post(SCRAPING_JOB_URL, headers=HEADERS, data=json.dumps(scraping_job_data))

    if trigger_response.status_code == 200:
        print('Scraping job started successfully.')

        # Extract the scraping job ID from the response
        scraping_job_id = trigger_response.json().get('data', {}).get('id')
        print(f'Scraping Job ID: {scraping_job_id}')

        # Wait for 10 minutes (600 seconds)
        print('Waiting for 10 minutes before checking the scraping job status...')
        time.sleep(600)

        # Poll the scraping job status until it's finished
        job_finished = False
        while not job_finished:
            get_job_url = f'https://api.webscraper.io/api/v1/scraping-job/{scraping_job_id}?api_token={API_TOKEN}'
            job_response = requests.get(get_job_url, headers=HEADERS)

            if job_response.status_code == 200:
                job_data = job_response.json().get('data', {})
                print('Scraping job details:')
                print(json.dumps(job_data, indent=4))

                # Check the status of the job
                if job_data['status'] == 'finished':
                    job_finished = True
                    print('Scraping job finished successfully.')

                    # Fetch the scraped data
                    if job_data.get('stored_record_count', 0) > 0:
                        get_data_url = f'https://api.webscraper.io/api/v1/scraping-job/{scraping_job_id}/csv?api_token={API_TOKEN}'
                        data_response = requests.get(get_data_url, headers=HEADERS)

                        if data_response.status_code == 200:
                            # Save the CSV content to a file
                            with open('scraping_results.csv', 'w', newline='', encoding='utf-8') as file:
                                file.write(data_response.text)
                            print('Scraping results saved to scraping_results.csv')
                        else:
                            print(f'Failed to retrieve scraping results. Status code: {data_response.status_code}')
                            print(data_response.text)
                    else:
                        print('No data was stored by the scraping job.')
                else:
                    print(f'Scraping job is still in progress. Status: {job_data["status"]}')
                    time.sleep(300)  # Wait for 5 minutes before checking again
            else:
                print(f'Failed to get scraping job details. Status code: {job_response.status_code}')
                print(job_response.text)
                break

    else:
        print(f'Failed to start scraping job. Status code: {trigger_response.status_code}')
        print(trigger_response.text)

else:
    print(f'Failed to modify sitemap. Status code: {response.status_code}')
    print(response.text)

def convert_to_number_with_dollar_sign(amount):
    """Convert amount string like '$3M' or '$4k' to a full numeric value with a dollar sign."""
    if isinstance(amount, str) and len(amount) > 1:
        if amount.endswith('M'):
            return "$" + f"{float(amount[1:-1]) * 1000000:,.2f}"
        elif amount.endswith('k'):
            return "$" + f"{float(amount[1:-1]) * 1000:,.2f}"
        elif amount.startswith('$'):
            return amount  # already in the correct format
    elif isinstance(amount, (int, float)):
        return "$" + f"{amount:,.2f}"
    return amount  # if it's not a recognized format

def clean_text_field(text):
    """Remove extra spaces, text in brackets, periods, and specific unwanted URLs from the text."""
    if isinstance(text, str):
        # Remove text within brackets (and the brackets themselves)
        text = re.sub(r'\s*\(.*?\)', '', text)
        # Remove periods
        text = text.replace('.', '')
        # Remove specific unwanted URL
        text = text.replace('https://crypto-fundraising.info/blog/', '')
        # Remove leading/trailing spaces and collapse multiple spaces into one
        text = re.sub(r'\s+', ' ', text).strip()
    return text

def process_data(input_csv):
    # Read data from the CSV file
    with open(input_csv, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        data = list(reader)
    
    # Process each row (skip the first row as it contains the header)
    cleaned_data = []
    for i in range(1, len(data)):
        row = data[i]

        # Clean 'Project' and 'Investors' fields
        project_name = clean_text_field(row[0])
        investor_names = clean_text_field(row[6])

        # Convert the raised amount to numerical value with dollar sign
        raised_amount = convert_to_number_with_dollar_sign(row[3])

        # Clean other fields as well
        description = clean_text_field(row[2])
        website_href = clean_text_field(row[7])
        twitter_href = clean_text_field(row[9])
        blog_href = clean_text_field(row[11])
        docs_href = clean_text_field(row[13])
        github_href = clean_text_field(row[21])
        whitepaper_href = clean_text_field(row[23])
        medium_href = clean_text_field(row[15])
        discord_href = clean_text_field(row[19])

        # Collect cleaned data in a dictionary
        record = {
            '🏛 Companies/Protocols': project_name,
            'Date': row[4],
            'Round size': raised_amount,
            'Investors': investor_names,
            'Stage': row[5],
            'Description': description,
            'Website': website_href,
            'Twitter (X)': twitter_href,
            'Blog': blog_href,
            'Docs': docs_href,
            'Github': github_href,
            'Whitepaper': whitepaper_href,
            'Medium': medium_href,
            'Discord': discord_href
        }

        cleaned_data.append(record)
    
    return cleaned_data

def upload_to_airtable(cleaned_data, api_key, base_id, table_id):
    url = f'https://api.airtable.com/v0/{base_id}/{table_id}'
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

    for record in cleaned_data:
        airtable_data = {'fields': record}
        response = requests.post(url, json=airtable_data, headers=headers)
        
        if response.status_code == 200:
            print(f"Record added successfully: {record['🏛 Companies/Protocols']}")
        else:
            print(f"Failed to add record: {record['🏛 Companies/Protocols']}")
            print(f"Response: {response.text}")

# Process the data
if os.path.exists('scraping_results.csv'):
    cleaned_data = process_data('scraping_results.csv')
    # Upload to Airtable
    upload_to_airtable(cleaned_data, AIRTABLE_API_KEY, BASE_ID, TABLE_ID)
else:
    print("No CSV file found, skipping Airtable upload.")
