import os
from pyairtable import Api
from pyairtable.formulas import match
import re

# Set Airtable API Key
API_KEY = os.getenv('AIRTABLE_API_KEY')

# Base and Table IDs
BASE_ID = 'appmaoZN0UuSrbWaW'
COMPANIES_TABLE_ID = 'tblMvPqfxBLHLvUpf'
SEARCH_TABLE_ID = 'tblFRDxa0n7qYTYPO'
INVESTORS_TABLE_ID = 'tblMbgSueiqkqGnwP'
OUTPUT_TABLE_ID = 'tblfZgkeDf4IC4OEv'

# Initialize API and tables
api = Api(API_KEY)
companies_table = api.table(BASE_ID, COMPANIES_TABLE_ID)
search_table = api.table(BASE_ID, SEARCH_TABLE_ID)
investors_table = api.table(BASE_ID, INVESTORS_TABLE_ID)
output_table = api.table(BASE_ID, OUTPUT_TABLE_ID)

# Function to clean up names by removing common suffixes
def clean_name(name):
    suffixes = ["DAO", "Fund", "Network", "Capital", "Labs", "Foundation", "Ventures", "Partners", "VC", "Venture Capital"]
    name = re.sub(r'\([^)]*\)', '', name)  # Remove text inside parentheses
    name = re.sub(r'[\W_]+', ' ', name)  # Remove special characters
    name = name.strip()
    
    for suffix in suffixes:
        name = re.sub(f"\\b{suffix}\\b", '', name, flags=re.IGNORECASE)
    name = name.strip()
    
    return name if len(name) > 2 else None

# Search for company or investor in the search table
def search_record(name, table):
    # Try an exact match
    formula = match({"Name": name})
    result = table.all(formula=formula)
    
    if not result:
        # Clean name and try again
        clean_name_value = clean_name(name)
        if clean_name_value:
            formula = match({"Name": clean_name_value})
            result = table.all(formula=formula)
    
    if result:
        return result[0]['id']  # Return the first match
    else:
        # If not found, create a new record
        new_record = table.create({"Name": name})
        return new_record['id']

# Main processing function
def process_records():
    records = companies_table.all()
    
    for record in records:
        company_name = record['fields'].get('🏛 Companies/Protocols', '')
        investors_list = record['fields'].get('Investors', '').split(', ')
        
        # Search for company in the search table
        company_id = search_record(company_name, search_table)
        
        # Search for investors
        investor_ids = set()  # Use a set to ensure uniqueness
        for investor in investors_list:
            investor_id = search_record(investor, investors_table)
            investor_ids.add(investor_id)  # Add to the set (automatically handles duplicates)
        
        # Convert set back to list for the Airtable API
        investor_ids = list(investor_ids)
        
        # Convert 'Round size' from text to integer (assuming it's in plain numeric form like '16000000')
        round_size_text = record['fields'].get('Round size', '')
        round_size_value = int(round_size_text) if round_size_text.isdigit() else None  # Convert to integer if it's a valid number
        
        # Prepare data for the output table
        output_data = {
            "🏛 Companies/Protocols": [company_id],
            "Investors": investor_ids,
            "Description": record['fields'].get('Description', ''),
            "Date": record['fields'].get('Date', ''),
            "Round size": round_size_value,  # Set this as a number for currency field
            "Stage": record['fields'].get('Stage', ''),
            "Website": record['fields'].get('Website', ''),
            "Twitter (X)": record['fields'].get('Twitter (X)', ''),
            "Blog": record['fields'].get('Blog', ''),
            "Docs": record['fields'].get('Docs', ''),
            "Github": record['fields'].get('Github', ''),
            "Whitepaper": record['fields'].get('Whitepaper', ''),
            "Medium": record['fields'].get('Medium', ''),
            "Discord": record['fields'].get('Discord', ''),
        }
        
        # Create a new record in the output table
        created_record = output_table.create(output_data)
        
        if created_record:
            # Delete the record from the source table to prevent it from being triggered again
            companies_table.delete(record['id'])

if __name__ == "__main__":
    process_records()
