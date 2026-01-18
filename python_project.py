import pandas as pd
import requests

print("Libraries imported successfully.")

resource_id = '19eac040-0b94-49fa-b239-4f2fd8677d53' # This Resource ID was found by checking the API documentation for the dataset on data.gov.in
api_key='579b464db66ec23bdd00000144bab08a34614d71405562fe03c815b5'
api_url = f'https://api.data.gov.in/resource/{resource_id}?api-key={api_key}&format=json&limit=100000'

try:
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = response.json()

    # The actual data is usually nested under a 'records' key or similar
    records = data.get('records', [])
    if not isinstance(records, list) or len(records) == 0:
        print("No records found in API response. Keys present:", list(data.keys()) if isinstance(data, dict) else type(data))
    else:
        df = pd.json_normalize(records)
        print("Data loaded successfully into DataFrame.")
        print("\nFirst 5 rows of the DataFrame:")
        print(df.head())
        print("\nDataFrame Info:")
        df.info()

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
    print("Please ensure the API URL is correct and check your internet connection or API key if required.")
except ValueError as e:
    print(f"Error parsing JSON response: {e}")
    print("The API might not be returning valid JSON. Please check the API response content.")
except KeyError as e:
    print(f"Error processing JSON structure: {e}")
    print("The expected 'records' key was not found or the JSON structure is different. Adjust parsing logic.")