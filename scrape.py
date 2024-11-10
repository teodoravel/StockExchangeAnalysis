import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import time

# URL of the page with the stock data
url = 'https://www.mse.mk/mk/stats/symbolhistory/alk'

# Define the date range (last 10 years)
to_date = datetime.now().strftime('%d.%m.%Y')  # Today's date
from_date = (datetime.now().replace(year=datetime.now().year - 10)).strftime('%d.%m.%Y')  # 10 years ago

# Function to fetch the stock symbols from the dropdown
def get_stock_symbols_from_dropdown():
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching the page. Status code: {response.status_code}")
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    dropdown = soup.find('select', {'id': 'Code'})
    options = dropdown.find_all('option')
    symbols = [option['value'] for option in options]
    return symbols

# Helper function to get data for each symbol
def get_data_for_symbol(symbol, from_date, to_date, retries=3, delay=5):
    data = {
        'FromDate': from_date,
        'ToDate': to_date,
        'Code': symbol
    }

    for attempt in range(retries):
        response = requests.get(url, params=data)
        if response.status_code == 200:
            print(f"Successfully fetched data for {symbol} from {from_date} to {to_date}")
            return response.text
        elif response.status_code == 503:
            print(f"Error 503 for {symbol} from {from_date} to {to_date}, retrying... (Attempt {attempt + 1}/{retries})")
            time.sleep(delay)  # Wait before retrying
        else:
            print(f"Failed to retrieve data for {symbol} from {from_date} to {to_date}. Status code: {response.status_code}")
            break
    return None

# Parse the table data
def parse_stock_table(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'id': 'resultsTable'})
    if not table:
        print("No table found!")
        return []

    rows = table.find_all('tr')
    data = []

    for row in rows[1:]:  # Skip header row
        cols = row.find_all('td')
        if len(cols) > 1:
            data.append({
                'Date': cols[0].text.strip(),
                'Price': cols[1].text.strip(),
                'Max': cols[2].text.strip(),
                'Min': cols[3].text.strip(),
                'Avg': cols[4].text.strip(),
                'Percent Change': cols[5].text.strip(),
                'Quantity': cols[6].text.strip(),
                'Best Turnover': cols[7].text.strip(),
                'Total Turnover': cols[8].text.strip()
            })
    return data

# Function to save data to CSV
def save_to_csv(symbol, data):
    filename = f"{symbol}_stock_data.csv"
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['Date', 'Price', 'Max', 'Min', 'Avg', 'Percent Change', 'Quantity', 'Best Turnover', 'Total Turnover'])
        writer.writeheader()
        writer.writerows(data)
    print(f"Data saved to {filename}")

# Main function to scrape data for all symbols
def scrape_data():
    symbols = get_stock_symbols_from_dropdown()

    for symbol in symbols:
        all_data = []

        # Loop through each year in the last 10 years
        for year in range(datetime.now().year - 10, datetime.now().year):
            from_date = f"01.01.{year}"
            to_date = f"31.12.{year}"

            html = get_data_for_symbol(symbol, from_date, to_date)

            if html:
                data = parse_stock_table(html)
                if data:
                    all_data.extend(data)
                else:
                    print(f"No data found for {symbol} in {year}.")
            else:
                print(f"Error fetching data for {symbol} in {year}.")

        if all_data:
            save_to_csv(symbol, all_data)
        else:
            print(f"No data found for {symbol} in the last 10 years.")

# Run the scraper
scrape_data()
