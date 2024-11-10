import sqlite3
import csv
from datetime import datetime

# Path to the stock data database
DB_PATH = '../stock_data.db'

# Function to calculate average price for each publisher
def calculate_average_prices():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''SELECT publisher_code, AVG(price) AS avg_price
                      FROM stock_data
                      GROUP BY publisher_code''')

    avg_prices = cursor.fetchall()
    conn.close()

    return avg_prices

# Function to identify the highest and lowest price for each publisher
def calculate_price_extremes():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''SELECT publisher_code, MAX(price) AS max_price, MIN(price) AS min_price
                      FROM stock_data
                      GROUP BY publisher_code''')

    price_extremes = cursor.fetchall()
    conn.close()

    return price_extremes

# Function to detect trends based on average monthly or yearly performance
def calculate_yearly_trends():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''SELECT publisher_code, strftime('%Y', date) AS year,
                             AVG(price) AS avg_price
                      FROM stock_data
                      GROUP BY publisher_code, year
                      ORDER BY publisher_code, year''')

    yearly_trends = cursor.fetchall()
    conn.close()

    return yearly_trends

# Function to save data to a CSV file
def save_to_csv(filename, data, headers):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)  # Write headers
        writer.writerows(data)  # Write data
    print(f"Data saved to {filename}")

# Main function for running all analyses
def main():
    # Calculate average prices for each publisher and save to CSV
    avg_prices = calculate_average_prices()
    save_to_csv('average_prices.csv', avg_prices, headers=['Publisher Code', 'Average Price'])

    # Calculate max and min prices for each publisher and save to CSV
    price_extremes = calculate_price_extremes()
    save_to_csv('price_extremes.csv', price_extremes, headers=['Publisher Code', 'Max Price', 'Min Price'])

    # Calculate yearly trends and save to CSV
    yearly_trends = calculate_yearly_trends()
    save_to_csv('yearly_trends.csv', yearly_trends, headers=['Publisher Code', 'Year', 'Average Price'])

    print("Data analysis and export completed.")

# Run the main function
if __name__ == '__main__':
    main()
