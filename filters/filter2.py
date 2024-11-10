import requests
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
import time
from datetime import timedelta

# URL на Македонската берза за историски податоци
BASE_URL = 'https://www.mse.mk/mk/stats/symbolhistory/'

# Врска со базата на податоци
DB_PATH = '../stock_data.db'  # Или апсолутен пат до базата на податоци

# Функција за автоматски да го земеме последниот достапен датум за дадениот издавач
def get_last_data_date(publisher_code):
    conn = sqlite3.connect(DB_PATH)  # Врска со базата
    cursor = conn.cursor()

    # Креирање на табела ако не постои
    cursor.execute('''CREATE TABLE IF NOT EXISTS stock_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        publisher_code TEXT,
                        date TEXT,
                        price TEXT,
                        max TEXT,
                        min TEXT,
                        avg TEXT,
                        percent_change TEXT,
                        quantity TEXT,
                        best_turnover TEXT,
                        total_turnover TEXT,
                        UNIQUE(publisher_code, date) ON CONFLICT REPLACE
                    )''')

    # Проверка за последниот датум
    cursor.execute("SELECT MAX(date) FROM stock_data WHERE publisher_code = ?", (publisher_code,))
    last_date = cursor.fetchone()[0]  # Земаме го последниот датум
    conn.close()

    return last_date if last_date else None

# Функција за преземање на податоците од веб-страницата на Македонската берза
def fetch_stock_data(publisher_code, from_date, to_date):
    params = {
        'FromDate': from_date,
        'ToDate': to_date,
        'Code': publisher_code
    }
    response = requests.get(BASE_URL + publisher_code, params=params)

    if response.status_code == 200:
        return response.text
    else:
        print(f"Грешка при преземање на податоците за {publisher_code}. Статусен код: {response.status_code}")
        return None

# Парсирање на HTML за податоците од таблицата
def parse_stock_table(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'id': 'resultsTable'})
    if not table:
        print("Не пронајдена таблица!")
        return []

    rows = table.find_all('tr')
    data = []

    for row in rows[1:]:  # Пропуштаме ја главната ред
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

# Функција за зачувување на податоците во базата на податоци
def save_to_database(publisher_code, data):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for record in data:
        cursor.execute('''INSERT OR REPLACE INTO stock_data (publisher_code, date, price, max, min, avg, percent_change, quantity, best_turnover, total_turnover)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            publisher_code,
            record['Date'],
            record['Price'],
            record['Max'],
            record['Min'],
            record['Avg'],
            record['Percent Change'],
            record['Quantity'],
            record['Best Turnover'],
            record['Total Turnover']
        ))

    conn.commit()
    conn.close()
    print(f"Податоците за {publisher_code} се зачувани во базата на податоци.")

# Главна функција за обработка на сите издавачи
def process_publishers(publisher_codes):
    for publisher_code in publisher_codes:
        last_date = get_last_data_date(publisher_code)

        if not last_date:
            print(f"Не се најдени податоци за {publisher_code}. Ќе преземеме сите податоци.")
            from_date = '01.01.2014'  # Започнуваме од 01.01.2014 ако нема податоци
        else:
            print(f"Последниот датум за {publisher_code}: {last_date}. Ќе преземеме податоци од {last_date}.")
            # Додадете 1 ден на последниот датум за да започнете од следниот ден
            last_date_obj = datetime.strptime(last_date, '%d.%m.%Y')
            from_date = (last_date_obj + timedelta(days=1)).strftime('%d.%m.%Y')

        # Преземете нови податоци
        to_date = datetime.now().strftime('%d.%m.%Y')
        html = fetch_stock_data(publisher_code, from_date, to_date)

        if html:
            data = parse_stock_table(html)
            if data:
                save_to_database(publisher_code, data)
            else:
                print(f"Нема податоци за {publisher_code} за периодот од {from_date} до {to_date}.")
        else:
            print(f"Не успеавме да ги преземеме податоците за {publisher_code}.")

        # Чекање од 2 секунди пред следниот издавач
        time.sleep(2)

# Функција за земање на издавачите од веб-страницата
def fetch_publisher_codes():
    url = 'https://www.mse.mk/mk/stats/symbolhistory/avk'
    response = requests.get(url)

    if response.status_code != 200:
        print("Не успеа да се преземе страницата")
        return []

    # Анализирање на HTML содржината
    soup = BeautifulSoup(response.text, 'html.parser')

    # Извлекување на сите издавачи од падот на менијата
    publisher_codes = []
    dropdown = soup.find('select', {'id': 'Code'})
    if dropdown:
        options = dropdown.find_all('option')
        for option in options:
            code = option.get('value')
            if code and code.isalpha():  # Филтрирање на кодовите што содржат само букви
                publisher_codes.append(code)

    return publisher_codes

# Главна функција
def main():
    # Преземање на издавачите
    publisher_codes = fetch_publisher_codes()

    if publisher_codes:
        print(f"Пронајдени се {len(publisher_codes)} издавачи.")
        process_publishers(publisher_codes)
    else:
        print("Не беа пронајдени издавачи.")

# Извршување на програмата
if __name__ == '__main__':
    main()
