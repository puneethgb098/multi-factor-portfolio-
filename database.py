import sqlite3
import yfinance as yf
import pandas as pd

indices = ['^GSPC', '^FTSE', '^N225']
data_frames = []

for index in indices:
    df = yf.download(index, start='2010-01-01', end='2023-01-01', interval='1mo')
    df['Index'] = index
    data_frames.append(df)

all_data = pd.concat(data_frames)
all_data.reset_index(inplace=True)

all_data['Date'] = pd.to_datetime(all_data['Date'])
conn = sqlite3.connect('equity_indices.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS equity_indices (
    id INTEGER PRIMARY KEY,
    date DATE,
    close_price REAL,
    index_name TEXT
)
''')

for _, row in all_data.iterrows():
    if not (pd.isna(row['Date']) or pd.isna(row['Close']) or pd.isna(row['Index'])):
        date_value = row['Date'].strftime('%Y-%m-%d') 
        close_price_value = float(row['Close']) 
        index_name_value = str(row['Index'])  
        
        cursor.execute('''
        INSERT INTO equity_indices (date, close_price, index_name)
        VALUES (?, ?, ?)
        ''', (date_value, close_price_value, index_name_value))

conn.commit()
cursor.execute('SELECT * FROM equity_indices')

results = cursor.fetchall()
for row in results:
    print(row)

conn.close()
