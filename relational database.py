class Data:
    
    def __init__(self, indices, start_date, end_date, interval, db_name):
        self.fetch_data(indices, start_date, end_date, interval)
        self.combine_data()
        self.setup_database(db_name)
        self.populate_database()
        self.fetch_and_process_data()
        self.close_connection()

    def fetch_data(self, indices, start_date, end_date, interval):
        self.data_frames = []
        for index in indices:
            df = yf.download(index, start=start_date, end=end_date, interval=interval)
            df['Index'] = index
            self.data_frames.append(df)
        
    def combine_data(self):
        if not self.data_frames:
            raise ValueError("No data retrieved for the specified indices.")
        self.all_data = pd.concat(self.data_frames)
        self.all_data.reset_index(inplace=True)
        if isinstance(self.all_data.columns, pd.MultiIndex):
            self.all_data.columns = [' '.join(col).strip() for col in self.all_data.columns.values]
        self.all_data['Date'] = pd.to_datetime(self.all_data['Date'])

    def setup_database(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS equity_indices ( 
            id INTEGER PRIMARY KEY, 
            date DATE, 
            close_price REAL, 
            index_name TEXT 
        ) 
        ''')

    def populate_database(self):
        for _, row in self.all_data.iterrows():
            close_column = f"Close {row['Index']}"
            if not (pd.isna(row['Date']) or pd.isna(row[close_column]) or pd.isna(row['Index'])):
                date_value = row['Date'].strftime('%Y-%m-%d')
                close_price_value = float(row[close_column])
                self.cursor.execute(''' 
                    INSERT INTO equity_indices (date, close_price, index_name)
                    VALUES (?, ?, ?)
                ''', (date_value, close_price_value, row['Index']))
        self.conn.commit()

    def fetch_and_process_data(self):
        self.df = pd.read_sql_query('SELECT * FROM equity_indices', self.conn)
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df = self.df.groupby(['date', 'index_name'], as_index=False).agg({'close_price': 'mean'})
        self.df['returns'] = self.df.groupby('index_name')['close_price'].pct_change()
        self.df.dropna(inplace=True)
        self.df_pivot = self.df.pivot(index='date', columns='index_name', values=['close_price', 'returns'])
        self.df_pivot.columns = ['_'.join(col).strip() for col in self.df_pivot.columns.values]

    def get_data(self):
        return self.df_pivot

    def close_connection(self):
        self.conn.close()
