import streamlit as st
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import time
from typing import Dict, Any

import pytz
import datetime
def change_time(uinxtime):
    timestamp = uinxtime/1000 # convert to seconds
    utc_time = datetime.datetime.utcfromtimestamp(timestamp)
    ist_time = utc_time + datetime.timedelta(hours =5  , minutes=30)
    return ist_time.strftime('%Y-%m-%d %H:%M:%S')





# Set up Streamlit app
st.set_page_config(page_title='Cryptocurrency Prices', page_icon=':money_with_wings:', layout='wide')
st.title('Cryptocurrency Prices')

# Define function to fetch data from SQLite3 database
def fetch_data(coin: str) -> pd.DataFrame:
    # Connect to database
    conn = sqlite3.connect('crypto2.db')
    
    # Fetch last 25 records for the specified coin
    query = f"SELECT * FROM {coin.lower()} WHERE timestamp >= strftime('%s', 'now') - 3600 ORDER BY timestamp DESC"
    df = pd.read_sql_query(query, conn)
    
    # Close database connection
    conn.close()
    
    return df

# Define function to create line chart
def create_line_chart(df: pd.DataFrame, coin: str) -> None:
    fig, ax = plt.subplots()
    ax.plot(df['timestamp'], df['price'])
    ax.set_xlabel('Timestamp')
    ax.set_ylabel('Price (USD)')
    ax.set_title(f'{coin} - Last Hour')
    
    # Convert chart to base64 for display in Streamlit
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    plt.close(fig)
    img_str = base64.b64encode(buffer.getvalue()).decode()

    # Display chart in Streamlit
    st.image(f"data:image/png;base64,{img_str}")

# Define function to calculate total volume and total price per table
def calculate_totals(df: pd.DataFrame) -> Dict[str, Any]:
    avg_volume = df['volume'].mean()
    avg_price = (df['volume'] * df['price']).mean()
    timestamp = df['timestamp'].iloc[0]
    timestamp_str = change_time(timestamp)
    return {'total_volume': avg_volume, 'total_price': avg_price, 'timestamp_str': timestamp_str}

# Define list of supported coins
coins = ['Bitcoin', 'Ethereum', 'Tether', 'Dogecoin', 'Others']

# Create dropdown menu to select coin
coin = st.selectbox('Select a coin', coins)

# Fetch data and create line chart
df = fetch_data(coin)
create_line_chart(df, coin)

# Calculate and display totals
totals = calculate_totals(df)
st.write(f"Avg Volume: {totals['total_volume']:.2f}")
st.write(f"Avg Price: {totals['total_price']:.2f}")
st.write(f"Timestamp: {totals['timestamp_str']} (IST)")
