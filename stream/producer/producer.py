import os
import sys
import json
import requests
import time
import random
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

import yliveticker
import yfinance
import pandas as pd

brokers = os.environ["KAFKA"].split(',')
print('Brokers URLs: %s' % brokers)
EMIT_INTERVAL = os.environ['DUMMY_EMIT_INTERVAL']
if EMIT_INTERVAL == "":
    EMIT_INTERVAL = 10
else:
    EMIT_INTERVAL = int(EMIT_INTERVAL)

def create_topics():
    admin_client = KafkaAdminClient(bootstrap_servers=brokers, client_id='test')

    topic_list = []
    topic_list.append(NewTopic(name="price", num_partitions=1, replication_factor=3))
    topic_list.append(NewTopic(name="price_raw", num_partitions=1, replication_factor=3))
    topic_list.append(NewTopic(name="prediction", num_partitions=1, replication_factor=3))
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except Exception as e:
        print("Failed to add topic reason: {}".format(e))

def get_tickers():
    link = (
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#S&P_500_component_stocks"
    )
    df = pd.read_html(link, header=0)[0]
    return df['Symbol'].head(20).tolist()

def on_new_msg(ws, msg):
    # TODO
    producer = KafkaProducer(bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print('Received: %s' % msg)
    json_data = {'time': time.time(), 'price': msg["Close"], 'ticker': msg['Ticker']}
    try:
        producer.send('price_raw', json_data)
    except Exception as e:
        print('Sending data failed: %s' % str(e))
    print('Sent: %s' % json_data)

def start_streaming(tickers):
    yliveticker.YLiveTicker(on_ticker=on_new_msg, ticker_names=tickers)

def download_and_stream(tickers):
    producer = KafkaProducer(bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    df = yfinance.download(tickers, interval='1m', start='2024-01-16', end='2024-01-20', rounding=True, auto_adjust=True)
    for index, row in df.iterrows():
        now = time.time()
        for t in tickers:
            price = row[('Close', t)]
            if pd.isnull(price):
                continue

            json_data = {'time': time.time(), "ticker": t, 'price': price}
            print('Emitting %s' % json_data)
            producer.send('price_raw', json_data)
        to_sleep = EMIT_INTERVAL - time.time() + now
        print('Sleeping %s' % to_sleep)
        time.sleep(to_sleep)


if __name__ == "__main__":
    real = True if os.environ['STREAM_MODE'] == 'REAL' else False
    create_topics()
    tickers = get_tickers()
    print('Will be getting %s tickers' % tickers)
    if real:
        start_streaming(tickers)
    else:
        download_and_stream(tickers)
