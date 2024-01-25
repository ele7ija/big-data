import numpy as np
import os
import faust


if 'KAFKA' not in os.environ:
    brokers = "localhost:9092,localhost:9093,localhost:9094"
else:
    brokers = os.environ['KAFKA']
brokers = brokers.split(',')
print('Brokers URLs: %s' % brokers)

app = faust.App('predictor', broker=["kafka://" + broker for broker in brokers], key_serializer='json')

# monitor last NUM_POINTS points
m = {}

if 'NUM_POINTS' not in os.environ:
    NUM_POINTS = 3
else:
    NUM_POINTS = int(os.environ['NUM_POINTS'])


price_raw_topic = app.topic('price_raw', value_type=bytes)
price_topic = app.topic('price', value_type=bytes)
prediction_topic = app.topic('prediction', value_type=bytes)

prediction_channel = app.channel()


@app.agent(price_raw_topic,  sink=[price_topic])
async def pipe(prices):
    async for price in prices:
        if price['ticker'] in m:
            ticker_prices = m[price['ticker']]
            ticker_prices.append(price['price'])
            print("Last prices of ticker %s are: %s" % (price['ticker'], m[price['ticker']]))
            if len(ticker_prices) == NUM_POINTS:
                await prediction_channel.send(value=price)
        else:
            print("Adding ticker %s to cache" % price['ticker'])
            m[price['ticker']] = [price['price']]
        
        yield price

# TODO linear regression is much better since polyfit overfits.
@app.agent(prediction_channel, sink=[prediction_topic])
async def predict(prices):
    x = np.arange(NUM_POINTS)
    async for price in prices:
        ticker_prices = m[price['ticker']]
        y = np.array(ticker_prices)
        z = np.polyfit(x, y, NUM_POINTS-1)
        p = np.poly1d(z)
        prediction = p(float(NUM_POINTS))
        price['price'] = prediction
        m[price['ticker']] = ticker_prices[1:]
        print("Yielding prediction %s for ticker: %s" % (prediction, price['ticker']))
        yield price


