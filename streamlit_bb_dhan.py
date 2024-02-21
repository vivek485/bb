import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import plotly.graph_objects as go
import numpy as np
import time
import ta
from ta.volatility import bollinger_hband,bollinger_lband
import asyncio
import aiohttp

import nest_asyncio
from list import symbols, symbol200




st.set_page_config(layout="wide")
st.title("Bb Screener")
dt = st.number_input(label='Days_back for signal', min_value=1, max_value=50)
tframe = st.number_input(label='Timeframe', min_value=1, max_value=50000)
st.text('Enter 1 min,3 min 5 min 15 min,60 min ,240 min ,1440 min,10080 min,43800 min')
dy_back = st.number_input(label='Daysback', min_value=1, max_value=5000)
st.text('Enter 500 for 1day')

myst = []

dic_buy = {'buy_symbol': [], 'buydate': []}
dic_sell = {'sell_symbol': [], 'selldate': []}
interval = tframe  # enter 15,60,240,1440,10080,43800
dayback = dy_back
ed = datetime.now()
stdate = ed - timedelta(dayback)


def conunix(ed):
    ed1 = str(round(time.mktime(ed.timetuple())))
    ed1 = (ed1[:-1])
    ed1 = (ed1 + '0000')
    return ed1


fromdate = conunix(stdate)
todate = conunix(ed)
stt = time.time()


async def getdata(session, stock):
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:96.0) Gecko/20100101 Firefox/96.0',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.5',
        # 'Accept-Encoding': 'gzip, deflate, br'
    }
    url = f'https://groww.in/v1/api/charting_service/v2/chart/exchange/NSE/segment/CASH/{stock}?endTimeInMillis={todate}&intervalInMinutes={interval}&startTimeInMillis={fromdate}'
    async with session.get(url, headers=headers) as response:
        try:
            resp = await response.json()
            candle = resp['candles']
            dt = pd.DataFrame(candle)
            fd = dt.rename(columns={0: 'time', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume'})
            tim = []
            for each in fd['time']:
                a = each
                a = datetime.fromtimestamp(a).strftime('%Y-%m-%d %H:%M:%S')
                tim.append(a)
            dt = pd.DataFrame(tim)
            fd = pd.concat([dt, fd['time'], fd['Open'], fd['High'], fd['Low'], fd['Close'], fd['Volume']],
                           axis=1).rename(columns={0: 'datetime'})
            fd['symbol'] = stock
            pd.options.mode.chained_assignment = None
            final_df = fd

            final_df['Open'] = final_df['Open'].astype(float)
            final_df['Close'] = final_df['Close'].astype(float)
            final_df['High'] = final_df['High'].astype(float)
            final_df['Low'] = final_df['Low'].astype(float)
            final_df['Volume'] = final_df['Volume'].astype(float)
            final_df['datetime'] = final_df['datetime'].astype('datetime64[ns]')
            final_df.set_index(final_df.datetime, inplace=True)

            final_df['prev1open'] = final_df['Open'].shift(1)
            final_df['prev1close'] = final_df['Close'].shift(1)

            final_df['CloseH'] = (final_df['Open'] + final_df['High'] + final_df['Low'] + final_df['Close']) / 4
            final_df['OpenH'] = (final_df['prev1open'] + final_df['prev1close']) / 2
            final_df['HighH'] = final_df['High']
            final_df['LowH'] = final_df['Low']

            final_df.drop(['time', 'datetime'], axis=1, inplace=True)

            final_df['bbh'] = round(ta.volatility.bollinger_hband(close=final_df['CloseH'], window=100, window_dev=3))
            final_df['bbl'] = round(ta.volatility.bollinger_lband(close=final_df['CloseH'], window=100, window_dev=3))

            final_df['symbol'] = stock

            # conditions
            final_df['sig_buy'] = np.where((final_df.CloseH < final_df.bbl), 1, 0)
            final_df['sig_sell'] = np.where((final_df.CloseH > final_df.bbh), 2, 0)
            df_buy = (final_df[final_df['sig_buy'] > 0])
            df_sell = (final_df[final_df['sig_sell'] > 0])
            stt = ed - timedelta(days=2)
            df_buy1 = df_buy[df_buy.index > stt]
            df_sell1 = df_sell[df_sell.index > stt]
            if len(df_buy1) > 0:
                g = (df_buy1.iloc[-1].symbol)
                dic_buy['buy_symbol'].append(g)
                dic_buy['buydate'].append(df_buy1.iloc[-1].name)

                fig = go.Figure(data=[go.Candlestick(x=final_df.index, open=final_df.OpenH, close=final_df.CloseH, high=final_df.HighH,low=final_df.LowH,name=stock),
                                      go.Scatter(x=final_df.index, y=final_df.bbh, line=dict(color='blue', width=1),name='Upperband'),

                                      go.Scatter(x=final_df.index, y=final_df.bbl, line=dict(color='blue', width=1), name='Lowerband')])

                fig.update_layout(autosize=False, width=1800, height=800, xaxis_rangeslider_visible=False)
                fig.layout.xaxis.type = 'category'
                st.title(g )
                st.write(g +' buymy_bb stratergy')
                st.plotly_chart(fig)


                #


            if len(df_sell1) > 0:
                h = (df_sell1.iloc[-1].symbol)
                dic_sell['sell_symbol'].append(h)
                dic_sell['selldate'].append(df_sell1.iloc[-1].name)
                fig = go.Figure(data=[
                    go.Candlestick(x=final_df.index, open=final_df.OpenH, close=final_df.CloseH, high=final_df.HighH,
                                   low=final_df.LowH, name=stock),
                    go.Scatter(x=final_df.index, y=final_df.bbh, line=dict(color='blue', width=1), name='Upperband'),

                    go.Scatter(x=final_df.index, y=final_df.bbl, line=dict(color='blue', width=1), name='Lowerband')])

                fig.update_layout(autosize=False, width=1800, height=800, xaxis_rangeslider_visible=False)
                fig.layout.xaxis.type = 'category'
                st.title(h)
                st.write(h +' sellb stratergy')
                st.plotly_chart(fig)
            last_candle = final_df.iloc[-1]

            if last_candle['sig_buy'] == 1:
                print(last_candle['symbol'] + ' buymy_bb_stratergy  ')
                myst.append(last_candle['symbol'])

            if last_candle['sig_sell'] == 2:
                print(last_candle['symbol'] + ' sellmy_bb_stratergy  ')
                myst.append(last_candle['symbol'])






            return
        except:
            pass


async def main():
    async with aiohttp.ClientSession() as session:

        tasks = []
        for stocks in symbols:
            try:
                stock = stocks

                task = asyncio.ensure_future(getdata(session, stock))

                tasks.append(task)
            except:
                pass
        df = await asyncio.gather(*tasks)
        # print(df)


nest_asyncio.apply()
button = st.button(label='Run_bb_channel', key='bb')
if button:
    asyncio.run(main())
    st.write(pd.DataFrame(myst))
    bcdate = ed - timedelta(dt)

    d_buydf = pd.DataFrame(dic_buy)

    d_selldf = pd.DataFrame(dic_sell)

    col1, col2 = st.columns(2, gap='small')

    col1.write(d_buydf[d_buydf.buydate > bcdate])
    col2.write(d_selldf[d_selldf.selldate > bcdate])

# print('mykelter')
# print(myst)
# bcdate = ed - timedelta(1)
# print(bcdate)
#
# d_buydf = pd.DataFrame(dic_buy)
#
#
# d_selldf = pd.DataFrame(dic_sell)
#
# print(d_buydf[d_buydf.buydate > bcdate])
# print(d_selldf[d_selldf.selldate > bcdate])
#
# ett = time.time()
# totaltime = ett - stt
# wait = 100 - totaltime
# print(('time taken  '), totaltime)
# print('sleeping')
# time.sleep(wait)

#
# fig = go.Figure(data=[
#     go.Candlestick(x=final_df.index, open=final_df.Open, close=final_df.Open, high=final_df.High, low=final_df.Low,
#                    name=stock),
#     go.Scatter(x=final_df.index, y=final_df.ma200, line=dict(color='red', width=1), name='Ma200'),
#     go.Scatter(x=final_df.index, y=final_df.highband, line=dict(color='blue', width=1), name='Upperband'),
#     go.Scatter(x=final_df.index, y=final_df.middleband, line=dict(color='blue', width=1), name='Middleband'),
#     go.Scatter(x=final_df.index, y=final_df.lowerband, line=dict(color='blue', width=1), name='Lowerband')])
# fig.update_layout(autosize=False, width=1800, height=800, xaxis_rangeslider_visible=False)
# fig.layout.xaxis.type = 'category'
# st.title(stock)
# st.plotly_chart(fig)