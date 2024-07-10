from loguru import logger
import requests
from akshare.stock.cons import (
    hk_js_decode,
    zh_sina_a_stock_hist_url,
)
from py_mini_racer import py_mini_racer
from zipline.data.bundles import register

import pandas as pd
import numpy as np


def _stock_zh_a_daily(symbol, session=None):
    if not session:
        session = requests.Session()
    res = session.get(zh_sina_a_stock_hist_url.format(symbol))
    js_code = py_mini_racer.MiniRacer()
    js_code.eval(hk_js_decode)
    dict_list = js_code.call(
        "d", res.text.split("=")[1].split(";")[0].replace('"', "")
    )  # 执行js解密代码
    data_df = pd.DataFrame(dict_list)
    if not dict_list:
        return data_df
    try:
        data_df['date'] = pd.to_datetime(data_df["date"])
    except Exception as e:
        logger.info(e)
        print(data_df)
        print(res.text)
        print(dict_list)
    data_df.index = data_df.date
    return data_df

def load_equities():
    return [(0, "sz000001", "平安银行")]


def ticker_generator():
    """
    Lazily return (sid, ticker) tuple
    """
    return (v for v in load_equities())


def data_generator(sessions):
    for sid, symbol, asset_name in ticker_generator():
        df = pd.DataFrame()

        df = _stock_zh_a_daily(symbol=symbol)
        df = df.reindex(sessions.tz_localize(None), copy=False,).fillna(0.0)
        start_date = df.index[0]
        end_date = df.index[-1]

        first_traded = start_date
        auto_close_date = end_date + pd.Timedelta(days=1)
        exchange = 'SSE'

        yield (sid, df), symbol, asset_name, start_date, end_date, first_traded, auto_close_date, exchange

def metadata_frame():
    dtype = [
        ('symbol', 'object'),
        ('asset_name', 'object'),
        ('start_date', 'datetime64[ns]'),
        ('end_date', 'datetime64[ns]'),
        ('first_traded', 'datetime64[ns]'),
        ('auto_close_date', 'datetime64[ns]'),
        ('exchange', 'object'), ]
    return pd.DataFrame(np.empty(len(load_equities()), dtype=dtype))

def data_to_bundle(interval='1m'):
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir
               ):
        metadata = metadata_frame()
        logger.info(metadata)
        sessions = calendar.sessions_in_range(start_session, end_session)
        def daily_data_generator():
            return (sid_df for (sid_df, *metadata.iloc[sid_df[0]]) in data_generator(sessions))

        data = daily_data_generator()
        logger.info(metadata)
        daily_bar_writer.write(data, show_progress=True)

        metadata.dropna(inplace=True)
        asset_db_writer.write(equities=metadata)
        # adjustment_writer.write(splits=pd.read_hdf)
        # dividends do not work
        # adjustment_writer.write(dividends=pd.read_hdf)

    return ingest


register('SSE', data_to_bundle(), calendar_name='SSE')
