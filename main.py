import pandas.io.sql
import requests
from bs4 import BeautifulSoup
from pprint import pprint
import json
import sys
from datetime import datetime
import pandas as pd
import sqlite3
from time import sleep

sys.setrecursionlimit(10000)
NEWS_URL = 'https://www.binance.com/ru/support/announcement/c-49?navId=49'


def get_delist_rec(_obj, _news):
    if isinstance(_obj, dict):
        if 'title' in _obj.keys() and (
                _obj.get('title').lower().find('delist') > 0 or _obj.get('title').lower().find('делист') > 0):
            _item = {'title': _obj.get('title'),
                     'time': datetime.fromtimestamp(_obj.get('releaseDate') / 1e3).strftime('%d.%m.%Y')
                     }
            _news.append(_item)
        else:
            for key in _obj.keys():
                child = _obj.get(key)
                get_delist_rec(child, _news)
    elif isinstance(_obj, list):
        for child in _obj:
            get_delist_rec(child, _news)


def get_binance_news():
    url = NEWS_URL
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')

    script = soup.find('script', {'id': '__APP_DATA'}).text
    data = json.loads(script)
    news = []
    get_delist_rec(data, news)
    return pd.DataFrame(news)


def send_to_telegram(df):
    pprint(df)


if __name__ == '__main__':
    while True:
        df = get_binance_news()
        con = sqlite3.connect('news.db')
        try:
            df_sql = pd.read_sql('SELECT * FROM news', con)
        except pandas.io.sql.DatabaseError:
            df_sql = pd.DataFrame(columns=['title', 'time'])
        df.to_sql('news', con=con, if_exists='replace', index=False)
        df = df.merge(df_sql, on=['title', 'time'], how='left', indicator=True)
        df = df[df['_merge'] == 'left_only'].drop('_merge', axis=1)
        send_to_telegram(df)
        sleep(300)