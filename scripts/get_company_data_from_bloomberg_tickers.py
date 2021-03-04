#!/usr/bin/env python
"""
# Get Company Metadata from list of Bloomberg Tickers
#
# Generates an output csv file with columns company_name, address,sector, industry and bloomberg_ticker
"""
import os

from selenium.webdriver import FirefoxProfile
from tqdm import tqdm

"""
system requirements:
this script requires geckodriver to remote control firefox https://firefox-source-docs.mozilla.org/testing/geckodriver/
on macOS this is easy to do via:
`brew install geckodriver`

python package requirements:
`pip install joblib requests scrapy selenium bs4 -U`


"""
import pandas as pd

import time
from collections import Counter

from random import randint
from typing import Optional, Dict

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException

from joblib import Parallel, delayed

BLOOMBERG_BASE_URL = 'https://www.bloomberg.com/profile/company/'
TICKER_TABLE_PROPERTY = ['ADDRESS', 'SECTOR', 'INDUSTRY', ]
TICKER_TABLE_OUTPUT_FILENAME = 'company_data.csv'
N_JOBS = 1

USE_HEADLESS_BROWSER = True

NUMERAI_TICKER_MAP_URL = 'https://raw.githubusercontent.com/hellno/numerai-signals-tickermap/main/ticker_map.csv'
ROBOT_SCRAPE_WARNING = 'Are you a robot?'
SCRAPE_BLOCKED_FLAG = 'SCRAPE_BLOCKED'

NOT_FOUND_WARNING = 'no matches. Try the symbol search'
NOT_FOUND_FLAG = 'NOT_FOUND'


class BlockedScrapeException(Exception):
    pass


def get_driver():
    options = webdriver.FirefoxOptions()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                         + "AppleWebKit/537.36 (KHTML, like Gecko)"
                         + "Chrome/87.0.4280.141 Safari/537.36")

    profile = FirefoxProfile()
    profile.set_preference("intl.accept_languages", "en-US")

    if USE_HEADLESS_BROWSER:
        options.add_argument('--headless')

    return webdriver.Firefox(options=options, firefox_profile=profile)


def get_bloomberg_url_from_ticker(ticker: str):
    return f"{BLOOMBERG_BASE_URL}{ticker.replace(' ', ':')}"


def get_table_property(bs: BeautifulSoup, property: str) -> str:
    return bs.find('h2', text=property).findNext().text


def get_company_data_from_beautiful_soup(bs: BeautifulSoup) -> Dict[str, str]:
    company_data = {}

    try:
        company_data['company_name'] = bs.find('h1').text
        for table_property in TICKER_TABLE_PROPERTY:
            company_data[table_property.lower()] = get_table_property(bs, table_property)
    except AttributeError as err:
        h1 = bs.find('h1')
        print(f'failed to get company data - h1: {h1} - err: {err}')
    return company_data


def get_warning_or_none_from_beautiful_soup(bs) -> Optional[str]:
    if ROBOT_SCRAPE_WARNING in bs.text:
        return SCRAPE_BLOCKED_FLAG
    elif NOT_FOUND_WARNING in bs.text:
        return NOT_FOUND_FLAG
    else:
        return None


def scrape_company_data_from_bloomberg_ticker_using_webdriver(driver, ticker: str):
    url = get_bloomberg_url_from_ticker(ticker)
    try:
        driver.get(url)
    except TimeoutException:
        print(f'got timeout exception - will sleep 20secs - url {url}')
        time.sleep(20)
        driver.get(url)

    time.sleep(5)

    bs = BeautifulSoup(driver.page_source, 'lxml')
    warning = get_warning_or_none_from_beautiful_soup(bs)
    return get_company_data_from_beautiful_soup(bs) if warning is None else warning


def get_company_data_from_ticker(ticker: str, with_sleep: bool = True) -> Dict[str, str]:
    driver = get_driver()
    company_data = scrape_company_data_from_bloomberg_ticker_using_webdriver(driver, ticker)
    driver.close()
    del driver

    if company_data == SCRAPE_BLOCKED_FLAG:
        raise BlockedScrapeException(f'scrape was blocked for ticker {ticker}')
    elif company_data == NOT_FOUND_FLAG:
        company_data = {}
    company_data['bloomberg_ticker'] = ticker

    if with_sleep:
        time.sleep(randint(5, 15))

    return company_data


ticker_map = pd.read_csv()

output_file_exists = os.path.exists(TICKER_TABLE_OUTPUT_FILENAME)
if output_file_exists:
    df = pd.read_csv(TICKER_TABLE_OUTPUT_FILENAME)
    print(f'Found existing file with data for {len(df)} companies')
else:
    df = pd.DataFrame(dict(company_name=[], address=[], sector=[], industry=[], bloomberg_ticker=[]))

remaining_tickers_to_scrape = list(set(ticker_map.bloomberg.values) - set(df.bloomberg_ticker.values))
print(f'There are {len(remaining_tickers_to_scrape)} tickers to scrape')

results = []
should_run_parallel = N_JOBS > 1

if not should_run_parallel:
    for ticker in tqdm(remaining_tickers_to_scrape):
        try:
            company_data = get_company_data_from_ticker(ticker)
            if company_data:
                results.append(company_data)
        except BlockedScrapeException as ex:
            print('stop scraping because of exception', ex)
            break
else:
    results = Parallel(n_jobs=N_JOBS, prefer="threads")(
        delayed(get_company_data_from_ticker)(ticker) for ticker in remaining_tickers_to_scrape
    )

df_new = pd.DataFrame(results)

print(f'Scraped {len(results)} new company data with {len(df_new[df_new.company_name.isna()])} failed')
df = df.append(df_new)
df.to_csv(TICKER_TABLE_OUTPUT_FILENAME, index=False)

not_found_bloomberg_tickers = df[df.company_name.isna()].bloomberg_ticker.values

if len(not_found_bloomberg_tickers) > 0:
    print(f'Summary:\nFailed to get {len(not_found_bloomberg_tickers)} companies out of total {len(df)} {len(not_found_bloomberg_tickers) / len(df) * 100}')
    c = Counter([x.split(' ')[1] for x in not_found_bloomberg_tickers])
    print(f'Grouped by Exchange Ticker:\n{c}')
