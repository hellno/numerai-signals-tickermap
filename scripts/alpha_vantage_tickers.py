"""
Read old ticker mapping from S3 and add alpha_vantage column.
Script might need to re-run because of API timeout after daily request limit (only for free plans)
"""
import os
from time import sleep

import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from tqdm import tqdm

tqdm.pandas()

TICKER_MAP_URL = 'https://numerai-signals-public-data.s3-us-west-2.amazonaws.com/signals_ticker_map_w_bbg.csv'
ALPHA_VANTAGE_API_KEY = 'YOUR_API_KEY'
TICKER_MAP_FNAME = 'ticker_map.csv'

ALPHA_VANTAGE_TICKER_COLUMN = '1. symbol'
SYMBOL_MATCH_CONFIDENCE_THRESHOLD = 0.9
TIMEOUT_SLEEP_SEC = 20
TIMEOUT = 'TIMEOUT'
SYMBOL_NOT_FOUND = 'SYMBOL_NOT_FOUND'

has_api_timeout = False
print(f'initialize alpha vantage api')
ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')


def get_ticker_info(ticker: str):
    try:
        data, _ = ts.get_symbol_search(ticker)
    except ValueError as err:
        print(f'got err from alpha vantage: {err}')
        return TIMEOUT

    if len(data) == 0:
        return SYMBOL_NOT_FOUND

    symbol_candidate = data.iloc[0].to_dict()
    symbol_match_score = float(symbol_candidate['9. matchScore'])
    is_confident_candidate = symbol_match_score >= SYMBOL_MATCH_CONFIDENCE_THRESHOLD

    return symbol_candidate if is_confident_candidate else SYMBOL_NOT_FOUND


def get_ticker_info_with_retry(ticker: str, retry_count: int = 4):
    retries = 0
    symbol = None

    while retries < retry_count:
        symbol = get_ticker_info(ticker)
        is_api_timeout = symbol == TIMEOUT
        if is_api_timeout:
            retries += 1
            sleep(TIMEOUT_SLEEP_SEC)
        else:
            break

    return symbol


def get_alpha_vantage_ticker_symbol_for_row(row, verbose: bool = True):
    global has_api_timeout

    if row.alpha_vantage is not None and row.alpha_vantage != TIMEOUT:
        return row.alpha_vantage

    if has_api_timeout:
        return TIMEOUT

    yahoo_ticker = row.yahoo
    if verbose:
        print(f'getting info for ticker: {yahoo_ticker}')

    ticker_info = get_ticker_info_with_retry(yahoo_ticker)
    if verbose:
        print(f'response: {ticker_info}')

    if ticker_info == SYMBOL_NOT_FOUND:
        return ticker_info
    elif ticker_info == TIMEOUT:
        print(f'got timeout after retries, might be temporarily blocked from API access')
        has_api_timeout = True
        return TIMEOUT

    return ticker_info[ALPHA_VANTAGE_TICKER_COLUMN]


def get_alpha_vantage_ticker_symbols(ticker_map):
    global has_api_timeout
    has_api_timeout = False

    ticker_map['alpha_vantage'] = ticker_map.progress_apply(get_alpha_vantage_ticker_symbol_for_row, axis=1)
    if has_api_timeout:
        ticker_timeout_count = len(ticker_map[ticker_map.alpha_vantage == TIMEOUT])
        print(f'did not finish all tickers due to API timeout ({ticker_timeout_count} tickers remaining)')


def main():
    new_ticker_map_file_exists = os.path.exists(TICKER_MAP_FNAME)
    if new_ticker_map_file_exists:
        print(f'using local ticker map from file {TICKER_MAP_FNAME}')
        ticker_map = pd.read_csv(TICKER_MAP_FNAME)
    else:
        print(f'using local ticker map from file {TICKER_MAP_FNAME}')
        ticker_map = pd.read_csv(TICKER_MAP_URL)
    if 'alpha_vantage' not in ticker_map.columns:
        ticker_map['alpha_vantage'] = None

    get_alpha_vantage_ticker_symbols(ticker_map)
    nr_alpha_vantage_tickers = len(ticker_map[
                                       ~ticker_map.alpha_vantage.isna() &
                                       (ticker_map.alpha_vantage != TIMEOUT) &
                                       (ticker_map.alpha_vantage != SYMBOL_NOT_FOUND)
                                       ])
    print(f'Alpha Vantage tickers available: {nr_alpha_vantage_tickers} '
          f'({nr_alpha_vantage_tickers / len(ticker_map) * 100:.2f}%)')

    ticker_map.to_csv(TICKER_MAP_FNAME, index=False)
    print(f'done - saved tickers to file {TICKER_MAP_FNAME}')


if __name__ == '__main__':
    main()
