"""
Read old ticker mapping from S3 and add alpha_vantage column.
Script might need to re-run because of API timeout after daily request limit (only for free plans)
"""
from typing import Optional

import numerapi
import pandas as pd

TICKER_MAP_URL = 'https://numerai-signals-public-data.s3-us-west-2.amazonaws.com/signals_ticker_map_w_bbg.csv'
TICKER_MAP_FNAME = 'ticker_map.csv'
SYMBOL_NOT_FOUND = 'SYMBOL_NOT_FOUND'

UNSUPPORTED_MARKET_SUFFIXES = [
    ' PW', ' CP', ' TB', ' HB', ' IT', ' ID', ' PM',
    ' SP', ' FH', ' AV', ' GA', ' DC', ' NZ', ' TI',
    ' NO', ' SM', ' IJ', ' MF', ' SJ', ' AU', ' SW',
    ' SS', ' KS', ' MK', ' IM', ' JP', ' TT', ' HK',
]


def is_available_on_alpha_vantage(ticker: str) -> bool:
    return not any(ticker.endswith(market_suffix) for market_suffix in UNSUPPORTED_MARKET_SUFFIXES)


def get_alpha_vantage_suffix_for_bloomberg_ticker(ticker: str) -> Optional[str]:
    if ticker.endswith(' US'):
        return ''
    elif ticker.endswith(' NA'):
        return '.AMS'
    elif ticker.endswith(' CA'):
        return '.TRT'
    elif ticker.endswith(' BB'):
        return '.BRU'
    elif ticker.endswith(' FP'):
        return '.PAR'
    elif ticker.endswith(' GR'):
        return '.DEX'
    elif ticker.endswith(' PL'):
        return '.LIS'
    elif ticker.endswith(' LN'):
        return '.LON'
    elif ticker.endswith(' BZ'):
        return '.SAO'
    else:
        return None


def convert_bloomberg_ticker_to_alpha_vantage_format(ticker: str) -> str:
    if ticker.endswith('/A.LON'):
        ticker = ticker.replace('/', '-')
    if ticker.endswith('LON'):
        ticker = ticker.replace('/', '')
        ticker = ticker.replace('*', '')
    if '/' in ticker:
        ticker = ticker.replace('/', '-')
    return ticker


def get_alpha_vantage_ticker_for_row(row: pd.Series) -> str:
    if not row['is_available_on_alpha_vantage']:
        return SYMBOL_NOT_FOUND

    bloomberg_ticker = row['bloomberg_ticker']
    ticker = row['ticker']

    suffix = get_alpha_vantage_suffix_for_bloomberg_ticker(bloomberg_ticker)
    if suffix is not None:
        ticker = ticker + suffix
        ticker = convert_bloomberg_ticker_to_alpha_vantage_format(ticker)
    else:
        ticker = SYMBOL_NOT_FOUND
    return ticker


def get_public_numerai_ticker_map() -> pd.DataFrame:
    # current public ticker map includes bloomberg to yahoo tickers

    ticker_map = pd.read_csv(TICKER_MAP_URL)
    ticker_map.drop('ticker', axis=1, inplace=True)

    ticker_map['yahoo'] = ticker_map['yahoo'].str.replace('/.', '.', regex=False)
    ticker_map.loc[ticker_map['yahoo'] == 'BT/A.L', 'yahoo'] = 'BT-A.L'
    return ticker_map


def print_ticker_map_stats(ticker_map: pd.DataFrame):
    for column in ticker_map.columns:
        nr_tickers_not_found = len(ticker_map[ticker_map[column] == SYMBOL_NOT_FOUND])
        coverage_percentage = 100 * (1 - nr_tickers_not_found / len(ticker_map))
        info = f'({nr_tickers_not_found} of {len(ticker_map)} tickers unavailable)' if nr_tickers_not_found else ''
        print(f'{column} covers {coverage_percentage:.2f}% {info}')


def main():
    numerai_tickers = pd.DataFrame(numerapi.SignalsAPI().ticker_universe(), columns=['bloomberg_ticker'])
    public_ticker_map = get_public_numerai_ticker_map()
    unique_bloomberg_tickers = list(set(numerai_tickers.bloomberg_ticker.tolist() +
                                        public_ticker_map.bloomberg_ticker.tolist()))

    print(f'Tickers in current Numerai Signals tournament: {len(numerai_tickers)}')
    print(f'Tickers in public numerai mapping: {len(public_ticker_map)}')
    print(f'Total number of unique bloomberg tickers: {len(unique_bloomberg_tickers)}')

    ticker_map = pd.DataFrame({'bloomberg_ticker': unique_bloomberg_tickers, 'alpha_vantage': None})
    ticker_map['ticker'] = ticker_map.bloomberg_ticker.str[:-3]
    ticker_map['is_available_on_alpha_vantage'] = ticker_map.bloomberg_ticker.apply(is_available_on_alpha_vantage)
    ticker_map['alpha_vantage'] = ticker_map.apply(get_alpha_vantage_ticker_for_row, axis=1)
    ticker_map.drop(['ticker', 'is_available_on_alpha_vantage'], axis=1, inplace=True)

    ticker_map = ticker_map.merge(public_ticker_map, how='left', on='bloomberg_ticker')
    ticker_map.yahoo.fillna(SYMBOL_NOT_FOUND, inplace=True)
    print_ticker_map_stats(ticker_map)
    ticker_map.to_csv(TICKER_MAP_FNAME, index=False)
    print(f'done - saved tickers to file {TICKER_MAP_FNAME}')


if __name__ == '__main__':
    main()
