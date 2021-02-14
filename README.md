# numerai-signals-tickermap
Numerai Signals tournament
Players can bring any signal from any dataset into the tournament.

## Purpose

Symbols in the financial world have many different formats to describe the same entity / equities in the tournament.
The tournament is easier to play in and gets more diverse signals if we have a direct conversion between many different names and formats.

## The file
`ticker_map.csv` has the the latest mapping between ticker symbols for the Numerai Signals universe.  
You can download use it with `wget https://raw.githubusercontent.com/hellno/numerai-signals-tickermap/master/ticker_map.csv`
or in your python code with `pd.read_csv('https://raw.githubusercontent.com/hellno/numerai-signals-tickermap/master/ticker_map.csv')'`.  
A field can have the ticker symbol or `SYMBOL_NOT_FOUND` if it is not available in a format.


## Ticker symbol formats

### Bloomberg
The official Numerai Signals data is given in Bloomberg tickers

### Yahoo
Python package: https://github.com/ranaroussi/yfinance  
Website: https://finance.yahoo.com/

### Alpha Vantage
Python package: https://github.com/RomelTorres/alpha_vantage  
Website: https://www.alphavantage.co/documentation/

### RIC (Reuters Instrument Code)
https://en.wikipedia.org/wiki/Reuters_Instrument_Code  

## Known problems
- Only a problem if we have >1 stocks from one company in the Numerai Signals universe: 
ISIN identifies the security but not the exchange on which it trades. 
We lose information if a company's stock is traded on different exchanges and all have the same ISIN.
- 