# numerai-signals-tickermap
[Numerai Signals](https://signals.numer.ai/tournament) lets you upload stock market signals and find out how original they are compared to all other signals on Numerai. 
Signals can be staked with the NMR cryptocurrency to earn rewards.
Players can bring any signal from any dataset into the tournament.

## Purpose of this tickermap

Symbols in the financial world have many different formats to describe the same entity.
It is easier to play in the tournament and everyone can contribute diverse signals if we have a direct conversion between different formats to the Numerai universe.

## The file
[`ticker_map.csv`](/ticker_map.csv) has the the latest mapping between ticker symbols for the Numerai Signals universe.  
You can download it with 
```shell script
wget https://raw.githubusercontent.com/hellno/numerai-signals-tickermap/main/ticker_map.csv
```
or in your python code with 
```python
import pandas as pd
df = pd.read_csv('https://raw.githubusercontent.com/hellno/numerai-signals-tickermap/main/ticker_map.csv')
```  
Each cell in the mapping has either the ticker symbol or `SYMBOL_NOT_FOUND` (if it is not available)

## Help
This repo is open for anyone to create pull requests, comments or issues.  
There are many different formats to add. At best we can add a script for each 
so everyone can check the symbols are valid and we can re-run periodically.  
The original mapping was started by jparyani and wsouza.

<img src="https://i.imgur.com/gY5sizZ.jpeg" width="500"><br />
thanks to meme dealer: hone5com

### ToDo
- add more scripts for more ticker formats
- ...

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
- Ticker symbols change over time, so we need to sync between formats periodically
- ...