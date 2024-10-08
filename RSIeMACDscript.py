#!/usr/bin/env python3
import time
from datetime import datetime
import os
import ccxt as ccxt
import pandas as pd
import numpy as np
import talib
import logging

# Initialize Variables
CANDLE_DURATION_IN_MIN = 1
RSI_OVERSOLD = 25
RSI_OVERBOUGHT = 75
RSI_PERIOD = 14

INVESTMENT_AMOUNT_PER_TRADE = 10
HOLDING_QUANTITY = 0

CCXT_TICKER_NAME = 'BTC/USDT'
TRADING_TICKER_NAME = 'BTC/USDT'

try:
    exchange = ccxt.binance({
        'apiKey': os.environ.get('BINANCE_API_KEY'),
        'secret': os.environ.get('BINANCE_SECRET')
        'enableRateLimit': True,
        'options': {
            'adjustForTimeDifference': True,  # This will adjust for any time differences automatically
    })
    
    # Tenta di accedere alle informazioni del conto
    balance = exchange.fetch_balance()
    print("Connessione riuscita!")
    print(f"Saldo disponibile in USDT: {balance['USDT']['free']}")
    
except ccxt.AuthenticationError:
    print("Errore di autenticazione. Verifica le tue credenziali API.")
except Exception as e:
    print(f"Si è verificato un errore: {str(e)}")

# STEP 1: FETCH THE DATA
def fetch_data(ticker):
    global exchange
    bars, ticker_df = None, None
    try:
        # Fetch OHLCV data
        bars = exchange.fetch_ohlcv(ticker, timeframe=f'{CANDLE_DURATION_IN_MIN}m', limit=100)
    except Exception as e:
        # Log the specific error
        logging.error(f"Error fetching data for {ticker}: {str(e)}")
    
    if bars is not None:
        # Create DataFrame from the fetched data
        ticker_df = pd.DataFrame(bars[:-1], columns=['at', 'open', 'high', 'low', 'close', 'vol'])
        ticker_df['Date'] = pd.to_datetime(ticker_df['at'], unit='ms')
        ticker_df['symbol'] = ticker
    return ticker_df



# STEP 2: COMPUTE THE TECHNICAL INDICATORS & APPLY THE TRADING STRATEGY
def get_trade_recommendation(ticker_df):
    macd_result, final_result = 'WAIT', 'WAIT'

    # Calculate MACD
    macd, signal, hist = talib.MACD(ticker_df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    last_hist = hist.iloc[-1]
    prev_hist = hist.iloc[-2]
    
    # Check for MACD crossover
    if not np.isnan(prev_hist) and not np.isnan(last_hist):
        macd_crossover = (prev_hist < 0 < last_hist) or (prev_hist > 0 > last_hist)  # Detect crossover
        if macd_crossover:
            macd_result = 'BUY' if last_hist > 0 else 'SELL'

    # If a MACD signal is generated, check RSI to confirm the signal
    if macd_result != 'WAIT':
        rsi = talib.RSI(ticker_df['close'], timeperiod=14)
        last_rsi = rsi.iloc[-1]

        if not np.isnan(last_rsi):
            if last_rsi <= RSI_OVERSOLD and macd_result == 'BUY':
                final_result = 'BUY'
            elif last_rsi >= RSI_OVERBOUGHT and macd_result == 'SELL':
                final_result = 'SELL'
    
    return final_result


# STEP 3: EXECUTE THE TRADE
def execute_trade(trade_rec_type, trading_ticker):
    global exchange, HOLDING_QUANTITY, INVESTMENT_AMOUNT_PER_TRADE
    order_placed = False
    side_value = 'buy' if trade_rec_type == "BUY" else 'sell'
    
    try:
        # Fetch current ticker price
        ticker_request = exchange.fetch_ticker(trading_ticker)
        if ticker_request is not None:
            current_price = float(ticker_request['info']['last_price'])

            # Calculate scrip quantity for the order
            if trade_rec_type == "BUY":
                scrip_quantity = round(INVESTMENT_AMOUNT_PER_TRADE / current_price, 5)
            else:
                scrip_quantity = HOLDING_QUANTITY

            # Log order details before placing
            order_time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            epoch_time = int(time.time() * 1000)
            logging.info(f"PLACING ORDER {order_time}: {trading_ticker}, {side_value}, {current_price}, {scrip_quantity}, {epoch_time}")
            
            # Place the order on the exchange
            order_response = exchange.create_limit_order(trading_ticker, side_value, scrip_quantity, current_price)
            
            # Log the response and update holding quantity if the order is a buy
            if order_response:
                logging.info(f'ORDER PLACED. RESPONSE: {order_response}')
                if trade_rec_type == "BUY":
                    HOLDING_QUANTITY = scrip_quantity

                order_placed = True
            else:
                logging.error("Order response was empty or invalid.")
        else:
            logging.error(f"Failed to fetch ticker data for {trading_ticker}.")
    
    except Exception as e:
        logging.error(f"ALERT!!! UNABLE TO COMPLETE THE ORDER. ERROR: {str(e)}")
    
    return order_placed

def run_bot_for_ticker(ccxt_ticker, trading_ticker):
    currently_holding = False
    while True:
        try:
            # STEP 1: FETCH THE DATA
            ticker_data = fetch_data(ccxt_ticker)
            if ticker_data is not None:
                # STEP 2: COMPUTE TECHNICAL INDICATORS & APPLY THE TRADING STRATEGY
                trade_rec_type = get_trade_recommendation(ticker_data)
                logging.info(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}  TRADING RECOMMENDATION: {trade_rec_type}')

                # STEP 3: EXECUTE THE TRADE
                if (trade_rec_type == 'BUY' and not currently_holding) or \
                   (trade_rec_type == 'SELL' and currently_holding):
                    logging.info(f'Placing {trade_rec_type} order')
                    trade_successful = execute_trade(trade_rec_type, trading_ticker)
                    currently_holding = not currently_holding if trade_successful else currently_holding

                # Sleep until the next candle duration
                time.sleep(CANDLE_DURATION_IN_MIN * 60)
            else:
                logging.warning(f'Unable to fetch ticker data for {ccxt_ticker}. Retrying in 5 seconds.')
                time.sleep(5)

        except Exception as e:
            logging.error(f"Error in bot execution: {str(e)}")
            time.sleep(10)  # Wait before retrying to avoid hammering the API


run_bot_for_ticker(CCXT_TICKER_NAME, TRADING_TICKER_NAME)
