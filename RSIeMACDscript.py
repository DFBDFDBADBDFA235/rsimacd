#!/usr/bin/env python3
import time
from datetime import datetime
import os
import ccxt as ccxt
import pandas as pd
import numpy as np
import talib
import logging
import socket  # for network-related errors
import requests.exceptions  # assuming you might be using requests or other libraries for API
import signal

# Initialize Variables
CANDLE_DURATION_IN_MIN = 1
RSI_OVERSOLD = 25
RSI_OVERBOUGHT = 75
RSI_PERIOD = 14

INVESTMENT_AMOUNT_PER_TRADE = 10
HOLDING_QUANTITY = 0

CCXT_TICKER_NAME = 'BTC/USDT'
TRADING_TICKER_NAME = 'BTC/USDT'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("trading_bot.log"),
                              logging.StreamHandler()])

try:
    exchange = ccxt.binance({
        'apiKey': os.environ.get('BINANCE_API_KEY'),
        'secret': os.environ.get('BINANCE_SECRET'),
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

        # Verifica se nessun dato è stato ricevuto
        if not bars:
            raise ValueError(f"No data fetched for ticker {ticker}")

        # Crea DataFrame dai dati
        ticker_df = pd.DataFrame(bars[:-1], columns=['at', 'open', 'high', 'low', 'close', 'vol'])
        ticker_df['Date'] = pd.to_datetime(ticker_df['at'], unit='ms')
        ticker_df['symbol'] = ticker

        # Controllo se il DataFrame è vuoto
        if ticker_df.empty:
            raise ValueError(f"Received empty DataFrame for ticker {ticker}")

        # Controllo valori NaN nel DataFrame
        if ticker_df.isna().any().any():  # Se esistono NaN in qualsiasi colonna
            logging.warning(f"Missing data detected in DataFrame for {ticker}")

            # Opzioni di gestione dei dati mancanti
            # 1. Rimuovi le righe con valori NaN
            ticker_df = ticker_df.dropna()
            
            # 2. Oppure, riempi i NaN con un valore specifico (es. 0 o la media della colonna)
            # ticker_df.fillna(0, inplace=True)  # Sostituisce NaN con 0
            
            # 3. Oppure interpolare i dati mancanti
            # ticker_df.interpolate(method='linear', inplace=True)

    except ConnectionError as ce:
        logging.error(f"Connection error while fetching data for {ticker}: {str(ce)}")
        
    except TimeoutError as te:
        logging.error(f"Timeout error while fetching data for {ticker}: {str(te)}")
        
    except ValueError as ve:
        logging.error(f"Value error while processing data for {ticker}: {str(ve)}")
        
    except Exception as e:
        logging.error(f"An unexpected error occurred while fetching data for {ticker}: {str(e)}")

    return ticker_df

# Definisci la soglia per la distanza
MIN_MACD_DISTANCE = 0.01  # Soglia minima per la distanza, può essere modificata

# STEP 2: COMPUTE THE TECHNICAL INDICATORS & APPLY THE TRADING STRATEGY
def get_trade_recommendation(ticker_df):
    macd_result, final_result = 'WAIT', 'WAIT'

    # Calcola il MACD
    macd, signal, hist = talib.MACD(ticker_df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    
    last_hist = hist.iloc[-1]
    prev_hist = hist.iloc[-2]
    
    # Controllo per il crossover MACD con considerazione della distanza
    if not np.isnan(prev_hist) and not np.isnan(last_hist):
        macd_crossover = (prev_hist < 0 < last_hist) or (prev_hist > 0 > last_hist)  # Detect crossover

        # Controlla la distanza tra MACD e la linea del segnale
        if macd_crossover and abs(last_hist) > MIN_MACD_DISTANCE:
            macd_result = 'BUY' if last_hist > 0 else 'SELL'

    # Se viene generato un segnale MACD, controlla l'RSI per confermare il segnale
    if macd_result != 'WAIT':
        rsi = talib.RSI(ticker_df['close'], timeperiod=14)
        last_rsi = rsi.iloc[-1]

        if not np.isnan(last_rsi):
            if last_rsi <= RSI_OVERSOLD and macd_result == 'BUY':
                final_result = 'BUY'
            elif last_rsi >= RSI_OVERBOUGHT and macd_result == 'SELL':
                final_result = 'SELL'
    
    return final_result

# STEP 3: check liquidity
def check_liquidity(trading_ticker, scrip_quantity):
    """Controlla se ci sono sufficienti fondi per vendere."""
    try:
        # Fetch il saldo corrente
        account_balance = exchange.fetch_balance()
        available_balance = account_balance['total'][trading_ticker.split('/')[0]]  # Assumendo che il ticker sia nel formato 'BTC/USD'
        return available_balance >= scrip_quantity
    except Exception as e:
        logging.error(f"Error fetching balance for liquidity check: {str(e)}")
        return False

def validate_trade_params(trade_rec_type, scrip_quantity):
    """Controlla se le condizioni per l'operazione sono valide."""
    if trade_rec_type == "SELL" and scrip_quantity > HOLDING_QUANTITY:
        logging.error("Tentativo di vendere più di quanto si possiede.")
        return False
    return True

# STEP 4: EXECUTE THE TRADE (both buy and sell)
import logging
from datetime import datetime
import time

def execute_trade(trade_rec_type, trading_ticker):
    global exchange, HOLDING_QUANTITY, INVESTMENT_AMOUNT_PER_TRADE
    order_placed = False
    side_value = 'buy' if trade_rec_type == "BUY" else 'sell'
    
    try:
        # Fetch current ticker price
        ticker_request = exchange.fetch_ticker(trading_ticker)
        if ticker_request is not None:
            current_price = float(ticker_request['info']['last_price'])

            # Calculate the quantity for the order
            if trade_rec_type == "BUY":
                scrip_quantity = round(INVESTMENT_AMOUNT_PER_TRADE / current_price, 5)
            else:
                # For sell orders, use the quantity being held
                scrip_quantity = HOLDING_QUANTITY
            
            # Ensure not selling more than held
            if trade_rec_type == "SELL":
                if scrip_quantity > HOLDING_QUANTITY:
                    logging.error("Attempting to sell more than held.")
                    return order_placed  # Exit without placing the order
                
                # Check for liquidity
                if not check_liquidity(trading_ticker, scrip_quantity):
                    logging.error("Insufficient funds to complete the sell order.")
                    return order_placed  # Exit without placing the order

            # Log the order details before placing it
            order_time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            epoch_time = int(time.time() * 1000)
            logging.info(f"PLACING ORDER {order_time}: Ticker: {trading_ticker}, Side: {side_value}, "
                         f"Price: {current_price}, Quantity: {scrip_quantity}, Timestamp: {epoch_time}")
            
            # Place the order on the exchange
            order_response = exchange.create_limit_order(trading_ticker, side_value, scrip_quantity, current_price)
            
            # Log the response and update the held quantity
            if order_response:
                logging.info(f'ORDER PLACED SUCCESSFULLY. RESPONSE: {order_response}')
                # Log trade details for executed trades
                trade_execution_time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                logging.info(f"TRADE EXECUTED {trade_execution_time}: {trade_rec_type} {scrip_quantity} "
                             f"at {current_price} for {trading_ticker}")
                
                if trade_rec_type == "BUY":
                    HOLDING_QUANTITY += scrip_quantity  # Add the purchased quantity
                else:
                    HOLDING_QUANTITY -= scrip_quantity  # Subtract the sold quantity

                order_placed = True
            else:
                logging.error("Order response was empty or invalid.")
        else:
            logging.error(f"Failed to fetch ticker data for {trading_ticker}.")
    
    except Exception as e:
        logging.error(f"ALERT!!! UNABLE TO COMPLETE THE ORDER. ERROR: {str(e)}")
    
    return order_placed

#run bot managing errors and shutting down with signal handling and file monitoring

# Global variable to control bot execution
shutdown_requested = False

def signal_handler(sig, frame):
    """Handle signals to initiate a graceful shutdown."""
    global shutdown_requested
    logging.info("Shutdown signal received. Initiating shutdown...")
    shutdown_requested = True

def check_shutdown_file(file_path):
    """Check if a specific shutdown file exists."""
    return os.path.isfile(file_path)

# Global variable to track the timestamp of the last processed candle
last_candle_time = None

def run_bot_for_ticker(ccxt_ticker, trading_ticker, shutdown_file_path):
    global shutdown_requested, last_candle_time
    currently_holding = False

    # Register the signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    while not shutdown_requested:
        try:
            # STEP 1: FETCH THE DATA
            ticker_data = fetch_data(ccxt_ticker)
            if ticker_data is not None:
                # Extract the timestamp of the latest candle (this is the 'at' column in ticker_df)
                latest_candle_time = ticker_data.iloc[-1]['at']  # Last row in the DataFrame

                # Check if a new candle has been generated
                if last_candle_time is None or latest_candle_time > last_candle_time:
                    # Log that a new candle is available
                    logging.info(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - New candle detected.')

                    # Update the last candle time
                    last_candle_time = latest_candle_time

                    # Log the current price fetched from the ticker
                    current_price = ticker_data.iloc[-1]['close']  # Get the closing price of the latest candle
                    logging.info(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - Ticker: {ccxt_ticker}, Current Price: {current_price}')

                    # STEP 2: COMPUTE TECHNICAL INDICATORS & APPLY THE TRADING STRATEGY
                    trade_rec_type = get_trade_recommendation(ticker_data)
                    logging.info(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - TRADING RECOMMENDATION: {trade_rec_type}')

                    # STEP 3: EXECUTE THE TRADE
                    if (trade_rec_type == 'BUY' and not currently_holding) or \
                       (trade_rec_type == 'SELL' and currently_holding):

                        logging.info(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - Placing {trade_rec_type} order for {trading_ticker}')

                        # Execute the trade
                        trade_successful = execute_trade(trade_rec_type, trading_ticker)

                        if trade_successful:
                            logging.info(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - Trade {trade_rec_type} for {trading_ticker} successful.')
                            currently_holding = not currently_holding
                        else:
                            logging.error(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - Failed to execute {trade_rec_type} order for {trading_ticker}')
                
                # No new candle, sleep for a shorter duration
                else:
                    time.sleep(10)  # Sleep for 10 seconds before checking again

            else:
                logging.warning(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - Unable to fetch ticker data for {ccxt_ticker}. Retrying in 5 seconds.')
                time.sleep(5)

            # Check for shutdown file
            if check_shutdown_file(shutdown_file_path):
                logging.info(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - Shutdown file detected. Initiating shutdown...')
                shutdown_requested = True

        except Exception as e:
            logging.error(f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')} - Error in bot execution: {str(e)}")
            time.sleep(10)  # Wait before retrying to avoid hammering the API

    # Cleanup and exit
    logging.info(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - Bot has shut down safely.')

 try:
       run_bot_for_ticker(CCXT_TICKER_NAME, TRADING_TICKER_NAME)
   except KeyboardInterrupt:
       logging.info("Bot stopped manually.")

