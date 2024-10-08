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

# STEP 4: EXECUTE THE TRADE (both buy and sell)
def execute_trade(trade_rec_type, trading_ticker): 
    global exchange, HOLDING_QUANTITY, INVESTMENT_AMOUNT_PER_TRADE
    order_placed = False
    side_value = 'buy' if trade_rec_type == "BUY" else 'sell'
    
    try:
        # Fetch current ticker price
        ticker_request = exchange.fetch_ticker(trading_ticker)
        if ticker_request is not None:
            current_price = float(ticker_request['info']['last_price'])

            # Calcola la quantità di scrip per l'ordine
            if trade_rec_type == "BUY":
                scrip_quantity = round(INVESTMENT_AMOUNT_PER_TRADE / current_price, 5)
            else:
                # Per le vendite, utilizza la quantità detenuta
                scrip_quantity = HOLDING_QUANTITY
            
            # Assicurati di non vendere più di quanto si possiede
            if trade_rec_type == "SELL":
                if scrip_quantity > HOLDING_QUANTITY:
                    logging.error("Tentativo di vendere più di quanto si possiede.")
                    return order_placed  # Esci senza effettuare l'ordine
                
                # Controlla la liquidità
                if not check_liquidity(trading_ticker, scrip_quantity):
                    logging.error("Fondi insufficienti per completare la vendita.")
                    return order_placed  # Esci senza effettuare l'ordine

            # Log dei dettagli dell'ordine prima di piazzarlo
            order_time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            epoch_time = int(time.time() * 1000)
            logging.info(f"PLACING ORDER {order_time}: {trading_ticker}, {side_value}, {current_price}, {scrip_quantity}, {epoch_time}")
            
            # Effettua l'ordine sull'exchange
            order_response = exchange.create_limit_order(trading_ticker, side_value, scrip_quantity, current_price)
            
            # Log della risposta e aggiornamento della quantità detenuta
            if order_response:
                logging.info(f'ORDER PLACED. RESPONSE: {order_response}')
                if trade_rec_type == "BUY":
                    HOLDING_QUANTITY += scrip_quantity  # Aggiungi la quantità acquistata
                else:  # Se è una vendita
                    HOLDING_QUANTITY -= scrip_quantity  # Sottrai la quantità venduta

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
    retry_count = 0
    max_retries = 5
    backoff_factor = 1.5

    while True:
        try:
            # STEP 1: FETCH THE DATA WITH BACKOFF RETRY LOGIC
            while retry_count < max_retries:
                try:
                    ticker_data = fetch_data(ccxt_ticker)
                    if ticker_data is not None:
                        retry_count = 0  # Reset retry count if successful
                        break  # Break if data is fetched successfully
                except Exception as e:
                    retry_count += 1
                    wait_time = backoff_factor ** retry_count
                    logging.error(f"Failed to fetch data. Retry {retry_count}/{max_retries}. Waiting {wait_time} seconds.")
                    time.sleep(wait_time)

            if ticker_data is None:
                logging.error(f"Exceeded max retries ({max_retries}) for fetching ticker data. Retrying in 10 seconds.")
                time.sleep(10)
                continue

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

        except Exception as e:
            logging.error(f"Error in bot execution: {str(e)}")
            time.sleep(10)  # Wait before retrying to avoid hammering the API

 try:
       run_bot_for_ticker(CCXT_TICKER_NAME, TRADING_TICKER_NAME)
   except KeyboardInterrupt:
       logging.info("Bot stopped manually.")

