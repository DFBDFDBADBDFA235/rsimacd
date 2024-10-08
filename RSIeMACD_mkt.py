#!/usr/bin/env python3
import time
from datetime import datetime
import os
import sys
import ccxt
import pandas as pd
import numpy as np
import talib
import logging
import signal

# Inizializza Variabili
CANDLE_DURATION_IN_MIN = 1
RSI_OVERSOLD = 25
RSI_OVERBOUGHT = 75
RSI_PERIOD = 14

INVESTMENT_AMOUNT_PER_TRADE = 10
HOLDING_QUANTITY = 0

CCXT_TICKER_NAME = 'BTC/USDT'
TRADING_TICKER_NAME = 'BTC/USDT'

# Parametri per il monitoraggio degli ordini
ORDER_CHECK_INTERVAL = 5  # Intervallo in secondi tra i controlli dello stato dell'ordine
ORDER_TIMEOUT = 300       # Timeout totale in secondi per l'esecuzione dell'ordine

# Configurazione del Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Funzione per gestire errori
def handle_error(error_message, critical=False):
    logger.error(error_message)

    if critical:
        logger.error("Errore critico rilevato, chiusura del bot.")
        remove_shutdown_file(SHUTDOWN_FILE_PATH)
        sys.exit(1)
    else:
        logger.info("Errore non critico, il bot riproverà a ripartire in 30 secondi...")
        time.sleep(30)  # Attende 30 secondi prima di riprovare

# Funzione per creare il file di shutdown
def create_shutdown_file(file_path):
    try:
        with open(file_path, 'w') as f:
            f.write("Shutdown requested.")
        logger.info(f"Shutdown file {file_path} creato.")
    except Exception as e:
        logger.error(f"Errore nella creazione del file di shutdown: {str(e)}")

# Funzione per rimuovere il file di shutdown
def remove_shutdown_file(file_path):
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
            logger.info(f"Shutdown file {file_path} rimosso.")
    except Exception as e:
        logger.error(f"Errore nella rimozione del file di shutdown: {str(e)}")

# Funzione per controllare se esiste il file di shutdown
def check_shutdown_file(file_path):
    return os.path.isfile(file_path)

# Gestore dei segnali per un shutdown pulito
shutdown_requested = False
def signal_handler(sig, frame):
    global shutdown_requested
    logger.info("Segnale di shutdown ricevuto. Avvio della chiusura...")
    shutdown_requested = True

# Registra i signal per il shutdown
signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Terminate signal

# Funzione di shutdown
def shutdown_bot():
    logger.info("Chiusura del bot in corso...")
    remove_shutdown_file(SHUTDOWN_FILE_PATH)
    sys.exit(0)

# Inizializza l'exchange e il saldo
def initialize_exchange():
    global exchange, HOLDING_QUANTITY
    try:
        # Controllo delle variabili di ambiente
        api_key = os.environ.get('BINANCE_API_KEY')
        api_secret = os.environ.get('BINANCE_SECRET')

        if not api_key or not api_secret:
            handle_error("BINANCE_API_KEY o BINANCE_SECRET non sono impostate nelle variabili di ambiente.", critical=True)

        exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'adjustForTimeDifference': True,  # This will adjust for any time differences automatically
            }
        })

        # Tenta di accedere alle informazioni del conto
        balance = exchange.fetch_balance()
        logger.info("Connessione riuscita!")
        logger.info(f"Saldo disponibile in USDT: {balance['USDT']['free']}")

        # Inizializza HOLDING_QUANTITY basandosi sul saldo reale
        asset = TRADING_TICKER_NAME.split('/')[0]  # 'BTC' in questo caso
        HOLDING_QUANTITY = balance['total'].get(asset, 0)
        logger.info(f"Inizializzazione HOLDING_QUANTITY: {HOLDING_QUANTITY} {asset}")

    except ccxt.AuthenticationError:
        handle_error("Errore di autenticazione. Verifica le tue credenziali API.", critical=True)
    except Exception as e:
        handle_error(f"Si è verificato un errore durante la connessione: {str(e)}", critical=True)

# STEP 1: FETCH THE DATA
def fetch_data(ticker):
    global exchange
    ticker_df = None
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
            logger.warning(f"Missing data detected in DataFrame for {ticker}")

            # Opzioni di gestione dei dati mancanti
            ticker_df = ticker_df.dropna()  # Rimuove le righe con NaN

    except ccxt.NetworkError as ce:
        handle_error(f"Connection error while fetching data for {ticker}: {str(ce)}", critical=False)
    except ccxt.ExchangeError as ee:
        handle_error(f"Exchange error while fetching data for {ticker}: {str(ee)}", critical=False)
    except TimeoutError as te:
        handle_error(f"Timeout error while fetching data for {ticker}: {str(te)}", critical=False)
    except ValueError as ve:
        handle_error(f"Value error while processing data for {ticker}: {str(ve)}", critical=False)
    except Exception as e:
        handle_error(f"An unexpected error occurred while fetching data for {ticker}: {str(e)}", critical=False)

    return ticker_df

# Definisci la soglia per la distanza
MIN_MACD_DISTANCE = 0.01  # Soglia minima per la distanza, può essere modificata

# STEP 2: COMPUTE THE TECHNICAL INDICATORS & APPLY THE TRADING STRATEGY
def get_trade_recommendation(ticker_df):
    macd_result, final_result = 'WAIT', 'WAIT'

    # Calcola il MACD
    macd, signal, hist = talib.MACD(ticker_df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    
    # Calcola l'RSI
    rsi = talib.RSI(ticker_df['close'], timeperiod=RSI_PERIOD)

    if rsi.iloc[-1] < RSI_OVERSOLD and macd.iloc[-1] - signal.iloc[-1] > MIN_MACD_DISTANCE:
        final_result = "BUY"
    elif rsi.iloc[-1] > RSI_OVERBOUGHT and macd.iloc[-1] - signal.iloc[-1] < -MIN_MACD_DISTANCE:
        final_result = "SELL"

    return final_result

# STEP 3: EXECUTE THE TRADES
def execute_trade(trade_rec_type, trading_ticker):
    global exchange, HOLDING_QUANTITY, INVESTMENT_AMOUNT_PER_TRADE
    order_placed = False
    side_value = 'buy' if trade_rec_type == "BUY" else 'sell'
    
    try:
        # Fetch current ticker price
        ticker_request = exchange.fetch_ticker(trading_ticker)
        if ticker_request is not None:
            current_price = float(ticker_request['last'])  # Utilizza 'last' per il prezzo più recente

            # Calculate the quantity for the order
            if trade_rec_type == "BUY":
                scrip_quantity = round(INVESTMENT_AMOUNT_PER_TRADE / current_price, 5)
            else:
                # Per gli ordini di vendita, usa la quantità posseduta
                scrip_quantity = HOLDING_QUANTITY

            # Ensure not selling more than held
            if trade_rec_type == "SELL":
                if scrip_quantity > HOLDING_QUANTITY:
                    handle_error("Attempting to sell more than held.", critical=False)
                    return order_placed  # Exit without placing the order
                
                # Check for liquidity
                if not check_liquidity(trading_ticker, scrip_quantity):
                    handle_error("Insufficient funds to complete the sell order.", critical=False)
                    return order_placed  # Exit without placing the order

            # Log the order details before placing it
           
