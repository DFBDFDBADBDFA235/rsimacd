#!/usr/bin/env python3
import time
from datetime import datetime, timedelta
import os
import ccxt
import pandas as pd
import numpy as np
import talib
import logging
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

# Parametri per il monitoraggio degli ordini
ORDER_CHECK_INTERVAL = 5  # Intervallo in secondi tra i controlli dello stato dell'ordine
ORDER_TIMEOUT = 300       # Timeout totale in secondi per l'esecuzione dell'ordine

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
        }
    })

    # Tenta di accedere alle informazioni del conto
    balance = exchange.fetch_balance()
    print("Connessione riuscita!")
    print(f"Saldo disponibile in USDT: {balance['USDT']['free']}")

    # Inizializza HOLDING_QUANTITY basandosi sul saldo reale
    asset = TRADING_TICKER_NAME.split('/')[0]  # 'BTC' in questo caso
    HOLDING_QUANTITY = balance['total'].get(asset, 0)
    logging.info(f"Inizializzazione HOLDING_QUANTITY: {HOLDING_QUANTITY} {asset}")

except ccxt.AuthenticationError:
    logging.error("Errore di autenticazione. Verifica le tue credenziali API.")
    exit(1)
except Exception as e:
    logging.error(f"Si è verificato un errore durante la connessione: {str(e)}")
    exit(1)

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
            logging.warning(f"Missing data detected in DataFrame for {ticker}")

            # Opzioni di gestione dei dati mancanti
            ticker_df = ticker_df.dropna()  # Rimuove le righe con NaN

    except ccxt.NetworkError as ce:
        logging.error(f"Connection error while fetching data for {ticker}: {str(ce)}")
    except ccxt.ExchangeError as ee:
        logging.error(f"Exchange error while fetching data for {ticker}: {str(ee)}")
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
        rsi = talib.RSI(ticker_df['close'], timeperiod=RSI_PERIOD)
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
        # Sincronizza il bilancio prima di controllare la liquidità
        sync_holdings()
        asset = trading_ticker.split('/')[0]  # 'BTC' in questo caso
        available_balance = HOLDING_QUANTITY  # Poiché abbiamo sincronizzato
        return available_balance >= scrip_quantity
    except Exception as e:
        logging.error(f"Error fetching balance for liquidity check: {str(e)}")
        return False

def validate_trade_params(trade_rec_type, scrip_quantity):
    """Controlla se le condizioni per l'operazione sono valide."""
    # Sincronizza il bilancio prima di validare
    sync_holdings()
    if trade_rec_type == "SELL" and scrip_quantity > HOLDING_QUANTITY:
        logging.error("Tentativo di vendere più di quanto si possiede.")
        return False
    return True

# STEP 4: EXECUTE THE TRADE (both buy and sell)
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
            
            # Log the response
            if order_response:
                order_id = order_response['id']
                logging.info(f'ORDER PLACED SUCCESSFULLY. RESPONSE: {order_response}')

                # Monitor the order status
                order_status = monitor_order(order_id, trading_ticker)

                if order_status == 'closed':
                    logging.info(f"ORDER EXECUTED: {trade_rec_type} {scrip_quantity} at {current_price} for {trading_ticker}")
                    
                    if trade_rec_type == "BUY":
                        HOLDING_QUANTITY += scrip_quantity  # Add the purchased quantity
                    else:
                        HOLDING_QUANTITY -= scrip_quantity  # Subtract the sold quantity

                    # Sincronizza immediatamente il bilancio dopo il trade
                    sync_holdings()

                    order_placed = True
                elif order_status == 'canceled':
                    logging.warning(f"ORDER CANCELED: {trade_rec_type} {scrip_quantity} at {current_price} for {trading_ticker}")
                else:
                    logging.warning(f"ORDER NOT FILLED: {trade_rec_type} {scrip_quantity} at {current_price} for {trading_ticker}")
            else:
                logging.error("Order response was empty or invalid.")
        else:
            logging.error(f"Failed to fetch ticker data for {trading_ticker}.")

    except Exception as e:
        logging.error(f"ALERT!!! UNABLE TO COMPLETE THE ORDER. ERROR: {str(e)}")
    
    return order_placed

def monitor_order(order_id, trading_ticker):
    """
    Monitora lo stato dell'ordine fino a quando non viene eseguito, annullato o scade il timeout.
    Restituisce lo stato finale dell'ordine: 'closed', 'canceled', 'open', o 'expired'.
    """
    global exchange
    start_time = time.time()
    while True:
        try:
            order = exchange.fetch_order(order_id, TRADING_TICKER_NAME)
            status = order['status']
            logging.debug(f"Monitora ordine {order_id}: Stato attuale: {status}")

            if status == 'closed':
                return 'closed'
            elif status == 'canceled':
                return 'canceled'
            elif status in ['open', 'partial']:
                if time.time() - start_time > ORDER_TIMEOUT:
                    logging.warning(f"ORDER TIMEOUT: {order_id} for {trading_ticker} has not been filled within {ORDER_TIMEOUT} seconds.")
                    # Annulla l'ordine se non è stato eseguito entro il timeout
                    try:
                        exchange.cancel_order(order_id, TRADING_TICKER_NAME)
                        logging.info(f"ORDER CANCELED DUE TO TIMEOUT: {order_id} for {trading_ticker}")
                        return 'canceled'
                    except Exception as e:
                        logging.error(f"Error canceling order {order_id}: {str(e)}")
                        return 'expired'
                time.sleep(ORDER_CHECK_INTERVAL)
            else:
                return status  # Restituisce lo stato se non è né 'open' né 'partial'
        except ccxt.NetworkError as ce:
            logging.error(f"Network error while monitoring order {order_id}: {str(ce)}")
        except ccxt.ExchangeError as ee:
            logging.error(f"Exchange error while monitoring order {order_id}: {str(ee)}")
        except Exception as e:
            logging.error(f"Unexpected error while monitoring order {order_id}: {str(e)}")
        
        # Attendi prima di ritentare
        time.sleep(ORDER_CHECK_INTERVAL)

# Funzione per sincronizzare il saldo
def sync_holdings():
    global HOLDING_QUANTITY
    try:
        balance = exchange.fetch_balance()
        asset = TRADING_TICKER_NAME.split('/')[0]  # 'BTC' in questo caso
        real_holdings = balance['total'].get(asset, 0)
        if real_holdings != HOLDING_QUANTITY:
            logging.info(f"Sincronizzazione HOLDING_QUANTITY: {HOLDING_QUANTITY} -> {real_holdings}")
            HOLDING_QUANTITY = real_holdings
    except Exception as e:
        logging.error(f"Error during holdings synchronization: {str(e)}")

# Funzioni per gestire il shutdown
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

def run_bot_for_ticker(ccxt_ticker, trading_ticker, shutdown_file_path='shutdown.txt'):
    global shutdown_requested, last_candle_time
    currently_holding = False

    # Register the signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

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

# Avvio del bot
if __name__ == "__main__":
    try:
        SHUTDOWN_FILE_PATH = 'shutdown_bot.txt'
        run_bot_for_ticker(CCXT_TICKER_NAME, TRADING_TICKER_NAME, SHUTDOWN_FILE_PATH)
    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")


