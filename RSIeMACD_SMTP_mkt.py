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
import smtplib
from email.mime.text import MIMEText
from twilio.rest import Client  # Per inviare SMS

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

# Configurazione delle Email
EMAIL_SENDER = "your-email@example.com"
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')
EMAIL_RECEIVER = "recipient-email@example.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Configurazione di Twilio
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
TWILIO_PHONE_NUMBER = os.environ.get('TWILIO_PHONE_NUMBER')
RECIPIENT_PHONE_NUMBER = os.environ.get('RECIPIENT_PHONE_NUMBER')

# Funzione per inviare email
def send_email(subject, message):
    try:
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECEIVER

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        logger.info("Email inviata con successo.")
    except Exception as e:
        logger.error(f"Errore nell'invio dell'email: {str(e)}")

# Funzione per inviare SMS
def send_sms(message):
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        client.messages.create(
            body=message,
            from_=TWILIO_PHONE_NUMBER,
            to=RECIPIENT_PHONE_NUMBER
        )
        logger.info("SMS inviato con successo.")
    except Exception as e:
        logger.error(f"Errore nell'invio dell'SMS: {str(e)}")

# Funzione per gestire errori
def handle_error(error_message, critical=False):
    logger.error(error_message)
    send_email("Trading Bot Error", error_message)
    send_sms(f"Trading Bot Alert: {error_message}")

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
            order_time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            epoch_time = int(time.time() * 1000)
            logger.info(f"PLACING MARKET ORDER {order_time}: Ticker: {trading_ticker}, Side: {side_value}, "
                        f"Quantity: {scrip_quantity}, Timestamp: {epoch_time}")
            
            # Place the market order on the exchange
            order_response = exchange.create_market_order(trading_ticker, side_value, scrip_quantity)
            
            # Log the response
            if order_response:
                order_id = order_response['id']
                logger.info(f'ORDER PLACED SUCCESSFULLY. RESPONSE: {order_response}')

                # Monitor the order status
                order_status = monitor_order(order_id, trading_ticker)

                if order_status == 'closed':
                    logger.info(f"ORDER EXECUTED: {trade_rec_type} {scrip_quantity} at {current_price} for {trading_ticker}")
                    
                    if trade_rec_type == "BUY":
                        HOLDING_QUANTITY += scrip_quantity  # Add the purchased quantity
                    else:
                        HOLDING_QUANTITY -= scrip_quantity  # Subtract the sold quantity

                    # Sincronizza immediatamente il bilancio dopo il trade
                    sync_holdings()

                    order_placed = True
                elif order_status == 'canceled':
                    logger.warning(f"ORDER CANCELED: {trade_rec_type} {scrip_quantity} at {current_price} for {trading_ticker}")
                else:
                    logger.warning(f"ORDER NOT FILLED: {trade_rec_type} {scrip_quantity} for {trading_ticker}")
            else:
                handle_error("Order response was empty or invalid.", critical=False)
        else:
            handle_error(f"Failed to fetch ticker data for {trading_ticker}.", critical=False)

    except Exception as e:
        handle_error(f"ALERT!!! UNABLE TO COMPLETE THE ORDER. ERROR: {str(e)}", critical=False)
    
    return order_placed

# STEP 4: MONITOR ORDER STATUS
def monitor_order(order_id, trading_ticker):
    global exchange
    start_time = time.time()

    while time.time() - start_time < ORDER_TIMEOUT:
        try:
            # Retrieve order status from the exchange
            order = exchange.fetch_order(order_id, trading_ticker)
            if order and order['status'] == 'closed':
                return 'closed'
            elif order and order['status'] == 'canceled':
                return 'canceled'

            time.sleep(ORDER_CHECK_INTERVAL)  # Wait before the next check

        except Exception as e:
            handle_error(f"Error monitoring order {order_id}: {str(e)}", critical=False)

    logger.warning(f"Order {order_id} timed out after {ORDER_TIMEOUT} seconds.")
    return 'timeout'

# STEP 5: CHECK LIQUIDITY
def check_liquidity(ticker, quantity):
    try:
        # Fetch order book data
        order_book = exchange.fetch_order_book(ticker)
        best_bid = order_book['bids'][0][0] if order_book['bids'] else None
        best_ask = order_book['asks'][0][0] if order_book['asks'] else None

        if not best_bid or not best_ask:
            return False  # No liquidity available

        # Check if there is sufficient liquidity to execute the trade
        return True
    except Exception as e:
        handle_error(f"Error checking liquidity for {ticker}: {str(e)}", critical=False)
        return False

# Sincronizza le posizioni attuali con l'exchange
def sync_holdings():
    global exchange, HOLDING_QUANTITY
    try:
        balance = exchange.fetch_balance()
        asset = TRADING_TICKER_NAME.split('/')[0]
        HOLDING_QUANTITY = balance['total'].get(asset, 0)
        logger.info(f"Updated HOLDING_QUANTITY: {HOLDING_QUANTITY} {asset}")
    except Exception as e:
        handle_error(f"Error syncing holdings: {str(e)}", critical=False)

# LOOP PRINCIPALE
if __name__ == "__main__":
    SHUTDOWN_FILE_PATH = "shutdown.txt"

    # Avvia l'exchange e sincronizza il saldo
    initialize_exchange()

    # Loop principale del bot
    while not shutdown_requested and not check_shutdown_file(SHUTDOWN_FILE_PATH):
        try:
            # Step 1: Fetch the data
            df = fetch_data(CCXT_TICKER_NAME)

            if df is not None:
                # Step 2: Apply the trading strategy
                trade_signal = get_trade_recommendation(df)
                logger.info(f"Signal: {trade_signal}")

                # Step 3: Execute the trades
                if trade_signal != "WAIT":
                    order_executed = execute_trade(trade_signal, TRADING_TICKER_NAME)
                    if order_executed:
                        logger.info("Trade eseguito con successo.")
                    else:
                        logger.info("Nessun trade eseguito.")

            time.sleep(60)  # Pausa di 60 secondi tra ogni ciclo

        except Exception as e:
            handle_error(f"Unhandled exception in the main loop: {str(e)}", critical=False)

    # Chiusura del bot
    shutdown_bot()
