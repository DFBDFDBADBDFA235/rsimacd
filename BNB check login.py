import ccxt

try:
    exchange = ccxt.binance({
        'apiKey': os.environ.get('BINANCE_API_KEY'),
        'secret': os.environ.get('BINANCE_SECRET')
    })
    
    # Tenta di accedere alle informazioni del conto
    balance = exchange.fetch_balance()
    print("Connessione riuscita!")
    print(f"Saldo disponibile in USDT: {balance['USDT']['free']}")
    
except ccxt.AuthenticationError:
    print("Errore di autenticazione. Verifica le tue credenziali API.")
except Exception as e:
    print(f"Si è verificato un errore: {str(e)}")
	