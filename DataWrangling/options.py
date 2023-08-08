import yfinance as yf

def get_option_data(ticker_symbol, expiration_date):
    """
    Fetch option data for a specific ticker and expiration date using yfinance.
    
    Parameters:
    - ticker_symbol (str): The ticker symbol of the stock (e.g., "AAPL" for Apple Inc.).
    - expiration_date (str): The expiration date in the format 'YYYY-MM-DD'.
    
    Returns:
    - dict: A dictionary containing call and put option data.
    """
    # Create a Ticker object
    ticker = yf.Ticker(ticker_symbol)
    
    # Fetch option data
    opts = ticker.option_chain(expiration_date)
    
    # Return call and put option data
    return {
        'calls': opts.calls,
        'puts': opts.puts
    }

# Example usage:
data = get_option_data("AAPL", "2023-09-07")
print("Calls: ")
print(data['calls'])
print()
print("Puts: ")
print(data['puts'])


def calculate_option_profit(initial_premium, final_premium, contracts=1, shares_per_contract=100):
    """
    Calculate the profit from trading an option.

    Parameters:
    - initial_premium (float): The initial cost (premium) of the option per share.
    - final_premium (float): The selling price (premium) of the option per share at the end.
    - contracts (int): The number of option contracts. Default is 1.
    - shares_per_contract (int): The number of shares represented by one option contract. Default is 100.

    Returns:
    - float: The profit from the trade.
    """
    initial_cost = initial_premium * shares_per_contract * contracts
    final_value = final_premium * shares_per_contract * contracts

    profit = final_value - initial_cost
    return profit



if __name__ == '__main__':
    # Example usage:
    initial_premium = 2.0  # $2 per share
    final_premium = 4.5    # $4.5 per share
    contracts = 1          # 1 contract

    # We buy one contract, one contract encompassing 100 shares, by default
    # In the yfinance table, the initial_premium correspond to the 'lastPrice' column

    profit = calculate_option_profit(initial_premium, final_premium, contracts)
    print(f"Profit from the trade: ${profit:.2f}")

