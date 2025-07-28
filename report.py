import re

def extract_strike(symbol):
    # Match 2 digits (year), then capture the strike before CE/PE
    match = re.search(r'\d{2}[A-Z]{3}\d{2}(\d+)(CE|PE)', symbol)
    if match:
        return int(match.group(1))
    else:
        return None

# Example usage:
symbol = "HDFCBANK26JAN251834524CE"
strike = extract_strike(symbol)
print(f"Strike price: {strike}")