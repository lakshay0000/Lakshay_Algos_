def _create_option_signal(
        self,
        symbol:str,
        spot_data: dict,
        execution_data: dict,
        option_type: str,
        ts: int
    ) -> Optional[Signal]:
        """
        Creates an option entry signal with metadata.
        """
        spot_price = spot_data.get('c')
        if spot_price is None:
            return None
                
        # 1. Select Strike
        strike_price = int(round(spot_price / 50) * 50)
        
        # 2. Determine Expiry
        current_date_str = spot_data.get('ti_str', '')
        # expiry_dates = spot_data.get('expiry', [])
        # print(spot_data.get('CurrentExpiry'))
        
        # if not expiry_dates:
        #     return None
        
        # If current date is expiry day, use next expiry
        if current_date_str == spot_data.get('CurrentExpiry'):
            expiry = spot_data.get('NextExpiry')
        else:
            expiry = spot_data.get('CurrentExpiry')
        
        # 3. Construct Symbol
        strike_symbol = f"{symbol}{expiry}{strike_price}{option_type}"
        
        # 4. Check Data Availability
        # execution_data is now a pre-resolved dict for the current timestamp: { symbol: {c: ...} }
        opt_data = execution_data.get(strike_symbol)
        if not opt_data or 'c' not in opt_data:
            self.log(f"Data missing for selected option: {strike_symbol} at {ts}", level="WARNING")
            return None
            
        option_price = opt_data['c']
        
        # 5. Action (PE=Buy, CE=Sell)
        action = "BUY" if option_type == "PE" else "SELL"
        
        # 6. Metadata stored in trade.metadata dict
        candle_stop = spot_data.get('min_low', 0.0) if option_type == 'PE' else spot_data.get('max_high', 0.0)
        
        self.log(f"Generating {action} signal for {strike_symbol} at {option_price}. Spot: {spot_price}, Strike: {strike_price}, Expiry: {expiry}")

        # FIX: Store expiry date in metadata for proper exit logic
        return self.create_signal(
            symbol=strike_symbol,
            action=action,
            quantity=self.quantity,
            timestamp=ts,
            reason="RSI_CROSS_ENTRY",
            price=option_price,
            custom_metadata={
                "type": option_type,
                "expiry": expiry,  # ← CRITICAL: Store expiry for risk management
                "candle_stop_loss": candle_stop,
                "strike": float(strike_price),
                "underlying_entry": spot_price
            }
        )
