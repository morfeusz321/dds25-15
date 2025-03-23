import redis 

def init_checkout_state(db, order_id: str):
    """Initialize the checkout state for an order. If the state already exists, return and error."""
    checkout_key = f"checkout_state:{order_id}"
    
    if db.exists(checkout_key):
        # get the state of the order
        state = get_check_state(db, order_id)

        if state["stock_subtracted"] ==  -1 and state["payment_made"] == -1:
            return "Order already in progress"

        if state["stock_subtracted"] == 1 and state["payment_made"] == 1:
            return "Order already completed"
        
        
    
    db.hset(f"checkout_state:{order_id}", mapping={
        "stock_subtracted": -1,
        "payment_made": -1
    })
    return "Order state initialized"

def update_checkout_statedb (db, order_id: str, field: str, value: int):
    """Thread-safe update for a specific order's checkout state using Redis transactions."""
    checkout_key = f"checkout_state:{order_id}"
    
    with db.pipeline() as pipe:
        try:
            pipe.watch(checkout_key)  # Watch the order_id row
            current_state = db.hgetall(checkout_key)  # Read current state

            # Simulate some processing (optional)
            print(f"Current State Before Update: {current_state}")

            # Start transaction
            pipe.multi()
            pipe.hset(checkout_key, field, value)  # Update field atomically
            pipe.execute()  # Commit changes

            print(f"Updated {checkout_key}: {field} -> {value}")
        except redis.WatchError:
            print(f"Conflict detected for {checkout_key}, retrying...")


def get_check_state(db, order_id: str):
    """Retrieve the checkout state for an order."""
    state = db.hgetall(f"checkout_state:{order_id}")
    return {k.decode(): int(v) for k, v in state.items()} if state else None
