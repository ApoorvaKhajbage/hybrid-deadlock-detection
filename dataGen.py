import pandas as pd
import uuid
import random
from datetime import datetime, timedelta

# Generate random transaction data
def generate_transactions(num_transactions):
    transactions = []
    start_time = datetime.now()

    for i in range(num_transactions):
        transaction_id = uuid.uuid4()
        timestamp = start_time + timedelta(seconds=i)
        lock_held = random.choice([True, False])
        lock_requested = random.choice([True, False])
        deadlock_occurred = random.choice([0, 1])  # Randomly assign deadlock occurrence for historical data

        transactions.append({
            'transaction_id': str(transaction_id),
            'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'lock_held': lock_held,
            'lock_requested': lock_requested,
            'deadlock_occurred': deadlock_occurred
        })

    return transactions

# Create a DataFrame and save to CSV
num_transactions = 50
transaction_data = generate_transactions(num_transactions)
df = pd.DataFrame(transaction_data)
df.to_csv('transactions.csv', index=False)
print("Transaction data generated and saved to transactions.csv")