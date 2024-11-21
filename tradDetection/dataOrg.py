import uuid
import random
import pandas as pd
from datetime import datetime, timedelta

# Function to generate random transaction data
def generate_transaction_data(num_transactions):
    transaction_data = {
        'transaction_id': [],
        'timestamp': [],
        'lock_held': [],
        'lock_requested': []
    }
    
    # Start timestamp
    start_time = datetime.now()

    for i in range(num_transactions):
        transaction_id = str(uuid.uuid4())
        transaction_data['transaction_id'].append(transaction_id)
        
        # Generate timestamp incrementing by 1 minute
        timestamp = start_time + timedelta(minutes=i)
        transaction_data['timestamp'].append(timestamp)

        # Randomly determine if locks are held or requested
        lock_held = random.choice([True, False])
        lock_requested = random.choice([True, False])
        
        transaction_data['lock_held'].append(lock_held)
        transaction_data['lock_requested'].append(lock_requested)

    return transaction_data

# Generate data for 30 transactions
num_transactions = 30
data = generate_transaction_data(num_transactions)

# Create a DataFrame for better visualization and export
df = pd.DataFrame(data)

# Save the data to a CSV file
csv_file = 'transactions.csv'
df.to_csv(csv_file, index=False)

print(f"Generated transaction data for {num_transactions} transactions and saved to {csv_file}.")