from cassandra.cluster import Cluster
from collections import defaultdict
import uuid
from datetime import datetime
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra cluster IP
session = cluster.connect('test')  # Replace with your keyspace name

# Create deadlock_info table (if it doesn't exist already)
session.execute("""
    CREATE TABLE IF NOT EXISTS deadlock_info (
        deadlock_id UUID PRIMARY KEY,
        timestamp TIMESTAMP,
        involved_transactions LIST<UUID>,
        status TEXT
    )
""")

# Load the transaction log (Replace this with fetching from Cassandra or CSV data)
data = {
    'transaction_id': [
        'eeb9197b-e67c-4654-838a-5a1fca78f6d0', 'cc2cdb38-a287-4a69-90c9-e19b49e6daff',
        'df4d8a6a-0aa6-42d9-a19e-5a7af4bbba07', 'dd357ec6-6cf7-4e85-8ea0-fec86d877559',
        '11c5e0c7-dd10-4ee2-bb54-afdb00262590', 'b6e7dc07-84cd-47b2-a774-e8fa92dd0ece',
        '1ff270a7-5865-4b48-9a67-632e31c437f6', '31cd1831-b1d9-4a2a-83dc-9427ffb0c70c',
        'a448fbd6-8ed8-45fb-89d6-343714ff3d17', '19b6434a-5fd1-4eab-903d-e5a4ac8cd28f'
    ],
    'timestamp': [
        '2024-10-17 23:12:00', '2024-10-17 23:13:00', '2024-10-17 23:14:00',
        '2024-10-17 23:15:00', '2024-10-17 23:16:00', '2024-10-17 23:17:00',
        '2024-10-17 23:18:00', '2024-10-17 23:19:00', '2024-10-17 23:20:00',
        '2024-10-17 23:21:00'
    ],
    'lock_held': [True, False, False, True, False, False, True, False, True, True],
    'lock_requested': [True, True, True, True, True, False, False, True, True, False],
    'deadlock_occurred': [1, 0, 0, 1, 0, 0, 1, 0, 1, 0]  # Historical label for deadlock occurrence
}

# Convert data to DataFrame
df = pd.DataFrame(data)

# Prepare features and labels
X = df[['lock_held', 'lock_requested']]
y = df['deadlock_occurred']

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train Random Forest Classifier
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Evaluate the model
y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred))

# Dependency graph: maps transactions waiting for a lock to transactions holding the lock
dependency_graph = defaultdict(list)

# Build the dependency graph
for i, transaction_id in enumerate(data['transaction_id']):
    if data['lock_requested'][i]:  # If a transaction is requesting a lock
        for j, holder_transaction_id in enumerate(data['transaction_id']):
            if data['lock_held'][j] and i != j:  # Another transaction is holding a lock
                dependency_graph[transaction_id].append(holder_transaction_id)

# Function to detect cycles in the dependency graph using DFS
def detect_cycle(graph):
    visited = set()  # Track visited nodes
    stack = set()  # Track nodes in the current recursion stack (for cycle detection)
    involved_transactions = []

    def dfs(transaction_id):
        if transaction_id in stack:  # Cycle detected
            return True
        if transaction_id in visited:
            return False

        visited.add(transaction_id)
        stack.add(transaction_id)
        involved_transactions.append(transaction_id)

        # Visit all dependent transactions (those holding the locks this transaction is waiting for)
        for dependent_id in graph[transaction_id]:
            if dfs(dependent_id):
                return True

        stack.remove(transaction_id)
        return False

    # Check for cycles in all components of the graph
    for transaction_id in graph:
        if dfs(transaction_id):
            return True, involved_transactions
    return False, []

# Detecting deadlock and storing information in Cassandra
is_deadlock, involved_transactions = detect_cycle(dependency_graph)

# Predict deadlock using the ML model based on current locks
current_features = [[data['lock_held'][-1], data['lock_requested'][-1]]]  # Using the last transaction for prediction
predicted_deadlock = model.predict(current_features)

if is_deadlock or predicted_deadlock[0] == 1:
    print("Deadlock detected!")
    
    # Convert involved_transactions to UUIDs
    involved_transactions_uuid = [uuid.UUID(tid) for tid in involved_transactions]

    # Insert the deadlock information into the deadlock_info table
    deadlock_id = uuid.uuid4()
    timestamp = datetime.now()

    session.execute("""
        INSERT INTO deadlock_info (deadlock_id, timestamp, involved_transactions, status)
        VALUES (%s, %s, %s, %s)
    """, (deadlock_id, timestamp, involved_transactions_uuid, 'active'))

    print(f"Deadlock information stored with ID {deadlock_id}")
else:
    print("No deadlock detected.")