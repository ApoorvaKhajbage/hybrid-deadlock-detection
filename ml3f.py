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

# Load the transaction log from CSV
df = pd.read_csv('transactions2.csv')

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
for i, transaction_id in enumerate(df['transaction_id']):
    if df['lock_requested'][i]:  # If a transaction is requesting a lock
        for j, holder_transaction_id in enumerate(df['transaction_id']):
            if df['lock_held'][j] and i != j:  # Another transaction is holding a lock
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

# Predict deadlock using the ML model based on the last transaction
current_features = [[df['lock_held'].iloc[-1], df['lock_requested'].iloc[-1]]]  # Using the last transaction for prediction
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