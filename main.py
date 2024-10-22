import os
import requests
import pandas as pd
from io import StringIO
from time import sleep
from cfonts import render
import mysql.connector  # MySQL connector for Python

# MySQL connection details (as per your configuration)
DB_HOST = 'localhost'
DB_PORT = 3306
DB_USERNAME = 'root'
DB_PASSWORD = '1234'  # If you have a password, add it here
DB_NAME = 'demo'

# API details
ADMIN_API_KEY = os.getenv('ADMIN_API_KEY', '16VcVIY5vclCGtKPbkfzcR6dE8c80erkSRjcuoIVnrfdcCvJL42NwAcGFjM21c')
API_BASE_URL = os.getenv('API_BASE_URL', 'https://public.zylyty.com/31964')

# API endpoints
endpoints = {
    'accounts': f'{API_BASE_URL}/download/accounts.csv',
    'clients': f'{API_BASE_URL}/download/clients.csv',
    'transactions': f'{API_BASE_URL}/transactions'  # Assuming this is a large file from the API
}

# Define headers for the request
headers = {
    'Authorization': f'Bearer {ADMIN_API_KEY}',
}

# Print welcome message
print(render('Hello ZYLYTY!', colors=['cyan', 'magenta'], align='center', font='3d'))
print(f"Admin API Key: {ADMIN_API_KEY}")
print(f"Database Host: {DB_HOST}")
print(f"Database Port: {DB_PORT}")
print(f"Database Username: {DB_USERNAME}")
print(f"Database Name: {DB_NAME}")

# MySQL connection function
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            print("Successfully connected to MySQL database.")
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Function to create tables and views in the database
def create_tables_and_views(connection):
    cursor = connection.cursor()

    # Create tables
    create_tables_sql = """
    CREATE TABLE IF NOT EXISTS clients (
        client_id VARCHAR(36),
        client_name VARCHAR(100),
        client_email VARCHAR(255),
        client_birth_date DATE
    );

    CREATE TABLE IF NOT EXISTS accounts (
        account_id INT,
        client_id VARCHAR(36),
        PRIMARY KEY (account_id)
    );

    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id INT,
        timestamp DATETIME,
        account_id INT,
        amount DECIMAL(10, 2),
        type VARCHAR(50),
        medium VARCHAR(255),
        PRIMARY KEY (transaction_id)
    );
    """

    # Create views
    create_views_sql = """
    CREATE VIEW IF NOT EXISTS clients_with_debit_no_other AS
    SELECT DISTINCT
        c.client_id,
        c.client_name,
        c.client_email
    FROM clients c
    JOIN accounts a ON c.client_id = a.client_id
    JOIN transactions t ON a.account_id = t.account_id
    WHERE t.type = 'True'
    AND t.medium != 'other'
    AND NOT EXISTS (
        SELECT 1
        FROM transactions t2
        JOIN accounts a2 ON t2.account_id = a2.account_id
        WHERE a2.client_id = c.client_id
        AND t2.medium = 'other'
    );

    CREATE VIEW IF NOT EXISTS monthly_high_debits AS
    SELECT 
        DATE_FORMAT(timestamp, '%Y-%m-01') AS month,
        account_id,
        SUM(amount) AS total_debits,
        COUNT(transaction_id) AS transaction_count
    FROM transactions
    WHERE type = 'True'
    GROUP BY account_id, DATE_FORMAT(timestamp, '%Y-%m-01')
    HAVING SUM(amount) > 10000
    ORDER BY month ASC, account_id ASC;

    CREATE VIEW IF NOT EXISTS total_daily_transactions AS
    SELECT 
        DATE(timestamp) AS date,
        SUM(CASE WHEN medium = 'card' THEN ABS(amount) ELSE 0 END) AS card_absolute,
        SUM(CASE WHEN medium = 'card' AND type = 'True' THEN amount
                 WHEN medium = 'card' AND type = 'False' THEN -amount ELSE 0 END) AS card_net,
        SUM(CASE WHEN medium = 'online' THEN ABS(amount) ELSE 0 END) AS online_absolute,
        SUM(CASE WHEN medium = 'online' AND type = 'True' THEN amount
                 WHEN medium = 'online' AND type = 'False' THEN -amount ELSE 0 END) AS online_net,
        SUM(CASE WHEN medium = 'transfer' THEN ABS(amount) ELSE 0 END) AS transfer_absolute,
        SUM(CASE WHEN medium = 'transfer' AND type = 'True' THEN amount
                 WHEN medium = 'transfer' AND type = 'False' THEN -amount ELSE 0 END) AS transfer_net
    FROM transactions
    GROUP BY DATE(timestamp)
    HAVING card_absolute > 0 OR online_absolute > 0 OR transfer_absolute > 0
    ORDER BY date ASC;
    """

    try:
        # Execute SQL to create tables and views
        cursor.execute(create_tables_sql, multi=True)
        cursor.execute(create_views_sql, multi=True)

        connection.commit()
        print("Tables and views have been created or already exist.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()

# Function to download CSV data
def download_csv(file_name, url):
    try:
        # Make the GET request to download the CSV file
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            print(f"Request for {file_name} was successful!")

            # Save the content of the response as a CSV file
            with open(file_name, 'wb') as file:
                file.write(response.content)

            print(f"CSV file has been saved to {file_name}")

        else:
            print(f"Failed to retrieve {file_name}. Status code: {response.status_code}")
            print("Response Text:", response.text)

    except Exception as e:
        print(f"An error occurred while downloading {file_name}: {e}")

# Function to save data into MySQL
def save_to_database(data, table_name, connection):
    cursor = connection.cursor()

    if table_name == 'accounts':
        for _, row in data.iterrows():
            try:
                query = ("INSERT INTO accounts (account_id, client_id) " 
                         "VALUES (%s, %s)")
                cursor.execute(query, (row['account_id'], row['client_id']))
                cursor.fetchall()  # Ensures that any results are cleared

            except mysql.connector.Error as err:
                print(f"Error: {err}")

    elif table_name == 'clients':
        for _, row in data.iterrows():
            try:
                query = ("INSERT INTO clients (client_id, client_name, client_email, client_birth_date) "
                         "VALUES (%s, %s, %s, %s)")
                cursor.execute(query, (row['client_id'], row['client_name'], row['client_email'], row['client_birth_date']))
                cursor.fetchall()
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    elif table_name == 'transactions':
        for transaction in data:
            try:
                query = ("INSERT INTO transactions (transaction_id, timestamp, account_id, amount, type, medium) "
                         "VALUES (%s, %s, %s, %s, %s, %s)")
                cursor.execute(query, (transaction['transaction_id'], transaction['timestamp'], transaction['account_id'],
                                       transaction['amount'], transaction['type'], transaction['medium']))
                cursor.fetchall()
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    connection.commit()
    cursor.close()

# Function to fetch and process large transaction data
# Fetch transactions (if it's a large file, ensure proper handling)
def fetch_transactions():
    print("Fetching transactions...")
    transactions = []

    response = requests.get(endpoints['transactions'], headers=headers)

    if response.status_code == 200:
        transaction_data = response.json()

        # Check if response is a list (direct transactions data)
        if isinstance(transaction_data, list):
            transactions.extend(transaction_data)  # Add transactions directly
            print(f"Fetched {len(transaction_data)} transactions, Total so far: {len(transactions)}")
        else:
            print(f"Unexpected data format. Expected a list, got: {type(transaction_data)}")
    else:
        print(f"Failed to fetch transactions. Status code: {response.status_code}")
        print("Response Text:", response.text)

    return transactions


# Main function to orchestrate data import
def main():
    connection = connect_to_db()
    if connection is None:
        print("Unable to connect to the database. Exiting.")
        return

    # Create tables and views
    create_tables_and_views(connection)

    valid_clients_count = 0
    valid_accounts_count = 0
    valid_transactions_count = 0

    # Download and process accounts CSV
    download_csv('./accounts.csv', endpoints['accounts'])
    accounts_data = pd.read_csv('./accounts.csv')
    print(f"Fetched {len(accounts_data)} accounts from CSV.")
    valid_accounts_count += len(accounts_data)
    save_to_database(accounts_data, 'accounts', connection)

    # Download and process clients CSV
    download_csv('./clients.csv', endpoints['clients'])
    clients_data = pd.read_csv('./clients.csv')
    print(f"Fetched {len(clients_data)} clients from CSV.")
    valid_clients_count += len(clients_data)
    save_to_database(clients_data, 'clients', connection)

    # Fetch and process transactions (large data file)
    transactions_data = fetch_transactions()
    print(f"Fetched {len(transactions_data)} transactions.")
    valid_transactions_count += len(transactions_data)
    save_to_database(transactions_data, 'transactions', connection)

    # Print results
    print(f"Valid clients: {valid_clients_count}")
    print(f"Valid accounts: {valid_accounts_count}")
    print(f"Valid transactions: {valid_transactions_count}")

    connection.close()

if __name__ == '__main__':
    main()
