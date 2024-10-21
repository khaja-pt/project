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
    'transactions': f'{API_BASE_URL}/transactions'
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
            print("Rdiresponse Text:", response.text)

    except Exception as e:
        print(f"An error occurred while downloading {file_name}: {e}")

# Loop through both endpoints (accounts and clients) and save them to files
for key, url in endpoints.items():
    file_name = f'{key}.csv'  # The file name will be accounts.csv or clients.csv
    download_csv(file_name, url)

# Function to fetch paginated transaction data and clean it
def fetch_transactions():
    page = 0
    all_transactions = []
    while True:
        try:
            url = f"{endpoints['transactions']}?page={page}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            transactions = response.json()
            
            if not transactions:  # Exit if no more transactions
                break
            
            # Clean transactions (remove duplicates and invalid)
            transactions = clean_transactions(transactions)
            all_transactions.extend(transactions)
            page += 1
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching transactions (page {page}): {e}")
            sleep(2)  # Adding a small delay before retrying
            
    return all_transactions

# Function to clean invalid or duplicated transactions
def clean_transactions(transactions):
    valid_transactions = [tx for tx in transactions if tx.get('valid', True)]
    seen = set()
    cleaned_transactions = []
    for tx in valid_transactions:
        tx_id = tx.get('transaction_id')
        if tx_id and tx_id not in seen:
            seen.add(tx_id)
            cleaned_transactions.append(tx)
    
    return cleaned_transactions

# Function to save data into MySQL
def save_to_database(data, table_name, connection):
    cursor = connection.cursor()

    if table_name == 'accounts':
        for _, row in data.iterrows():
            try:
                query = ("INSERT INTO accounts (account_id, client_id) "
                         "VALUES (%s, %s)")
                cursor.execute(query, (row['account_id'], row['client_id']))
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    elif table_name == 'clients':
        for _, row in data.iterrows():
            try:
                query = ("INSERT INTO clients (client_id, client_name, client_email, client_birth_date) "
                         "VALUES (%s, %s, %s, %s)")
                cursor.execute(query, (row['client_id'], row['client_name'], row['client_email'], row['client_birth_date']))
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    elif table_name == 'transactions':
        for transaction in data:
            try:
                query = ("INSERT INTO transactions (transaction_id, timestamp, account_id, amount, type, medium) "
                         "VALUES (%s, %s, %s, %s, %s, %s)")
                cursor.execute(query, (transaction['transaction_id'], transaction['timestamp'], transaction['account_id'],
                                       transaction['amount'], transaction['type'], transaction['medium']))
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    connection.commit()

# Main function to orchestrate data import
# Main function to orchestrate data import
def main():
    connection = connect_to_db()
    if connection is None:
        print("Unable to connect to the database. Exiting.")
        return

    valid_clients_count = 0
    valid_accounts_count = 0
    valid_transactions_count = 0

    # Download and process accounts CSV
    download_csv('accounts.csv', endpoints['accounts'])  # Pass both file_name and url
    accounts_df = pd.read_csv('accounts.csv')  # Read the downloaded CSV file
    if accounts_df is not None:
        valid_accounts_count = accounts_df['account_id'].nunique()
        save_to_database(accounts_df, 'accounts', connection)

    # Download and process clients CSV
    download_csv('clients.csv', endpoints['clients'])  # Pass both file_name and url
    clients_df = pd.read_csv('clients.csv')  # Read the downloaded CSV file
    if clients_df is not None:
        valid_clients_count = clients_df['client_id'].nunique()
        save_to_database(clients_df, 'clients', connection)

    # Fetch and process transactions
    transactions = fetch_transactions()
    if transactions:
        valid_transactions_count = len(transactions)
        save_to_database(transactions, 'transactions', connection)

    # Print the final required string with counts
    print(f"ZYLYTY Data Import Completed [{valid_clients_count}, {valid_accounts_count}, {valid_transactions_count}]")

    connection.close()

if __name__ == "__main__":
    main()

