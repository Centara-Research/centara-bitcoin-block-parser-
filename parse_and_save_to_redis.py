import time
import redis
from datetime import datetime
from blockchain_parser.blockchain import Blockchain

# Redis connection setup (adjust host and port as needed)
r = redis.Redis(host='localhost', port=6379, db=0)

blk_path = '/root/.bitcoin/blocks'
blockchain = Blockchain(blk_path)

def save_transactions_and_balances(transaction_limit=3):
    from blockchain_parser.block import Block

    total_transactions = 0
    start_time = time.time()

    # Initialize the wallet balances dictionary in memory
    wallet_balances = {}

    print("Started processing transactions")
    block_count = 0

    # Dictionary to hold transaction outputs for reference (UTXO set)
    outputs_dict = {}

    for block in blockchain.get_unordered_blocks():
        block_count += 1
        block_data = block.hex

        try:
            blk_obj = Block.from_hex(block_data)
        except Exception as e:
            print(f"Error processing block {block.hash}: {e}")
            continue

        for tx_index, tx in enumerate(blk_obj.transactions):
            try:
                total_transactions += 1

                # Get sender addresses from inputs and calculate sent amount
                senders = []
                sent_amounts = []  # List to store individual sent amounts
                amount_sent = 0

                for input in tx.inputs:
                    input_txid = input.transaction_hash
                    input_index = input.transaction_index

                    if input_txid in outputs_dict and input_index < len(outputs_dict[input_txid]):
                        previous_output = outputs_dict[input_txid][input_index]
                        sent_amounts.append(previous_output['value'])  # Store each sent amount
                        amount_sent += previous_output['value']

                        sender_address = previous_output['addresses'][0].address if previous_output['addresses'] else "Unknown"
                        senders.append(sender_address)

                        # Update sender's balance
                        if sender_address != "Unknown":
                            wallet_balances[sender_address] = wallet_balances.get(sender_address, 0) - previous_output['value']

                # Get receiver addresses and amounts from outputs
                receivers = []
                received_amounts = []  # List to store individual received amounts
                amount_received = 0
                outputs_dict[tx.txid] = []

                for output in tx.outputs:
                    output_value = output.value / 100000000  # Convert satoshis to Bitcoin
                    received_amounts.append(output_value)  # Store each received amount
                    amount_received += output_value

                    receiver_address = output.addresses[0].address if output.addresses else "Unknown"
                    receivers.append(receiver_address)

                    # Store the output for future reference (UTXO)
                    outputs_dict[tx.txid].append({'value': output_value, 'addresses': output.addresses})

                    # Update receiver's balance
                    if receiver_address != "Unknown":
                        wallet_balances[receiver_address] = wallet_balances.get(receiver_address, 0) + output_value

                # Calculate transaction fee
                fee = amount_sent - amount_received if amount_sent > 0 else 0

                # Join senders and receivers into semicolon-separated strings
                sender_addresses = '; '.join(senders) if senders else "Coinbase"
                receiver_addresses = '; '.join(receivers) if receivers else "Unknown"

                # Save transaction to Redis using an index
                transaction_key = f"transaction:index:{total_transactions}"  # Change key to index
                r.hmset(transaction_key, {
                    'block_hash': blk_obj.hash,
                    'timestamp': blk_obj.header.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    'sender': sender_addresses,
                    'receiver': receiver_addresses,
                    'individual_sent': str(sent_amounts),  # Store as a string
                    'individual_received': str(received_amounts),  # Store as a string
                    'fee': fee
                })

                print(f"index: {total_transactions}, block_hash: {blk_obj.hash}, timestamp: {blk_obj.header.timestamp}, "
                      f"sender: {sender_addresses}, receiver: {receiver_addresses}, amount received: {amount_received}, "
                      f"sent amounts: {sent_amounts}, received amounts: {received_amounts}, fee: {fee}")

                if total_transactions >= transaction_limit:
                    print(f"Transaction limit reached: {transaction_limit} transactions.")
                    break

            except Exception as e:
                print(f"Error processing transaction {tx_index + 1} in block {blk_obj.hash}: {e}")
                continue

        if total_transactions >= transaction_limit:
            break

    if block_count == 0:
        print("No blocks were processed. Please check your block file path and indexing.")

    print(f"Processed {total_transactions} transactions.")

    # Save wallet balances to Redis
    for wallet, balance in wallet_balances.items():
        r.set(f"wallet_balance:{wallet}", balance)

    processing_time = time.time() - start_time
    print(f"Total processing time and saving to Redis: {processing_time:.2f} seconds")

# Fetch transaction by index from Redis
def get_transaction_by_index(index):
    transaction_key = f"transaction:index:{index}"
    transaction = r.hgetall(transaction_key)
    if transaction:
        # Redis returns bytes, so we need to decode them
        decoded_transaction = {k.decode('utf-8'): v.decode('utf-8') for k, v in transaction.items()}
        return decoded_transaction
    else:
        return None


def get_transactions_by_date(start_date, end_date=None):
    """
    Fetch transactions from Redis based on one or two date inputs.
    If one date is provided, return all transactions for that date.
    If two dates are provided, return all transactions between those dates.

    Parameters:
    start_date (str): The date to search for in 'YYYY-MM-DD HH:MM:SS' format.
    end_date (str, optional): The end date for the range search in 'YYYY-MM-DD HH:MM:SS' format.

    Returns:
    list: A list of transactions found for the given date(s).
    """
    transactions = []
    keys = r.keys("transaction:index:*")  # Fetch all transaction keys

    # Convert start_date to a datetime object
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    
    for key in keys:
        transaction = r.hgetall(key)
        if transaction:
            # Decode the transaction data
            decoded_transaction = {k.decode('utf-8'): v.decode('utf-8') for k, v in transaction.items()}
            transaction_datetime = datetime.strptime(decoded_transaction['timestamp'], "%Y-%m-%d %H:%M:%S")

            # Check if we are looking for a single date or a date range
            if end_date is None:
                # One date provided, match against that date
                if transaction_datetime == start_date_dt:
                    transactions.append(decoded_transaction)
            else:
                # Convert end_date to a datetime object
                end_date_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                
                # Two dates provided, match against the date range
                if start_date_dt <= transaction_datetime <= end_date_dt:
                    transactions.append(decoded_transaction)

    return transactions



# Main function to start the process
def main():
    # Uncomment the line below to save transactions and balances
    save_transactions_and_balances(transaction_limit=100000)

    # Fetch and print all transactions
    '''
    transactions = get_all_transactions()
    if transactions:
        print(f"Total Transactions: {len(transactions)}")
        for tx in transactions:
            print(tx)
            break  # Print only the first transaction for brevity
    else:
        print("No transactions found.")
    

    # Test reading by index
    index_to_search = 33  # Change this index as needed
    transaction = get_transaction_by_index(index_to_search)
    if transaction:
        print(f"Transaction found by index {index_to_search}: {transaction}")
    else:
        print(f"No transaction found for index {index_to_search}.")
    '''

    single_date = "2009-01-15 10:21:46"  # Replace with the desired date
    transactions_for_single_date = get_transactions_by_date(single_date)
    print(f"Transactions for {single_date}:")
    for tx in transactions_for_single_date:
        print(tx)

    # Test fetching transactions between two dates
    
    start_date = "2009-01-14 20:05:03"  # Replace with the desired start date
    end_date = "2009-01-15 01:01:01"  # Replace with the desired end date
    transactions_in_date_range = get_transactions_by_date(start_date, end_date)
    print(f"Transactions from {start_date} to {end_date}:")
    for tx in transactions_in_date_range:
        print(tx)


if __name__ == "__main__":
    main()
