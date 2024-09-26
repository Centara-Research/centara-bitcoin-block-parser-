import csv
import time
from blockchain_parser.blockchain import Blockchain
from blockchain_parser.output import Output

blk_path = '/root/.bitcoin/blocks'
blockchain = Blockchain(blk_path)
csv_filename = 'transactions_output.csv'
balances_filename = 'wallet_balances.csv'

def get_transaction_by_block_height(height, csv_filename):
    try:
        with open(csv_filename, mode='r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                if int(row['index']) == height:
                    return row
    except FileNotFoundError:
        print("The CSV file was not found.")
    return None

def get_transactions_by_timestamp(timestamp, csv_filename):
    transactions = []
    try:
        with open(csv_filename, mode='r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                if row['timestamp'] == timestamp:
                    transactions.append(row)
    except FileNotFoundError:
        print("The CSV file was not found.")
    return transactions

def save_transactions_and_balances(transaction_limit=3):
    from blockchain_parser.block import Block

    total_transactions = 0
    start_time = time.time()

    # Initialize the wallet balances dictionary
    wallet_balances = {}

    with open(csv_filename, mode='w', newline='') as csv_file:
        # Adding 'sent amounts' and 'received amounts' as separate fields
        fieldnames = ['index', 'block_hash', 'timestamp', 'sender', 'receiver', 'amount_received', 'amount_sent', 'individual_sent', 'individual_received', 'fee']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

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

                    # Write the formatted row to CSV
                    writer.writerow({
                        'index': total_transactions,
                        'block_hash': blk_obj.hash,
                        'timestamp': blk_obj.header.timestamp,
                        'sender': sender_addresses,
                        'receiver': receiver_addresses,
                        'amount_received': amount_received,
                        'amount_sent': amount_sent,
                        'individual_sent': sent_amounts,  # Individual sent amounts
                        'individual_received': received_amounts,  # Individual received amounts
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

    # Save the wallet balances to a CSV file
    with open(balances_filename, mode='w', newline='') as balances_file:
        balance_fieldnames = ['wallet_id', 'balance']
        balance_writer = csv.DictWriter(balances_file, fieldnames=balance_fieldnames)
        balance_writer.writeheader()

        for wallet, balance in wallet_balances.items():
            balance_writer.writerow({'wallet_id': wallet, 'balance': balance})

    processing_time = time.time() - start_time
    print(f"Total processing time and saving to CSV: {processing_time:.2f} seconds")

# Main function to start the process
def main():
    save_transactions_and_balances(transaction_limit=3000)

    # Test reading by block height
    height_to_search = 33
    transaction = get_transaction_by_block_height(height_to_search, csv_filename)
    if transaction:
        print(f"Transaction found by block height {height_to_search}: {transaction}")
    else:
        print(f"No transaction found for block height {height_to_search}.")

    # Test reading by timestamp
    timestamp_to_search = "2010-10-17 12:07:04"  # Enter a valid timestamp
    transactions = get_transactions_by_timestamp(timestamp_to_search, csv_filename)
    if transactions:
        print(f"Transactions found for timestamp {timestamp_to_search}:")
        for tx in transactions:
            print(tx)
    else:
        print(f"No transactions found for timestamp {timestamp_to_search}.")

if __name__ == "__main__":
    main()
