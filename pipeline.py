
# sytem libs
import copy

#external libs
from pymongo import MongoClient
import pymysql

#internal libs
import config

# pipeline constants
RESET_MONGO_COLLECTIONS_ON_UPDATE = True
PRINT_INFO = True
PRINT_RESULTS = True


def initialize_mysql():
    """
    Initializes and returns a MySQL database based on config.
    """
    return pymysql.connect(
        host = config.MYSQL_HOST,
        user = config.MYSQL_USERNAME,
        password = config.MYSQL_PASSWORD,
        db = config.MYSQL_DB,
    )

def initialize_mongo():
    """
    Initializes and returns MongoDB database based on config.
    """
    return MongoClient(config.MONGO_HOST, config.MONGO_PORT)[config.MONGO_DB]

def extract_data(mysql_cursor):
    """
    Given a cursor, extracts data from MySQL dataset and returns all the tables
    with the data.
    """
    general_ledger_accounts = execute_mysql_query('select * from general_ledger_accounts', mysql_cursor, 'fetchall')
    terms = execute_mysql_query('select * from terms', mysql_cursor, 'fetchall') 
    vendors = execute_mysql_query('select * from vendors', mysql_cursor, 'fetchall')
    invoices= execute_mysql_query('select * from invoices', mysql_cursor, 'fetchall')
    invoice_line_items= execute_mysql_query('select * from invoice_line_items', mysql_cursor, 'fetchall')
    vendor_contacts= execute_mysql_query('select * from vendor_contacts', mysql_cursor, 'fetchall')
    invoice_archive= execute_mysql_query('select * from invoice_archive', mysql_cursor, 'fetchall')

    tables = (general_ledger_accounts, terms, vendors, invoices, invoice_line_items, vendor_contacts, invoice_archive)

    return tables

def execute_mysql_query(sql, cursor, query_type):
    """
    executes a given sql, pymsql cursor and type.
    """
    if query_type == 'fetchall':
        cursor.execute(sql)
        return cursor.fetchall()

    elif query_type == 'fetchone':
        cursor.execute(sql)
        return cursor.fetchall()
    else:
        pass

def transform_data(dataset, table):
    """
    Transforms the data to load it into MongoDB, returns a JSON object.
    """
    dataset_collection = []
    tmp_collection = {}

    if table == 'general_ledger_accounts':
        for item in dataset[0]:
            tmp_collection['account_number'] = item[0]
            tmp_collection['account_description'] = item[1]
            dataset_collection.append(copy.copy(tmp_collection))

        return dataset_collection

    elif table == 'terms':
        for item in dataset[1]:
            tmp_collection['terms_id'] = item[0]
            tmp_collection['terms_description'] = item[1]
            tmp_collection['terms_due_days'] = item[2]
            dataset_collection.append(copy.copy(tmp_collection))

        return dataset_collection

    elif table == 'vendors':
        for item in dataset[2]:
            tmp_collection['vendor_id'] = item[0]
            tmp_collection['vendor_name'] = item[1]
            tmp_collection['vendor_address1'] = item[2]
            tmp_collection['vendor_address2'] = item[3]
            tmp_collection['vendor_city'] = item[4]
            tmp_collection['vendor_state'] = item[5]
            tmp_collection['vendor_zipcode'] = item[6]
            tmp_collection['vendor_phone'] = item[7]
            tmp_collection['vendor_contact_last_name'] = item[8]
            tmp_collection['vendor_contact_first_name'] = item[9]
            tmp_collection['vendor_terms_id'] = item[10]
            tmp_collection['vendor_account_number'] = item[11]
            dataset_collection.append(copy.copy(tmp_collection))

        return dataset_collection

    elif table == "invoices":
        for item in dataset[3]:
            tmp_collection['invoice_id'] = item[0]
            tmp_collection['vendor_id'] = item[1]
            tmp_collection['invoice_number'] = item[2]
            tmp_collection['invoice_date'] = item[3]
            tmp_collection['invoice_total'] = item[4]
            tmp_collection['payment_total'] = item[5]
            tmp_collection['credit_total'] = item[6]
            tmp_collection['terms_id'] = item[7]
            tmp_collection['invoice_due_date'] = item[8]
            tmp_collection['payment_date'] = item[9]
            dataset_collection.append(copy.copy(tmp_collection))

        return dataset_collection

    elif table == "invoice_line_items":
        for item in dataset[4]:
            tmp_collection['invoice_id'] = item[0]
            tmp_collection['invoice_sequence'] = item[1]
            tmp_collection['account_number'] = item[2]
            tmp_collection['line_item_amount'] = item[3]
            tmp_collection['line_item_description'] = item[4]
            dataset_collection.append(copy.copy(tmp_collection))

        return dataset_collection
    
    elif table == 'vendor_contacts':
        for item in dataset[5]:
            tmp_collection['vendor_id'] = item[0]
            tmp_collection['last_name'] = item[1]
            tmp_collection['first_name'] = item[2]
            dataset_collection.append(copy.copy(tmp_collection))

        return dataset_collection

    elif table == "invoice_archive":
        for item in dataset[6]:
            tmp_collection['invoice_id'] = item[0]
            tmp_collection['vendor_id'] = item[1]
            tmp_collection['invoice_number'] = item[2]
            tmp_collection['invoice_date'] = item[3]
            tmp_collection['invoice_total'] = item[4]
            tmp_collection['payment_total'] = item[5]
            tmp_collection['credit_total'] = item[6]
            tmp_collection['terms_id'] = item[7]
            tmp_collection['invoice_due_date'] = item[8]
            tmp_collection['payment_date'] = item[9]
            dataset_collection.append(copy.copy(tmp_collection))

        return dataset_collection

def load_data(mongo_collection, dataset_collection):
    """
    Loads the data into MongoDB and returns the results.
    """
    if RESET_MONGO_COLLECTIONS_ON_UPDATE:
        mongo_collection.delete_many({})

    return mongo_collection.insert_many(dataset_collection)

def main():
    """
    Main method starts a pipeline, extracts data, transforms it and loads it 
    into a Mongo client.
    """
    if PRINT_INFO:
        print('Starting data pipeline')
        print('Initializing MySQL connection')
    mysql = initialize_mysql()

    if PRINT_INFO:
        print('MySQL connection completed.')
        print('Starting data pipeline stage 1: Extracting data from MySQL.')
    mysql_cursor = mysql.cursor()
    mysql_data = extract_data(mysql_cursor)

    if PRINT_INFO:
        print('Stage 1 completed! Data successfully extracted from MySQL')
        print('Starting data pipeline stage 2: Transforming data from MySQL for MongoDB')
        print('Transforming genres dataset')
    general_ledger_accounts_collection = transform_data(mysql_data, "general_ledger_accounts")
    
    if PRINT_INFO:
        print('Successfully transformed genres dataset')
        print('Transforming users dataset')
    terms_collection = transform_data(mysql_data, "terms")
    
    if PRINT_INFO:
        print('Successfully transformed users dataset')
        print('Transforming movies dataset')
    vendors_collection = transform_data(mysql_data, "vendors")

    if PRINT_INFO:
        print('Successfully transformed genres dataset')
        print('Transforming users dataset')
    invoices_collection = transform_data(mysql_data, "invoices")
    
    if PRINT_INFO:
        print('Successfully transformed users dataset')
        print('Transforming movies dataset')
    invoice_line_items_collection = transform_data(mysql_data, "invoice_line_items")

    if PRINT_INFO:
        print('Successfully transformed genres dataset')
        print('Transforming users dataset')
    vendor_contacts_collection = transform_data(mysql_data, "vendor_contacts")
    
    if PRINT_INFO:
        print('Successfully transformed users dataset')
        print('Transforming movies dataset')
    invoice_archive_collection = transform_data(mysql_data, "invoice_archive")
    
    if PRINT_INFO:
        print('Successfully transformed users dataset')
        print('Stage 2 completed! Data successfully transformed')
        print('Intialising MongoDB connection')
    mongo = initialize_mongo()

    if PRINT_INFO:
        print('MongoDB connection Completed')
        print('Starting data pipeline stage 3: Loading data into MongoDB')
    result = load_data(mongo['general_ledger_accounts'], general_ledger_accounts_collection)
    
    if PRINT_RESULTS:
        print('Successfully loaded general_ledger_accounts')
        print(result)
    result = load_data(mongo['terms'], terms_collection)
    
    if PRINT_RESULTS:
        print('Successfully loaded terms')
        print(result)
    result = load_data(mongo['vendors'], vendors_collection)

    if PRINT_RESULTS:
        print('Successfully loaded vendors')
        print(result)
    result = load_data(mongo['invoices'], invoices_collection)
    
    if PRINT_RESULTS:
        print('Successfully loaded invoices')
        print(result)
    result = load_data(mongo['invoice_line_items'], invoice_line_items_collection)

    if PRINT_RESULTS:
        print('Successfully loaded invoice_line_items')
        print(result)
    result = load_data(mongo['vendor_contacts'], vendor_contacts_collection)
    
    if PRINT_RESULTS:
        print('Successfully loaded vendor_contacts')
        print(result)
    result = load_data(mongo['invoice_archive'], invoice_archive_collection)
    
    if PRINT_RESULTS:
        print('Successfully loaded invoice_archive')
        print(result)
    
    if PRINT_INFO:
        print('Stage 3 completed! Data successfully loaded')
        print('Closing MySQL connection')
    mysql.close()
    if PRINT_INFO:
        print('MySQL connection closed successfully')
        print('Ending data pipeline')

if __name__ == "__main__":
    main()