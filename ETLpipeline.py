import apache_beam as beam
import csv
import psycopg2
import datetime

# Define the PostgreSQL connection details
db_host = 'localhost'
db_port = '5432'
db_name = 'apacheBeam'
db_user = 'postgres'
db_password = 'pass'

# Define the CSV file path
csv_file = 'Data\dataset.csv'



# Function for filtering open payments
def filterOpenPayments(payment):
    return payment[18]


# # Function for filtering the payments which exceed due date
# def filterDueDateExtendingPayments(payment):
    
#     date_string = payment[9]
#     date_format = "%Y%m%d"
#     # Convert the string to a datetime object
#     date = datetime.strptime(date_string, date_format).date()

#     # Get today's date
#     today = datetime.date.today()

#     # Compare the dates
#     if date < today:
#         return True
#     else:
#         return False

# Define a function to insert data into the database
def insert_to_postgres(row):
        
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password)

        # Create open-payments table if it doesn't exists in database
        with conn.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS open_payments (
                    business_code VARCHAR(10),
                    cust_number VARCHAR(20),
                    name_customer VARCHAR(100),
                    clear_date VARCHAR(20),
                    business_year INT,
                    doc_id BIGINT,
                    posting_date VARCHAR(20),
                    document_create_date INT,
                    document_create_date_1 INT,
                    due_in_date INT,
                    invoice_currency VARCHAR(10),
                    document_type VARCHAR(10),
                    posting_id INT,
                    area_business VARCHAR(100),
                    total_open_amount DECIMAL(18, 2),
                    baseline_create_date INT,
                    cust_payment_terms VARCHAR(20),
                    invoice_id VARCHAR(12),
                    isOpen INT
                    );
             ''')
            # cursor.execute("INSERT INTO test1 (data1, data2) VALUES (%s, %s)", row)
            cursor.execute("INSERT INTO open_payments (business_code, cust_number, name_customer, clear_date, business_year, doc_id, posting_date, document_create_date, document_create_date_1, due_in_date, invoice_currency, document_type, posting_id, area_business, total_open_amount, baseline_create_date, cust_payment_terms, invoice_id, isOpen) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",row)

        conn.commit()

# Define the Beam pipeline
with beam.Pipeline() as pipeline:
    # Read the CSV file
    lines = pipeline | 'Read CSV' >> beam.io.ReadFromText(csv_file, skip_header_lines=1)

    #Filtering the open Payments
    filtered_data = lines | "Filtering out openPayments" >> beam.Filter(filterOpenPayments)
    

    # Transform the data into dictionaries
    data = filtered_data | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line])))
    
    # Write the data to the PostgreSQL database
    data | 'Insert to PostgreSQL' >> beam.Map(insert_to_postgres)
