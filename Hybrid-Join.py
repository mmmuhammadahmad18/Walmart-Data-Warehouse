import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from collections import deque, defaultdict
from datetime import datetime
import threading
import time
import sys
from queue import Queue
import warnings
warnings.filterwarnings('ignore')

class HybridJoin:
    def __init__(self, hs=10000, vp=500):
        """
        Initialize HYBRIDJOIN algorithm components.
        
        Args:
            hs: Hash table size (number of slots)
            vp: Disk partition size (number of tuples per partition)
        """
        self.hs = hs  # Hash table 
        self.vp = vp  # Disk partition size buffer
        self.w = hs   # Slots that are available in hash table
        
        # Usage of Data structures
        self.hash_table = defaultdict(list)  # tuples for multimap key
        self.queue = deque()  # For join keys FIFO is used here
        self.stream_buffer = Queue()  # Buffer used for incoming tuples
        self.disk_buffer = []  # Usage of buffer for disk partition
        
        # Showing Stats
        self.processed_count = 0
        self.matched_count = 0
        self.running = True
    
    def sanitize_value(self, value):
        """Handling of numpy to python types (converting to python types)."""
        if pd.isna(value):
            return None
        if hasattr(value, 'item'):  # numpy types have .item() method used
            return value.item()
        return value
        
    def hash_function(self, key):
        """Hash function for mapping key to slot numbers."""
        return hash(key) % self.hs
    
    def load_stream_tuples(self):
        """Loading up to w tuples from stream buffer and sending to hash table."""
        loaded = 0
        max_to_load = min(self.w, 1000)  # Loading maximum 1000 at time
        
        while loaded < max_to_load and not self.stream_buffer.empty():
            tuple_data = self.stream_buffer.get()
            key = tuple_data['Customer_ID']
            
            # Adding  into hash table
            slot = self.hash_function(key)
            queue_node = {'key': key, 'data': tuple_data}
            self.hash_table[key].append(queue_node)
            
            # Adding into queue
            self.queue.append(queue_node)
            loaded += 1
            
        self.w -= loaded  # Decreasing available slots by amount loaded
        return loaded
    
    def load_disk_partition(self, key, master_data):
        """
        Loading relevant partition from master data based on key and dictionary used for master and customer data.
        """
        self.disk_buffer = []
        
        # Finding records that are matched in master data
        customer_matches = master_data['customers'][
            master_data['customers']['Customer_ID'] == key
        ]
        
        if not customer_matches.empty:
            self.disk_buffer.extend(customer_matches.to_dict('records'))
        
        return len(self.disk_buffer)
    
    def probe_and_join(self, conn):
        """
        Probing hash table with disk buffer tuples and performing join.
        """
        matched_keys = set()
        joined_records = []
        
        for disk_tuple in self.disk_buffer:
            key = disk_tuple['Customer_ID']
            
            if key in self.hash_table:
                # If a Match is found - perform join
                for queue_node in self.hash_table[key]:
                    stream_tuple = queue_node['data']
                    
                    # Creating joined record
                    joined_record = {
                        **stream_tuple,
                        **{k: v for k, v in disk_tuple.items() if k not in stream_tuple}
                    }
                    joined_records.append(joined_record)
                    matched_keys.add(key)
                    self.matched_count += 1
                
        # Removing tuples that are matched from hash table and queue
        for key in matched_keys:
            if key in self.hash_table:
                # Counting freed slots
                freed_slots = len(self.hash_table[key])
                self.w += freed_slots
                
                # Removing from hash table
                del self.hash_table[key]
                
                # Removing from queue
                self.queue = deque([node for node in self.queue if node['key'] != key])
        
        return joined_records
    
    def execute_join(self, conn, master_data):
        """
        Main HYBRIDJOIN execution loop method.
        """
        print("Starting HYBRIDJOIN execution...")
        batch = []
        batch_size = 100
        last_progress = time.time()
        
        while self.running or not self.stream_buffer.empty() or len(self.queue) > 0:
            # Step 1: Loading stream tuples into hash table if space is available
            if self.w > 0 and not self.stream_buffer.empty():
                loaded = self.load_stream_tuples()
                if loaded > 0:
                    print(f"Loaded {loaded} tuples from stream buffer into hash table")
                    last_progress = time.time()
            
            # Step 2: Processing the oldest key from queue
            if len(self.queue) > 0:
                oldest_node = self.queue[0]
                oldest_key = oldest_node['key']
                
                # Step 3: Loading disk partitions
                partition_size = self.load_disk_partition(oldest_key, master_data)
                
                if partition_size > 0:
                    # Step 4: Probing and joinning
                    joined_records = self.probe_and_join(conn)
                    
                    # loading joined records into batches
                    if joined_records:
                        batch.extend(joined_records)
                        
                        if len(batch) >= batch_size:
                            self.load_to_warehouse(conn, batch, master_data)
                            self.processed_count += len(batch)
                            print(f"Processed {len(batch)} records. Total: {self.processed_count}")
                            batch = []
                            last_progress = time.time()
                else:
                    # If no match found, remove it from queue and free up space
                    if len(self.queue) > 0 and self.queue[0]['key'] == oldest_key:
                        removed_nodes = [n for n in self.queue if n['key'] == oldest_key]
                        self.w += len(removed_nodes)
                        self.queue = deque([n for n in self.queue if n['key'] != oldest_key])
                        if oldest_key in self.hash_table:
                            del self.hash_table[oldest_key]
                        print(f"Removed unmatched customer {oldest_key}, freed {len(removed_nodes)} slots")
            
            # Checking for execution - if no progress for 30 seconds and buffer has data
            if time.time() - last_progress > 30:
                if not self.stream_buffer.empty() and self.w == 0 and len(self.queue) == 0:
                    print(f"WARNING: Potential deadlock detected!")
                    print(f"Buffer has data but w=0 and queue empty. Resetting w to allow progress...")
                    self.w = min(1000, self.hs // 10)  # Reseting some slots
                    last_progress = time.time()
            
            # Adding a Small delay to prevent CPU spinning
            if self.stream_buffer.empty() and len(self.queue) == 0 and self.w == 0:
                time.sleep(0.1)
            
            if time.time() - last_progress > 120:  # 2 minutes time laps for no progress
                print("ERROR: No progress for 2 minutes. Breaking loop.")
                print(f"Stream buffer size: {self.stream_buffer.qsize()}")
                print(f"Queue size: {len(self.queue)}")
                print(f"Hash table size: {len(self.hash_table)}")
                print(f"Available slots (w): {self.w}")
                break
        
        # Loading remaining batches
        if batch:
            self.load_to_warehouse(conn, batch, master_data)
            self.processed_count += len(batch)
        
        print(f"HYBRIDJOIN completed. Processed: {self.processed_count}, Matched: {self.matched_count}")
    
    def load_to_warehouse(self, conn, records, master_data):
        """Loading the joined records into data warehouse."""
        cursor = conn.cursor()
        
        for record in records:
            try:
                # Getting or creating dimension keys
                date_key = self.get_date_key(cursor, record.get('date'))
                customer_key = self.get_customer_key(cursor, record)
                product_key = self.get_product_key(cursor, record, master_data)
                store_key = self.get_store_key(cursor, record, master_data)
                
                # Getting prices from product master data
                product_id = self.sanitize_value(record['Product_ID'])
                product_info = master_data['products'][
                    master_data['products']['Product_ID'] == product_id
                ]
                
                price = product_info.iloc[0]['price$'] if not product_info.empty else 0
                quantity = record.get('quantity', 1)
                
                # Converting numpy types to Python types
                price = self.sanitize_value(price)
                quantity = self.sanitize_value(quantity)
                customer_id = self.sanitize_value(record['Customer_ID'])
                
                # Inserting into fact table
                cursor.execute("""
                    INSERT INTO fact_sales 
                    (date_key, customer_key, product_key, store_key, 
                     user_id, product_id, purchase_amount, quantity)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    date_key, customer_key, product_key, store_key,
                    customer_id, product_id,
                    price, quantity
                ))
                
            except Exception as e:
                print(f"Error loading record: {e}")
                conn.rollback()
                continue
        
        conn.commit()
    
    def get_date_key(self, cursor, date_value):
        """Get or create date dimension key."""
        if pd.isna(date_value) or date_value == '':
            date_obj = datetime.now()
        else:
            try:
                # Different date formats
                if isinstance(date_value, str):
                    for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y']:
                        try:
                            date_obj = datetime.strptime(str(date_value), fmt)
                            break
                        except:
                            continue
                    else:
                        date_obj = pd.to_datetime(date_value)
                else:
                    date_obj = pd.to_datetime(date_value)
            except:
                date_obj = datetime.now()
        
        date_key = int(date_obj.strftime('%Y%m%d'))
        
        cursor.execute("SELECT date_key FROM dim_date WHERE date_key = %s", (date_key,))
        if cursor.fetchone() is None:
            # Inserting new date dimension
            cursor.execute("""
                INSERT INTO dim_date 
                (date_key, full_date, day_of_week, day_of_month, day_of_year,
                 week_of_year, month, month_name, quarter, quarter_name, year,
                 is_weekend, season)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                date_key, date_obj.date(),
                date_obj.strftime('%A'), date_obj.day, date_obj.timetuple().tm_yday,
                date_obj.isocalendar()[1], date_obj.month, date_obj.strftime('%B'),
                (date_obj.month - 1) // 3 + 1, f'Q{(date_obj.month - 1) // 3 + 1}',
                date_obj.year, date_obj.weekday() >= 5,
                self.get_season(date_obj.month)
            ))
        
        return date_key
    
    def get_season(self, month):
        """Determining the season based on month."""
        if month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        elif month in [9, 10, 11]:
            return 'Fall'
        else:
            return 'Winter'
    
    def get_customer_key(self, cursor, record):
        """Creating customer dimension key."""
        customer_id = self.sanitize_value(record['Customer_ID'])
        
        cursor.execute("SELECT customer_key FROM dim_customer WHERE customer_id = %s", (customer_id,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # Creating the new customer
        cursor.execute("SELECT COALESCE(MAX(customer_key), 0) + 1 FROM dim_customer")
        customer_key = cursor.fetchone()[0]
        
        age_val = record.get('Age', 0)
        try:
            age = int(self.sanitize_value(age_val)) if not pd.isna(age_val) else 0
        except:
            age = 0
            
        age_group = self.get_age_group(age)
        
        cursor.execute("""
            INSERT INTO dim_customer 
            (customer_key, customer_id, gender, age, age_group, occupation,
             city_category, stay_in_current_city_years, marital_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            customer_key, customer_id, 
            str(self.sanitize_value(record.get('Gender', 'U')))[:1],
            age, age_group, 
            str(self.sanitize_value(record.get('Occupation', 'Unknown')))[:50],
            str(self.sanitize_value(record.get('City_Category', 'A')))[:1],
            str(self.sanitize_value(record.get('Stay_In_Current_City_Years', '0')))[:10],
            int(self.sanitize_value(record.get('Marital_Status', 0)))
        ))
        
        return customer_key
    
    def get_age_group(self, age):
        """Determining age group."""
        if age < 18:
            return '0-17'
        elif age < 26:
            return '18-25'
        elif age < 36:
            return '26-35'
        elif age < 46:
            return '36-45'
        elif age < 56:
            return '46-55'
        else:
            return '55+'
    
    def get_product_key(self, cursor, record, master_data):
        """Creating product dimension key."""
        product_id = self.sanitize_value(record['Product_ID'])
        
        cursor.execute("SELECT product_key FROM dim_product WHERE product_id = %s", (product_id,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # Creating new products
        cursor.execute("SELECT COALESCE(MAX(product_key), 0) + 1 FROM dim_product")
        product_key = cursor.fetchone()[0]
        
        # Fetching product details from master data
        product_info = master_data['products'][
            master_data['products']['Product_ID'] == product_id
        ]
        
        if not product_info.empty:
            prod = product_info.iloc[0]
            
            # Fetching product category 
            category_col = None
            for col in prod.index:
                if 'category' in col.lower() or 'product_c' in col.lower():
                    category_col = col
                    break
            
            category_str = str(self.sanitize_value(prod.get(category_col, ''))) if category_col else ''
            
            cat1 = category_str[:50] if category_str and category_str != 'nan' else None
            
            supplier_name = str(self.sanitize_value(prod.get('supplierName', 'Unknown')))[:100]
            
            cursor.execute("""
                INSERT INTO dim_product 
                (product_key, product_id, product_category_1, product_category_2,
                 product_category_3, product_name, supplier_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                product_key, product_id,
                cat1, None, None,
                f"Product {product_id}",
                supplier_name
            ))
        else:
            cursor.execute("""
                INSERT INTO dim_product 
                (product_key, product_id, product_category_1, product_category_2,
                 product_category_3, product_name, supplier_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                product_key, product_id,
                None, None, None, f"Product {product_id}", 'Unknown'
            ))
        
        return product_key
    
    def get_store_key(self, cursor, record, master_data):
        """Creating store dimension key."""
        # Fetching store details from product master data
        product_id = self.sanitize_value(record['Product_ID'])
        product_info = master_data['products'][
            master_data['products']['Product_ID'] == product_id
        ]
        
        if not product_info.empty:
            prod = product_info.iloc[0]
            store_id = str(self.sanitize_value(prod.get('storeID', 'STORE001')))
            store_name = str(self.sanitize_value(prod.get('storeName', f'Walmart Store {store_id}')))
        else:
            store_id = 'STORE001'
            store_name = 'Walmart Store Default'
        
        cursor.execute("SELECT store_key FROM dim_store WHERE store_id = %s", (store_id,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # Creating the new store
        cursor.execute("SELECT COALESCE(MAX(store_key), 0) + 1 FROM dim_store")
        store_key = cursor.fetchone()[0]
        
        city_category = str(self.sanitize_value(record.get('City_Category', 'A')))
        
        cursor.execute("""
            INSERT INTO dim_store 
            (store_key, store_id, store_name, store_location)
            VALUES (%s, %s, %s, %s)
        """, (
            store_key, store_id[:20],
            store_name[:100], 
            city_category
        ))
        
        return store_key


def stream_producer(hybrid_join, transaction_file, batch_size=100):
    """
    Thread function to continuously feed stream buffer with transaction data in batches.
    """
    print("Starting stream producer thread...")
    
    try:
        # Reading the transactional data in chunks
        chunk_count = 0
        for chunk in pd.read_csv(transaction_file, chunksize=batch_size):
            for _, row in chunk.iterrows():
                if not hybrid_join.running:
                    break
                
                # Adding it to stream buffer
                hybrid_join.stream_buffer.put(row.to_dict())
                
                # Simulating real-time arrival (small delay)
                time.sleep(0.01)
            
            chunk_count += 1
            if chunk_count % 10 == 0:
                print(f"Stream producer: Loaded {chunk_count * batch_size} transactions into buffer")
            
            if not hybrid_join.running:
                break
        
        print("Stream producer completed reading all transactions")
        hybrid_join.running = False
        
    except Exception as e:
        print(f"Error in stream producer: {e}")
        import traceback
        traceback.print_exc()
        hybrid_join.running = False


def main():
    print("=" * 60)
    print("Walmart Data Warehouse - HYBRIDJOIN Implementation")
    print("=" * 60)
    
    # Fetching database credentials
    print("\nEnter Database Connection Details:")
    db_host = input("Host (default: localhost): ") or "localhost"
    db_port = input("Port (default: 5432): ") or "5432"
    db_name = input("Database name: ")
    db_user = input("Username: ")
    db_password = input("Password: ")
    
    # file paths
    print("\nEnter CSV File Paths:")
    customer_file = input("Customer master data file: ")
    product_file = input("Product master data file: ")
    transaction_file = input("Transaction data file: ")
    
    # Connecting to database
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        print("\n✓ Database connection established")
    except Exception as e:
        print(f"\n✗ Database connection failed: {e}")
        return
    
    # Loading the master data
    try:
        print("\nLoading master data...")
        customers_df = pd.read_csv(customer_file)
        products_df = pd.read_csv(product_file)
        
        master_data = {
            'customers': customers_df,
            'products': products_df
        }
        print(f"✓ Loaded {len(customers_df)} customers and {len(products_df)} products")
        
        # Showing column names 
        print("\nCustomer columns:", customers_df.columns.tolist())
        print("Product columns:", products_df.columns.tolist())
        
    except Exception as e:
        print(f"\n✗ Failed to load master data: {e}")
        import traceback
        traceback.print_exc()
        conn.close()
        return
    
    # Initializing HYBRIDJOIN
    hybrid_join = HybridJoin(hs=10000, vp=500)
    
    # Starting the stream producer thread
    producer_thread = threading.Thread(
        target=stream_producer,
        args=(hybrid_join, transaction_file),
        daemon=True
    )
    producer_thread.start()
    
    # Executing the HYBRIDJOIN
    try:
        hybrid_join.execute_join(conn, master_data)
        print("\n✓ HYBRIDJOIN execution completed successfully")
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        hybrid_join.running = False
    except Exception as e:
        print(f"\n✗ Error during execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        print("\n✓ Database connection closed")
    
    print("\n" + "=" * 60)
    print("Execution Summary:")
    print(f"  Records processed: {hybrid_join.processed_count}")
    print(f"  Records matched: {hybrid_join.matched_count}")
    print("=" * 60)


if __name__ == "__main__":
    main()