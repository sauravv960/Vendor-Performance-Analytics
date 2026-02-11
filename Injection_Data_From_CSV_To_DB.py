import mysql.connector
import pandas as pd
import csv
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
import os
import logging

logging.basicConfig(
    filename="logs/.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


''' Connection Build With MySQL And Database:- '''

class MySQL_Connection_Manager:

    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    # Connect to MySQL Server (no database selected)
    def Connect_To_Server(self):
        try:
            self.connection = mysql.connector.connect(
                host = self.host,
                user = self.user,
                password = self.password
            )
            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
                print("Python Connected To Mysql Server Successfully")
        except Exception as Error:
            print(f"Error connecting to server:- {Error}")
            
        return self.connection

    # Create database if not exists and switch to it
    def Create_And_Switch_Database(self,Database_Name):
        try:
            if not self.connection or not self.connection.is_connected():
                print("No Active Server Is Connnected")
                return
                
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {Database_Name}")
            self.cursor.execute(f"USE {Database_Name}")
            print(f"{Database_Name} Database Is Created/Selected Successfully")
    
        except Exception as Error:
            print(f"Error in create/use database :- {Error}")
            
     # Directly connect to a specific database
    def Create_Database_Connection(self,Database_Name):
        try:
            self.connection = mysql.connector.connect(
                host =  self.host,
                user = self.user,
                password = self.password,
                database = Database_Name
            )
            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
                print(f"Connected To Database {Database_Name} Successfully")
        except Exception as Error:
            print(f"Error COnnecting To Database:-{Error}")
        
        return self.connection

    # Close connection safely
    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection or self.connection.is_connected():
                self.connection.close()
            print("MySQL Connection Is Closed Successfully")
        except Exception as Error:
            print(f"Error While Closing Connection:- {Error}")

''' Create Tables/Schema:- '''

class Table_Creation:
    
    def __init__(self, connection, Folder_Name):
        """
        Initialize the Table_Creation object.

        Parameters:
        - connection : active MySQL connection object
        - Folder_Name : path of folder containing CSV files
        """
        self.connection = connection                  # MySQL connection
        self.cursor = self.connection.cursor()       # Cursor for executing queries
        self.Folder_Name = Folder_Name                # Folder with CSV files

    def Create_Tables(self, Table_Creation_Query, Table_Name):
        """
        Execute CREATE TABLE query for a single table.
        Returns True if successful, False otherwise.
        """
        try:
            # Execute CREATE TABLE query
            self.cursor.execute(Table_Creation_Query)

            # Commit the DDL operation
            self.connection.commit()

            logging.info(f"{Table_Name} table created successfully.")
            return True

        except Exception as Error:
            # Log SQL execution error
            logging.error(f"Error in {Table_Name} table creation :- {Error}")
            return False

    def Pandas_To_Sql_dtype(self, pd_dtype):
        """
        Map pandas dtype to corresponding MySQL datatype.
        """
        try:
            if pd_dtype == "int64":
                return "INT"
            elif pd_dtype == "float64":
                return "FLOAT"
            elif pd_dtype == "bool":
                return "BOOLEAN"
            elif pd_dtype == "datetime64[ns]":
                return "DATETIME"
            else:
                # Default for object/string and unknown types
                return "VARCHAR(255)"

        except Exception as Error:
            logging.error(f"Error in Pandas_To_Sql_dtype :- {Error}")
            return "VARCHAR(255)"

    def Generate_Create_Tables_Query(self, df_sample, Table_Name):
        """
        Generate CREATE TABLE query dynamically from pandas DataFrame schema.
        """
        try:
            Lines = []   # Will store column definitions

            # Loop through each column and its dtype
            for col, dtype in df_sample.dtypes.items():
                sql_dtype = self.Pandas_To_Sql_dtype(str(dtype))
                Lines.append(f"    {col} {sql_dtype}")

            # Join all column definitions with comma and newline
            Column_sql = ",\n".join(Lines)

            # Final CREATE TABLE query
            Table_Creation_Query = (
                f"CREATE TABLE IF NOT EXISTS {Table_Name}(\n"
                f"{Column_sql}\n"
                f");"
            )

            return Table_Creation_Query

        except Exception as Error:
            logging.error(f"Error in Generate_Create_Tables_Query for {Table_Name} :- {Error}")
            return None

    def Fetch_And_Create_Tables(self):
        """
        Main pipeline:
        - Loop through all CSV files in folder
        - Generate CREATE TABLE query
        - Create tables in MySQL
        - Return lists of created and failed tables
        """

        Created_Tables = []     # Successfully created tables
        Failed_Tables = []      # Failed table creations

        try:
            # Safety check: connection must be active
            if not self.connection or not self.connection.is_connected():
                logging.error("No Active Server Is Connected!! Please First Connect The Server")
                return Created_Tables, Failed_Tables

            # Loop over all files in the folder
            for file in os.listdir(self.Folder_Name):

                try:
                    # Process only CSV files (case-insensitive)
                    if file.lower().endswith(".csv"):

                        # Full path of CSV file
                        File_Path = os.path.join(self.Folder_Name, file)

                        # Read only first 1000 rows to infer schema
                        df_sample = pd.read_csv(File_Path, nrows=1000)

                        # Table name derived from file name
                        Table_Name = os.path.splitext(file)[0]

                        # Generate CREATE TABLE query
                        Create_Table_Query = self.Generate_Create_Tables_Query(df_sample, Table_Name)

                        # If query generation failed, mark as failed
                        if not Create_Table_Query:
                            Failed_Tables.append(Table_Name)
                            continue

                        # Execute table creation
                        Success_Cre_Tab = self.Create_Tables(Create_Table_Query, Table_Name)

                        # Track success or failure
                        if Success_Cre_Tab:
                            Created_Tables.append(Table_Name)
                        else:
                            Failed_Tables.append(Table_Name)

                except Exception as Error:
                    # File-level error: log and continue with next file
                    logging.error(f"Failed Processing File {file} :- {Error}")
                    Failed_Tables.append(file)

        except Exception as Error:
            # Fatal error in whole pipeline
            logging.error(f"Error In Fetch_And_Create :- {Error}")

        # Always return result lists (never None)
        return Created_Tables, Failed_Tables


''' Injection The Data In Table '''

class Insert_Data:
    def __init__(self, connection, Folder_Name):
        self.connection = connection
        self.cursor = self.connection.cursor()
        self.Folder_Name = Folder_Name

    # Fast check: does table already have any data?
    def Is_Table_Empty(self, table_name):
        try:
            # fetch only 1 row
            self.cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            row = self.cursor.fetchone()
            return row is None   # True = empty, False = already has data
        except Exception as e:
            logging.error(f"Error checking table {table_name} emptiness :- {e}")
            return False

    def Insert_Many_From_CSV(self, Create_Table_List, Batch_size=100000):

        Injected_Tables = []
        Failed_Injection_Tables = []

        try:
            for Table_Name in Create_Table_List:

                try:
                    # FIRST: Check table before touching big CSV file
                    if not self.Is_Table_Empty(Table_Name):
                        logging.info(f"Table {Table_Name} already has data. Skipping insertion completely.")
                        print(f"Skipping {Table_Name} (already loaded)")
                        continue

                    print(f"Insertion Started Of Table {Table_Name}")
                    logging.info(f"-------- Insertion Started Of Table {Table_Name} --------")

                    # Build CSV file path safely
                    File_Path = os.path.join(self.Folder_Name, f"{Table_Name}.csv")

                    with open(File_Path, "r", encoding="utf-8") as file:
                        reader = csv.reader(file)

                        # Read header row (column names)
                        header = next(reader)

                        batch = []
                        total = 0

                        # Build dynamic INSERT query
                        Column_Names = ", ".join(header)
                        Place_holder = ", ".join(["%s"] * len(header))

                        Insert_Query = f"""
                        INSERT INTO {Table_Name} ({Column_Names})
                        VALUES ({Place_holder})
                        """

                        # Read and insert rows in batches
                        for row in reader:

                            # Convert empty strings to None (SQL NULL)
                            row = [None if x == "" else x for x in row]

                            batch.append(tuple(row))

                            # When batch size reached, insert
                            if len(batch) == Batch_size:
                                self.cursor.executemany(Insert_Query, batch)
                                self.connection.commit()

                                total += len(batch)
                                logging.info(f"{Table_Name}: Inserted {total} rows")

                                batch.clear()

                        # Insert remaining rows
                        if batch:
                            self.cursor.executemany(Insert_Query, batch)
                            self.connection.commit()

                            total += len(batch)
                            logging.info(f"{Table_Name}: Final inserted {total} rows")

                    Injected_Tables.append(Table_Name)
                    logging.info(f"-------- Completed Insertion For Table: {Table_Name} --------")
                    print(f"Insertion Completed Of Table {Table_Name}")

                except Exception as Error:
                    logging.error(f"Error Inserting Rows Into Table {Table_Name} :- {Error}")
                    Failed_Injection_Tables.append(Table_Name)

        except Exception as Error:
            logging.error(f"Fatal Error In Insert_Many_From_CSV :- {Error}")

        # Always return status lists
        return Injected_Tables, Failed_Injection_Tables

if __name__ == "__main__":
    
    Manager = MySQL_Connection_Manager(
        host = "localhost",
        user = "root",
        password = "password"
    )
    
    Manager.Connect_To_Server()
    Manager.Create_And_Switch_Database("inventory")
    Conn = Manager.Create_Database_Connection("inventory")
    # Manager.close()
    
    Table_manager = Table_Creation(Conn, "data")
    
    created, failed = Table_manager.Fetch_And_Create_Tables()
    
    print("Created tables:", created)
    print("Failed tables:", failed)
    
    Inserter = Insert_Data(Conn, "data")

    injected_tables, failed_inserts = Inserter.Insert_Many_From_CSV(created, Batch_size=100000)
    
    print("Injected tables:", injected_tables)
    print("Failed inserts:", failed_inserts)