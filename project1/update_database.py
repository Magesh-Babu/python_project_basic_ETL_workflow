import pyodbc
import logging

logging.basicConfig(
    filename='logs/project.log',
    format='[%(asctime)s][%(levelname)s] %(message)s',
    level=logging.DEBUG
)

class DatabaseUpdater:
    def __init__(self, conn_str):
        self.conn_str = conn_str
        self.logger = logging.getLogger(__name__)
        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):
        """Establish the database connection and cursor."""
        try:
            self.conn = pyodbc.connect(self.conn_str)
            self.cursor = self.conn.cursor()
            self.logger.info("Database connection established successfully.")
        except Exception as e:
            self.logger.error(f"Error connecting to the database: {e}")
            raise

    def table_exists(self, table_name):
        """
        Checks if a table exists in the SQL Server database.
        
        Args:
            table_name (str): The name of the table to check.
            
        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            check_query = """
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_name = ?
            """
            self.cursor.execute(check_query, table_name)
            exists = self.cursor.fetchone()[0] == 1
            return exists
        except Exception as e:
            self.logger.error(f"Error checking if table exists: {e}")
            raise

    def create_table(self, table_name):
        """
        Creates a new table in the database if it doesn't exist. 
        The table schema is defined inside this function.
        
        Args:
            table_name (str): The name of the table to create.
        """
        try:
            if self.table_exists(table_name):
                self.logger.info(f"Table '{table_name}' already exists. Skipping creation.")
                return

            # Table creation logic based on the table_name
            if table_name == 'laptop_new':
                create_table_query = """
                CREATE TABLE laptop_new (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    Company NVARCHAR(50),
                    TypeName NVARCHAR(50),
                    resolution NVARCHAR(50),
                    display_type NVARCHAR(50),
                    ram INT,
                    os NVARCHAR(50),
                    weight NVARCHAR(50)
                )
                """
            else:
                self.logger.error(f"Table schema for '{table_name}' is not defined.")
                raise ValueError(f"Table schema for '{table_name}' is not defined.")

            self.cursor.execute(create_table_query)
            self.conn.commit()
            self.logger.info(f"Table '{table_name}' created successfully.")
        except Exception as e:
            self.logger.error(f"Error creating table '{table_name}': {e}")
            raise

    def insert_data(self, df, table_name):
        """
        Inserts data from a pandas DataFrame into a SQL Server table.
        Creates the table if it doesn't exist.
        
        Args:
            df (pandas.DataFrame): The DataFrame containing the data to insert.
            table_name (str): The name of the target SQL Server table.
        """
        try:
            self.logger.info(f"Ensuring table '{table_name}' exists.")
            self.create_table(table_name)  # Create the table if it doesn't exist

            self.logger.info(f"Inserting data into '{table_name}' table.")
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['?' for _ in df.columns])
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            for index, row in df.iterrows():
                self.cursor.execute(insert_query, tuple(row))

            self.conn.commit()
            self.logger.info(f"Data inserted into '{table_name}' successfully.")
        except Exception as e:
            self.logger.error(f"Error inserting data into {table_name}: {e}")
            raise

    def close(self):
        """Closes the database connection and cursor."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("Database connection closed.")
