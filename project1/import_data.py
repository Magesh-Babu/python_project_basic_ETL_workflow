import pandas as pd
import pyodbc
import logging

logging.basicConfig(
    filename='logs/project.log', 
    format='[%(asctime)s][%(levelname)s] %(message)s', 
    level=logging.DEBUG)

class DataImporter:
    def __init__(self, conn_str):
        self.conn_str = conn_str
        self.logger = logging.getLogger(__name__)

    def import_data(self, table_name):
        """
        Imports data from a SQL Server database into a pandas DataFrame.
        
        Args:
            table_name (str): The name of the table.
        
        Returns:
            pandas.DataFrame: The imported data as a DataFrame.
        """
        try:
            self.logger.info("Importing data")
            # Connect to SQL Server
            conn = pyodbc.connect(self.conn_str)

            # Read data into a DataFrame
            query = f'SELECT * FROM {table_name}'
            df = pd.read_sql(query, conn)

            conn.close()
            return df
        except Exception as e:
            logging.error(f"Error importing data from {table_name}: {e}")
            raise