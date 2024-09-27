from import_data import DataImporter
from process_data import DataProcessor
from update_database import DatabaseUpdater
import logging

logging.basicConfig(
    filename='logs/project.log', 
    format='[%(asctime)s][%(levelname)s] %(message)s', 
    level=logging.DEBUG)

def main():
    try:
        conn_str = (
            r'DRIVER={ODBC Driver 17 for SQL Server};'
            r'SERVER=DESKTOP-H7O4R1L;'
            r'DATABASE=dataset;'
            r'Trusted_Connection=yes;'
        )

        table = 'laptop'
        update_table = 'laptop_new'

        # Import Data
        importer = DataImporter(conn_str)
        df = importer.import_data(table)

        # Process Data
        processor = DataProcessor()

        df = processor.process_resolution(df)
        df = processor.process_display_type(df)
        df = processor.process_weight(df)
        df = processor.process_ram(df)
        df = processor.process_os(df)
        df = processor.convert_data_types(df)

        
        df = df[['Company', 'TypeName', 'resolution', 'display_type', 'weight', 'ram', 'os']]

        # Insert Data
        updater = DatabaseUpdater(conn_str)
        updater.insert_data(df, update_table)
        updater.close()

    except Exception as e:
        logging.error(f"An error occurred in the main process: {e}")
        raise

if __name__ == "__main__":
    main()
