import pandas as pd
import logging

logging.basicConfig(
    filename='logs/project.log', 
    format='[%(asctime)s][%(levelname)s] %(message)s', 
    level=logging.DEBUG)

logger = logging.getLogger(__name__)

class DataProcessor:
            
    def process_resolution(self, data):
        """
        Extracts the resolution from the column 'ScreenResolution' string.
        
        Args:
            data (pandas.DataFrame): The input DataFrame containing the 'ScreenResolution' column
            
        Returns:
            str: The extracted resolution in the format "widthxheight" if found, otherwise None.
        """        
        try:
            logger.info("Processing resolution column")
            resolution_list = []
            for resolution in data['ScreenResolution']:
                if pd.notna(resolution):
                    parts = resolution.split()
                    found_resolution = False
                    for part in parts:
                        if 'x' in part: 
                            width_height = part.replace(' ', '').split('x') 
                            if len(width_height) == 2 and all(w.isdigit() for w in width_height):
                                resolution_list.append(f"{width_height[0]}x{width_height[1]}")
                                found_resolution = True
                                break
                    if not found_resolution:
                        resolution_list.append('unknown')
                else:
                    resolution_list.append('unknown')
            
            data['resolution'] = resolution_list  
            return data
        except Exception as e:
            logging.error(f"Error processing resolution column: {e}")
            raise

            
    def process_display_type(self, data):
        """
        Processes the display type for each row in the resolution column of the DataFrame.

       Args:
           data (pandas.DataFrame): The input DataFrame containing the 'resolution' column

        Returns:
            pd.DataFrame: The DataFrame with an additional 'display_type' column.
            """
        try:
            logger.info("Processing display type column")
            display_type_list = []
            
            for resolution in data['resolution']:
                try:
                    if pd.isna(resolution):
                        display_type_list.append('Unknown')
                        continue
                    
                    width, height = map(int, resolution.split('x'))
                    
                    if width >= 3840 and height >= 2160:
                        display_type_list.append('4K')
                    elif width >= 2560 and height >= 1440:
                        display_type_list.append('Quad HD')
                    elif width >= 1920 and height >= 1080:
                        display_type_list.append('Full HD')
                    elif width >= 1280 and height >= 720:
                        display_type_list.append('HD')
                    else:
                        display_type_list.append('Other')
                except Exception as inner_e:
                    logging.error(f"Error processing resolution '{resolution}': {inner_e}")
                    display_type_list.append('Invalid Resolution')
    
            data['display_type'] = display_type_list
            return data
        except Exception as e:
            logging.error(f"Error processing display type column: {e}")
            raise


    def process_os(self, data):
        """
        Extracts the operating system from the 'OpSys' column and converts to lowercase.
        
        Args:
            data (pandas.DataFrame): The input DataFrame containing the 'OpSys' column.
            
        Returns:
            pandas.DataFrame: The modified DataFrame with an additional 'os' column containing the extracted operating systems.
        """
        try:
            logger.info("Processing OpSys column")
            os_list = []
            for os in data['OpSys']:
                if pd.notna(os):
                    os_list.append(os.split(' ')[0].lower())
                else:
                    os_list.append('unknown')
            data['os'] = os_list
            return data
        except Exception as e:
            logging.error(f"Error processing OS: {e}")
            raise


    def process_weight(self, data):
        """
        Extracts weight from the 'Weight' column and converts to float.
        
        Args:
            data (pandas.DataFrame): The input DataFrame containing the 'Weight' column.

        Returns:
            pandas.DataFrame: The modified DataFrame with an additional 'weight' column containing the extracted weight values.
        """
        try:
            logger.info("Processing Weight column")
            weight_list = []
            for weight in data['Weight']:
                if pd.notna(weight):
                    weight_list.append(float(weight.split('kg')[0]))
                else:
                    weight_list.append('unknown')
            data['weight'] = weight_list
            return data
        except Exception as e:
            logging.error(f"Error processing weight: {e}")
            raise


    def process_ram(self, data):
        """
        Extracts RAM value from the 'Ram' column and converts to integer.
        
        Args:
            data (pandas.DataFrame): The input DataFrame containing the 'Ram' column.

        Returns:
            pandas.DataFrame: The modified DataFrame with an additional 'ram' column containing the extracted RAM values.
        """
        try:
            logger.info("Processing Ram column")
            ram_list = []
            for ram in data['Ram']:
                if pd.notna(ram):
                    ram_list.append(int(ram.split('GB')[0]))
                else:
                    ram_list.append(0)
            data['ram'] = ram_list
            return data
        except Exception as e:
            logging.error(f"Error processing RAM: {e}")
            raise


    def convert_data_types(self, df):
        """
        Converts specified columns to the appropriate data types.
    
        Args:
            df (pd.DataFrame): The DataFrame to be cleaned and converted.
    
        Returns:
            pd.DataFrame: The DataFrame with updated data types.
        """
        try:
            logger.info("Converting data types")
            df['weight'] = df['weight'].astype(float)
            df['resolution'] = df['resolution'].astype('string')
            df['display_type'] = df['display_type'].astype('string')
            df['os'] = df['os'].astype('string')
            return df
        except Exception as e:
            logging.error(f"Error converting data types: {e}")
            raise
