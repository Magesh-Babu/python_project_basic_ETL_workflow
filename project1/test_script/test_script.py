import pytest
from import_data import DataImporter
from process_data import DataProcessor

conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=DESKTOP-H7O4R1L;'
    r'DATABASE=dataset;'
    r'Trusted_Connection=yes;'
)

@pytest.fixture
def sample_data():
    importer = DataImporter(conn_str)
    df = importer.import_data('laptop_test')
    return df

def test_process_resolution(sample_data):
    processor = DataProcessor()
    df = processor.process_resolution(sample_data)
    assert 'resolution' in df.columns
    assert df['resolution'].iloc[0] == '1920x1080'

def test_process_display_type(sample_data):
    processor = DataProcessor()
    df = processor.process_resolution(sample_data)
    df = processor.process_display_type(df)
    assert 'display_type' in df.columns
    assert df['display_type'].iloc[0] == 'Full HD'

def test_process_os(sample_data):
    processor = DataProcessor()
    df = processor.process_os(sample_data)
    assert 'os' in df.columns
    assert df['os'].iloc[0] == 'windows'
    
def test_process_weight(sample_data):
    processor = DataProcessor()
    df = processor.process_weight(sample_data)
    assert 'weight' in df.columns
    assert df['weight'].iloc[0] == 2.5
    
def test_process_ram(sample_data):
    processor = DataProcessor()
    df = processor.process_ram(sample_data)
    assert 'ram' in df.columns
    assert df['ram'].iloc[0] == 8