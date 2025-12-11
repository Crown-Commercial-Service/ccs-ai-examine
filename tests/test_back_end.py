import pandas as pd
from scripts.generate_dummy_data import generate_dummy_contracts_data

dummy_contracts_data = generate_dummy_contracts_data()

def test_generate_dummy_contracts_data_type():
    """
    Tests for correct type of contracts dummy data
    """
    if type(dummy_contracts_data) == pd.DataFrame:
        assert True
    else:
        assert False

def test_generate_dummy_contracts_data_dims():
    """
    Tests for correct shape of contracts dummy data
    """
    if dummy_contracts_data.shape[0] == 5 and dummy_contracts_data.shape[1] == 10:
        assert True
    else:
        assert False