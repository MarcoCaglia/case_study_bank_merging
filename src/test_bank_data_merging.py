import bank_data_merging
import pandas as pd


def test_case_study():
    test_instance = bank_data_merging.BankMerge('./test_data/data')
    test_instance.merge('./test_data/test_dict.json')
    test_instance.export(name='test_csv.csv',
                         output_type='csv',
                         output_path='./test_data')
    test_instance.export(name='test_json.json',
                         output_type='json',
                         output_path='./test_data')
    test_instance.export(name='test_xml.xml',
                         output_type='xml',
                         output_path='./test_data')

    actual_output = pd.read_csv('./test_data/test_csv.csv')
    expected_output = pd.read_csv('./test_data/expected_csv.csv')

    assert actual_output.equals(expected_output)
