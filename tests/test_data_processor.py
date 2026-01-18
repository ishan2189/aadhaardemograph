import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.data_processor import AadhaarDataProcessor

class TestAadhaarDataProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = AadhaarDataProcessor(api_key="test_key")

    @patch('src.data_processor.requests.get')
    def test_fetch_data_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'records': [{'date': '01-01-2022', 'state': 'TestState', 'district': 'TestDist', 'pincode': '123456', 'age_0_5': '10'}]
        }
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        data = self.processor.fetch_data('test_resource')
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['state'], 'TestState')

    def test_clean_enrolment_data(self):
        raw_data = {
            'date': ['01-01-2022', 'invalid_date'],
            'state': [' state1 ', 'state2'],
            'district': ['dist1', 'dist2'],
            'pincode': ['111111', '222222'],
            'age_0_5': ['10', 'nan'],
            'age_5_17': [5, None],
            'age_18_greater': ['20', '30']
        }
        df = pd.DataFrame(raw_data)
        cleaned_df = self.processor.clean_enrolment_data(df)

        self.assertEqual(cleaned_df['state'].iloc[0], 'State1')
        self.assertEqual(cleaned_df['age_0_5'].iloc[1], 0)
        self.assertEqual(cleaned_df['age_5_17'].iloc[1], 0)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned_df['date']))

    def test_clean_update_data(self):
        raw_data = {
            'date': ['01-01-2022'],
            'state': ['state1'],
            'district': ['dist1'],
            'pincode': ['111111'],
            'demo_age_5_17': ['15'],
            'demo_age_17_': ['25']
        }
        df = pd.DataFrame(raw_data)
        cleaned_df = self.processor.clean_update_data(df)

        self.assertIn('demo_age_18_plus', cleaned_df.columns)
        self.assertEqual(cleaned_df['demo_age_18_plus'].iloc[0], 25)

    def test_merge_datasets(self):
        enrol_df = pd.DataFrame({
            'date': pd.to_datetime(['2022-01-01']),
            'state': ['State1'],
            'district': ['Dist1'],
            'pincode': ['111111'],
            'age_0_5': [10]
        })
        update_df = pd.DataFrame({
            'date': pd.to_datetime(['2022-01-01']),
            'state': ['State1'],
            'district': ['Dist1'],
            'pincode': ['111111'],
            'demo_age_5_17': [5]
        })

        merged_df = self.processor.merge_datasets(enrol_df, update_df)
        self.assertEqual(len(merged_df), 1)
        self.assertIn('age_0_5', merged_df.columns)
        self.assertIn('demo_age_5_17', merged_df.columns)

if __name__ == '__main__':
    unittest.main()
