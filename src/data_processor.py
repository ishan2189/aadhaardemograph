import glob
import os
import pandas as pd
import requests
import time
from typing import List, Dict, Optional

class AadhaarDataProcessor:
    def __init__(self, api_key: str):
        self.api_key = api_key
        # Resource IDs
        self.enrolment_resource_id = 'ecd49b12-3084-4521-8f7e-ca8bf72069ba'
        self.update_resource_id = '19eac040-0b94-49fa-b239-4f2fd8677d53'
        self.base_url = "https://api.data.gov.in/resource/"

    def fetch_data(self, resource_id: str, limit: int = 1000, offset: int = 0) -> List[Dict]:
        """
        Fetches data from the API with pagination support (basic implementation).
        """
        url = f"{self.base_url}{resource_id}"
        params = {
            'api-key': self.api_key,
            'format': 'json',
            'limit': limit,
            'offset': offset
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get('records', [])
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {resource_id}: {e}")
            return []

    def _load_enrolment_from_csv(self, folder_path: Optional[str] = None) -> pd.DataFrame:
        """
        Loads and concatenates enrolment CSV files from the given folder.
        """
        base_folder = folder_path or os.path.join(os.path.dirname(__file__), "api_csv")
        pattern = os.path.join(base_folder, "api_data_aadhar_enrolment_*.csv")

        csv_files = sorted(glob.glob(pattern))
        if not csv_files:
            print(f"No CSV files found at {pattern}")
            return pd.DataFrame()

        frames = []
        for file_path in csv_files:
            try:
                frames.append(pd.read_csv(file_path))
            except Exception as exc:  # pandas can raise many errors depending on file issues
                print(f"Error reading {file_path}: {exc}")

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def get_enrolment_data(self, limit: int = 1000, use_local_csv: bool = True, csv_folder: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieves enrolment data.

        By default, uses local CSVs under src/api_csv to avoid API calls.
        Set use_local_csv=False to fall back to the remote API.
        """
        if use_local_csv:
            df = self._load_enrolment_from_csv(folder_path=csv_folder)
            if df.empty:
                print("No local enrolment data loaded.")
            return df

        print(f"Fetching enrolment data (limit={limit})...")
        records = self.fetch_data(self.enrolment_resource_id, limit=limit)
        if not records:
            print("No enrolment records fetched.")
            return pd.DataFrame()

        df = pd.json_normalize(records)
        return df

    def get_update_data(self, limit: int = 1000) -> pd.DataFrame:
        print(f"Fetching update data (limit={limit})...")
        records = self.fetch_data(self.update_resource_id, limit=limit)
        if not records:
            print("No update records fetched.")
            return pd.DataFrame()

        df = pd.json_normalize(records)
        return df

    def clean_enrolment_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the enrolment dataframe.
        Expected columns: date, state, district, pincode, age_0_5, age_5_17, age_18_greater
        """
        if df.empty:
            return df

        # Standardize column names (strip whitespace, lowercase)
        df.columns = df.columns.str.strip().str.lower()

        # Rename columns for clarity if needed
        # Ensuring expected columns exist
        expected_numeric_cols = ['age_0_5', 'age_5_17', 'age_18_greater']
        for col in expected_numeric_cols:
            if col not in df.columns:
                df[col] = 0 # Default to 0 if missing

        # Convert numeric columns
        for col in expected_numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Convert date
        if 'date' in df.columns:
            # Assuming dd-mm-yyyy based on observation
            df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')

        # Handle string columns
        for col in ['state', 'district', 'pincode']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()

        return df

    def clean_update_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the update dataframe.
        Expected columns: date, state, district, pincode, demo_age_5_17, demo_age_17_
        """
        if df.empty:
            return df

        # Standardize column names
        df.columns = df.columns.str.strip().str.lower()

        # Rename 'demo_age_17_' to something more readable like 'demo_age_18_plus'
        if 'demo_age_17_' in df.columns:
            df.rename(columns={'demo_age_17_': 'demo_age_18_plus'}, inplace=True)

        expected_numeric_cols = ['demo_age_5_17', 'demo_age_18_plus']
        for col in expected_numeric_cols:
            if col not in df.columns:
                df[col] = 0

        # Convert numeric columns
        for col in expected_numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Convert date
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')

        # Handle string columns
        for col in ['state', 'district', 'pincode']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()

        return df

    def merge_datasets(self, enrolment_df: pd.DataFrame, update_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merges enrolment and update datasets on Date, State, District, Pincode.
        """
        # Ensure keys are present and have same type
        join_keys = ['date', 'state', 'district', 'pincode']

        # Check if keys exist in both
        for key in join_keys:
            if key not in enrolment_df.columns or key not in update_df.columns:
                error_msg = f"Join key '{key}' missing in one of the dataframes."
                print(f"Error: {error_msg}")
                raise ValueError(error_msg)

        merged_df = pd.merge(enrolment_df, update_df, on=join_keys, how='outer', suffixes=('_enrolment', '_update'))

        # Fill NaNs in numeric columns with 0
        numeric_cols = [c for c in merged_df.columns if 'age' in c]
        merged_df[numeric_cols] = merged_df[numeric_cols].fillna(0).astype(int)

        return merged_df

    def preprocess_for_lifecycle_optimization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregates data to help optimize Aadhaar lifecycle (Mandatory Biometric Updates).
        Focus: 0-5 (Need update at 5) and 5-17 (Need update at 15).
        """
        if df.empty:
            return df

        # Aggregate by District and Month to see trends
        # Create 'month_year' column
        df['month_year'] = df['date'].dt.to_period('M')

        # Group by State, District, Month
        group_cols = ['state', 'district', 'month_year']
        # sum numeric columns
        agg_cols = {col: 'sum' for col in df.columns if 'age' in col}

        if not agg_cols:
            return df

        summary = df.groupby(group_cols).agg(agg_cols).reset_index()

        # Calculate potential backlog or demand
        # Enrolments 0-5 indicate future demand for "Update at 5"
        # We can rename columns for clarity

        return summary
