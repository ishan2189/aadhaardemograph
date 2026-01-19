import os
import sys

import pandas as pd

from src.data_processor import AadhaarDataProcessor

def main():
    api_key = os.environ.get('DATA_GOV_IN_API_KEY')
    use_local_enrol = True
    use_update_api = bool(api_key)

    if not api_key:
        print("DATA_GOV_IN_API_KEY not set. Using local enrolment CSVs and skipping update API calls.")
        # Empty string is fine because we avoid API usage when key is missing
        api_key = ""

    processor = AadhaarDataProcessor(api_key=api_key)

    print("Starting Aadhaar Data Processing...")

    # 1. Fetch Data
    limit = 5000

    enrol_df = processor.get_enrolment_data(
        limit=limit,
        use_local_csv=use_local_enrol,
        csv_folder=os.path.join(os.path.dirname(__file__), "api_csv"),
    )

    update_df = processor.get_update_data(limit=limit) if use_update_api else pd.DataFrame()

    if enrol_df.empty and update_df.empty:
        print("No data fetched. Exiting.")
        return

    # 2. Clean Data
    print("Cleaning data...")
    enrol_clean = processor.clean_enrolment_data(enrol_df)
    update_clean = processor.clean_update_data(update_df)

    # 3. Merge Data
    print("Merging datasets...")
    if update_clean.empty:
        merged_df = enrol_clean.copy()
    else:
        try:
            merged_df = processor.merge_datasets(enrol_clean, update_clean)
        except ValueError as e:
            print(f"Merge failed: {e}")
            return

    # 4. Preprocess / Aggregation
    print("Aggregating data...")
    summary_df = processor.preprocess_for_lifecycle_optimization(merged_df)

    # 5. Output
    output_file = 'processed_aadhaar_data.csv'
    if not merged_df.empty:
        merged_df.to_csv(output_file, index=False)
        print(f"\nProcessed data saved to {output_file}")
        print(f"Total Records: {len(merged_df)}")
        print("\nSample Data:")
        print(merged_df.head())

        if not summary_df.empty:
            summary_file = 'aadhaar_lifecycle_summary.csv'
            summary_df.to_csv(summary_file, index=False)
            print(f"\nSummary data saved to {summary_file}")
            print("\nSummary of potential future updates (Age 0-5 enrolments indicate need for updates in ~5 years):")
            if 'age_0_5' in summary_df.columns:
                top_districts = summary_df.groupby(['state', 'district'])['age_0_5'].sum().sort_values(ascending=False).head(5)
                print(top_districts)
    else:
        print("Merged dataframe is empty.")

if __name__ == "__main__":
    main()
