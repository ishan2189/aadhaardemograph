import pandas as pd
from src.data_processor import AadhaarDataProcessor
import os
import sys

def main():
    api_key = os.environ.get('DATA_GOV_IN_API_KEY')
    if not api_key:
        print("Error: DATA_GOV_IN_API_KEY environment variable not set.")
        print("Please set it with: export DATA_GOV_IN_API_KEY='your_api_key'")
        # Fallback for demo purposes if user forgets, but warn heavily
        # In a real scenario, we would exit. Here I will exit to be safe.
        sys.exit(1)

    processor = AadhaarDataProcessor(api_key=api_key)

    print("Starting Aadhaar Data Processing...")

    # 1. Fetch Data
    limit = 5000

    enrol_df = processor.get_enrolment_data(limit=limit)
    update_df = processor.get_update_data(limit=limit)

    if enrol_df.empty and update_df.empty:
        print("No data fetched. Exiting.")
        return

    # 2. Clean Data
    print("Cleaning data...")
    enrol_clean = processor.clean_enrolment_data(enrol_df)
    update_clean = processor.clean_update_data(update_df)

    # 3. Merge Data
    print("Merging datasets...")
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
