# Aadhaar Lifecycle Optimization

This project provides a toolset to analyze Aadhaar enrolment and update trends, specifically focusing on optimizing the Mandatory Biometric Update process for children turning 5 and 15.

## Overview

The solution fetches data from the Open Government Data (OGD) Platform India, cleans it, and provides insights into:
- **Future Demand:** Areas with high 0-5 age enrolments today will see a surge in "Mandatory Update at 5 years" requests in the future.
- **Current Activity:** Areas with high update activity in the 5-17 age band.

## Structure

- `src/`: Source code for data processing and the main application.
    - `data_processor.py`: Handles API interaction, data cleaning, and merging.
    - `main.py`: Main execution script.
- `tests/`: Unit tests.
- `aadhaar_analysis.ipynb`: Jupyter Notebook for interactive analysis and visualization.

## Setup

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    pip install matplotlib seaborn nbformat jupyter
    ```

2.  **Set API Key:**
    Get your API key from [data.gov.in](https://data.gov.in/).
    ```bash
    export DATA_GOV_IN_API_KEY='your_api_key_here'
    ```

## Usage

### Run the Main Script
To fetch data, process it, and generate CSV reports (`processed_aadhaar_data.csv`, `aadhaar_lifecycle_summary.csv`):

```bash
python -m src.main
```

### Run the Notebook
To view visualizations and explore the data interactively:

```bash
jupyter notebook aadhaar_analysis.ipynb
```

## Data Sources
- **Aadhaar Monthly Enrolment Data:** [Resource ID: ecd49b12-3084-4521-8f7e-ca8bf72069ba]
- **Aadhaar Demographic Monthly Update Data:** [Resource ID: 19eac040-0b94-49fa-b239-4f2fd8677d53]
