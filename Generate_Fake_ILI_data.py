import pandas as pd
import numpy as np
import os

np.random.seed(42)

# Parameters
num_records_per_company = 100
companies = [
    {"name": "FakePipeCo", "pipeline_id": "PIPE-001"},
    {"name": "ImaginaryFlow", "pipeline_id": "PIPE-002"},
    {"name": "PhantomPipes", "pipeline_id": "PIPE-003"},
    {"name": "GhostLine", "pipeline_id": "PIPE-004"},
]

inspection_date = "2025-09-30"
all_data = []

for company in companies:
    data = {
        "Company": [company["name"]] * num_records_per_company,
        "PipelineID": [company["pipeline_id"]] * num_records_per_company,
        "InspectionDate": [inspection_date] * num_records_per_company,
        "FeatureID": [f"{company['pipeline_id']}-FTR-{i+1:04d}" for i in range(num_records_per_company)],
        "FeatureType": np.random.choice(["Dent", "Corrosion", "Crack", "Weld Anomaly"], num_records_per_company),
        "Location_km": np.round(np.random.uniform(0, 50, num_records_per_company), 2),
        "Depth_mm": np.round(np.random.uniform(0.5, 15, num_records_per_company), 2),
        "Length_mm": np.round(np.random.uniform(10, 200, num_records_per_company), 2),
        "Width_mm": np.round(np.random.uniform(5, 100, num_records_per_company), 2),
        "Severity": np.random.choice(["Low", "Medium", "High"], num_records_per_company),
        "RepairRecommended": np.random.choice([True, False], num_records_per_company)
    }
    df = pd.DataFrame(data)
    all_data.append(df)

# Combine all company data
final_df = pd.concat(all_data, ignore_index=True)
print(final_df.head())

# Save to CSV in the same folder as the script
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, "fake_ili_data_multiple_companies.csv")
final_df.to_csv(csv_path, index=False)