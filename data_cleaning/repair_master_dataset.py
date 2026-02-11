
import pandas as pd

# Define the file paths
master_file = 'World_Class_Healthcare_Master_cleaned.csv'
responses_file = 'dataset/data_tables/responses.csv'
output_file = 'World_Class_Healthcare_Master_Final.csv'

# The 13 columns to keep in the final output
elite_13_columns = [
    'Facility ID', 'ZIP Code', 'State', 'City/Town', 'Facility Name', 
    'Hospital Type', 'Hospital Ownership', 'Measure Name', 'Score', 
    'Compared to National', 'Hospital overall rating', 'Response Rate (%)', 
    'Median_Household_Income'
]

# 1. Load the datasets
print("Loading datasets...")
try:
    master_df = pd.read_csv(master_file, dtype={'Facility ID': str, 'ZIP Code': str})
    responses_df = pd.read_csv(responses_file, dtype={'Facility ID': str})
    print("Datasets loaded successfully.")
except FileNotFoundError as e:
    print(f"Error loading files: {e}. Please ensure the files are in the correct directory.")
    exit()

# 2. Standardize 'Facility ID' columns
print("Standardizing Facility IDs...")
master_df['Facility ID'] = master_df['Facility ID'].str.zfill(6)
responses_df['Facility ID'] = responses_df['Facility ID'].str.zfill(6)
print("Facility IDs standardized.")

# 3. Filter for the latest 'Release Period' in responses data
latest_release_period = '07_2023'
print(f"Filtering responses for the latest release period: {latest_release_period}...")
responses_latest_df = responses_df[responses_df['Release Period'] == latest_release_period].copy()
# Drop duplicates to ensure one entry per facility
responses_latest_df.drop_duplicates(subset='Facility ID', keep='first', inplace=True)
print("Responses filtered.")

# 4. Perform the left join
print("Performing left join...")
# We need to drop the existing (mostly empty) 'Response Rate (%)' column from the master file before merging
if 'Response Rate (%)' in master_df.columns:
    master_df = master_df.drop(columns=['Response Rate (%)'])

# Select only the necessary columns from the responses dataframe for the merge
responses_to_merge = responses_latest_df[['Facility ID', 'Response Rate (%)']]

repaired_df = pd.merge(master_df, responses_to_merge, on='Facility ID', how='left')
print("Join completed.")

# 5. Clean 'Response Rate (%)' column
print("Cleaning 'Response Rate (%)' column...")
repaired_df['Response Rate (%)'] = pd.to_numeric(repaired_df['Response Rate (%)'], errors='coerce')
print("Column cleaned.")

# 6. Ensure all 'Elite 13' columns are present and in order
print("Finalizing columns...")
# Add any missing elite columns with NaN values if they weren't in the merge result
for col in elite_13_columns:
    if col not in repaired_df.columns:
        repaired_df[col] = pd.NA

# Reorder and filter to keep only the elite 13
final_df = repaired_df[elite_13_columns]
print("Columns finalized.")

# 7. Save the final repaired file
print(f"Saving repaired file to {output_file}...")
final_df.to_csv(output_file, index=False)
print("File saved.")

# 8. Verify the fix
print("\nVerification: First 5 rows of the 'Response Rate (%)' column:")
print(final_df['Response Rate (%)'].head())

print("\nScript finished successfully!")
