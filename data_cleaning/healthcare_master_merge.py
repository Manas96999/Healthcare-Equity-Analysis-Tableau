import pandas as pd
import os

# Set your data directory
DATA_DIR = './dataset/'

# 1. LOAD DATASETS
hosp_info = pd.read_csv(os.path.join(DATA_DIR, 'dataset/Hospital_General_Information.csv'))
clinical = pd.read_csv(os.path.join(DATA_DIR, 'dataset/Complications_and_Deaths-Hospital.csv'))
census = pd.read_csv(os.path.join(DATA_DIR, 'dataset/ACSST5Y2023.S1901-Data.csv'), low_memory=False, skiprows=1)
survey = pd.read_csv(os.path.join(DATA_DIR, 'dataset/data_tables/responses.csv'))

# 2. THE CRITICAL FIX: Standardize Facility IDs
# We force all IDs to be 6-digit strings (e.g., '10001' becomes '010001')
def standardize_id(df, col):
    df[col] = df[col].astype(str).str.replace('.0', '', regex=False).str.zfill(6)
    return df

hosp_info = standardize_id(hosp_info, 'Facility ID')
clinical = standardize_id(clinical, 'Facility ID')
survey = standardize_id(survey, 'Facility ID')

# 3. FILTER FOR LATEST SURVEY DATA
# The survey file has data from 2015-2023. We only want the most recent (07_2023)
latest_period = survey['Release Period'].max()
survey_latest = survey[survey['Release Period'] == latest_period].copy()

# 4. CLEAN ZIP CODES & CENSUS
hosp_info['ZIP Code'] = hosp_info['ZIP Code'].astype(str).str.replace('.0', '', regex=False).str.zfill(5)
census['ZIP_Key'] = census['Geography'].str[-5:]
census = census.rename(columns={'Estimate!!Households!!Median income (dollars)': 'Median_Income'})
census['Median_Income'] = pd.to_numeric(census['Median_Income'], errors='coerce')

# 5. EXECUTE THE MERGE PIPELINE
# Merge Hospital Info with Clinical Outcomes
master = pd.merge(hosp_info, clinical[['Facility ID', 'Measure Name', 'Score']], on='Facility ID', how='left')

# Merge with the Latest Survey Responses
# This is where the 'Response Rate (%)' will now correctly attach
master = pd.merge(master, survey_latest[['Facility ID', 'Response Rate (%)']], on='Facility ID', how='left')

# Merge with Census Data
master = pd.merge(master, census[['ZIP_Key', 'Median_Income']], left_on='ZIP Code', right_on='ZIP_Key', how='left')

# 6. FINAL CLEANUP & EXPORT
master.drop(columns=['ZIP_Key'], inplace=True)

# Important: Convert 'Response Rate (%)' to numeric for Power BI math
master['Response Rate (%)'] = pd.to_numeric(master['Response Rate (%)'], errors='coerce')

master.to_csv('World_Class_Healthcare_Master.csv', index=False)

print(f"Success! Master File updated with {latest_period} survey data.")
print(f"Sample Response Rate: {master['Response Rate (%)'].dropna().head(1).values}")