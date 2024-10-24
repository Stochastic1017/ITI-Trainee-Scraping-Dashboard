import pandas as pd
from os import listdir
from os.path import isfile, join

# Get all files
mypath = 'combined_data'
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
df_list = [pd.read_csv(f'{mypath}/{f}') for f in onlyfiles]

# Concatenate all dataframes
merged_df = pd.concat(df_list, ignore_index = True)

# Drop duplicate columns if necessary, and combine columns with similar names
common_cols = ['Trainee Reg No', 'TraineeName', 'Gender', 'FatherGuardianName', 'MotherName', 'Trade', 'Year', 'State']

# Drop duplicate rows based on all columns
merged_df = merged_df.drop_duplicates()

# Export result to csv
merged_df.to_csv(f'By_State_Year_Stream_ITICategory.csv', index = False)