
import pandas as pd
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s')

if len(sys.argv) != 3:
    print("Usage: python aggregate_from_merged.py <input_csv> <output_csv>")
    sys.exit(1)

input_csv = sys.argv[1]
output_csv = sys.argv[2]

def aggregate_counts(input_csv):
    df = pd.read_csv(input_csv)

    # Aggregate data
    aggregated_data = df.groupby('State').agg(
        Year=pd.NamedAgg(column='Year', aggfunc='first'),
        Count=pd.NamedAgg(column='State', aggfunc='size'),
        Category=pd.NamedAgg(column='Category', aggfunc=lambda x: sorted(set(x)))
    ).reset_index()

    return aggregated_data

if __name__ == '__main__':
    aggregated_df = aggregate_counts(input_csv)
    aggregated_df.to_csv(output_csv, index=False)
    logging.info(f'Aggregated data saved to {output_csv}')
