
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
    try:
        df = pd.read_csv(input_csv)
        logging.info(f'Read input CSV: {input_csv}')

        # Aggregate data
        aggregated_data = df.groupby(['State', 'Year', 'Category']).size().reset_index(name='Count')

        logging.info(f'Aggregated data from CSV')
        return aggregated_data

    except Exception as e:
        logging.error(f'Error processing file: {input_csv}, Error: {e}')
        sys.exit(1)

if __name__ == '__main__':
    aggregated_df = aggregate_counts(input_csv)
    aggregated_df.to_csv(output_csv, index=False)
    logging.info(f'Aggregated data saved to {output_csv}')
