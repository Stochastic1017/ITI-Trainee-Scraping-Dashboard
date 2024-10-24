import os
import shutil
import glob

def move_files_to_year_folders(src_folder):
    # Create a list of all CSV files in the source folder
    csv_files = glob.glob(os.path.join(src_folder, '*.csv'))
    
    for file_path in csv_files:
        # Extract the year from the filename
        filename = os.path.basename(file_path)
        parts = filename.split('_')
        if len(parts) > 1:
            year = parts[1]
            
            # Create a new folder with the name of the year if it doesn't exist
            year_folder = os.path.join(src_folder, year)
            os.makedirs(year_folder, exist_ok=True)
            
            # Move the file to the year folder
            shutil.move(file_path, os.path.join(year_folder, filename))
            print(f'Moved {filename} to {year_folder}')
        else:
            print(f'Filename {filename} does not match the expected pattern')

# Example usage
src_folder = 'data'
move_files_to_year_folders(src_folder)
