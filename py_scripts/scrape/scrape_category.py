
#####################
### Required Modules
#####################

import time
import os
import sys
import logging
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
from multiprocessing import Manager, Pool, Process, current_process
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException, TimeoutException, StaleElementReferenceException

###########################
### Command line arguments
###########################

logging.basicConfig(level = logging.INFO, 
                    format = '%(asctime)s [%(process)d] %(levelname)s: %(message)s')

if len(sys.argv) != 2:
    print("Usage: python scrape_NCVT_MIS_category.py <output_folder>")
    sys.exit(1)

output_folder = sys.argv[1]

####################
### Helper Functions
####################

def create_folder_if_not_exists(folder_path):

    '''
    Create a folder if it does not exist.

    ------------input--------------
    folder_path: path to the folder (str) 
    '''
    
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logging.info(f"Folder '{folder_path}' created.")
    else:
        logging.info(f"Folder '{folder_path}' already exists.")

def safe_print(*args, **kwargs):

    '''
    Thread-safe print function.

    ------------input--------------
    *args: positional arguments
    **kwargs: keyword arguments
    '''
    
    with lock:
        print(*args, **kwargs)

def get_xpath(element):

    '''
    Get the xpath of an element.

    ------------input--------------
    element: BeautifulSoup element
    '''

    if element is None:
        return None
    components = []
    child = element if element.name else element.parent
    
    for parent in child.parents:
        siblings = parent.find_all(child.name, recursive=False)
        if len(siblings) > 1:
            components.append(f"{child.name}[{siblings.index(child) + 1}]")
        else:
            components.append(child.name)
        child = parent
    
    components.reverse()
    xpath = f"/{'/'.join(components)}"
    return xpath

def initialize_dropdowns(driver):

    '''
    Initialize dropdown filters.

    ------------input--------------
    driver: Selenium WebDriver
    '''

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    dropdown_menus = soup.find_all(class_='slicer-dropdown-menu')
    
    # only relevant dropdowns are selected
    # dropdowns are selected based on their position in the dropdown_menus list
    dropdown_filters = {
        'STATE_NAME': get_xpath(dropdown_menus[0]),       # Dropdown to select state
        'ACADEMIC_SESSION': get_xpath(dropdown_menus[1]), # Dropdown to select academic session
        'CATEGORY': get_xpath(dropdown_menus[11])         # Dropdown to select category
    }

    return dropdown_filters

def clear_filters(driver):

    '''
    Clear all filters.

    ------------input--------------
    driver: Selenium WebDriver
    '''
    
    try:
        # Click 'Clear All Filter' button
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//div[@aria-label="Bookmark . Clear All Filter"]'))).click()
        time.sleep(1)

    except TimeoutException:
        safe_print("Timeout while trying to click 'Clear All Filter' button.")
    
    except Exception as e:
        safe_print(f"An error occurred while clearing filters: {e}")

def extract_visible_rows(driver):

    '''
    Extract rows from the table.
    Note: This is only to scrape the VISIBLE rows of the table. Other rows need to be scrolled to be visible.

    ------------input--------------
    driver: Selenium WebDriver 
    '''
    
    retries = 5
    while retries > 0:
        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "div[role='row'][class*='row']")
            extracted_data = []
            for row in rows:
                row_data = []
                cells = row.find_elements(By.CSS_SELECTOR, "div[role='gridcell']")
                for cell in cells[1:]:
                    row_data.append(cell.text)
                extracted_data.append(row_data)
            return extracted_data[1:]
        
        except StaleElementReferenceException:
            retries -= 1            

def update_progress_bar(progress, total_tasks):

    '''
    Update progress bar.

    ------------input--------------
    progress: dictionary to track progress
    total_tasks: total number of tasks 
    '''
    
    pbar = tqdm(total = total_tasks)
    while sum(progress.values()) < total_tasks:
        pbar.n = sum(progress.values())
        pbar.refresh()
        time.sleep(0.5)
    pbar.close()

def scrape_data(driver):

    '''
    Scrape all rows from the table. Involves scrolling to scrape all rows.

    ------------input--------------
    driver: Selenium WebDriver
    '''
    
    try:
        columns = ["Trainee Reg No", "TraineeName", "Gender", "FatherGuardianName", "MotherName", "Trade"]
        df = pd.DataFrame(columns = columns)

        retries = 5
        while retries > 0:
            new_rows = extract_visible_rows(driver)
            new_df = pd.DataFrame(new_rows, columns = columns)

            if not df.empty and df.iloc[-len(new_df):].reset_index(drop = True).equals(new_df):
                retries -= 1
            else:
                retries = 5

            df = pd.concat([df, new_df], ignore_index = True).drop_duplicates().reset_index(drop = True)
            
            last_element = driver.find_elements(By.CSS_SELECTOR, "div[role='row'][class*='row']")[-1]
            last_element.location_once_scrolled_into_view
            
        return df

    except Exception as e:

        if "'NoneType' object has no attribute 'name'" in str(e):
            logging.warning("No rows to scrape.")

        else:
            logging.error(f"Error: {e}")

def select_option_from_dropdown(driver, dropdown_filters, filter_name, option_value):

    '''
    Click relevant dropdown and select an option.
    Note: This is only for dropdowns where all options are visible.

    ------------input--------------
    driver: Selenium WebDriver
    dropdown_filters: dictionary containing xpath of dropdown filters
    filter_name: name of the filter (str)
    option_value: value to select (str) 
    '''
    
    # Click dropdown to reveal options
    dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, dropdown_filters[filter_name])))
    dropdown.click()
    time.sleep(1)

    # Select option
    option_xpath = f'//div[@title="{str(option_value)}"]'
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, option_xpath))).click()
    time.sleep(1)

    # Close dropdown
    dropdown.click()

def search_select_from_dropdown(driver, dropdown_filters, filter_name, filter_option):

    '''
    Search and select an option from the dropdown.
    Note: This is only for dropdowns where search bar is available.

    ------------input--------------
    driver: Selenium WebDriver
    dropdown_filters: dictionary containing xpath of dropdown filters
    filter_name: name of the filter (str)
    filter_option: option to select (str) 
    '''
    
    try:
        time.sleep(1)
        dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, dropdown_filters[filter_name])))
        dropdown.click()
        
        time.sleep(1)
        action = ActionChains(driver)
        action.send_keys(filter_option).perform()

        time.sleep(1)
        option_xpath = f'//div[@title="{filter_option}"]'
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, option_xpath))).click()

        time.sleep(1)
        dropdown.click()

    except (ElementClickInterceptedException, NoSuchElementException, TimeoutException) as e:
        logging.error(f"An error occurred while selecting option from dropdown filter: {e}")

######################
### Initialize Filter
######################

states = [
        'ANDAMAN AND NICOBAR ISLANDS',
        'ANDHRA PRADESH', 
        'ARUNACHAL PRADESH', 
        'ASSAM', 
        'BIHAR', 
        'CHANDIGARH', 
        'CHHATTISGARH', 
        'DELHI', 
        'GOA', 
        'GUJARAT', 
        'HARYANA', 
        'HIMACHAL PRADESH', 
        'JAMMU AND KASHMIR', 
        'JHARKHAND', 
        'KARNATAKA', 
        'KERALA', 
        'LADAKH', 
        'LAKSHADWEEP', 
        'MADHYA PRADESH', 
        'MAHARASHTRA', 
        'MANIPUR', 
        'MEGHALAYA', 
        'MIZORAM', 
        'NAGALAND', 
        'ODISHA', 
        'PUDUCHERRY', 
        'PUNJAB', 
        'RAJASTHAN', 
        'SIKKIM', 
        'TAMIL NADU', 
        'TELANGANA', 
        'THE DADRA AND NAGAR HAVELI AND DAMAN AND DIU', 
        'TRIPURA', 
        'UTTAR PRADESH', 
        'UTTARAKHAND', 
        'WEST BENGAL'
        ]

categories = [  
                'GEN', 
                'OBC', 
                'Others', 
                'SC', 
                'ST'    
            ]

years = [2015]

#################
### Scrape Data
#################

def parallel_scrape_over_categories(state, category, year, progress, lock):
    
    '''
    Scrape data for a given state, category and year, in parallel.

    ------------input--------------
    state: state name (str)
    category: category name (str)
    year: year (int)
    progress: dictionary to track progress
    lock: multiprocessing lock 
    '''
    
    chrome_options = Options()                              # Set up Chrome options
    chrome_options.add_argument("--headless")               # Run in headless mode
    chrome_options.add_argument("--disable-extensions")     # Disable extensions
    chrome_options.add_argument("--disable-gpu")            # Disable GPU
    chrome_options.add_argument("--no-sandbox")             # No sandbox
    chrome_options.add_argument("--disable-dev-shm-usage")  # Disable dev-shm usage
    driver = webdriver.Chrome(options = chrome_options)     # Initialize Chrome WebDriver

    try:
        link = "https://app.powerbi.com/view?r=eyJrIjoiZmJhZTY4ZjQtNTk0OS00ZmZlLTg1OTItNTNlMDJmN2I5MjE1IiwidCI6ImJhNTdjY2MxLTEyYzgtNGExOC04NWE3LTMxM2RkNWJmYTZjMSJ9"
        driver.get(link)

        WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'transform.bringToFront')))
        time.sleep(5)

        clear_filters(driver)
        dropdown_filters = initialize_dropdowns(driver)
        logging.info(f'Process {current_process().pid}: Finished setting up web-driver...')
        clear_filters(driver)
        logging.info(f'Process {current_process().pid}: Cleared all filters...')
        time.sleep(1)
        
        # select state
        search_select_from_dropdown(driver, dropdown_filters, 'STATE_NAME', state)
        logging.info(f'Process {current_process().pid}: Selected State: {state}')
        time.sleep(1)
        
        # select year
        select_option_from_dropdown(driver, dropdown_filters, 'ACADEMIC_SESSION', year)
        logging.info(f'Process {current_process().pid}: Selected Year: {year}')
        time.sleep(1)

        # select category
        select_option_from_dropdown(driver, dropdown_filters, 'CATEGORY', category)
        logging.info(f'Process {current_process().pid}: Selected Category: {category}')
        time.sleep(1)

        print('\n')
        logging.info(f'Process {current_process().pid}: Currently at:')
        logging.info(f'State: {state} <-> Year: {year} <-> Category: {category}')

        if len(extract_visible_rows(driver)) == 0:
            logging.warning(f'Process {current_process().pid}: No data available for state: {state}, category: {category}, year: {year}')
            raise Exception('Skipping this state-category-year combination...')

        # start scraping data, and generate additional columns for the dataframe
        df = scrape_data(driver)
        df['State'] = state
        df['Category'] = category
        df['Year'] = year

        # save data to csv
        df.drop_duplicates(inplace = True)
        df.reset_index(drop = True, inplace = True)

        # create folder if it does not exist
        create_folder_if_not_exists(f'{output_folder}')
        create_folder_if_not_exists(f'{output_folder}/{year}')

        # save data to csv
        df.to_csv(f'{output_folder}/{year}/{state}_{year}_{category}.csv', index = False)
        logging.info(f'Length of data: {len(df)}')
        logging.info(f'Process {current_process().pid}: Saved data for state: {state}, category: {category}, year: {year}')

    except Exception as e:
        logging.error(f"Process {current_process().pid}: Error: {e}")

    finally:
        driver.quit()
        logging.info(f'Process {current_process().pid}: Closed driver...')
        with lock:
            progress[(state, category, year)] += 1

if __name__ == '__main__':

    '''
    Main function to scrape data for all states, categories and years. 
    '''

    time.sleep(1)
    start = time.time()
    manager = Manager()
    progress = manager.dict({(state, category, year): 0 for state in states for category in categories for year in years})  # Track progress for each state-category-year combination
    lock = manager.Lock()
    
    # all possible tasks, every state, category and year
    total_tasks = len(states) * len(categories) * len(years)
    
    # Start the progress bar updater process
    progress_bar_process = Process( target = update_progress_bar, 
                                    args = (progress, total_tasks) )
    progress_bar_process.start()
    
    with Pool(5) as p:
        p.starmap(parallel_scrape_over_categories, [(state, category, year, progress, lock) for state in states for category in categories for year in years])
    
    progress_bar_process.join()

    logging.info(f"Time taken: {time.time() - start:.2f} seconds")
