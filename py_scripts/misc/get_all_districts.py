
#####################
### Required Modules
#####################

import time
import pickle
import os
import sys
import logging
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
        'STATE_NAME': get_xpath(dropdown_menus[0]),         # Dropdown to select state
        'ACADEMIC_SESSION': get_xpath(dropdown_menus[1]),   # Dropdown to select academic session
        'DISTRICT_NAME': get_xpath(dropdown_menus[2]),      # Dropdown to select district
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

def get_scroll_region(driver, dropdown_filters):

    '''
    Get the scroll regions for all dropdown filters.

    ------------input--------------
    driver: Selenium WebDriver object
    dropdown_filters: dictionary containing the XPath of dropdown filters
    '''
    
    scroll_regions = {}
    for xpath in dropdown_filters.values():
        current_dropdown = driver.find_element(By.XPATH, xpath)
        current_dropdown.click()
        time.sleep(1)
        current_dropdown.click()
        time.sleep(1)
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    dropdown_scroll_region = soup.find_all('div', class_='scrollRegion')
    
    for filter_name, scroll_region in zip(dropdown_filters.keys(), dropdown_scroll_region):
        scroll_regions[filter_name] = get_xpath(scroll_region)
    
    return scroll_regions

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

def get_all_dropdown_options(driver, all_dropdown_xpaths, dropdown_filters, filter_name):

    def flatten(xss):
        return [x for xs in xss for x in xs]

    all_options = []
    dropdown = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, dropdown_filters[filter_name])))
    dropdown.click()
    time.sleep(1)

    retries = 5
    previous_length = 0
    while retries > 0:

        options = driver.find_elements(By.XPATH, all_dropdown_xpaths[filter_name])
        for option in options:
            new_options = option.text.split('\n')
            all_options.append([option for option in new_options if option != 'Select all'])

        current_options = sorted(list(set(flatten(all_options))))
        current_length = len(current_options)
        if current_length == previous_length:
            retries -= 1

        last_element = driver.find_element(By.XPATH, f'//div[@title="{current_options[-3]}"]')
        last_element.location_once_scrolled_into_view
        previous_length = current_length

    dropdown.click()
    return current_options

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

year = 2022

######################
### Initialize Filter
######################

def parallel_scrape_over_categories(state, progress, lock):
    '''
    Scrape data for a given state, category and year, in parallel.

    ------------input--------------
    state: state name (str)
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

        # get all dropdown scroll regions
        all_dropdown_xpaths = get_scroll_region(driver, dropdown_filters)
        logging.info(f'Process {current_process().pid}: Finished getting all scroll regions...')
        time.sleep(1)

        # select year
        select_option_from_dropdown(driver, dropdown_filters, 'ACADEMIC_SESSION', year)
        logging.info(f'Process {current_process().pid}: Selected Year: {year}')
        time.sleep(1)
        
        # select state
        search_select_from_dropdown(driver, dropdown_filters, 'STATE_NAME', state)
        logging.info(f'Process {current_process().pid}: Selected State: {state}')
        time.sleep(1)
        
        # get all district options
        districts = get_all_dropdown_options(driver, all_dropdown_xpaths, dropdown_filters, 'DISTRICT_NAME')
        logging.info(f'Process {current_process().pid}: Finished getting all district options...')
        time.sleep(1)

        logging.info(f'Process {current_process().pid}: Finished scraping districts for {state}...')
        logging.info(f'Process {current_process().pid}: Districts: \n{districts}')
        logging.info(f'Process {current_process().pid}: Length of districts: {len(districts)}')

        return {state: districts}

    except Exception as e:
        logging.error(f"Process {current_process().pid}: Error: {e}")
        return {state: []}

    finally:
        driver.quit()
        logging.info(f'Process {current_process().pid}: Closed driver...')
        with lock:
            progress[(state)] += 1

if __name__ == '__main__':
    '''
    Main function to scrape districts for all states. 
    '''

    time.sleep(1)
    start = time.time()
    manager = Manager()
    progress = manager.dict({(state): 0 for state in states})  # Track progress for each state-category-year combination
    lock = manager.Lock()
    
    # all possible tasks, every state, category and year
    total_tasks = len(states)
    
    # Start the progress bar updater process
    progress_bar_process = Process( target=update_progress_bar, 
                                    args=(progress, total_tasks) )
    progress_bar_process.start()
    
    merged_data = {}
    with Pool(5) as p:
        results = p.starmap(parallel_scrape_over_categories, [(state, progress, lock) for state in states])
    
    for result in results:
        merged_data.update(result)
    
    progress_bar_process.join()

    # Save the merged data as a pickle file
    with open(f'all_districts.pkl', 'wb') as file:
        pickle.dump(merged_data, file)

    logging.info(f"Time taken: {time.time() - start:.2f} seconds")
