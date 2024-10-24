#!/bin/bash

# Update and upgrade packages
sudo apt-get update
sudo apt-get upgrade -y

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install selenium beautifulsoup4 pandas tqdm multiprocess

# Install Google Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install -y ./google-chrome-stable_current_amd64.deb

# Install ChromeDriver
CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+')
wget -N https://chromedriver.storage.googleapis.com/$CHROME_VERSION/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
sudo mv chromedriver /usr/local/bin/

# Cleanup
rm google-chrome-stable_current_amd64.deb
rm chromedriver_linux64.zip