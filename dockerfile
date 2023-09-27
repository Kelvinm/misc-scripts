# Use the Python 3.11 base image
FROM python:3.11

# Install Selenium, Pandas, Jupyterlab
RUN pip install selenium pandas jupyterlab

# Install Chrome and matching Chromedriver version
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update -qqy \
    && apt-get -qqy install google-chrome-stable

RUN wget https://chromedriver.storage.googleapis.com/117.0.5304.62/chromedriver_linux64.zip \
    && unzip chromedriver_linux64.zip \
    && rm chromedriver_linux64.zip \
    && mv chromedriver /usr/bin/chromedriver \
    && chmod +x /usr/bin/chromedriver

# Set working directory
WORKDIR /app

# Expose ports for Jupyterlab
EXPOSE 8888

# Run Jupyterlab when container starts  
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser"]
