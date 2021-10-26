FROM apache/airflow:2.2.1rc1-python3.9

# Install additional dependencies
USER root
RUN apt-get update && \
	apt-get install -y --no-install-recommends \
		gconf-service libasound2 libatk1.0-0 libc6 libcairo2 \
		libcups2 libdbus-1-3 libexpat1 libfontconfig1 libgcc1 \
		libgconf-2-4 libgdk-pixbuf2.0-0 libglib2.0-0 libgtk-3-0 \
		libnspr4 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 \
		libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 \
		libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 \
		libxrender1 libxss1 libxtst6 ca-certificates \
		fonts-liberation libnss3 lsb-release xdg-utils wget && \
	apt dist-upgrade -y && \
	apt-get autoremove -yqq --purge && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Install additional python packages
USER 50000
COPY requirements.txt /docker-context-files/requirements.txt
RUN pip install --no-cache-dir --user --no-dependencies -r /docker-context-files/requirements.txt

# Run pyppeteer download/install chromium script
RUN pyppeteer-install
