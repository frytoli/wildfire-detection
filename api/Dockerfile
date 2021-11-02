FROM python:3.9-slim

# Set bash to default shell
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

# Do not prompt apt for user input when installing packages
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt update && \
		apt install -y \
			build-essential \
      supervisor \
      software-properties-common && \
		apt dist-upgrade -y

# Create virtual environment and add to path
ENV VIRTUAL_ENV=/venv
RUN python3.9 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Upgrade pip
RUN python -m pip install --upgrade pip

# Install python packages
RUN pip install --no-dependencies \
  anyio==3.3.4 \
  asgiref==3.4.1 \
  click==8.0.3 \
  fastapi==0.70.0 \
  h11==0.12.0 \
  idna==3.3 \
  pydantic==1.8.2 \
  redis==3.5.3 \
  sniffio==1.2.0 \
  starlette==0.16.0 \
  typing-extensions==3.10.0.2 \
  uvicorn==0.15.0

# Add supervisord app config files
ADD conf/supervise-* /etc/supervisor/conf.d/

# Add app files
COPY app.py /home/app.py

# Set working dir
WORKDIR /home

# Let 'er rip
CMD /usr/bin/supervisord -n
