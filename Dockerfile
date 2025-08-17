FROM python:3.12-bookworm

# Install system dependencies
RUN apt-get update && apt-get -y install libpq-dev
RUN pip install --upgrade pip

# Install Python dependencies (this layer will be cached)
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

# Set up application directory
RUN mkdir /code
WORKDIR /code

# Copy application files (these change frequently)
COPY docker-entrypoint.sh /code/
RUN chmod a+x /code/docker-entrypoint.sh
COPY changes/ /code/
COPY tests/ /app/tests/
COPY pytest.ini /app/

ENTRYPOINT ["/code/docker-entrypoint.sh"]
