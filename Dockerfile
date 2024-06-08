FROM python:3.12-bookworm

RUN mkdir /code
WORKDIR /code
RUN apt-get update && apt-get -y install libpq-dev
RUN pip install --upgrade pip
COPY requirements.txt /code/
COPY docker-entrypoint.sh /code/
RUN chmod a+x /code/docker-entrypoint.sh
COPY changes/ /code/

RUN pip install -r requirements.txt

ENTRYPOINT ["/code/docker-entrypoint.sh"]
