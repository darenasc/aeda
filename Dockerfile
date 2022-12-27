FROM python:3.10-slim-buster

# Install dependencies

RUN apt update \
    && apt install git vim tree gcc unixodbc-dev python3-dev wget libmariadb3 libmariadb-dev -y --no-install-recommends \
    && pip install pipenv \
    && git clone https://github.com/darenasc/aeda.git \
    && cd aeda \
    && pipenv install

RUN cd src/aeda/streamlit \
    && streamlit run aeda_app.py &