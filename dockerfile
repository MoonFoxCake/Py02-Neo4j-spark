FROM python:3.12

WORKDIR /app

RUN pip install poetry
COPY . .
RUN poetry install

CMD poetry run python index.py
