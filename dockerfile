FROM python:3.10.7-slim-buster

WORKDIR /usr/src/app

COPY data /usr/src/data/
COPY app /usr/src/app/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt



CMD ["python", "app.py"]

