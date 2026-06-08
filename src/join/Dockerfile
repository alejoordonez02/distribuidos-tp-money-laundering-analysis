FROM python:3-alpine

RUN pip3 install pika msgpack

COPY common ./common
COPY join .

ENTRYPOINT ["python3", "main.py"]
