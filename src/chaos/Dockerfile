FROM python:3-alpine
RUN apk add --no-cache docker-cli
COPY chaos .
ENTRYPOINT ["python3", "main.py"]
