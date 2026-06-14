FROM python:3-alpine
# docker-cli: detection is heartbeat-based, but reviving dead nodes uses Docker
RUN pip3 install pika msgpack && apk add --no-cache docker-cli
COPY common ./common
COPY supervisor ./supervisor
ENTRYPOINT ["python3", "-m", "supervisor.main"]
