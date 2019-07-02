import io
import json
import kafka
import logging
import os
import requests
import tarfile
import tempfile
import sys


TOPIC=os.environ.get("QUEUE", "platform.upload.leapp-reporting")
VALIDATION_TOPIC=os.environ.get("RESPONSE_QUEUE", "platform.upload.validation")
BOOT_SERVERS=os.environ.get("KAFKAMQ", "platform-mq-ci-kafka-bootstrap.platform-mq-ci.svc:9092").split(',')
DB_API=os.environ.get("DB_API", "http://reporting-db-api:8080")

logging.basicConfig()
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def fetch_report(url):
    res = []
    req = requests.get(url)
    if not req.ok:
        LOG.warning("[%s] Error during GET %s", req.status_code, url)
    else:
        report = io.BytesIO(req.content)
        try:
            tar = tarfile.open(fileobj=report)
            for report in tar:
                f = tar.extractfile(report)
                json_report = json.loads(f.read())
                res.append(json_report)
        except (tarfile.ReadError, json.decoder.JSONDecodeError) as e:
            LOG.warning("Bad payload: %s", str(e))
    return res


def store_in_db(report):
    upload_url = DB_API + "/report"
    res = requests.post(upload_url, json=report)
    return res.json()


def _deserializer(m):
    try:
        return json.loads(m.decode("utf-8"))
    except json.decoder.JSONDecodeError:
        return {}


def main(consumer_topic=TOPIC, producer_topic=VALIDATION_TOPIC, boot_servers=BOOT_SERVERS):
    consumer = kafka.KafkaConsumer(consumer_topic,
                                   bootstrap_servers=boot_servers,
                                   auto_offset_reset="earliest",
                                   value_deserializer=_deserializer)
    producer = kafka.KafkaProducer(bootstrap_servers=boot_servers,
                                   value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    def validate_data(request_id, result="success"):
        msg = {"request_id": request_id, "validation": result}
        producer.send(producer_topic, msg)

    for msg in consumer:
        data = msg.value
        if not data or not isinstance(data, dict):
            # smth is wrong, not processing
            continue
        reports = fetch_report(data["url"])
        if not reports:
            # couldn't fetch report data, discarding message
            validate_data(data.get("request_id"), result="handoff")
        else:
            LOG.info("Fetched and validated tgz from %s", data["url"])
            # data is valid, waving green flag
            validate_data(data.get("request_id"))
            # process data (upload to db in our case)
            for report in reports:
                res = store_in_db(report)
                LOG.info("Uploaded: %s", json.dumps(res).encode('utf-8'))


if __name__ == "__main__":
    main()
