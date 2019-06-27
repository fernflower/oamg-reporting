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
BOOT_SERVERS=os.environ.get("KAFKAMQ", "platform-mq-ci-kafka-bootstrap.platform-mq-ci.svc:9092").split(',')
LOG = logging.getLogger()


def fetch_report(url):
    res = []
    req = requests.get(url)
    if not req.ok:
        LOG.warn("[{}] Error during GET {}".format(req.status_code, url))
    else:
        report = io.BytesIO(req.content)
        tar = tarfile.open(fileobj=report)
        for report in tar:
            f = tar.extractfile(report)
            json_report = json.loads(f.read())
            res.append(json_report)
    return res


def _deserializer(m):
    try:
        return json.loads(m.decode("utf-8"))
    except json.decoder.JSONDecodeError:
        return {}


def main():
    consumer = kafka.KafkaConsumer(TOPIC,
                                   bootstrap_servers=BOOT_SERVERS,
                                   auto_offset_reset="earliest",
                                   value_deserializer=_deserializer)
    for msg in consumer:
        data = msg.value
        if not data or not isinstance(data, dict):
            continue
        reports = fetch_report(data["url"])
        # XXX FIXME POC
        sys.stdout.write("====================\n")
        for report in reports:
            sys.stdout.write("Received: %s\n" % report)
        sys.stdout.write("====================\n")


if __name__ == "__main__":
    main()
