import json
import kafka
import os
import tarfile
import sys


TOPIC=os.environ.get("QUEUE", "platform.upload.leapp-reporting")
BOOT_SERVERS=os.environ.get("KAFKAMQ", "kafka:29092").split(',')


def unpack(report_tgz):
    res = []
    tar = tarfile.open(report_tgz)
    for report in tar:
        f = tar.extractfile(report)
        json_report = json.loads(f.read())
        res.append(json_report)
    return res


def main():
    consumer = kafka.KafkaConsumer(TOPIC,
                                   bootstrap_servers=BOOT_SERVERS,
                                   auto_offset_reset="earliest")
    for msg in consumer:
        data = msg.value
        # XXX FIXME POC
        sys.stdout.write("====================\n")
        sys.stdout.write("Received: %s" % data)
        sys.stdout.write("====================\n")


if __name__ == "__main__":
    main()
