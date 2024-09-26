import paho.mqtt.client as mqtt
import logging
import time
import base64
import csv
import json
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('MQTTRecorder')

date_formats_in = [
    '%Y-%m-%dT%H:%M:%S.%f%Z',
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%dT%H:%M:%S.%f',
]


def strdateout(dt: datetime) -> str:
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


class SslContext:

    def __init__(self, enable, ca_cert, certfile, keyfile, tls_insecure):
        self.enable = enable
        self.ca_cert = ca_cert
        self.certfile = certfile
        self.keyfile = keyfile
        self.tls_insecure = tls_insecure


class MqttRecorder:

    def __init__(self, host: str, port: int, client_id: str, file_name: str, username: str,
                 password: str, ssl_context: SslContext, encode_b64: bool, timestamp_delay: float):
        self.__recording = False
        self.__messages = list()
        self.__file_name = file_name
        self.__last_message_time = None
        self.__encode_b64 = encode_b64
        self.__timestamp_delay = timestamp_delay
        self.__client = mqtt.Client(client_id=client_id)
        self.__client.on_connect = self.__on_connect
        self.__client.on_message = self.__on_message
        if username is not None:
            self.__client.username_pw_set(username, password)
        if ssl_context.enable:
            self.__client.tls_set(ssl_context.ca_cert, ssl_context.certfile, ssl_context.keyfile)
            if ssl_context.tls_insecure is True:
                self.__client.tls_insecure_set(True)
        self.__client.connect(host=host, port=port)
        self.__client.loop_start()

    def start_recording(self, topics_file: str, qos: int = 0):
        self.__last_message_time = time.time()
        if topics_file:
            with open(topics_file) as json_file:
                data = json.load(json_file)
                for topic in data['topics']:
                    self.__client.subscribe(topic, qos=qos)
        else:
            self.__client.subscribe('#', qos=qos)
        self.__recording = True

    def start_replay(self, loop: bool):
        def decode_payload(payload, encode_b64):
            return base64.b64decode(payload) if encode_b64 else payload

        with open(self.__file_name, newline='') as csvfile:
            logger.info('Starting replay')
            reader = csv.reader(csvfile)
            messages = list(reader)

            while True:
                logger.info(f"Starting timestamp update, delay {self.__timestamp_delay} s")
                first_timestamp = True
                dt_delay = timedelta(seconds=self.__timestamp_delay)
                for row in messages:
                    pos = 0
                    while True:
                        pos = row[1].find('timestamp', pos)
                        if pos == -1:
                            break
                        pos = pos + 10
                        pos_start = row[1].find('"', pos) + 1  # start position of time string
                        pos_end = row[1].find('"', pos_start)
                        time_str = row[1][pos_start:pos_end]

                        for date_format in date_formats_in:
                            try:
                                dt = datetime.strptime(time_str, date_format)
                            except ValueError:
                                pass
                            else:
                                break
                        if not dt:
                            logger.error('Unknown time stamp format')
                            exit(1)
                        if first_timestamp:
                            dt_offset = datetime.utcnow() + dt_delay - dt

                            logger.debug(f'dt = {dt}')
                            logger.debug(f'dt_offset = {dt_offset}')
                            first_timestamp = False
                        new_time_str = strdateout(dt + dt_offset)
                        row[1] = row[1].replace(time_str, new_time_str, 1)

                    row[4] = str(
                        datetime.timestamp(datetime.fromtimestamp(float(row[4])) + dt_offset)
                    )
                logger.info("... done")

                first_message = True
                rec_start_time = float(messages[0][4])
                logger.debug(f'rec_start_time = {rec_start_time}')
                logger.debug(f'time = {time.time()}')
                logger.info(f'Start of replay')
                for row in messages:
                    mqtt_payload = decode_payload(row[1], self.__encode_b64)
                    retain = False if row[3] == '0' else True
                    if first_message:
                        logger.debug(
                            f'First message, sleep time {max(rec_start_time - time.time(), 0.0)}')
                        if self.__timestamp_delay:
                            time.sleep(max(rec_start_time - time.time(), 0.0))
                        start_time = time.time()
                        delta_t = start_time - rec_start_time
                        first_message = False
                    else:
                        time_target = float(row[4]) + delta_t
                        time.sleep(max(time_target - time.time(), 0.0))
                    self.__client.publish(
                        topic=row[0],
                        payload=mqtt_payload,
                        qos=int(row[2]),
                        retain=retain,
                    )
                    message_time = time.time()
                logger.info(f'End of replay')
                logger.info(f'....record time {float(messages[-1][4]) - rec_start_time} s')
                logger.info(f'...elapsed time {message_time - start_time} s')
                if loop:
                    logger.info('Restarting replay')
                    time.sleep(1)
                else:
                    break

    def stop_recording(self):
        self.__client.loop_stop()
        logger.info('Recording stopped')
        self.__recording = False
        logger.info('Saving messages to output file')
        with open(self.__file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for message in self.__messages:
                writer.writerow(message)

    def __on_connect(self, client, userdata, flags, rc):
        logger.info("Connected to broker!")

    def __on_message(self, client, userdata, msg):
        def encode_payload(payload, encode_b64):
            return base64.b64encode(msg.payload).decode() if encode_b64 else payload.decode()

        if self.__recording:
            logger.info("[MQTT Message received] Topic: %s QoS: %s Retain: %s",
                        msg.topic, msg.qos, msg.retain)
            time_now = time.time()
            time_delta = time_now - self.__last_message_time
            payload = encode_payload(msg.payload, self.__encode_b64)
            row = [msg.topic, payload, msg.qos, msg.retain, time_now, time_delta]
            self.__messages.append(row)
            self.__last_message_time = time_now
