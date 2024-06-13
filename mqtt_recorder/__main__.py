import argparse
import time
from logging import DEBUG

from mqtt_recorder.recorder import MqttRecorder, SslContext, logger

parser = argparse.ArgumentParser(
    prog='mqtt_recorder',
    description="Tool for recording and replaying mqtt messages"
)

parser.add_argument(
    '--host',
    type=str,
    required=True,
    help='MQTT broker address'
)

parser.add_argument(
    '--port',
    type=int,
    default=1883,
    help='MQTT broker port'
)

parser.add_argument(
    '--client_id',
    type=str,
    default=None,
    required=False,
    help='MQTT Client ID'
)


parser.add_argument(
    '--username',
    type=str,
    default=None,
    required=False,
    help='MQTT broker username'
)

parser.add_argument(
    '--password',
    type=str,
    default=None,
    required=False,
    help='MQTT broker password'
)

parser.add_argument(
    '--enable_ssl',
    type=bool,
    default=False,
    required=False,
    help='True to enable MQTTs support, False otherwise'
)

parser.add_argument(
    '--tls_insecure',
    type=bool,
    default=False,
    required=False,
    help='If certs is self-generated, change to True'
)

parser.add_argument(
    '--ca_cert',
    type=str,
    default=None,
    required=False,
    help='path to the Certificate Authority certificate files'
)

parser.add_argument(
    '--certfile',
    type=str,
    default=None,
    required=False,
    help='path to the client certificate'
)

parser.add_argument(
    '--keyfile',
    type=str,
    default=None,
    required=False,
    help='path to the client private key'

)

parser.add_argument(
    '--mode',
    choices=["record", "replay"],
    help='mode: record/replay',
    required=True
)

parser.add_argument(
    '--file',
    type=str,
    help='output/input file',
    required=True
)

parser.add_argument(
    '--loop',
    action='store_true',
    help='looping replay',
)

parser.add_argument(
    '--qos',
    type=int,
    help='Quality of Service that will be used for subscriptions',
    default=0
)

parser.add_argument(
    '--topics',
    type=str,
    help='json file containing selected topics for subscriptions'
)

parser.add_argument(
    '--encode_b64',
    default=False,
    action='store_true',
    help='Store raw data as base64 encoded string in CSV file instead of UTF-8 encoded string. '
         'Should be used to record binary message payloads'
)

parser.add_argument(
    '--debug',
    action='store_true',
    help='debug logging',
)

parser.add_argument(
    '--timestamp_delay',
    type=float,
    help='Update timestamps, starting with current time plus timestamp_delay in s',
    default=0.0
)


def wait_for_keyboard_interrupt():
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        pass


def main():
    args = parser.parse_args()
    ssl_context = SslContext(
        args.enable_ssl,
        args.ca_cert,
        args.certfile,
        args.keyfile,
        args.tls_insecure
    )
    recorder = MqttRecorder(
        args.host,
        args.port,
        args.client_id,
        args.file,
        args.username,
        args.password,
        ssl_context,
        args.encode_b64,
        args.timestamp_delay,
    )
    if args.debug:
        logger.setLevel(DEBUG)
        logger.info('Log level set to DEBUG')
    if args.mode == 'record':
        recorder.start_recording(qos=args.qos, topics_file=args.topics)
        wait_for_keyboard_interrupt()
        recorder.stop_recording()
    elif args.mode == 'replay':
        try:
            recorder.start_replay(args.loop)
        except KeyboardInterrupt:
            pass
    else:
        print('Please select a mode record/replay')


if __name__ == "__main__":
    main()
