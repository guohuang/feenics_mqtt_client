import logging
import sys

import yaml

from feenics_client import FeenicsClient


def main():
    if len(sys.argv) == 1:
        sys.exit('Script config path argument is required.')
    # config path passed as first parameter
    config_path = sys.argv[1]

    config = []
    with open(config_path, 'r') as file:
        config = yaml.load(file, Loader=yaml.SafeLoader)

    script_config = config['script_config']
    username = script_config['username']
    password = script_config['password']
    instance_name = script_config['instance_name']

    broker_host = script_config['broker_host']
    broker_port = int(script_config['broker_port'])
    broker_path = script_config['broker_path']
    auth_url = script_config['auth_url']

    log_level = logging.getLevelName(script_config['log_level'])
    logger_name = script_config['logger_name']

    logger = logging.getLogger(logger_name)

    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
    sh.setFormatter(formatter)

    logger.addHandler(sh)

    logger.info("Start processing....")

    auth = {"auth_url": auth_url, "username": username, "password": password, "instance_name": instance_name}
    kwargs = {"auth": auth}

    def save_event(event):
        # add your own logic here to save the event
        logger.debug(event)

    feenices_client = FeenicsClient(logger, **kwargs)
    feenices_client.on_save_event = save_event

    client = feenices_client.connect_mqtt(broker_host, broker_port, broker_path)
    client.loop_forever()


if __name__ == "__main__":
    main()
