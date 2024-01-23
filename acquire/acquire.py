#!/usr/bin/env python3
from datetime import datetime
from logging.handlers import RotatingFileHandler
import traceback
import serial
import hbcapture as hb
import configparser
import signal
import numpy as np
import uuid
import time
import matplotlib.pyplot as plt
import sys
import os
import logging
import urllib3
import threading
import argparse
import sdnotify
from google.cloud import storage
from colorama import Fore, Back, Style
from minio import Minio
from minio.commonconfig import Tags
from minio.error import S3Error

class ConsoleFormatter(logging.Formatter):

    format = "%(asctime)s - [%(name)s] - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: Fore.MAGENTA + format + Style.RESET_ALL,
        logging.INFO: Fore.BLUE + format + Style.RESET_ALL,
        logging.WARNING: Fore.YELLOW + format + Style.RESET_ALL,
        logging.ERROR: Fore.RED + format + Style.RESET_ALL,
        logging.CRITICAL: Back.RED + Fore.BLACK + format + Style.RESET_ALL
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
    
class FileFormatter(logging.Formatter):

    fmt = "%(asctime)s - [%(name)s] - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    def format(self, record):
        formatter = logging.Formatter(self.fmt)
        return formatter.format(record)

class HeartbeatStorage:
    def __init__(self, url: str, access_key: str, secret_key: str, bucket: str):
        logging.getLogger("acq.storage").info(f"Connecting to {url} as {access_key}")
        self.client = Minio(url,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
            http_client=urllib3.PoolManager(
                timeout=urllib3.Timeout(connect=5.0, read=10.0),
                retries=urllib3.Retry(
                    total=1,  # Total number of retries
                    backoff_factor=0.2,  # Backoff factor for retries
                    status_forcelist=[500, 502, 503, 504],  # HTTP status codes to retry
                )
            )
        )

        # self.client = storage.Client.from_service_account_json('key.json')
        self.bucket = bucket
        self.upload_queue = []
        pass

    def init(self):
        logger = logging.getLogger("acq.storage")

        # try:
        #     if not self.client.lookup_bucket(self.bucket) is None:
        #         logger.info("Storage OK.")
        #     else:
        #         logger.error("Storage not found")

        #     self.gbucket = self.client.get_bucket(self.bucket)
        # except Exception as e:
        #     logger.error(e)
        #     logger.error("Unable to connect to storage")

        self.upload_thread = threading.Thread(target=self.loop, daemon=True)
        self.upload_thread.start()
        
        pass

    def loop(self):
        logger = logging.getLogger("acq.storage.upload_thread")
        while True:
            time.sleep(5)

            if len(self.upload_queue) == 0:
                logger.debug("No upload queue, skipping")
                continue

            logger.info("Attemping to upload %d files" % len(self.upload_queue))

            while len(self.upload_queue) > 0:
                filename, path = self.upload_queue[0]
                try:
                    self.client.fput_object(self.bucket, os.path.join(acq.node_id, filename), path)
                    tags = Tags.new_object_tags()
                    tags["capture_id"] = acq.capture_id.hex
                    tags["node_id"] = acq.node_id
                    self.client.set_object_tags(self.bucket, os.path.join(acq.node_id, filename), tags)
                    # blob = self.gbucket.blob(filename)
                    # metadata = { "capture_id": acq.capture_id.hex, "node_id": acq.node_id, "sample_rate": acq.sample_rate }
                    # blob.metadata = metadata
                    # blob.upload_from_filename(path)
                    logger.info(f"Uploaded {filename} to {self.bucket}")
                    self.upload_queue.pop(0)
                except urllib3.exceptions.MaxRetryError as e:
                    logger.error(e)
                    logger.error("Error uploading file, will retry later")
                    break

            

    def upload(self, filename: str, path: str): 
        logger = logging.getLogger("acq.storage")

        if not self.upload_thread.is_alive:
            logger.error("Upload thread died")
        logger.info(f"Queuing upload of {filename} to {self.bucket}")
        self.upload_queue.append((filename, path))


class HeartbeatAcquisition:

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.has_gps_fix = False
        self.is_clipping = False
        self.is_ready = False
        self.lines_written = 0

        pass

    def init(self):
        logger = logging.getLogger("acq")

        # make sure our config file exists
        if (not os.path.isfile(os.path.join(os.getcwd(), 'config.ini'))):
            logger.critical("config.ini not found")
            sys.exit(1)


        self.config.read('config.ini')

        self.root_dir = self.config["acquire"].get("root_dir", "./hb")
        logger.info(f"Using root directory {self.root_dir}")
        if not os.path.isdir(self.root_dir):
            logger.info(f"Creating root directory {self.root_dir}")
            os.mkdir(self.root_dir)

        # Generate random capture id
        self.capture_id = uuid.uuid4()
        logger.info(f"Capture ID: {self.capture_id}")

        # log to file
        fh = logging.FileHandler(os.path.join(self.root_dir, f'{self.capture_id}_aquisition.log'), encoding='utf-8', mode="w")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(FileFormatter())
        logger.addHandler(fh)


        # Open serial port
        logger.info(f"Opening serial port {self.config['teensy'].get('port')} at {self.config['teensy'].get('baudrate')} baud")
        try: 
            self.ser = serial.Serial(self.config["teensy"].get("port"), self.config['teensy'].get('baudrate'));
        except serial.SerialException:
            logger.critical("Could not open serial port")
            sys.exit(1)


        # Expected sample rate
        self.sample_rate = -1

        self.node_id = self.config["acquire"].get("node_id")
        if self.node_id is None:
            logger.error("Missing node_id in config")
            self.node_id = "UNKNOWN"

        logger.info(f"Node ID: {self.node_id}")

        # TODO finish writer stuff
        self.writer = hb.writer(root_dir=self.root_dir, capture_id=self.capture_id, node_id=self.node_id, sample_rate=self.sample_rate)
        self.writer.init()

        # Load storage
        logger.info("Loading cloud storage...")
        self.storage = HeartbeatStorage(url=self.config["minio"].get("host"), 
                                                   access_key=self.config["minio"].get("access_key"), 
                                                   secret_key=self.config["minio"].get("secret_key"),
                                                   bucket=self.config["minio"].get("bucket"))
        self.storage.init()

        self.is_ready = True
        logger.info("Ready for data acquisition")
        

    def tick(self):
        logger = logging.getLogger("acq.tick")
        logger.debug("Reading line from serial port")

        serial_line = self.ser.readline()
        try: 
            serial_line = serial_line.decode("utf-8").strip()
        except UnicodeDecodeError:
            # TODO still keep "bad" data, clean up later?
            logger.error("Could not decode serial line")
            return

        
        if serial_line.startswith("#"):
            logger.getLogger("acq.serial").info(f"SERIAL: {serial_line}")
            self.writer.write_line(serial_line)
        elif not serial_line.startswith("$"):
            return

        try:
            line: hb.HeartbeatCaptureLine = hb.parse_line(serial_line[1:])
        except ValueError as e:
            logger.error(e)
            logger.error("Could not parse line")
            return
        logger.info(f"Got data for {line.time}")

        # Write the line
        self.writer.write_line(line)
        self.lines_written += 1

        # Update status
        self.is_clipping = line.is_clipping()
        self.has_gps_fix = line.has_gps_fix()

        # Check on sample rate
        if self.sample_rate == -1:
            self.sample_rate = line.sample_rate
            logger.info(f"Using sample rate: {self.sample_rate} Hz")

        if self.sample_rate != line.sample_rate:
            logger.error(f"Sample rate changed from {self.sample_rate} Hz to {line.sample_rate} Hz")
            self.sample_rate = line.sample_rate

        if not line.has_gps_fix():
            logger.warning("No GPS fix (data may be misaligned for this second)")

        # rotate files as desired
        if self.lines_written % 5 == 0:
            logger.info(f"Moving to new file, {self.lines_written} lines written")
            header_file = self.writer.files[-1].get_header_filename()
            data_file = self.writer.files[-1].get_data_filename()
           
            self.writer.next_file()

            self.storage.upload(header_file, os.path.join(self.root_dir, header_file))
            self.storage.upload(data_file, os.path.join(self.root_dir, data_file))

    def shutdown(self):
        if not self.is_ready:
            logging.info("Nothing to shutdown")
            return
        self.ser.close()
        self.writer.done()

notifier = sdnotify.SystemdNotifier()

def main():
    parser = argparse.ArgumentParser(prog="acquire", 
                                     description="Eclipse data acquisition daemon",
                                     epilog="Thanks for using Heartbeat!")
    parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")
    
    args = parser.parse_args()

    # configure logger
    logger = logging.getLogger("acq")

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setFormatter(ConsoleFormatter())
    logger.addHandler(ch)

    logger.info("Welcome to heartbeat-acquisition, a data acquisition program!")

    acq = HeartbeatAcquisition()

    def signal_handler(sig, frame):
        if sig == signal.SIGINT or sig == signal.SIGTERM:
            if (acq.is_ready):
                logging.getLogger("acq").critical("Received SIGINT, shutting down...")
                acq.shutdown()
                logging.getLogger("acq").info("Goodbye.")
        else:
            logging.getLogger("acq").critical("Shutting down...")

        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        acq.init()
    except Exception as e:
        logging.getLogger("acq.init").error(e)
        logging.getLogger("acq.init").error(traceback.format_exc())
        sys.exit(1)

    # We're ready, signal
    notifier.notify("READY=1")

    while True:
        try:
            acq.tick()
        except FileNotFoundError as e:
            logging.getLogger("acq.tick").error(e)
            logging.getLogger("acq.tick").error(traceback.format_exc())
            # TODO attempt to upload existing files to server
            # logging.getLogger("acq.tick").info("Will attempt to upload existing data to server...")

            logging.getLogger("acq.tick").critical("Error in data acquisition, shutting down...")
            sys.exit(1)


if __name__ == "__main__":
    main()