#!/usr/bin/env python3
from datetime import datetime
from logging.handlers import RotatingFileHandler
import traceback
import serial
import hbcapture as hb
from hbcapture.capture import CaptureFileMetadata, CaptureFileWriter
from hbcapture.data import DataPoint
import configparser
import signal
import numpy as np
import uuid
import matplotlib.pyplot as plt
import sys
import os
import logging
import subprocess
import urllib3
import threading
import argparse
import sdnotify
import uuid
from time import sleep
from google.cloud import storage
from colorama import Fore, Back, Style
from minio import Minio
from minio.commonconfig import Tags
from minio.error import S3Error
from serial.serialutil import SerialException

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

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
        logging.getLogger("hb.acq.storage").info(f"Connecting to {url} as {access_key}")
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
        logger = logging.getLogger("hb.acq.storage")

        try:
            if self.client.bucket_exists(self.bucket):
                logger.debug(f"Bucket {self.bucket} found")
            else:
                logger.error(f"Bucket {self.bucket} not found")

        except Exception as e:
            logger.error(e)
            logger.error("Unable to connect to storage")

        self.upload_thread = threading.Thread(target=self.upload_thread, daemon=True)
        self.upload_thread.start()

    def upload_thread(self):
        logger = logging.getLogger("hb.acq.storage.upload_thread")
        while True:
            logger.debug("Sleeping for 5 seconds")
            sleep(5)

            if len(self.upload_queue) == 0:
                logger.debug("No upload queue, skipping")
                continue

            logger.info("Attemping to upload %d files" % len(self.upload_queue))

            while len(self.upload_queue) > 0:
                source_path, target_path, callback = self.upload_queue[0]
                try:
                    # self.client.fput_object(self.bucket, target_path, source_path)
                    logger.info(f"Uploaded {source_path} to {self.bucket}")
                    self.upload_queue.pop(0)
                    if callback:
                        thread = threading.Thread(target=callback, daemon=True, args=[source_path])
                        thread.start()
                except (S3Error, urllib3.exceptions.MaxRetryError) as e:
                    logger.error(e)
                    logger.error(f"Error uploading file {source_path}, will retry later")
                    break

                try:
                    tags = Tags.new_object_tags()
                    self.client.set_object_tags(self.bucket, target_path, tags)
                except (S3Error, urllib3.exceptions.MaxRetryError) as e:
                    logger.error(e)
                    logger.error(f"Error updating tags for file {target_path}, will retry later")
                    break
            

    def upload(self, source_path: str, target_path: str, callback=None): 
        logger = logging.getLogger("acq.storage")

        if not self.upload_thread.is_alive:
            logger.error("Upload thread died")
        logger.info(f"Queuing upload of {source_path} to {self.bucket}")
        self.upload_queue.append((source_path, target_path, callback))




class StatusManager:

    def __init__(self):
        self.logger = logging.getLogger("hb.acq.status")

    def start(self):
        self.logger.info("Starting status manager")
        self.status_thread = threading.Thread(target=self.reporter_thread, daemon=True)
        self.status_thread.start()

    def reporter_thread(self): 
        while True:
            self.logger.info("Sleeping for 5 seconds")
            time.sleep(5)


class HeartbeatApp(metaclass=Singleton):

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.has_gps_fix = False
        self.is_clipping = False
        self.is_ready = False
        self.lines_written = 0

    def init(self):
        logger = logging.getLogger("hb.acq")

        # make sure our config file exists
        if (not os.path.isfile(os.path.join(os.getcwd(), 'config.ini'))):
            logger.critical("config.ini not found")
            sys.exit(1)


        self.config.read('config.ini')

        if hasattr(os, "sched_setaffinity"):
            affinity = self.config["cpu"].getint("affinity") 
            if affinity is not None and affinity != -1:
                logger.info(f"Setting affinity to {self.config['cpu'].getint('affinity')}")
                os.sched_setaffinity(0, [self.config["cpu"].getint("affinity")])

        self.data_dir = self.config["acquire"].get("root_dir")
        if self.data_dir is None:
            logger.critical("Missing root_dir in config")
            sys.exit(1)

        logger.info(f"Using root directory {self.data_dir}")
        if not os.path.isdir(self.data_dir):
            logger.info(f"Creating root directory {self.data_dir}")
            os.mkdir(self.data_dir)

        # Generate random capture id
        self.capture_id = uuid.uuid4()
        logger.info(f"Capture ID: {self.capture_id}")

        # log to file
        fh = logging.FileHandler(os.path.join(self.data_dir, f'{self.capture_id}.log'), encoding='utf-8', mode="w")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(FileFormatter())
        logger.addHandler(fh)


        # Open serial port
        logger.info(f"Opening serial port {self.config['teensy'].get('port')} at {self.config['teensy'].get('baudrate')} baud")
        try: 
            self.connect_serial()
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

        # Load storage
        logger.info("Loading cloud storage...")
        self.storage = HeartbeatStorage(url=self.config["minio"].get("host"), 
                                                   access_key=self.config["minio"].get("access_key"), 
                                                   secret_key=self.config["minio"].get("secret_key"),
                                                   bucket=self.config["minio"].get("bucket"))
        self.storage.init()

        metadata = CaptureFileMetadata(self.capture_id, self.sample_rate)
        metadata.set_metadata("NODE_ID", self.node_id)

        if self.config["acquire"].get("location") is not None:
            metadata.set_metadata("LOCATION", self.config["acquire"].get("location"))

        if self.config["acquire"].get("operator") is not None:
            metadata.set_metadata("OPERATOR", self.config["acquire"].get("operator"))

        self.metadata = metadata

        self.file_count = 0
        time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.writer = CaptureFileWriter(path=os.path.join(self.data_dir, f'{self.node_id}_{time}_{self.capture_id.hex[:8]}_csv'), metadata=self.metadata)
        self.writer.open()

        self.is_ready = True
        logger.info("Ready for data acquisition")
        

    def connect_serial(self):
        self.ser = serial.Serial(self.config["teensy"].get("port"), baudrate=self.config['teensy'].get('baudrate'))

    def tick(self):
        logger = logging.getLogger("hb.acq.tick")
        logger.debug("Reading line from serial port")

        try:
            serial_line = self.ser.readline()
        except (serial.SerialException) as e:
            logger.critical("Could not read from serial port")
            logger.critical("Sleeping for 3 seconds before reconnect")
            sleep(3)
            logger.critical("Reconnecting...")
            try:
                self.connect_serial()
            except serial.SerialException:
                logger.critical("Could not open serial port")
            return
            # sys.exit(1)


        try: 
            serial_line = serial_line.decode("utf-8").strip()
        except UnicodeDecodeError:
            # TODO still keep "bad" data, clean up later?
            logger.error("Could not decode serial line")
            return

        
        if serial_line.startswith("#"):
            logger.getLogger("hb.acq.serial").info(f"SERIAL: {serial_line}")
        elif not serial_line.startswith("$"):
            return

        try:
            line = hb.data.parse(serial_line[1:])
        except ValueError as e:
            logger.error(e)
            logger.error("Could not parse line")
            return
        logger.info(f"Got data for {line.time}")

        # Update status
        self.is_clipping = line.is_clipping()
        self.has_gps_fix = line.has_gps_fix()

        # Check on sample rate
        if self.sample_rate == -1:
            self.sample_rate = line.sample_rate
            self.metadata.sample_rate = line.sample_rate
            self.writer.reset_file()
            logger.info(f"Using sample rate: {self.sample_rate} Hz")

        if self.sample_rate != line.sample_rate:
            logger.error(f"Sample rate changed from {self.sample_rate} Hz to {line.sample_rate} Hz")
            self.sample_rate = line.sample_rate

        if not line.has_gps_fix():
            logger.warning("No GPS fix (data may be misaligned for this second)")

        # Write the line
        self.writer.write_data(line)
        self.lines_written += 1

        # rotate files as desired
        if self.lines_written % 5 == 0:
            logger.info(f"Moving to new file, {self.lines_written} lines written")
            self.writer.close()
            filename = os.path.basename(self.writer.path)
            self.storage.upload(self.writer.path, os.path.join(self.node_id, filename), gzip_this)
            time = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.writer = CaptureFileWriter(path=os.path.join(self.data_dir, f'{self.node_id}_{time}_{self.capture_id.hex[:8]}.csv'), metadata=self.metadata)
            self.writer.open()

    def shutdown(self):
        if not self.is_ready:
            logging.info("Nothing to shutdown")
            return
        self.ser.close()
        logging.getLogger("hb.acq.serial").critical("CLOSING NOT IMPLEMENTED")

def gzip_this(path: str):
    logger = logging.getLogger("hb.acq.gzip")
    logger.debug(f"Compressing {path}")

    # Use the gzip command to compress the file
    try:
        subprocess.run(['gzip', path])
        logger.debug(f'{path} has been gzipped successfully.')
    except subprocess.CalledProcessError as e:
        logger.error(e)
        logger.error(f'Failed to gzip {path}.')

notifier = sdnotify.SystemdNotifier()

def main():
    parser = argparse.ArgumentParser(prog="acquire", 
                                     description="Eclipse data acquisition daemon",
                                     epilog="Thanks for using Heartbeat!")
    parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")
    
    args = parser.parse_args()

    # configure logger
    logger = logging.getLogger("hb")

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setFormatter(ConsoleFormatter())
    logger.addHandler(ch)

    logger = logging.getLogger("hb.acq")

    logger.info("Welcome to heartbeat-acquisition, a data acquisition program!")

    acq = HeartbeatApp()

    def signal_handler(sig, frame):
        if sig == signal.SIGINT or sig == signal.SIGTERM:
            if (acq.is_ready):
                logging.getLogger("hb.acq").critical("Received SIGINT, shutting down...")
                acq.shutdown()
                logging.getLogger("hb.acq").info("Goodbye.")
        else:
            logging.getLogger("hb.acq").critical("Shutting down...")

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