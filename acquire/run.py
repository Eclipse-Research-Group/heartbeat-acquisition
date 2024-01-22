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
import threading
from colorama import Fore, Back, Style
from minio import Minio
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
        logging.getLogger("ACQ.STORAGE").info(f"Connecting to {url} as {access_key}")
        self.client = Minio(url,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.bucket = bucket
        self.upload_queue = []
        pass

    def init(self):
        logger = logging.getLogger("acq.storage")
        self.client.bucket_exists(self.bucket)
        logger.info("Storage OK.")

        self.upload_thread = threading.Thread(target=self.loop, daemon=True)
        self.upload_thread.start()
        
        pass

    def loop(self):
        logger = logging.getLogger("acq.storage.upload_thread")
        while True:
            if len(self.upload_queue) == 0:
                time.sleep(10)
                continue

            logger.info("Uploading %d files" % len(self.upload_queue))

            while len(self.upload_queue) > 0:
                filename, path = self.upload_queue.pop(0)
                self.client.fput_object(self.bucket, os.path.join("/upload", filename), path)
                logger.info(f"Uploaded {filename} to {self.bucket}")

            

    def upload(self, filename: str, path: str): 
        logger = logging.getLogger("acq.storage")
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
        # configure logger
        logger = logging.getLogger("acq")
        logger.setLevel(logging.DEBUG)

        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(ConsoleFormatter())
        logger.addHandler(ch)


        # make sure our config file exists
        if (not os.path.isfile(os.path.join(os.getcwd(), 'config.ini'))):
            logger.critical("config.ini not found")
            sys.exit(1)


        self.config.read('config.ini')


        logger.info("Welcome to Heartbeat Acquisition")

        self.root_dir = self.config["acquire"].get("root_dir", "./hb")
        logger.info(f"Using root directory {self.root_dir}")
        if not os.path.isdir(self.root_dir):
            logger.info(f"Creating root directory {self.root_dir}")
            os.mkdir(self.root_dir)

        # Generate random capture id
        self.capture_id = uuid.uuid4()
        logger.info(f"Capture ID: {self.capture_id}")

        # log to file
        fh = logging.FileHandler(os.path.join(self.root_dir, f'{self.capture_id}.log'), encoding='utf-8', mode="w")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(FileFormatter())
        logger.addHandler(fh)


        # Open serial port
        logger.info(f"Opening serial port {self.config['teensy'].get('port')} at {self.config["teensy"].get("baudrate")} baud")
        try: 
            self.ser = serial.Serial(self.config["teensy"].get("port"), self.config["teensy"].get("baudrate"));
        except serial.SerialException:
            logger.critical("Could not open serial port")
            sys.exit(1)


        # Expected sample rate
        self.sample_rate = -1

        # TODO finish writer stuff
        self.writer = hb.writer(root_dir=self.root_dir, capture_id=self.capture_id, node_id="ET0001", sample_rate=self.sample_rate)
        self.writer.init()

        # Load storage
        logger.info("Loading minio...")
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
        serial_line = serial_line.decode('utf-8')
        
        if serial_line.startswith("#"):
            logger.getLogger("acq.serial").info(f"SERIAL: {serial_line}")
            self.writer.write_line(serial_line)
        elif not serial_line.startswith("$"):
            return

        line: hb.HeartbeatCaptureLine = hb.parse_line(serial_line[1:])
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
        if self.lines_written % 20 == 0:
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

acq = HeartbeatAcquisition()
    

def signal_handler(sig, frame):
    if sig == signal.SIGINT:
        if (acq.is_ready):
            logging.getLogger("acq").critical("Received SIGINT, shutting down...")
            acq.shutdown()
            logging.getLogger("acq").info("Goodbye.")
    else:
        logging.getLogger("acq").critical("Shutting down...")

    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

try:
    acq.init()
except Exception as e:
    logging.getLogger("acq.init").critical(e)
    logging.getLogger("acq.init").critical(traceback.format_exc())
    sys.exit(1)

while True:
    try:
        acq.tick()
    except FileNotFoundError as e:
        logging.getLogger("acq.tick").critical(e)
        logging.getLogger("acq.tick").critical(traceback.format_exc())
        # TODO attempt to upload existing files to server
        # logging.getLogger("acq.tick").info("Will attempt to upload existing data to server...")

        logging.getLogger("acq.tick").critical("Error in data acquisition, shutting down...")
        sys.exit(1)