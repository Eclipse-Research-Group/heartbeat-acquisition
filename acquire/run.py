from datetime import datetime
import serial
import hbcapture as hb
import configparser
import signal
import numpy as np
import uuid
import matplotlib.pyplot as plt
import sys
import os
import logging
from colorama import Fore, Back, Style

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

class HeartbeatUploader:
    def __init__(self):
        pass

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
        logger = logging.getLogger("ACQ")
        logger.setLevel(logging.DEBUG)

        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ConsoleFormatter())
        logger.addHandler(ch)


        # make sure our config file exists
        if (not os.path.isfile(os.path.join(os.getcwd(), 'acquire.ini'))):
            logger.critical("acquire.ini not found")
            sys.exit(1)


        self.config.read('acquire.ini')


        logger.info("Welcome to Heartbeat Acquisition")

        self.root_dir = self.config["acquire"].get("root_dir", "./hb")
        logger.info(f"Using root directory {self.root_dir}")
        if not os.path.isdir(self.root_dir):
            logger.info(f"Creating root directory {self.root_dir}")
            os.mkdir(self.root_dir)

        # log to file
        fh = logging.FileHandler(os.path.join(self.root_dir, 'acquire.log'), encoding='utf-8', mode="w")
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

        # Generate random capture id
        self.capture_id = uuid.uuid4()
        logger.info(f"Capture ID: {self.capture_id}")

        # Expected sample rate
        self.sample_rate = -1

        # TODO finish writer stuff
        self.writer = hb.writer(root_dir=self.root_dir, capture_id=self.capture_id, node_id="ET0001", sample_rate=self.sample_rate)
        self.writer.init()

        self.is_ready = True
        logger.info("Ready for data acquisition")
        

    def tick(self):
        logger = logging.getLogger("ACQ.TICK")
        logger.debug("Reading line from serial port")

        serial_line = self.ser.readline()
        serial_line = serial_line.decode('utf-8')
        
        if serial_line.startswith("#"):
            logger.getLogger("ACQ.SERIAL").info(f"SERIAL: {serial_line}")
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
        if self.lines_written % 100 == 0:
            logger.info(f"Moving to new file, {self.lines_written} lines written")
            self.writer.next_file()

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
            logging.getLogger("ACQ").critical("Received SIGINT, shutting down...")
            acq.shutdown()
            logging.getLogger("ACQ").info("Goodbye.")
    else:
        logging.getLogger("ACQ").critical("Shutting down...")

    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

acq.init()

while True:
    try:
        acq.tick()
    except Exception as e:
        logging.getLogger("ACQ.TICK").critical(e)
        # TODO attempt to upload existing files to server
        logging.getLogger("ACQ.TICK").critical("Error in data acquisition, shutting down...")
        logging.getLogger("ACQ.TICK").info("Will attempt to upload existing data to server...")
        sys.exit(1)