from datetime import datetime
import serial
import hbcapture as hb
import configparser
import signal
import numpy as np
import uuid
import matplotlib.pyplot as plt
import sys
from colorama import Fore, Back, Style



# Load config
config = configparser.ConfigParser()
config.read('acquire.ini')

# Open serial port
print(f"{Fore.BLUE}Using serial port {config["teensy"].get("port")}")

try: 
    ser = serial.Serial(config["teensy"].get("port"), 250000);
except serial.SerialException:
    print(f"{Fore.RED}Could not open serial port")
    sys.exit(1)

root_dir = config["main"].get("root_dir", "./hb")
capture_id = uuid.uuid4()
sample_rate = 20000
print(f"{capture_id}")
writer = hb.writer(root_dir=root_dir, capture_id=capture_id, node_id="ET0001", sample_rate=sample_rate)
writer.init()

print(Style.RESET_ALL)

running = True

def signal_handler(sig, frame):
    running = False
    print('You pressed Ctrl+C!')
    writer.done()
    packager = hb.HeartbeatCaptureWriterPackager(root_dir, capture_id)
    try:
        packager.package()
    except Exception as e:
        print(e)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

gps_searching = True

while running:
    line = ser.readline()
    line = line.decode()
    if line.startswith("#"):
        print(line)
        continue
    elif not line.startswith("$"):
        continue

    line = line[1:]

    capture_line = hb.HeartbeatCaptureLine.parse_line(line)
    
    if (capture_line.flags.clipping):
        print(Fore.RED + f"[{capture_line.time}] Audio signal clipping" + Style.RESET_ALL)

    if (not capture_line.flags.gps):
        gps_searching = True
        print(Fore.RED + f"[{capture_line.time}] No GPS fix" + Style.RESET_ALL)
    else:
        if gps_searching:
            print(Fore.GREEN + f"[{capture_line.time}] Got GPS fix" + Style.RESET_ALL)
            gps_searching = False


    writer.write_line(capture_line)