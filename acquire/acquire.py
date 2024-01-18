from datetime import datetime
import serial
import hbcapture as hb
import configparser
import uuid

# Load config
config = configparser.ConfigParser()
config.read('acquire.ini')



with hb.writer(config["main"].get("root_dir", "./hb"), 21010, uuid.uuid4(), "ET0001") as writer:
    line = hb.HeartbeatCaptureLine(time=datetime.fromtimestamp(234), data=[0, 0, 0, 0, 0, 0, 0, 0])
    print(line.time)
    writer.write_line(line)