#!/bin/bash

# installs arduino-cli to ./bin
curl -fsSL https://raw.githubusercontent.com/arduino/arduino-cli/master/install.sh | sh

# adds PJRC teensy
./bin/arduino-cli core update-index --additional-urls https://www.pjrc.com/teensy/package_teensy_index.json

# install Teensy core
./bin/arduino-cli core install teensy:avr --additional-urls https://www.pjrc.com/teensy/package_teensy_index.json

# install arduino packages
./bin/arduino-cli lib install "Adafruit GPS Library"@1.7.4