#!/bin/bash
./bin/arduino-cli compile -b teensy:avr:teensy40

PORT=$(./bin/arduino-cli board list | awk '/teensy/ {print $1}')

./bin/arduino-cli upload -p $PORT -b teensy:avr:teensy40