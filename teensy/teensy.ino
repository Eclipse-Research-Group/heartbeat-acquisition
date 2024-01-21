#include <HardwareSerial.h>
#include <Adafruit_GPS.h>
#include <TimeLib.h>
#include <time.h>


// Constants
#define PIN_GPS_PPS 2
#define PIN_GPS_FIX 5
#define PIN_LED_GPSSYNC 6
#define PIN_LED_XMIT 4
#define PIN_ANALOG_IN 0
#define SAMPLE_RATE 20000
#define CAPTURE_TIME_MS 360
#define PIN_GPS_RX 7
#define PIN_GPS_TX 8
#define SAMPLE_SIZE CAPTURE_TIME_MS*SAMPLE_RATE/1000
#define SAMPLE_TIME_US (1000000/SAMPLE_RATE)
#define CLIP_THRESHOLD 10


// ADC stuff
IntervalTimer ADCTimer;
IntervalTimer secondTimer;
bool second_timer_running = false;
volatile unsigned int data[SAMPLE_SIZE];
volatile unsigned int sample_count = 0;
volatile bool data_ready = false;
bool pps_start = false;

// GPS serial port
#define GPSSerial Serial2
Adafruit_GPS GPS(&GPSSerial);
bool GPS_fix = false;

// Serial port to raspberry pi
#define AcqSerial Serial

// Capture variables
bool capture_did_clip = false;

bool toggle = false;
void ISR_GPS_tick() {
  start_capture();
}

void ISR_GPS_fix() {
  GPS_fix = true;
}

void DoADC() {
  noInterrupts();
  int analog_value = 0;

  if (sample_count < SAMPLE_SIZE) {
    analog_value = analogRead(PIN_ANALOG_IN);

    if ((analog_value > 1023 - CLIP_THRESHOLD) || analog_value < CLIP_THRESHOLD) {
      // we're close to clipping
      capture_did_clip = true;
      digitalWrite(PIN_LED_XMIT, HIGH);
    }

    data[sample_count] = analog_value;
    sample_count++;
  } else {
    ADCTimer.end();
    data_ready = true;
  }
  interrupts();
}

void ISR_Second_tick() {
  start_capture();
}

void start_capture() {
  
  // Reset clipping
  capture_did_clip = false;
  digitalWrite(PIN_LED_XMIT, LOW);

  // Call ADC once before timer
  DoADC();

  // Start ADC timer
  ADCTimer.begin(DoADC, SAMPLE_TIME_US);

  // PPS start
  pps_start = true;
  digitalWrite(LED_BUILTIN, HIGH);
}

time_t get_unix_time(int year, int month, int day, int hour, int minute, int second) {
  tmElements_t tm;
  tm.Year = year - 1970;
  tm.Month = month;
  tm.Day = day;
  tm.Hour = hour;
  tm.Minute = minute;
  tm.Second = second;
  return makeTime(tm);
}

void setup() {
  AcqSerial.begin(9600);
  AcqSerial.println("Welcome to the ECLIPSE DATA ACQUISITION BOARD...");
  AcqSerial.printf("ADC sampling at %d Hz (%d us per sample).\n", SAMPLE_RATE, SAMPLE_TIME_US);
  AcqSerial.printf("%d time, %d sample size\n", CAPTURE_TIME_MS, SAMPLE_SIZE);


  // Configure interrupts
  attachInterrupt(digitalPinToInterrupt(PIN_GPS_PPS), ISR_GPS_tick, RISING);
  attachInterrupt(digitalPinToInterrupt(PIN_GPS_FIX), ISR_GPS_fix, RISING);

  // Configure pin modes
  pinMode(LED_BUILTIN, OUTPUT);
  pinMode(PIN_LED_XMIT, OUTPUT);
  pinMode(PIN_LED_GPSSYNC, OUTPUT);
  pinMode(PIN_GPS_FIX, INPUT);
  pinMode(PIN_GPS_PPS, INPUT);

  // Default pin state
  digitalWrite(LED_BUILTIN, HIGH);
  digitalWrite(PIN_LED_GPSSYNC, HIGH);

  GPSSerial.begin(9600);
  AcqSerial.begin(250000);

  // read some GPS
  int counter = 0;
  while (counter < 1000) {
    GPS.read();
    counter++;
  }
}

void loop() {
  GPS.read();

  if (GPS.fix > 0) {
    GPS_fix = true;
  } else {
    GPS_fix = false;
  }

  if (!GPS_fix && !second_timer_running) {
    second_timer_running = true;
    secondTimer.begin(ISR_Second_tick, 1e6);
  } else if (GPS_fix && second_timer_running) {
    secondTimer.end();
    second_timer_running = false;
  }

  if (GPS.newNMEAreceived()) {
    GPS.parse(GPS.lastNMEA());
  }

  if (data_ready) {
    time_t time = get_unix_time(2000 + GPS.year, GPS.month, GPS.day, GPS.hour, GPS.minute, GPS.seconds);

    // For whatever reason the time value is incorrect on the first tick when device is reset. This is a hacky fix.
    if (time < 1705819583) {
      goto acq_error;
    }

    AcqSerial.print("$");

    AcqSerial.print(time);
    AcqSerial.print(",");


    if (GPS_fix) {
      AcqSerial.print("G");
    }

    if (capture_did_clip) {
      AcqSerial.print("O");
    }
    AcqSerial.print(",");

    AcqSerial.print(SAMPLE_RATE);
    AcqSerial.print(",");

    // Print GPS
    AcqSerial.printf("%f,%f,%f,", GPS.latitudeDegrees, GPS.longitudeDegrees, GPS.altitude);

    // Print satellite count
    AcqSerial.printf("%d,", GPS.satellites);

    AcqSerial.printf("%f,%f,", GPS.speed, GPS.angle);

    noInterrupts();
    for (unsigned int i = 0; i < sample_count; i++) {
      AcqSerial.print(data[i]);
      if (i < sample_count - 1) {
        AcqSerial.print(","); 
      }
    }

    AcqSerial.println();
  acq_error:
    digitalWrite(LED_BUILTIN, LOW);

    data_ready = false;
    sample_count = 0;
    interrupts();

    setTime(GPS.hour, GPS.minute, GPS.seconds, GPS.day, GPS.month, GPS.year + 2000);
  }

}
