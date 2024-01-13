#include <TinyGPS++.h>
#include <HardwareSerial.h>


// Constants
#define PIN_GPS_PPS 2
#define PIN_GPS_FIX 5
#define PIN_LED_GPSSYNC 6
#define PIN_LED_XMIT 6
#define PIN_ANALOG_IN 0
#define SAMPLE_RATE 20000
#define PIN_GPS_RX 7
#define PIN_GPS_TX 8
#define SAMPLE_SIZE 1000
const int SAMPLE_TIME_US = 1000000/SAMPLE_RATE;

// GPS objects
TinyGPSPlus gps;

// ADC stuff
IntervalTimer ADCTimer;
volatile unsigned int data[SAMPLE_SIZE];
volatile unsigned int sample_count = 0;
volatile bool data_ready = false;
bool pps_start = false;

// Serial outputs
#define GPSSerial Serial2
#define AcqSerial Serial

bool toggle = false;
void ISR_GPS_tick() {
  // digitalWrite(PIN_LED_3, toggle ? LOW : HIGH);
  // toggle = !toggle;

  // Call ADC once before timer
  DoADC();

  // Start ADC timer
  ADCTimer.begin(DoADC, SAMPLE_TIME_US);

  // PPS start
  pps_start = true;
  digitalWrite(LED_BUILTIN, HIGH);
}

void ISR_GPS_fix() {

}

void DoADC() {
  noInterrupts();
  int analog_value = 0;

  if (sample_count < SAMPLE_SIZE) {
    analog_value = analogRead(PIN_ANALOG_IN);
    data[sample_count] = analog_value;
    sample_count++;
  } else {
    ADCTimer.end();
    data_ready = true;
  }
  interrupts();
}

void setup() {
  AcqSerial.begin(115200);
  AcqSerial.println("Welcome to the ECLIPSE DATA ACQUISITION BOARD...");
  AcqSerial.printf("ADC sampling at %d Hz (%d us per sample).\n", SAMPLE_RATE, SAMPLE_TIME_US);


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
}

void loop() {

  if (GPSSerial.available()) {
    if (gps.encode(GPSSerial.read())) {
      if (gps.satellites.isUpdated()) {
        AcqSerial.printf("F$%d\n",gps.satellites.value());
      }

      if (gps.location.isUpdated()) {
        AcqSerial.printf("G$%f,%f\n", gps.location.lat(), gps.location.lng());
      }
    }
  }

  if (data_ready) {
    noInterrupts();
    AcqSerial.print("D$");
    for (unsigned int i = 0; i < sample_count; i++) {
      AcqSerial.print(data[i]);
      if (i < sample_count - 1) {
        AcqSerial.print(","); 
      }
    }

    AcqSerial.println();
    digitalWrite(LED_BUILTIN, LOW);

    data_ready = false;
    sample_count = 0;
    interrupts();
  }

}
