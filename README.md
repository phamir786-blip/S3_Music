# S3 Music Receiver (Hi-Res Audiophile Edition)

Ultra-low-latency, high-resolution audio receiver firmware ported and optimized specifically for the **ESP32-S3 N16R8** (16MB Flash + 8MB Octal PSRAM).

---

## Key Hardware Upgrades over ESP32-C3

| Feature | ESP32-C3 (Old) | ESP32-S3 N16R8 (New Port) |
| :--- | :--- | :--- |
| **SoC Architecture** | Single-core RISC-V @ 160 MHz | **Dual-core Xtensa LX7 @ 240 MHz** |
| **Audio Processing** | Timeshared on Core 0 with Wi-Fi | **Dedicated Core 1 High-Priority Task (Lock-Free SPSC)** |
| **RAM / PSRAM** | ~400 KB internal SRAM only | **8 MB Octal (OPI) PSRAM @ 80 MHz** |
| **Audio Buffer Size** | 64 KB (~350 ms at 16-bit 44.1k) | **6 MB Lock-Free Ring Buffer (~11s @ 24-bit 96k, hours of stream)** |
| **Volume Scaling** | Linear integer math | **Studio-Grade 32-bit Logarithmic Perceptual Curve (-60dB to 0dB)** |
| **Max Audio Format** | 16-bit 44.1 / 48 kHz | **True 24-bit / 96 kHz Stereo (UDA1334A Optimized)** |
| **OLED Display** | I2C SSD1306 (bus locks & jitter) | **Completely Removed (Headless, 0% bus jitter)** |
| **Web UI & Sliders** | Refined touch sliders | **100% Retained & Expanded with PSRAM telemetry** |

---

## Recommended I2S Pinout (ESP32-S3)

The default GPIO pin assignments for external I2S DACs (e.g. UDA1334A, PCM5102A, ES9023, ES9038Q2M, MAX98357A):

| Signal | ESP32-S3 GPIO | Description |
| :--- | :--- | :--- |
| **BCLK (Bit Clock)** | `GPIO 4` | Continuous bit clock (6.144 MHz @ 24-bit 96kHz) |
| **LRCLK / WS (Word Select)** | `GPIO 5` | Left / Right stereo word select clock (96 kHz) |
| **DOUT (Data Out)** | `GPIO 6` | Serial PCM audio data stream |
| **GND** | `GND` | Common system ground |
| **VCC / VIN** | `3.3V` or `5V` | DAC power supply (check your DAC board specs) |

*(Note: Pins avoid GPIO 33–37 which are utilized internally by the Octal PSRAM and Flash).*

---

## Building & Flashing

### Using PlatformIO:
```bash
# Build firmware
pio run -e s3music

# Flash over USB CDC
pio run -e s3music -t upload

# Monitor serial output
pio device monitor -b 115200
```

### Automated GitHub Actions:
The included `.github/workflows/build.yml` compiles the firmware on Ubuntu runners and generates `merged.bin` for initial USB flash and `firmware.bin` for wireless Web OTA updates.
