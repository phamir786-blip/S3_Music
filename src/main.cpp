/*
  S3 Music Receiver (Hi-Res Audiophile Edition)
  ESP32-S3 N16R8 (16MB Flash + 8MB Octal PSRAM @ 240MHz)
  Headless Hi-Fi Architecture (OLED completely purged for zero bus jitter)
  Up to 24-bit / 96 kHz Audio Support · 4 MB PSRAM Ring Buffer
  Dedicated Core 1 High-Priority I2S DMA Pipeline

  I2S Default Pins (ESP32-S3):
  - BCLK:  GPIO 10
  - LRCLK: GPIO 11
  - DOUT:  GPIO 12
*/

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClient.h>
#include <WebServer.h>
#include <ESPmDNS.h>
#include <Preferences.h>
#include <Update.h>
#include <driver/i2s.h>
#include <esp_heap_caps.h>
#include <atomic>
#include <math.h>

static const char WIFI_SSID[] = "GFiber_2.4_Coverage_AECD9";
static const char WIFI_PASSWORD[] = "006BF4FD";
static const char PHONE_HOST[] = "192.168.254.119";

static const char DEVICE_NAME[] = "S3 Music Receiver";
static const char MDNS_HOSTNAME[] = "s3music";
static const char FIRMWARE_VERSION[] = "2.0.0-hires";

// I2S Pins optimized for ESP32-S3 (Avoiding Octal Flash/PSRAM GPIO 33-37 & USB CDC GPIO 19-20)
// Configured for UDA1334A I2S DAC (supports up to 24-bit 96kHz stereo via internal PLL)
static constexpr int I2S_BCLK_PIN = 10;
static constexpr int I2S_LRCLK_PIN = 11;
static constexpr int I2S_DOUT_PIN = 12;

static constexpr uint16_t TCP_DEFAULT_PORT = 50005;
static constexpr uint16_t HTTP_DEFAULT_PORT = 8080;

static constexpr uint32_t DEFAULT_SAMPLE_RATE = 96000;
static constexpr uint8_t DEFAULT_CHANNELS = 2;
static constexpr uint8_t DEFAULT_BITS_PER_SAMPLE = 24;

static constexpr uint8_t C3_PROTOCOL_VERSION = 1;
static constexpr size_t C3_FORMAT_HEADER_BYTES = 16;
static constexpr uint32_t TCP_FORMAT_HEADER_TIMEOUT_MS = 1000;

// Flagship 6 MB Lock-Free Audio Ring Buffer in 8MB Octal PSRAM (SPIRAM)
// Provides ~11 seconds of uncompressed 24-bit 96kHz PCM or hours of streaming audio
static constexpr size_t AUDIO_RING_BYTES = 6291456; 
static constexpr size_t NETWORK_READ_BYTES = 8192;
static constexpr size_t I2S_WRITE_BYTES = 4096;
static constexpr uint32_t STREAM_RETRY_MS = 2500;
static constexpr uint32_t WIFI_RETRY_MS = 10000;
static constexpr uint32_t STATUS_REFRESH_MS = 1000;

static constexpr uint32_t HTTP_HEADER_TIMEOUT_MS = 8000;
static constexpr uint32_t STREAM_READ_TIMEOUT_MS = 15000;
static constexpr uint32_t WAV_PARSE_MAX_BYTES = 4096;

// Simple 0-100% linear amplitude gain.
// 0% = silence, 100% = unity gain (bit-perfect bypass).
static inline uint32_t volumeMultiplier(uint8_t volume) {
  if (volume >= 100) return 65536;
  return ((uint32_t)volume * 65536UL) / 100UL;
}

enum StreamMode : uint8_t { STREAM_MODE_TCP = 0, STREAM_MODE_HTTP = 1 };
enum ReceiverState : uint8_t {
  RX_BOOTING = 0, RX_WIFI_CONNECTING, RX_WIFI_OFFLINE, RX_IDLE,
  RX_CONNECTING, RX_BUFFERING, RX_STREAMING, RX_STOPPED, RX_ERROR, RX_UPDATING
};

struct StreamFormat {
  uint32_t sampleRate = DEFAULT_SAMPLE_RATE;
  uint16_t channels = DEFAULT_CHANNELS;
  uint16_t bitsPerSample = DEFAULT_BITS_PER_SAMPLE;
  uint16_t audioFormat = 1;
  bool valid = false;
};

struct RuntimeStats {
  uint32_t reconnects = 0, underruns = 0, streamErrors = 0;
  uint32_t bytesReceived = 0, bytesPlayed = 0, sessionStartedMs = 0, lastReceiveMs = 0;
  int lastHttpStatus = 0;
  String lastError;
};

struct Settings {
  String phoneHost;
  uint16_t tcpPort = TCP_DEFAULT_PORT;
  uint16_t httpPort = HTTP_DEFAULT_PORT;
  StreamMode preferredMode = STREAM_MODE_TCP;
  bool autoFallback = true, autoReconnect = true, oledEnabled = false, streamEnabled = true;
  uint16_t targetBufferMs = 15;
  uint8_t volumePercent = 80;
};

static Preferences preferences;
static WebServer server(80);
static WiFiClient streamClient;
static Settings settings;
static StreamFormat streamFormat;
static RuntimeStats stats;
static volatile ReceiverState receiverState = RX_BOOTING;
static volatile bool stopRequested = false;
static volatile bool i2sReady = false;
static volatile bool bufferStarted = false;

// Pointer to dynamically allocated 6MB PSRAM buffer
static uint8_t *audioRing = nullptr;
static size_t actualRingBytes = AUDIO_RING_BYTES;

// Lock-Free Single-Producer Single-Consumer (SPSC) Atomic Indices
// Core 0 produces from Wi-Fi stream; Core 1 consumes to I2S DMA with ZERO lock contention!
static std::atomic<size_t> ringWriteIndex{0};
static std::atomic<size_t> ringReadIndex{0};

static uint8_t tcpPrefetch[C3_FORMAT_HEADER_BYTES];
static size_t tcpPrefetchLen = 0;
static SemaphoreHandle_t audioDataSemaphore = nullptr;
static SemaphoreHandle_t i2sMux = nullptr;
static SemaphoreHandle_t streamClientMux = nullptr;
static TaskHandle_t streamTaskHandle = nullptr, playbackTaskHandle = nullptr;
static uint32_t lastWifiAttemptMs = 0, lastStreamAttemptMs = 0, lastStatusRefreshMs = 0;
static bool mdnsStarted = false;

static void loadSettings() {
  preferences.begin("s3music", false);
  settings.phoneHost = preferences.getString("host", PHONE_HOST);
  settings.tcpPort = preferences.getUShort("tcpport", TCP_DEFAULT_PORT);
  settings.httpPort = preferences.getUShort("httpport", HTTP_DEFAULT_PORT);
  settings.preferredMode = (StreamMode)preferences.getUChar("mode", STREAM_MODE_TCP);
  settings.autoFallback = preferences.getBool("fallback", true);
  settings.autoReconnect = preferences.getBool("autorecon", true);
  settings.oledEnabled = false; // OLED purged
  settings.streamEnabled = preferences.getBool("enabled", true);
  settings.targetBufferMs = preferences.getUShort("buffer", 15);
  settings.volumePercent = preferences.getUChar("volume", 80);
  if (settings.volumePercent > 100) settings.volumePercent = 100;
  if (settings.phoneHost.length() == 0) settings.phoneHost = PHONE_HOST;
  if (settings.tcpPort == 0) settings.tcpPort = TCP_DEFAULT_PORT;
  if (settings.httpPort == 0) settings.httpPort = HTTP_DEFAULT_PORT;
  if (settings.targetBufferMs != 15 && settings.targetBufferMs != 50 && settings.targetBufferMs != 150 && settings.targetBufferMs != 300) settings.targetBufferMs = 15;
}

static void saveSettings() {
  preferences.putString("host", settings.phoneHost);
  preferences.putUShort("tcpport", settings.tcpPort);
  preferences.putUShort("httpport", settings.httpPort);
  preferences.putUChar("mode", settings.preferredMode);
  preferences.putBool("fallback", settings.autoFallback);
  preferences.putBool("autorecon", settings.autoReconnect);
  preferences.putBool("oled", false);
  preferences.putBool("enabled", settings.streamEnabled);
  preferences.putUShort("buffer", settings.targetBufferMs);
  preferences.putUChar("volume", settings.volumePercent);
}

static void resetSettings() {
  preferences.clear(); preferences.end();
  settings.phoneHost = PHONE_HOST;
  settings.tcpPort = TCP_DEFAULT_PORT;
  settings.httpPort = HTTP_DEFAULT_PORT;
  settings.preferredMode = STREAM_MODE_TCP;
  settings.autoFallback = true; settings.autoReconnect = true;
  settings.oledEnabled = false; settings.streamEnabled = true;
  settings.targetBufferMs = 15;
  settings.volumePercent = 80;
  preferences.begin("s3music", false); saveSettings();
}

static const char* receiverStateName(ReceiverState v) {
  switch(v) {
    case RX_BOOTING: return "Booting"; case RX_WIFI_CONNECTING: return "WiFi connecting";
    case RX_WIFI_OFFLINE: return "WiFi offline"; case RX_IDLE: return "Ready";
    case RX_CONNECTING: return "Connecting"; case RX_BUFFERING: return "Buffering";
    case RX_STREAMING: return "Streaming"; case RX_STOPPED: return "Stopped";
    case RX_ERROR: return "Error"; case RX_UPDATING: return "Updating";
    default: return "Unknown";
  }
}

static const char* streamModeName(StreamMode m) { return m == STREAM_MODE_HTTP ? "HTTP WAV" : "TCP PCM"; }

static void setReceiverState(ReceiverState value, const String &error = "") {
  receiverState = value;
  if (error.length() > 0) { stats.lastError = error; Serial.printf("[STATE] %s: %s\n", receiverStateName(value), error.c_str()); }
  else { Serial.printf("[STATE] %s\n", receiverStateName(value)); }
}

// Lock-Free SPSC atomic ring buffer write (Producer: Core 0 streamTask)
static bool ringWrite(const uint8_t *data, size_t length) {
  if (!audioRing || !data || length == 0) return true;
  size_t writeIdx = ringWriteIndex.load(std::memory_order_relaxed);
  size_t readIdx  = ringReadIndex.load(std::memory_order_acquire);

  size_t currentCount = (writeIdx >= readIdx) ? (writeIdx - readIdx) : (actualRingBytes - (readIdx - writeIdx));
  size_t freeBytes = actualRingBytes - 1 - currentCount;
  if (length > freeBytes) return false;

  size_t spaceToEnd = actualRingBytes - writeIdx;
  size_t first = (length < spaceToEnd) ? length : spaceToEnd;
  memcpy(audioRing + writeIdx, data, first);
  size_t second = length - first;
  if (second > 0) memcpy(audioRing, data + first, second);

  size_t nextWrite = (writeIdx + length) % actualRingBytes;
  ringWriteIndex.store(nextWrite, std::memory_order_release);
  if (audioDataSemaphore) xSemaphoreGive(audioDataSemaphore);
  return true;
}

// Lock-Free SPSC atomic ring buffer read (Consumer: Core 1 playbackTask)
static size_t ringRead(uint8_t *out, size_t maxLen, size_t frameSize = 1) {
  if (!audioRing || !out || maxLen == 0) return 0;
  if (frameSize == 0) frameSize = 1;

  size_t readIdx  = ringReadIndex.load(std::memory_order_relaxed);
  size_t writeIdx = ringWriteIndex.load(std::memory_order_acquire);

  size_t currentCount = (writeIdx >= readIdx) ? (writeIdx - readIdx) : (actualRingBytes - (readIdx - writeIdx));
  size_t len = (maxLen < currentCount) ? maxLen : currentCount;
  len = (len / frameSize) * frameSize; // Strictly maintain complete frame boundaries
  if (len == 0) return 0;

  size_t spaceToEnd = actualRingBytes - readIdx;
  size_t first = (len < spaceToEnd) ? len : spaceToEnd;
  memcpy(out, audioRing + readIdx, first);
  size_t second = len - first;
  if (second > 0) memcpy(out + first, audioRing, second);

  size_t nextRead = (readIdx + len) % actualRingBytes;
  ringReadIndex.store(nextRead, std::memory_order_release);
  return len;
}

static void ringClear() {
  ringWriteIndex.store(0, std::memory_order_relaxed);
  ringReadIndex.store(0, std::memory_order_relaxed);
  bufferStarted = false;
}

static size_t ringSize() {
  size_t writeIdx = ringWriteIndex.load(std::memory_order_relaxed);
  size_t readIdx  = ringReadIndex.load(std::memory_order_relaxed);
  return (writeIdx >= readIdx) ? (writeIdx - readIdx) : (actualRingBytes - (readIdx - writeIdx));
}

static uint8_t ringPercent() {
  return actualRingBytes ? (uint8_t)((ringSize() * 100ULL) / actualRingBytes) : 0;
}

static void beginMdnsIfNeeded();
static bool beginI2S(uint32_t sr, uint16_t ch, uint16_t bits);

static bool connectStreamHost(uint16_t port) {
  String host = settings.phoneHost;
  host.trim();
  if (host.length() == 0) return false;

  if (streamClientMux) xSemaphoreTake(streamClientMux, portMAX_DELAY);
  streamClient.stop();

  IPAddress ip;
  bool resolved = false;

  if (ip.fromString(host)) {
    resolved = true;
  } else if (host.endsWith(".local")) {
    ip = MDNS.queryHost(host, 2000);
    resolved = (ip != IPAddress(0,0,0,0));
  } else {
    resolved = WiFi.hostByName(host.c_str(), ip);
  }

  bool connected = false;
  if (resolved) {
    Serial.printf("[STREAM] Connecting to %s:%u\n", ip.toString().c_str(), port);
    connected = streamClient.connect(ip, port);
  } else {
    Serial.printf("[STREAM] Connecting to host %s:%u\n", host.c_str(), port);
    connected = streamClient.connect(host.c_str(), port);
  }

  if (streamClientMux) xSemaphoreGive(streamClientMux);
  return connected;
}

static void connectWifiIfNeeded() {
  if (WiFi.status() == WL_CONNECTED) { beginMdnsIfNeeded(); return; }
  if (millis() - lastWifiAttemptMs < WIFI_RETRY_MS) return;
  lastWifiAttemptMs = millis();
  setReceiverState(RX_WIFI_CONNECTING);
  WiFi.mode(WIFI_STA); WiFi.setHostname(MDNS_HOSTNAME);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  Serial.printf("[WIFI] Connecting to %s\n", WIFI_SSID);
}

static void beginMdnsIfNeeded() {
  if (mdnsStarted || WiFi.status() != WL_CONNECTED) return;
  if (MDNS.begin(MDNS_HOSTNAME)) { MDNS.addService("http","tcp",80); mdnsStarted = true; Serial.printf("[MDNS] http://%s.local/\n", MDNS_HOSTNAME); }
  else Serial.println("[MDNS] Failed");
}

// Supports up to 24-bit 96kHz PCM stereo audio
static bool supportedPcmFormat(const StreamFormat &f) {
  return f.audioFormat == 1 &&
         (f.bitsPerSample == 16 || f.bitsPerSample == 24 || f.bitsPerSample == 32) &&
         (f.channels == 1 || f.channels == 2) &&
         (f.sampleRate >= 22050 && f.sampleRate <= 96000);
}

static void endI2S() {
  if (i2sMux) xSemaphoreTake(i2sMux, portMAX_DELAY);
  if (i2sReady) { i2s_driver_uninstall(I2S_NUM_0); i2sReady = false; }
  if (i2sMux) xSemaphoreGive(i2sMux);
}

static uint32_t targetPrebufferBytes() {
  uint64_t bytesPerSecond = (uint64_t)streamFormat.sampleRate *
                            (uint64_t)streamFormat.channels *
                            (uint64_t)(streamFormat.bitsPerSample / 8);
  if (bytesPerSecond == 0) bytesPerSecond =
      (uint64_t)DEFAULT_SAMPLE_RATE * DEFAULT_CHANNELS * (DEFAULT_BITS_PER_SAMPLE / 8);
  uint64_t bytes = (bytesPerSecond * settings.targetBufferMs) / 1000ULL;
  if (bytes < 8192) bytes = 8192;
  const uint64_t maxBufferBytes = (uint64_t)actualRingBytes - 16384ULL;
  if (bytes > maxBufferBytes) bytes = maxBufferBytes;
  return (uint32_t)bytes;
}

static void stopStreamClient() {
  if (streamClientMux) xSemaphoreTake(streamClientMux, portMAX_DELAY);
  streamClient.stop();
  if (streamClientMux) xSemaphoreGive(streamClientMux);
}

static bool readExact(WiFiClient &c, uint8_t *buf, size_t len, uint32_t to) {
  size_t got = 0; uint32_t t = millis();
  while (got < len && !stopRequested) {
    int av = c.available();
    if (av > 0) {
      size_t rem = len - got;
      size_t w = ((size_t)av < rem) ? (size_t)av : rem;
      int n = c.read(buf + got, w);
      if (n > 0) { got += n; t = millis(); }
    }
    else { if (!c.connected() || millis() - t > to) return false; delay(1); }
  }
  return got == len;
}

static bool readLine(WiFiClient &c, String &line, uint32_t to) {
  line = ""; uint32_t t = millis();
  while (!stopRequested) {
    if (c.available()) { char ch = (char)c.read(); line += ch; if (line.endsWith(String("\r") + "\n")) return true; if (line.length() > 1024) return false; t = millis(); }
    else { if (!c.connected() || millis() - t > to) return false; delay(1); }
  }
  return false;
}

static bool parseHttpHeaders() {
  String line;
  if (!readLine(streamClient, line, HTTP_HEADER_TIMEOUT_MS)) { stats.lastError = "HTTP no status"; return false; }
  line.trim(); Serial.printf("[HTTP] %s\n", line.c_str());
  if (!line.startsWith("HTTP/") || line.indexOf(" 200") < 0) { stats.lastError = "HTTP " + line; return false; }
  stats.lastHttpStatus = 200;
  while (!stopRequested) {
    if (!readLine(streamClient, line, HTTP_HEADER_TIMEOUT_MS)) { stats.lastError = "HTTP header timeout"; return false; }
    if (line == String("\r") + "\n" || line.length() == 0) return true;
    line.trim(); if (line.length() > 0) Serial.printf("[HTTP] %s\n", line.c_str());
  }
  return false;
}

static uint32_t readLe32(const uint8_t *p){ return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24); }
static uint16_t readLe16(const uint8_t *p){ return p[0] | (p[1] << 8); }

static bool parseWavHeader(StreamFormat &f) {
  uint8_t riff[12];
  if (!readExact(streamClient, riff, sizeof(riff), HTTP_HEADER_TIMEOUT_MS)) { stats.lastError = "WAV header missing"; return false; }
  if (memcmp(riff, "RIFF", 4) != 0 || memcmp(riff + 8, "WAVE", 4) != 0) { stats.lastError = "Not RIFF/WAV"; return false; }
  bool fmtOk = false, dataOk = false; size_t sc = 12;
  while (!stopRequested && sc < WAV_PARSE_MAX_BYTES) {
    uint8_t hd[8];
    if (!readExact(streamClient, hd, sizeof(hd), HTTP_HEADER_TIMEOUT_MS)) { stats.lastError = "WAV chunk timeout"; return false; }
    sc += sizeof(hd);
    uint32_t sz = readLe32(hd + 4);
    if (memcmp(hd, "fmt ", 4) == 0) {
      if (sz < 16 || sz > 64) { stats.lastError = "Bad WAV fmt"; return false; }
      uint8_t buf[64];
      if (!readExact(streamClient, buf, sz, HTTP_HEADER_TIMEOUT_MS)) { stats.lastError = "WAV fmt timeout"; return false; }
      sc += sz;
      f.audioFormat = readLe16(buf + 0); 
      f.channels = readLe16(buf + 2); 
      f.sampleRate = readLe32(buf + 4); 
      f.bitsPerSample = readLe16(buf + 14);
      f.valid = supportedPcmFormat(f);
      Serial.printf("[WAV] fmt=%u ch=%u rate=%lu bits=%u (Hi-Res Valid=%d)\n", f.audioFormat, f.channels, (unsigned long)f.sampleRate, f.bitsPerSample, f.valid);
      if (!f.valid) { stats.lastError = "Unsupported WAV PCM (Max 24-bit 96kHz)"; return false; }
      fmtOk = true;
    } else if (memcmp(hd, "data", 4) == 0) { dataOk = true; Serial.printf("[WAV] data chunk (%lu bytes)\n", (unsigned long)sz); break; }
    else {
      uint8_t disc[128]; uint32_t rem = sz;
      while (rem > 0) {
        size_t p = ((uint32_t)sizeof(disc) < rem) ? (uint32_t)sizeof(disc) : rem;
        if (!readExact(streamClient, disc, p, HTTP_HEADER_TIMEOUT_MS)) { stats.lastError = "WAV skip timeout"; return false; } rem -= p; sc += p;
      }
      if (sz & 1U) { uint8_t pad; if (!readExact(streamClient, &pad, 1, HTTP_HEADER_TIMEOUT_MS)) { stats.lastError = "WAV pad timeout"; return false; } sc++; }
    }
  }
  if (!fmtOk || !dataOk) { stats.lastError = "WAV chunks invalid"; return false; }
  return true;
}

// TCP Format Header — Supports up to 24-bit / 96 kHz
static bool readTcpFormatHeader() {
  tcpPrefetchLen = 0;

  uint8_t header[C3_FORMAT_HEADER_BYTES];
  if (!readExact(streamClient, header, 4, TCP_FORMAT_HEADER_TIMEOUT_MS)) {
    stats.lastError = "TCP format header timeout";
    return false;
  }

  if (memcmp(header, "C3MS", 4) != 0) {
    stats.lastError = "Missing C3MS format header";
    streamFormat.sampleRate = 0;
    streamFormat.channels = 0;
    streamFormat.bitsPerSample = 0;
    streamFormat.audioFormat = 1;
    streamFormat.valid = false;
    Serial.println("[TCP] Rejected: missing C3MS format header");
    return false;
  }

  if (!readExact(streamClient, header + 4, C3_FORMAT_HEADER_BYTES - 4, TCP_FORMAT_HEADER_TIMEOUT_MS)) {
    stats.lastError = "TCP C3MS header incomplete";
    return false;
  }

  const uint8_t version = header[4];
  const uint16_t bits = header[5];
  const uint16_t channels = header[6];
  const uint32_t sampleRate = readLe32(header + 8);
  const uint32_t frameSize = readLe32(header + 12);
  const uint32_t expectedFrameSize = (uint32_t)channels * (uint32_t)(bits / 8);

  if (version == C3_PROTOCOL_VERSION &&
      (bits == 16 || bits == 24 || bits == 32) &&
      (channels == 1 || channels == 2) &&
      (sampleRate >= 22050 && sampleRate <= 96000) &&
      frameSize == expectedFrameSize &&
      expectedFrameSize > 0) {
    streamFormat.sampleRate = sampleRate;
    streamFormat.channels = channels;
    streamFormat.bitsPerSample = bits;
    streamFormat.audioFormat = 1;
    streamFormat.valid = true;

    Serial.printf("[TCP] C3MS v%u: %lu Hz, %u-bit, %s, frame=%lu\n",
                  version, (unsigned long)sampleRate, bits,
                  channels == 2 ? "Stereo" : "Mono",
                  (unsigned long)frameSize);
    return true;
  }

  // A valid C3MS header is required so I2S always uses the host-provided format.
  stats.lastError = "Invalid C3MS format header";
  streamFormat.sampleRate = 0;
  streamFormat.channels = 0;
  streamFormat.bitsPerSample = 0;
  streamFormat.audioFormat = 1;
  streamFormat.valid = false;
  Serial.println("[TCP] Rejected: invalid C3MS format header");
  return false;

}

static bool connectRawTcp() {
  setReceiverState(RX_CONNECTING);
  streamFormat.sampleRate = 0; streamFormat.channels = 0; streamFormat.bitsPerSample = 0; streamFormat.audioFormat = 1; streamFormat.valid = false;
  Serial.printf("[TCP] Connecting to %s:%u\n", settings.phoneHost.c_str(), settings.tcpPort);
  if (!connectStreamHost(settings.tcpPort)) { stats.lastError = "TCP host unavailable"; return false; }
  streamClient.setNoDelay(true);
  streamClient.setTimeout(3000);

  if (!readTcpFormatHeader()) {
    streamClient.stop();
    return false;
  }

  if (!beginI2S(streamFormat.sampleRate, streamFormat.channels, streamFormat.bitsPerSample)) { 
    stats.lastError = "I2S setup failed"; 
    streamClient.stop(); 
    return false; 
  }
  return true;
}

static bool connectHttpWav() {
  setReceiverState(RX_CONNECTING);
  streamFormat.sampleRate = 0; streamFormat.channels = 0; streamFormat.bitsPerSample = 0; streamFormat.audioFormat = 1; streamFormat.valid = false;
  stats.lastHttpStatus = 0;
  Serial.printf("[HTTP] Connecting to http://%s:%u/\n", settings.phoneHost.c_str(), settings.httpPort);
  if (!connectStreamHost(settings.httpPort)) { stats.lastError = "HTTP host unavailable"; return false; }
  streamClient.setNoDelay(true);
  streamClient.setTimeout(3000);
  streamClient.printf("GET / HTTP/1.1\r\nHost: %s:%u\r\nUser-Agent: S3MusicReceiver/%s\r\nAccept: audio/wav,audio/x-wav,*/*\r\nConnection: close\r\n\r\n", 
                      settings.phoneHost.c_str(), settings.httpPort, FIRMWARE_VERSION);
  if (!parseHttpHeaders()) { streamClient.stop(); return false; }
  StreamFormat pf;
  if (!parseWavHeader(pf)) { streamClient.stop(); return false; }
  streamFormat = pf;
  if (!beginI2S(streamFormat.sampleRate, streamFormat.channels, streamFormat.bitsPerSample)) { 
    stats.lastError = "I2S setup failed"; 
    streamClient.stop(); 
    return false; 
  }
  return true;
}

// DRAM Static buffers for Core 1 DMA feeding (8KB output buffer guarantees 0% overflow on any mono-to-stereo expansion)
static constexpr size_t PLAYBACK_IN_BYTES = 4096;
static uint8_t playbackIn[PLAYBACK_IN_BYTES];
static uint8_t playbackOut[PLAYBACK_IN_BYTES * 4];

// Low-latency I2S initialization on ESP32-S3 supporting up to 24-bit 96kHz
static bool beginI2S(uint32_t sr, uint16_t ch, uint16_t bits) {
  if (sr < 22050 || sr > 96000) return false;
  if (ch != 1 && ch != 2) return false;
  if (bits != 16 && bits != 24 && bits != 32) return false;
  
  if (i2sMux) xSemaphoreTake(i2sMux, portMAX_DELAY);
  if (i2sReady) { i2s_driver_uninstall(I2S_NUM_0); i2sReady = false; }

  // Keep the host sample width native for 16-bit and 24-bit PCM.
  i2s_bits_per_sample_t bps =
      (bits == 16) ? I2S_BITS_PER_SAMPLE_16BIT :
      (bits == 24) ? I2S_BITS_PER_SAMPLE_24BIT :
      I2S_BITS_PER_SAMPLE_32BIT;

  // Ultra-low latency DMA queue: 8 descriptors x 512 bytes in internal SRAM
  // Hardware latency is only ~5ms at 96kHz, while PSRAM provides megabytes of safety cushion!
  i2s_config_t cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX),
    .sample_rate = sr,
    .bits_per_sample = bps,
    .channel_format = I2S_CHANNEL_FMT_RIGHT_LEFT,
    .communication_format = I2S_COMM_FORMAT_STAND_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 8,
    .dma_buf_len = 510,
    .use_apll = false,            // ESP32-S3 supports APLL / precise PLL fractional clock
    .tx_desc_auto_clear = true,  // Clear DMA on underrun automatically
    .fixed_mclk = 0
  };
  
  if (i2s_driver_install(I2S_NUM_0, &cfg, 0, nullptr) != ESP_OK) {
    if (i2sMux) xSemaphoreGive(i2sMux);
    return false;
  }

  i2s_pin_config_t pin = {
    .bck_io_num = I2S_BCLK_PIN,
    .ws_io_num = I2S_LRCLK_PIN,
    .data_out_num = I2S_DOUT_PIN,
    .data_in_num = I2S_PIN_NO_CHANGE
  };

  if (i2s_set_pin(I2S_NUM_0, &pin) != ESP_OK) {
    i2s_driver_uninstall(I2S_NUM_0);
    if (i2sMux) xSemaphoreGive(i2sMux);
    return false;
  }

  i2s_zero_dma_buffer(I2S_NUM_0);
  i2sReady = true;
  if (i2sMux) xSemaphoreGive(i2sMux);

  Serial.printf("[I2S] %lu Hz, %u-bit (slot=%u-bit), %s, BCLK=%d WS=%d DOUT=%d (PSRAM Ring: %u MB)\n", 
                (unsigned long)sr, bits, (unsigned)bps, ch==2?"Stereo":"Mono", 
                I2S_BCLK_PIN, I2S_LRCLK_PIN, I2S_DOUT_PIN, (unsigned)(actualRingBytes / (1024 * 1024)));
  return true;
}

// DRAM Static buffer for network stream intake — zero task stack consumption
static uint8_t networkIn[NETWORK_READ_BYTES];

// DEDICATED CORE 1 PLAYBACK TASK — Ultra-Low Latency, Zero Task Stalling
static void playbackTask(void*) {
  for(;;) {
    if (!i2sReady || !bufferStarted) { vTaskDelay(pdMS_TO_TICKS(5)); continue; }

    size_t frameSize = (streamFormat.channels > 0 && streamFormat.bitsPerSample > 0)
        ? (size_t)(streamFormat.channels * (streamFormat.bitsPerSample / 8))
        : 4;
    if (frameSize == 0) frameSize = 4;

    // Read only complete, integer frame boundaries from ring buffer
    size_t maxFrames = sizeof(playbackIn) / frameSize;
    size_t want = maxFrames * frameSize;
    size_t n = ringRead(playbackIn, want, frameSize);
    if (n == 0) {
      stats.underruns++;
      if (audioDataSemaphore) xSemaphoreTake(audioDataSemaphore, pdMS_TO_TICKS(15));
      else vTaskDelay(pdMS_TO_TICKS(2));
      continue;
    }

    const uint8_t *writeBuf = playbackIn;
    size_t writeLen = n;
    uint8_t volume = settings.volumePercent;

    // --- 16-BIT AUDIO PROCESSING ---
    if (streamFormat.bitsPerSample == 16) {
      if (volume < 100) {
        uint32_t mult = volumeMultiplier(volume);
        int16_t *samples = reinterpret_cast<int16_t*>(playbackIn);
        size_t sampleCount = n / sizeof(int16_t);
        for (size_t i = 0; i < sampleCount; ++i) {
          samples[i] = (int16_t)(((int32_t)samples[i] * mult) >> 16);
        }
      }

      if (streamFormat.channels == 1) {
        size_t samples = n / sizeof(int16_t);
        int16_t *src = reinterpret_cast<int16_t*>(playbackIn);
        int16_t *dst = reinterpret_cast<int16_t*>(playbackOut);
        for (size_t i = 0; i < samples; i++) { 
          dst[i * 2]     = src[i]; 
          dst[i * 2 + 1] = src[i]; 
        }
        writeBuf = playbackOut;
        writeLen = samples * 4;
      }
    }
    // --- 24-BIT NATIVE AUDIO PROCESSING ---
    else if (streamFormat.bitsPerSample == 24) {
      if (streamFormat.channels == 2) {
        // Keep stereo packed 24-bit PCM as 3-byte samples; only apply volume in place.
        if (volume < 100) {
          uint32_t mult = volumeMultiplier(volume);
          size_t sampleCount = n / 3;
          for (size_t i = 0; i < sampleCount; ++i) {
            size_t idx = i * 3;
            int32_t sample = (int32_t)(playbackIn[idx] |
                                       (playbackIn[idx + 1] << 8) |
                                       (playbackIn[idx + 2] << 16));
            if (sample & 0x800000) sample |= 0xFF000000;
            sample = (int32_t)(((int64_t)sample * mult) >> 16);
            playbackIn[idx] = (uint8_t)sample;
            playbackIn[idx + 1] = (uint8_t)(sample >> 8);
            playbackIn[idx + 2] = (uint8_t)(sample >> 16);
          }
        }
      } else {
        // Mono still requires stereo expansion for the configured two I2S slots.
        uint32_t mult = (volume < 100) ? volumeMultiplier(volume) : 65536;
        size_t sampleCount = n / 3;
        int32_t *dst = reinterpret_cast<int32_t*>(playbackOut);
        for (size_t i = 0; i < sampleCount; ++i) {
          size_t idx = i * 3;
          int32_t s = (int32_t)(playbackIn[idx] |
                                (playbackIn[idx + 1] << 8) |
                                (playbackIn[idx + 2] << 16));
          if (s & 0x800000) s |= 0xFF000000;
          if (volume < 100) s = (int32_t)(((int64_t)s * mult) >> 16);
          int32_t slot = s << 8;
          dst[i * 2] = slot;
          dst[i * 2 + 1] = slot;
        }
        writeBuf = playbackOut;
        writeLen = sampleCount * 8;
      }
    }
   // --- 32-BIT HIGH-RESOLUTION AUDIO PROCESSING ---
    else if (streamFormat.bitsPerSample == 32) {
      // 1. Declare them here so they stay alive for the entire 32-bit block
      int32_t *samples32 = reinterpret_cast<int32_t*>(playbackIn);
      size_t sampleCount32 = n / sizeof(int32_t);

      // 2. Volume attenuation loop (optional inner check)
      if (volume < 100) {
        uint32_t mult = volumeMultiplier(volume);
        for (size_t i = 0; i < sampleCount32; ++i) {
          samples32[i] = (int32_t)(((int64_t)samples32[i] * mult) >> 16);
        }
      }

      // 3. Mono-to-stereo expansion loop (now safely in scope!)
      if (streamFormat.channels == 1) {
        int32_t *dst = reinterpret_cast<int32_t*>(playbackOut);
        for (size_t i = 0; i < sampleCount32; i++) {
          dst[i * 2]     = samples32[i];
          dst[i * 2 + 1] = samples32[i];
        }
        writeBuf = playbackOut;
        writeLen = sampleCount32 * 8;
      }
    }
    if (i2sMux) xSemaphoreTake(i2sMux, portMAX_DELAY);
    bool ready = i2sReady;
    size_t w = 0;
    esp_err_t r = ready ? i2s_write(I2S_NUM_0, writeBuf, writeLen, &w, pdMS_TO_TICKS(30)) : ESP_FAIL;
    if (i2sMux) xSemaphoreGive(i2sMux);

    if (r == ESP_OK && w > 0) stats.bytesPlayed += n;
    else vTaskDelay(pdMS_TO_TICKS(1));
  }
}

static bool runConnectedStream() {
  bool streamFailed = false;
  stats.sessionStartedMs = millis(); stats.lastReceiveMs = millis(); ringClear(); setReceiverState(RX_BUFFERING);

  if (tcpPrefetchLen > 0) {
    ringWrite(tcpPrefetch, tcpPrefetchLen);
    tcpPrefetchLen = 0;
  }

  while (!stopRequested && streamClient.connected() && WiFi.status() == WL_CONNECTED) {
    int av = streamClient.available();
    if (av > 0) {
      size_t availBytes = (size_t)av;
      size_t w = (availBytes < sizeof(networkIn)) ? availBytes : sizeof(networkIn);
      int n = streamClient.read(networkIn, w);
      if (n > 0) {
        stats.bytesReceived += n;
        stats.lastReceiveMs = millis();

        // Never discard received PCM when the ring is temporarily full.
        // Wait for playback to free enough space, then commit the entire TCP chunk.
        while (!stopRequested && ringSize() > (AUDIO_RING_BYTES - (size_t)n)) {
          vTaskDelay(pdMS_TO_TICKS(1));
        }
        if (!stopRequested) {
          ringWrite(networkIn, n);
        }

        if (!bufferStarted && ringSize() >= targetPrebufferBytes()) {
          bufferStarted = true;
          setReceiverState(RX_STREAMING);
        }
      }
    }
    else {
      if (!streamClient.connected()) {
        stats.lastError = "Stream disconnected";
        streamFailed = true;
        break;
      }
      if (millis() - stats.lastReceiveMs > STREAM_READ_TIMEOUT_MS) {
        stats.lastError = bufferStarted ? "Stream stalled" : "Stream idle";
        setReceiverState(RX_ERROR, stats.lastError);
        streamFailed = true;
        break;
      }
      vTaskDelay(pdMS_TO_TICKS(1));
    }
  }

  return streamFailed && !stopRequested && WiFi.status() == WL_CONNECTED;
}

// STREAM & NETWORK TASK — Runs on Core 0 with Wi-Fi stack
static void streamTask(void*) {
  for(;;) {
    if (WiFi.status() != WL_CONNECTED) {
      stopStreamClient();
      endI2S();
      ringClear();
      if (settings.streamEnabled && !stopRequested && receiverState != RX_WIFI_OFFLINE && receiverState != RX_WIFI_CONNECTING && receiverState != RX_UPDATING) {
        Serial.printf("[WIFI] Stream task sees disconnected (status=%d)\n", (int)WiFi.status());
        setReceiverState(RX_WIFI_OFFLINE, "WiFi disconnected");
      }
      vTaskDelay(pdMS_TO_TICKS(500));
      continue;
    }
    if (!settings.streamEnabled || stopRequested) { 
      streamClient.stop(); endI2S(); ringClear(); 
      if (receiverState != RX_STOPPED) setReceiverState(RX_STOPPED); 
      vTaskDelay(pdMS_TO_TICKS(150)); 
      continue; 
    }
    if (millis() - lastStreamAttemptMs < STREAM_RETRY_MS) { vTaskDelay(pdMS_TO_TICKS(20)); continue; }
    lastStreamAttemptMs = millis();
    bool ok = false; StreamMode tr = settings.preferredMode; StreamMode activeMode = tr;
    if (tr == STREAM_MODE_TCP) ok = connectRawTcp(); else ok = connectHttpWav();
    if (!ok && settings.autoFallback && !stopRequested) {
      stopStreamClient(); endI2S(); ringClear();
      StreamMode fb = (tr == STREAM_MODE_TCP) ? STREAM_MODE_HTTP : STREAM_MODE_TCP;
      activeMode = fb;
      Serial.printf("[STREAM] Primary failed; trying %s\n", streamModeName(fb));
      if (fb == STREAM_MODE_TCP) ok = connectRawTcp(); else ok = connectHttpWav();
    }
    if (ok) {
      stats.reconnects++;
      bool streamFailed = runConnectedStream();
      if (streamFailed && settings.autoFallback && !stopRequested && WiFi.status() == WL_CONNECTED) {
        stopStreamClient(); endI2S(); ringClear();
        StreamMode fb = (activeMode == STREAM_MODE_TCP) ? STREAM_MODE_HTTP : STREAM_MODE_TCP;
        activeMode = fb;
        Serial.printf("[STREAM] Active stream failed; trying %s fallback\n", streamModeName(fb));
        if (fb == STREAM_MODE_TCP) ok = connectRawTcp(); else ok = connectHttpWav();
        if (ok) {
          stats.reconnects++;
          runConnectedStream();
        } else {
          stats.streamErrors++;
          setReceiverState(RX_ERROR, stats.lastError.length() ? stats.lastError : "Fallback unavailable");
        }
      }
    }
    else { stats.streamErrors++; setReceiverState(RX_ERROR, stats.lastError.length() ? stats.lastError : "Stream unavailable"); }
    streamClient.stop(); endI2S(); ringClear();
    if (!stopRequested && settings.streamEnabled && settings.autoReconnect) {
      vTaskDelay(pdMS_TO_TICKS(STREAM_RETRY_MS));
    } else {
      if (!stopRequested && settings.streamEnabled && !settings.autoReconnect) {
        setReceiverState(RX_ERROR, stats.lastError.length() ? stats.lastError : "Auto reconnect disabled");
      }
      vTaskDelay(pdMS_TO_TICKS(250));
    }
  }
}

static void startStreaming() {
  stopRequested = false;
  streamFormat.valid = false;
  streamFormat.sampleRate = 0; streamFormat.channels = 0; streamFormat.bitsPerSample = 0;
  settings.streamEnabled = true; lastStreamAttemptMs = 0; saveSettings(); setReceiverState(RX_IDLE);
}
static void stopStreaming() {
  stopRequested = true; settings.streamEnabled = false;
  streamFormat.valid = false;
  streamFormat.sampleRate = 0; streamFormat.channels = 0; streamFormat.bitsPerSample = 0;
  saveSettings(); stopStreamClient(); ringClear(); endI2S(); setReceiverState(RX_STOPPED);
}
static void reconnectStreaming() {
  stopRequested = true; stopStreamClient(); ringClear(); endI2S(); delay(100);
  streamFormat.valid = false;
  streamFormat.sampleRate = 0; streamFormat.channels = 0; streamFormat.bitsPerSample = 0;
  stopRequested = false; settings.streamEnabled = true; lastStreamAttemptMs = 0; saveSettings(); setReceiverState(RX_IDLE);
}

static String jsonEscape(const String &in) { String o; o.reserve(in.length() + 8); for(size_t i = 0; i < in.length(); ++i) { char c = in[i]; switch(c) { case'\\':o+="\\\\";break; case'"':o+="\\\"";break; case'\n':o+="\\n";break; case'\r':o+="\\r";break; case'\t':o+="\\t";break; default: if((uint8_t)c >= 0x20) o += c; } } return o; }

static String makeStatusJson() {
  String ip = (WiFi.status() == WL_CONNECTED) ? WiFi.localIP().toString() : "";
  String ssid = (WiFi.status() == WL_CONNECTED) ? WiFi.SSID() : "";
  int rssi = (WiFi.status() == WL_CONNECTED) ? WiFi.RSSI() : 0;
  uint32_t sess = 0; if (stats.sessionStartedMs > 0 && (receiverState == RX_STREAMING || receiverState == RX_BUFFERING)) sess = (millis() - stats.sessionStartedMs) / 1000UL;
  String r; r.reserve(1300);
  r += "{";
  r += "\"device\":\"" + jsonEscape(DEVICE_NAME) + "\",";
  r += "\"version\":\"" + String(FIRMWARE_VERSION) + "\",";
  r += "\"state\":\"" + String(receiverStateName(receiverState)) + "\",";
  r += "\"stateCode\":" + String((int)receiverState) + ",";
  r += "\"mode\":\"" + String(streamModeName(settings.preferredMode)) + "\",";
  r += "\"streamEnabled\":" + String(settings.streamEnabled ? "true" : "false") + ",";
  r += "\"wifiConnected\":" + String(WiFi.status() == WL_CONNECTED ? "true" : "false") + ",";
  r += "\"ssid\":\"" + jsonEscape(ssid) + "\",";
  r += "\"ip\":\"" + jsonEscape(ip) + "\",";
  r += "\"hostname\":\"" + String(MDNS_HOSTNAME) + ".local\",";
  r += "\"rssi\":" + String(rssi) + ",";
  r += "\"host\":\"" + jsonEscape(settings.phoneHost) + "\",";
  r += "\"tcpPort\":" + String(settings.tcpPort) + ",";
  r += "\"httpPort\":" + String(settings.httpPort) + ",";
  r += "\"formatValid\":" + String(streamFormat.valid ? "true" : "false") + ",";
  r += "\"sampleRate\":" + String(streamFormat.valid ? streamFormat.sampleRate : 0) + ",";
  r += "\"channels\":" + String(streamFormat.valid ? streamFormat.channels : 0) + ",";
  r += "\"bits\":" + String(streamFormat.valid ? streamFormat.bitsPerSample : 0) + ",";
  r += "\"volume\":" + String(settings.volumePercent) + ",";
  r += "\"bufferBytes\":" + String((uint32_t)ringSize()) + ",";
  r += "\"bufferTotalBytes\":" + String((uint32_t)actualRingBytes) + ",";
  r += "\"bufferPercent\":" + String(ringPercent()) + ",";
  r += "\"bytesReceived\":" + String(stats.bytesReceived) + ",";
  r += "\"bytesPlayed\":" + String(stats.bytesPlayed) + ",";
  r += "\"underruns\":" + String(stats.underruns) + ",";
  r += "\"reconnects\":" + String(stats.reconnects) + ",";
  r += "\"streamErrors\":" + String(stats.streamErrors) + ",";
  r += "\"httpStatus\":" + String(stats.lastHttpStatus) + ",";
  r += "\"sessionSeconds\":" + String(sess) + ",";
  r += "\"heap\":" + String(ESP.getFreeHeap()) + ",";
  r += "\"psramTotal\":" + String(ESP.getPsramSize()) + ",";
  r += "\"psramFree\":" + String(ESP.getFreePsram()) + ",";
  r += "\"cpuTemp\":" + String(temperatureRead(), 1) + ",";
  r += "\"flashSize\":" + String(ESP.getFlashChipSize()) + ",";
  r += "\"oled\":false,";
  r += "\"lastError\":\"" + jsonEscape(stats.lastError) + "\"";
  r += "}";
  return r;
}

static String makeConfigJson() {
  String r; r.reserve(600);
  r += "{";
  r += "\"host\":\"" + jsonEscape(settings.phoneHost) + "\",";
  r += "\"tcpPort\":" + String(settings.tcpPort) + ",";
  r += "\"httpPort\":" + String(settings.httpPort) + ",";
  r += "\"mode\":\"" + String(settings.preferredMode == STREAM_MODE_HTTP ? "http" : "tcp") + "\",";
  r += "\"autoFallback\":" + String(settings.autoFallback ? "true" : "false") + ",";
  r += "\"autoReconnect\":" + String(settings.autoReconnect ? "true" : "false") + ",";
  r += "\"oled\":false,";
  r += "\"streamEnabled\":" + String(settings.streamEnabled ? "true" : "false") + ",";
  r += "\"bufferMs\":" + String(settings.targetBufferMs) + ",";
  r += "\"volume\":" + String(settings.volumePercent) + ",";
  r += "\"i2s\":{\"bclk\":" + String(I2S_BCLK_PIN) + ",\"lrclk\":" + String(I2S_LRCLK_PIN) + ",\"dout\":" + String(I2S_DOUT_PIN) + "}";
  r += "}";  
  return r;
}

static void sendJson(int code, const String &body) { 
  server.sendHeader("Cache-Control", "no-store,no-cache,must-revalidate,max-age=0"); 
  server.sendHeader("Pragma", "no-cache"); 
  server.send(code, "application/json", body); 
}

static bool requirePost() { 
  if (server.method() != HTTP_POST) { 
    sendJson(405, "{\"ok\":false,\"error\":\"POST required\"}"); 
    return false; 
  } 
  return true; 
}

// 100% PRESERVED UI/UX: EXACT STYLING, HIGHLIGHTED SLIDERS & TABS
static String htmlPage() {
  return R"HTML(<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#000000">
<title>S3 Music Receiver</title>
<style>
:root{--bg:#000;--surface:#090909;--surface2:#101010;--text:#f5f5f5;--muted:#929292;--line:rgba(255,255,255,.085);--line2:rgba(255,255,255,.15);--accent:#9fc5ff;--good:#b9f6c5;--bad:#ffb4ab;--radius:22px}
*{box-sizing:border-box}html{background:#000;color-scheme:dark}
body{margin:0;background:#000;color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;-webkit-font-smoothing:antialiased;padding-bottom:94px}
button,input,select{font:inherit}button{touch-action:manipulation}

.small{font-size:12px;color:var(--muted);line-height:1.45}
.chip{display:inline-flex;align-items:center;gap:7px;padding:7px 11px;border:1px solid var(--line2);border-radius:999px;background:var(--surface2);font-size:10px;font-weight:800;letter-spacing:.055em;text-transform:uppercase;color:#ddd;white-space:nowrap}
.chip:before{content:"";width:6px;height:6px;border-radius:50%;background:#777}.chip.ok{color:var(--good);border-color:rgba(185,246,197,.18)}.chip.ok:before{background:var(--good)}.chip.bad{color:var(--bad);border-color:rgba(255,180,171,.2)}.chip.bad:before{background:var(--bad)}.chip.connecting{color:var(--accent);border-color:rgba(159,197,255,.2)}.chip.connecting:before{background:var(--accent)}
main{max-width:760px;margin:auto;padding:18px 14px}.tab{display:none}.tab.active{display:block;animation:tabIn .2s cubic-bezier(.22,.61,.36,1)}.tab.active.from-left{animation-name:tabInLeft}.tab.active.from-right{animation-name:tabInRight}
@keyframes tabIn{from{opacity:.7;transform:translateY(4px)}to{opacity:1;transform:none}}@keyframes tabInLeft{from{opacity:.65;transform:translateX(-24px)}to{opacity:1;transform:none}}@keyframes tabInRight{from{opacity:.65;transform:translateX(24px)}to{opacity:1;transform:none}}
.card{background:linear-gradient(180deg,#0b0b0b,#070707);border:1px solid rgba(255,255,255,.18);border-radius:var(--radius);padding:18px;margin-bottom:12px;box-shadow:0 0 0 1px rgba(255,255,255,.10),0 0 22px rgba(255,255,255,.085),0 12px 28px rgba(0,0,0,.28),inset 0 1px 0 rgba(255,255,255,.06);position:relative;overflow:hidden}.card:before{content:"";position:absolute;inset:0;border-radius:inherit;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.022),transparent 42%,rgba(255,255,255,.008));}
.card.tight{padding:14px}.section-title{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:13px}
h2{font-size:17px;margin:0;font-weight:720;letter-spacing:-.015em}h3{font-size:14px;margin:0;font-weight:680}
.hero{font-size:31px;font-weight:800;letter-spacing:-.04em;margin:5px 0 7px;line-height:1.08}.hero::first-letter{}.muted{color:var(--muted)}
.format-line{font-size:13px;color:var(--muted);font-weight:600;letter-spacing:.01em}.info-strip{display:flex;justify-content:space-between;align-items:center;margin-top:16px;padding:11px 13px;border:1px solid var(--line);border-radius:14px;background:#090909;font-size:12px;color:var(--muted)}.info-strip strong{color:#e9e9e9;font-size:13px}.row{display:flex;justify-content:space-between;align-items:center;gap:14px;min-height:40px;border-bottom:1px solid var(--line);padding:8px 0}.row:last-child{border-bottom:0}
.label{color:var(--muted)}.value{text-align:right;max-width:62%;overflow-wrap:anywhere}
.meter{height:6px;background:#171717;border-radius:99px;overflow:hidden;margin:16px 0 8px}.meter i{display:block;height:100%;width:0;background:#fff;border-radius:inherit;transition:none}
.buttons{display:flex;flex-wrap:wrap;gap:9px;margin-top:14px}
button.action{min-height:44px;border:1px solid transparent;border-radius:14px;padding:10px 15px;background:#fff;color:#000;font-weight:750;cursor:pointer;transition:transform .16s ease,background .16s ease,border-color .16s ease,box-shadow .16s ease}
button.action:hover{box-shadow:0 0 0 1px rgba(255,255,255,.12),0 8px 22px rgba(255,255,255,.05)}button.action:active{transform:scale(.97)}
button.secondary{background:#0d0d0d;color:#eee;border-color:var(--line2)}button.danger{background:#160b0a;color:#ffb4ab;border-color:rgba(255,180,171,.25)}button:disabled{opacity:.48;cursor:not-allowed}
.field{margin:16px 0}.field label{display:block;color:var(--muted);font-size:12px;margin:0 0 8px 2px}
input[type=text],input[type=number]{width:100%;height:46px;border:1px solid var(--line2);outline:none;background:#090909;color:var(--text);border-radius:14px;padding:0 13px;transition:border-color .16s ease,box-shadow .16s ease}input[type=text]:focus,input[type=number]:focus{border-color:rgba(255,255,255,.35);box-shadow:0 0 0 3px rgba(255,255,255,.06)}
input[type=range]{--volume:80%;width:100%;height:34px;margin:2px 0;appearance:none;background:transparent;accent-color:#fff;cursor:pointer}
input[type=range]::-webkit-slider-runnable-track{height:7px;background:linear-gradient(90deg,rgba(255,255,255,.82) 0,var(--volume),rgba(255,255,255,.13) var(--volume),rgba(255,255,255,.13) 100%);border:1px solid rgba(255,255,255,.09);border-radius:99px;box-shadow:inset 0 1px 2px rgba(0,0,0,.55),0 0 8px rgba(255,255,255,.035)}
input[type=range]::-webkit-slider-thumb{appearance:none;width:24px;height:24px;border-radius:50%;background:#fff;margin-top:-9.5px;border:2px solid #000;box-shadow:0 2px 12px rgba(255,255,255,.18),0 0 0 3px rgba(255,255,255,.04);transition:transform .12s ease,box-shadow .12s ease}
input[type=range]:active::-webkit-slider-thumb{transform:scale(1.08);box-shadow:0 2px 14px rgba(255,255,255,.22),0 0 0 4px rgba(255,255,255,.035)}
input[type=range]::-moz-range-track{height:5px;background:linear-gradient(90deg,rgba(255,255,255,.72) 0,var(--volume),#252525 var(--volume),100%);border-radius:99px}input[type=range]::-moz-range-progress{height:5px;background:rgba(255,255,255,.72);border-radius:99px}input[type=range]::-moz-range-thumb{width:22px;height:22px;border-radius:50%;background:#fff;border:2px solid #000;box-shadow:0 2px 12px rgba(255,255,255,.16)}
.volume-row{display:flex;align-items:center;gap:12px}.volume-row input{flex:1;min-width:0}.mute-btn{flex:0 0 auto;height:40px;padding:0 13px;border:1px solid var(--line2);border-radius:12px;background:#101010;color:#d8d8d8;font-size:12px;font-weight:750;cursor:pointer;transition:background .16s ease,border-color .16s ease,transform .12s ease}.mute-btn:hover{border-color:rgba(255,255,255,.22)}.mute-btn:active{transform:scale(.96)}.mute-btn.muted{background:#171111;color:#ffb4ab;border-color:rgba(255,180,171,.22)}
.settings-intro{margin:-2px 2px 12px;color:var(--muted);font-size:12px;line-height:1.45}.settings-intro strong{color:#ddd;font-weight:700}
.slider-head{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:7px}.slider-value{font-size:20px;font-weight:800;letter-spacing:-.03em;color:#fff}
.switchrow{display:flex;justify-content:space-between;align-items:center;gap:14px;padding:11px 0;border-bottom:1px solid var(--line)}.switchrow:last-child{border-bottom:0}
.switch{appearance:none;flex:0 0 auto;width:48px;height:28px;border-radius:99px;background:#242424;border:1px solid #3a3a3a;position:relative;outline:none;cursor:pointer;transition:background .18s ease,border-color .18s ease}
.switch:before{content:"";position:absolute;width:20px;height:20px;left:3px;top:3px;background:#858585;border-radius:50%;transition:transform .18s ease,background .18s ease}
.switch:checked{background:#fff;border-color:#fff}.switch:checked:before{transform:translateX(20px);background:#000}
.select-wrap{position:relative}.select-button{width:100%;min-height:48px;padding:0 42px 0 14px;border-radius:15px;border:1px solid var(--line2);background:#0a0a0a;color:var(--text);text-align:left;cursor:pointer;position:relative;transition:border-color .16s ease,background .16s ease}
.select-button:after{content:"";position:absolute;right:16px;top:18px;width:8px;height:8px;border-right:1.5px solid #aaa;border-bottom:1.5px solid #aaa;transform:rotate(45deg);transition:transform .16s ease}
.select-wrap.open .select-button{border-color:rgba(255,255,255,.32);background:#101010}.select-wrap.open .select-button:after{transform:rotate(225deg);top:21px}
.select-menu{position:absolute;left:0;right:0;top:calc(100% + 7px);z-index:50;background:#0d0d0d;border:1px solid var(--line2);border-radius:16px;padding:6px;box-shadow:0 18px 42px rgba(0,0,0,.7);opacity:0;visibility:hidden;transform:translateY(-5px) scale(.99);transition:opacity .14s ease,transform .14s ease,visibility .14s ease}
.select-wrap.open .select-menu{opacity:1;visibility:visible;transform:none}.select-option{width:100%;border:0;background:transparent;color:#ddd;text-align:left;border-radius:11px;padding:12px;cursor:pointer;font-size:14px;min-height:43px}
.select-option:hover{background:#181818}.select-option.selected{background:#fff;color:#000;font-weight:720}.select-native{display:none}
.status-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;margin-top:12px}.stat{background:#090909;border:1px solid var(--line);border-radius:16px;padding:13px;min-height:76px}.stat .k{font-size:11px;color:var(--muted);margin-bottom:7px}.stat .v{font-size:17px;font-weight:760;overflow-wrap:anywhere}
.file{width:100%;padding:12px;border:1px dashed var(--line2);border-radius:15px;background:#070707;color:#bbb}.file::file-selector-button{border:1px solid var(--line2);background:#151515;color:#fff;border-radius:10px;padding:8px 11px;margin-right:9px;font-weight:650}
progress{width:100%;height:8px;accent-color:#fff;margin-top:12px}
nav .nav-inner button{font-size:0;line-height:0;display:flex;align-items:center;justify-content:center;position:relative}nav .nav-inner button svg{width:40px;height:40px;fill:none;stroke:currentColor;stroke-width:1.65;stroke-linecap:round;stroke-linejoin:round}nav .nav-inner button:first-child svg{fill:currentColor;stroke:none;width:36px;height:36px}nav .nav-inner button.active:after{content:"";position:absolute;bottom:5px;left:50%;width:18px;height:2px;border-radius:2px;background:var(--accent);transform:translateX(-50%)} nav{position:fixed;z-index:40;bottom:0;left:0;right:0;display:flex;justify-content:center;gap:6px;padding:8px 10px calc(8px + env(safe-area-inset-bottom));background:rgba(0,0,0,.88);backdrop-filter:blur(18px);border-top:1px solid var(--line)}
nav .nav-inner{width:min(520px,100%);display:flex;gap:6px}nav button{flex:1;min-height:48px;border:0;border-radius:15px;background:transparent;color:#777;font-weight:700;cursor:pointer;transition:background .16s ease,color .16s ease,transform .16s ease}
nav button.active{background:#151515;color:#fff;box-shadow:inset 0 0 0 1px rgba(255,255,255,.07)}nav button:active{transform:scale(.97)}
#toast{position:fixed;z-index:80;left:50%;bottom:88px;transform:translate(-50%,14px);opacity:0;pointer-events:none;background:#f2f2f2;color:#050505;border-radius:14px;padding:11px 15px;font-size:13px;font-weight:700;box-shadow:0 12px 30px rgba(0,0,0,.45);transition:opacity .18s ease,transform .18s ease;max-width:calc(100vw - 28px);text-align:center}
#toast.show{opacity:1;transform:translate(-50%,0)}.hidden{display:none!important}
@media(max-width:520px){main{padding:14px 11px}.card{padding:16px;border-radius:20px}.hero{font-size:27px}.buttons button{flex:1 1 auto}.value{max-width:58%}.volume-row{gap:9px}.mute-btn{padding:0 11px}.info-strip{margin-top:14px}}
@media(prefers-reduced-motion:reduce){*,*:before,*:after{animation-duration:.01ms!important;transition-duration:.01ms!important}}
.settings-icon{display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;color:var(--muted);flex:0 0 20px}.settings-icon svg{width:18px;height:18px;fill:none;stroke:currentColor;stroke-width:1.65;stroke-linecap:round;stroke-linejoin:round}
/* C3MUSIC_UI_REDESIGN_V2 REFINED SLIDER STYLES */
:root{
  --bg:#000;
  --surface:#0a0a0b;
  --surface2:#111113;
  --surface3:#17171a;
  --text:#f6f7f9;
  --muted:#8f9299;
  --line:rgba(255,255,255,.09);
  --line2:rgba(255,255,255,.16);
  --accent:#b9d2ff;
  --good:#bff6cb;
  --bad:#ffb7ae;
  --radius:24px;
}
html{
  background:
    radial-gradient(900px 420px at 50% -140px,rgba(255,255,255,.055),transparent 66%),
    #000;
}
body{
  min-height:100vh;
  background:
    radial-gradient(560px 360px at 100% 0%,rgba(255,255,255,.025),transparent 72%),
    radial-gradient(520px 320px at 0% 28%,rgba(255,255,255,.018),transparent 72%),
    #000;
}
main{
  width:min(820px,100%);
  padding:20px 16px 108px;
}
.tab.active{
  animation:uiFadeIn .22s ease-out;
}
@keyframes uiFadeIn{
  from{opacity:.72;transform:translateY(7px)}
  to{opacity:1;transform:none}
}
.card{
  background:
    linear-gradient(180deg,rgba(255,255,255,.028),rgba(255,255,255,.008)),
    linear-gradient(180deg,#0d0d0f,#070708);
  border:1px solid rgba(255,255,255,.13);
  border-radius:24px;
  padding:20px;
  margin-bottom:14px;
  box-shadow:
    inset 0 1px 0 rgba(255,255,255,.035),
    0 16px 38px rgba(0,0,0,.34);
}
.card.tight{
  padding:16px 17px;
}
.card:before{
  background:linear-gradient(135deg,rgba(255,255,255,.028),transparent 38%,rgba(255,255,255,.006));
}
.section-title{
  margin-bottom:15px;
}
.section-title h2{
  font-size:16px;
  letter-spacing:-.018em;
}
.hero{
  font-size:34px;
  letter-spacing:-.045em;
  line-height:1.02;
  margin:7px 0 8px;
}
.format-line{
  font-size:13px;
  letter-spacing:.012em;
}
.info-strip{
  margin-top:17px;
  padding:12px 14px;
  border-radius:15px;
  background:rgba(255,255,255,.018);
  border-color:var(--line);
}
.info-strip strong{
  font-size:13px;
  letter-spacing:.01em;
}
.status-grid{
  gap:11px;
}
.stat{
  background:rgba(255,255,255,.015);
  border-color:var(--line);
  border-radius:17px;
  padding:14px;
}
.stat .k{
  font-size:10px;
  letter-spacing:.04em;
  text-transform:uppercase;
}
.stat .v{
  font-size:18px;
  letter-spacing:-.02em;
}
.row{
  min-height:42px;
  padding:9px 0;
  border-bottom-color:rgba(255,255,255,.065);
}
.label{
  font-size:12px;
}
.value{
  font-size:12px;
  color:#ddd;
}
button.action{
  min-height:46px;
  border-radius:15px;
  padding:10px 16px;
  background:linear-gradient(180deg,#fff,#ededee);
  box-shadow:0 7px 20px rgba(0,0,0,.28);
}
button.action:hover{
  transform:translateY(-1px);
  box-shadow:0 10px 24px rgba(0,0,0,.34),0 0 0 1px rgba(255,255,255,.08);
}
button.secondary{
  background:linear-gradient(180deg,#141416,#0d0d0e);
  border-color:rgba(255,255,255,.13);
}
button.danger{
  background:linear-gradient(180deg,#1a100f,#120908);
}
.buttons{
  gap:10px;
}
.field{
  margin:18px 0;
}
.field label{
  font-size:11px;
  letter-spacing:.045em;
  text-transform:uppercase;
}
input[type=text],input[type=number]{
  height:48px;
  border-radius:15px;
  background:rgba(255,255,255,.02);
  border-color:rgba(255,255,255,.13);
}
input[type=text]:focus,input[type=number]:focus{
  border-color:rgba(185,210,255,.42);
  box-shadow:0 0 0 4px rgba(185,210,255,.055);
}
input[type=range]{
  height:38px;
}
input[type=range]::-webkit-slider-runnable-track{
  height:7px;
  background:linear-gradient(90deg,#f4f4f4 0,var(--volume),rgba(255,255,255,.12) var(--volume),rgba(255,255,255,.12) 100%);
}
input[type=range]::-webkit-slider-thumb{
  width:25px;
  height:25px;
  margin-top:-10px;
  border:2px solid #050505;
  box-shadow:0 3px 15px rgba(0,0,0,.4),0 0 0 3px rgba(255,255,255,.055);
}
input[type=range]:active::-webkit-slider-thumb{
  transform:scale(1.1);
}
.mute-btn{
  height:42px;
  min-width:78px;
  border-radius:13px;
  background:#111113;
  border-color:rgba(255,255,255,.13);
}
.mute-btn.muted{
  background:#17100f;
}
.switchrow{
  min-height:56px;
  padding:12px 0;
}
.switchrow>span{
  font-size:13px;
  color:#e6e6e8;
}
.switch{
  width:50px;
  height:29px;
  background:#252528;
  border-color:#3a3a3e;
}
.switch:before{
  width:21px;
  height:21px;
}
.switch:checked:before{
  transform:translateX(21px);
}
.select-button{
  min-height:50px;
  border-radius:15px;
  background:rgba(255,255,255,.018);
}
.select-menu{
  border-radius:17px;
  padding:7px;
  background:#101012;
  border-color:rgba(255,255,255,.13);
  box-shadow:0 20px 50px rgba(0,0,0,.72);
}
.select-option{
  min-height:45px;
  border-radius:12px;
}
.select-option.selected{
  box-shadow:inset 0 0 0 1px rgba(0,0,0,.08);
}
.settings-intro{
  margin:0 3px 14px;
  font-size:11px;
  letter-spacing:.02em;
}
.settings-intro strong{
  color:#f2f2f4;
}
.file{
  padding:13px;
  border-radius:15px;
  background:rgba(255,255,255,.018);
}
progress{
  height:8px;
  border-radius:99px;
  overflow:hidden;
}
nav{
  padding:9px 11px calc(9px + env(safe-area-inset-bottom));
  background:rgba(4,4,5,.91);
  border-top-color:rgba(255,255,255,.08);
  box-shadow:0 -10px 32px rgba(0,0,0,.34);
  backdrop-filter:blur(22px) saturate(120%);
}
nav .nav-inner{
  width:min(540px,100%);
  gap:7px;
}
nav button{
  min-height:50px;
  border-radius:16px;
}
nav button.active{
  background:linear-gradient(180deg,#18181b,#111113);
  color:#fff;
  box-shadow:
    inset 0 0 0 1px rgba(255,255,255,.075),
    0 6px 18px rgba(0,0,0,.22);
}
nav .nav-inner button.active:after{
  bottom:5px;
  width:20px;
  height:2px;
  background:#c4d7ff;
  box-shadow:0 0 10px rgba(196,215,255,.25);
}
#toast{
  bottom:96px;
  border:1px solid rgba(255,255,255,.12);
  background:rgba(241,241,243,.97);
  box-shadow:0 16px 34px rgba(0,0,0,.48);
  backdrop-filter:blur(14px);
}
.small{
  font-size:11px;
}
/* MATCH_NOW_PLAYING_TYPOGRAPHY */
.tab#wifi,.tab#settings{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.tab#wifi .section-title h2,.tab#settings .section-title h2{font-size:16px;line-height:1.2;letter-spacing:-.018em;font-weight:720}
.tab#wifi .label,.tab#settings .label{font-size:9px;line-height:1.2;letter-spacing:.16em;color:#747984;font-weight:800;text-transform:uppercase}
.tab#wifi .value,.tab#settings .value{font-size:12px;line-height:1.3;letter-spacing:0;color:#ddd;font-weight:400}
.tab#settings .settings-intro{font-size:10px;line-height:1.4;letter-spacing:.12em;color:#666b76}
.tab#settings .settings-intro strong{color:#f2f2f4;font-weight:800}
.tab#settings .field label{font-size:9px;line-height:1.2;letter-spacing:.16em;color:#747984;font-weight:800;text-transform:uppercase}
.tab#settings .select-button,.tab#settings .select-option,.tab#settings .switchrow>span,.tab#settings input[type=text],.tab#settings input[type=number],.tab#settings .file{font-size:12px;line-height:1.3;letter-spacing:0;font-weight:400}
.tab#wifi button.action,.tab#settings button.action{font-size:9px;line-height:1.2;letter-spacing:.13em;font-weight:850;text-transform:uppercase}
.tab#settings .small{font-size:10px;line-height:1.4;letter-spacing:0;color:#5f636d}
@media(min-width:700px){
  main{padding-left:20px;padding-right:20px}
  .card{padding:22px}
  .hero{font-size:37px}
}
@media(max-width:520px){
  main{padding:14px 11px 102px}
  .card{padding:17px;border-radius:21px}
  .card.tight{padding:15px}
  .hero{font-size:29px}
  .section-title{margin-bottom:13px}
}
@media(prefers-reduced-motion:reduce){
  .tab.active{animation:none}
  button.action:hover{transform:none}
}
/* NOW_PLAYING_CONTROL_DECK */
.deck-card{padding:22px;background:radial-gradient(520px 220px at 50% -40px,rgba(196,215,255,.09),transparent 70%),linear-gradient(180deg,#111216,#090a0c);border-color:rgba(255,255,255,.12);box-shadow:0 18px 46px rgba(0,0,0,.3),inset 0 1px 0 rgba(255,255,255,.045)}
.deck-top{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}.deck-kicker{font-size:11px;letter-spacing:.2em;font-weight:850;color:#c9d6ee}.deck-sub{font-size:10px;letter-spacing:.12em;color:#666b76;margin-top:5px}.deck-status{margin-top:1px}
.deck-state{font-size:clamp(31px,7vw,48px);font-weight:850;letter-spacing:-.055em;margin:30px 0 22px;line-height:1;color:#fff}
.format-specs{display:grid;grid-template-columns:1fr 1fr 1.3fr;gap:8px}.format-spec{min-height:68px;border:1px solid rgba(255,255,255,.09);background:rgba(255,255,255,.025);border-radius:17px;padding:12px 10px;display:flex;flex-direction:column;align-items:center;justify-content:center}.format-spec strong{font-size:20px;line-height:1;font-weight:850;letter-spacing:-.035em}.format-spec span{display:block;font-size:9px;line-height:1.2;letter-spacing:.14em;color:#747984;font-weight:800;text-transform:uppercase;margin-top:7px}
.deck-transport{display:flex;align-items:center;justify-content:center;gap:9px;color:#8d929d;font-size:11px;margin:16px 0 18px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.deck-transport span:nth-child(2){color:#4f535c}
.control-deck{display:grid;grid-template-columns:1fr 1fr 1fr;gap:9px}.control-btn{min-height:82px;border:1px solid rgba(255,255,255,.09);border-radius:19px;background:#0d0e10;color:#b8bdc8;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:7px;cursor:pointer;transition:transform .16s ease,background .16s ease,border-color .16s ease,box-shadow .16s ease}.control-btn:hover{transform:translateY(-1px);background:#14161a;border-color:rgba(255,255,255,.18)}.control-btn:active{transform:scale(.97)}.control-icon{font-size:23px;line-height:1;color:#e5e9f0}.control-btn span:last-child{font-size:9px;letter-spacing:.13em;font-weight:850}.control-btn.start{background:linear-gradient(180deg,#171a20,#0d0f12);border-color:rgba(196,215,255,.25);box-shadow:inset 0 0 20px rgba(196,215,255,.035)}.control-btn.start .control-icon{color:#d2e0ff}.control-btn.reconnect .control-icon{font-size:28px}
.status-strip{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-top:13px;padding:11px 12px;border-radius:14px;background:rgba(0,0,0,.25);border:1px solid rgba(255,255,255,.06);color:#747985;font-size:9px;letter-spacing:.05em}.status-strip span{display:flex;align-items:center;gap:6px;min-width:0}.status-strip b{color:#b7bcc6;font-size:9px;font-weight:750;white-space:nowrap}.status-strip i{width:6px;height:6px;border-radius:50%;background:#666;display:inline-block;box-shadow:0 0 8px rgba(255,255,255,.08)}.status-strip i.good{background:#bff6cb;box-shadow:0 0 9px rgba(191,246,203,.45)}.status-strip i.bad{background:#ffb7ae;box-shadow:0 0 9px rgba(255,183,174,.35)}
.volume-deck{margin-top:12px}.volume-deck .slider-head h3,.stream-health-card .section-title h2{font-size:9px;letter-spacing:.16em;color:#747984;font-weight:800;text-transform:uppercase}.mini-tile-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px;margin-bottom:14px}.mini-tile{margin:0;min-height:92px}.mini-label{font-size:9px;letter-spacing:.16em;color:#747984;font-weight:800}.mini-value{font-size:18px;font-weight:820;letter-spacing:-.03em;margin-top:9px}.mini-note{font-size:10px;color:#5f636d;margin-top:4px}
@media(max-width:520px){.deck-card{padding:19px}.deck-state{margin:26px 0 19px}.format-spec{min-height:63px}.format-spec strong{font-size:20px}.control-btn{min-height:76px}.status-strip{font-size:8px;padding:10px 9px}.status-strip b{font-size:8px}.mini-tile{min-height:86px}}
@media(max-width:390px){.status-strip{gap:5px}.status-strip span{gap:4px}.status-strip span:last-child{display:none}}
/* END NOW_PLAYING_CONTROL_DECK */
</style>
</head>
<body>
<main>
<section class="tab active" id="now">
<div class="card deck-card">
  <div class="deck-top"><div><div class="deck-kicker">NOW PLAYING</div><div class="deck-sub">S3 MUSIC RECEIVER</div></div><span class="chip deck-status" id="deviceStatus">Loading</span></div>
  <div class="deck-state" id="state">Connecting…</div>
  <div class="format-specs" aria-label="Current audio format">
    <div class="format-spec"><strong id="formatBits">—</strong><span>BIT</span></div>
    <div class="format-spec"><strong id="formatRate">—</strong><span>KHZ</span></div>
    <div class="format-spec wide"><strong id="formatChannels">—</strong><span>CHANNELS</span></div>
  </div>
  <div class="deck-transport"><span id="mode">—</span><span>•</span><span id="host">—</span></div>
  <div class="control-deck">
    <button class="control-btn start" onclick="act('/api/stream/start')"><span class="control-icon">▶</span><span>START</span></button>
    <button class="control-btn stop" onclick="act('/api/stream/stop')"><span class="control-icon">■</span><span>STOP</span></button>
    <button class="control-btn reconnect" onclick="act('/api/stream/reconnect')"><span class="control-icon">↻</span><span>RECONNECT</span></button>
  </div>
  <div class="status-strip"><span><i id="connectionDot"></i><b id="connectionStatus">Checking connection</b></span><span><i id="bufferDot"></i><b id="bufferHealth">Buffer —</b></span><span>LATENCY <b id="latencyText">15 ms</b></span></div>
</div>
<div class="card tight volume-deck">
  <div class="slider-head"><h3>Volume</h3><span class="slider-value" id="volumeText">80%</span></div>
  <div class="volume-row"><input id="volumeInput" type="range" min="0" max="100" value="80" oninput="volumePreview(this.value)" onchange="setVolume(this.value)"><button type="button" class="mute-btn" id="muteBtn" onclick="toggleMute()">Mute</button></div>
</div>
<div class="mini-tile-grid">
  <div class="card mini-tile"><div class="mini-label">BUFFER</div><div class="mini-value" id="bufferText">—</div><div class="mini-note">PSRAM ring</div></div>
  <div class="card mini-tile"><div class="mini-label">SESSION</div><div class="mini-value" id="session">—</div><div class="mini-note">Current stream</div></div>
</div>
<div class="card stream-health-card">
  <div class="section-title"><h2>Stream health</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M3 12h4l2-4 3 8 2-4h7"/></svg></span></div>
  <div class="status-grid"><div class="stat"><div class="k">Underruns</div><div class="v" id="underruns">0</div></div><div class="stat"><div class="k">Reconnects</div><div class="v" id="reconnects">0</div></div></div>
  <div class="row" style="margin-top:8px"><span class="label">Last error</span><span class="value" id="lastError">None</span></div>
</div>
</section>
<section class="tab" id="wifi"><div class="card"><div class="section-title"><h2>Wi‑Fi</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M4 9.5a12.5 12.5 0 0 1 16 0M7.2 13a7.5 7.5 0 0 1 9.6 0M10.3 16.2a2.8 2.8 0 0 1 3.4 0M12 19h.01"/></svg></span></div><div class="row"><span class="label">Status</span><span class="value" id="wifiStatus">—</span></div><div class="row"><span class="label">SSID</span><span class="value" id="ssid">—</span></div><div class="row"><span class="label">IP</span><span class="value" id="ip">—</span></div><div class="row"><span class="label">Hostname</span><span class="value">s3music.local</span></div><div class="row"><span class="label">Signal</span><span class="value" id="rssi">—</span></div><div class="buttons"><button class="action" onclick="act('/api/wifi/reconnect')">Reconnect Wi‑Fi</button></div></div></section>
<section class="tab" id="settings">
<div class="settings-intro"><strong>Settings</strong> · Connection, hardware, firmware and system controls</div>
<div class="card"><div class="section-title"><h2>Stream settings</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M7 17.5 3.5 14a3.5 3.5 0 0 1 0-5l2-2a3.5 3.5 0 0 1 5 0l1.5 1.5M17 6.5 20.5 10a3.5 3.5 0 0 1 0 5l-2 2a3.5 3.5 0 0 1-5 0L12 15.5M8.5 15.5l7-7"/></svg></span></div><div class="field"><label>Phone host / IP</label><input id="hostInput" type="text" autocomplete="off"></div>
<div class="field"><label>Preferred stream</label><div class="select-wrap" id="modeSelectWrap"><button type="button" class="select-button" id="modeSelectButton">Raw TCP PCM (24-bit 96k) — port 50005</button><div class="select-menu" role="listbox"><button type="button" class="select-option selected" data-value="tcp">Raw TCP PCM — port 50005</button><button type="button" class="select-option" data-value="http">HTTP WAV/PCM — port 8080</button></div><select id="modeInput" class="select-native" aria-hidden="true" tabindex="-1"><option value="tcp">Raw TCP PCM — port 50005</option><option value="http">HTTP WAV/PCM — port 8080</option></select></div></div>
<div class="field"><label>TCP port</label><input id="tcpInput" type="number" min="1" max="65535"></div><div class="field"><label>HTTP port</label><input id="httpInput" type="number" min="1" max="65535"></div><div class="field"><label>Target buffer (ms)</label><div class="select-wrap" id="bufferSelectWrap"><button type="button" class="select-button" id="bufferSelectButton">15 ms</button><div class="select-menu" role="listbox"><button type="button" class="select-option selected" data-value="15">15 ms</button><button type="button" class="select-option" data-value="50">50 ms</button><button type="button" class="select-option" data-value="150">150 ms</button><button type="button" class="select-option" data-value="300">300 ms</button></div><select id="bufferInput" class="select-native" aria-hidden="true" tabindex="-1"><option value="15">15 ms</option><option value="50">50 ms</option><option value="150">150 ms</option><option value="300">300 ms</option></select></div></div>
<div class="switchrow"><span>Automatic TCP/HTTP fallback</span><input class="switch" id="fallbackInput" type="checkbox"></div><div class="switchrow"><span>Automatic stream reconnect</span><input class="switch" id="autoreconnectInput" type="checkbox"></div><div class="buttons"><button class="action" onclick="saveCfg()">Save and reconnect</button></div></div>
<div class="card"><div class="section-title"><h2>Hardware & Memory</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><rect x="6" y="4" width="12" height="16" rx="2"/><path d="M9 7h6M9 17h6"/></svg></span></div><div class="row"><span class="label">SoC Architecture</span><span class="value">ESP32-S3 Dual-Core 240MHz</span></div><div class="row"><span class="label">Octal PSRAM</span><span class="value" id="psram">—</span></div><div class="row"><span class="label">Free Internal Heap</span><span class="value" id="heap">—</span></div><div class="row"><span class="label">Firmware</span><span class="value" id="version">—</span></div><div class="row"><span class="label">Audio Pipeline</span><span class="value">Core 1 DMA (Headless Hi-Fi)</span></div><div class="row"><span class="label">CPU temperature</span><span class="value" id="cpuTemp">—</span></div></div>
<div class="card"><div class="section-title"><h2>Firmware OTA</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M12 16V4M7.5 8.5 12 4l4.5 4.5M6 15v4h12v-4"/></svg></span></div><p class="small">Select a .bin file. Streaming will stop during update.</p><input id="firmware" class="file" type="file" accept=".bin"><div class="buttons"><button class="action" id="otaBtn" onclick="uploadFw()">Install firmware</button></div><progress id="otaProg" class="hidden" value="0" max="100"></progress><div class="small" id="otaTxt"></div></div>
<div class="card"><div class="section-title"><h2>Maintenance</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M12 8.7a3.3 3.3 0 1 0 0 6.6 3.3 3.3 0 0 0 0-6.6Z"/><path d="m19 13.4 1.3 1-1.8 3.1-1.6-.7a7.5 7.5 0 0 1-2.5 1.4L14.1 20h-3.6l-.3-1.8a7.5 7.5 0 0 1-2.5-1.4l-1.6.7-1.8-3.1 1.3-1a7.4 7.4 0 0 1 0-2.8l-1.3-1 1.8-3.1 1.6.7a7.5 7.5 0 0 1 2.5-1.4L10.5 4h3.6l.3 1.8a7.5 7.5 0 0 1 2.5 1.4l1.6-.7 1.8 3.1-1.3 1a7.4 7.4 0 0 1 0 2.8Z"/></svg></span></div><div class="buttons"><button class="action secondary" onclick="act('/api/system/clear-stats')">Clear stats</button><button class="action secondary" onclick="act('/api/system/reboot')">Restart</button><button class="action danger" onclick="if(confirm('Factory reset?'))act('/api/system/factory-reset')">Factory reset</button></div></div>
</section></main>
<nav><div class="nav-inner"><button class="active" data-tab="now" aria-label="Now Playing" title="Now Playing"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 6.5v11l8.5-5.5z"/></svg></button><button data-tab="wifi" aria-label="Wi-Fi" title="Wi-Fi"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9.5a12.5 12.5 0 0 1 16 0M7.2 13a7.5 7.5 0 0 1 9.6 0M10.3 16.2a2.8 2.8 0 0 1 3.4 0M12 19h.01"/></svg></button><button data-tab="settings" aria-label="Settings" title="Settings"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 8.7a3.3 3.3 0 1 0 0 6.6 3.3 3.3 0 0 0 0-6.6Z"/><path d="M19 13.4a7.4 7.4 0 0 0 0-2.8l1.5-1.1-1.8-3.1-1.8.7a7.5 7.5 0 0 0-2.4-1.4L14.3 4h-3.6l-.3 1.7A7.5 7.5 0 0 0 8 7.1l-1.8-.7-1.8 3.1L6 10.6a7.4 7.4 0 0 0 0 2.8l-1.5 1.1 1.8 3.1 1.8-.7a7.5 7.5 0 0 0 2.4 1.4l.3 1.7h3.6l.3-1.7a7.5 7.5 0 0 0 2.4-1.4l1.8.7 1.8-3.1Z"/></svg></button></div></nav><div id="toast"></div>
<script>
const $=id=>document.getElementById(id);let cfgLoaded=false;let lastVolume=80;
function toast(t){const x=$('toast');x.textContent=t;x.classList.add('show');clearTimeout(window.__toastTimer);window.__toastTimer=setTimeout(()=>x.classList.remove('show'),2500)}
function text(id,v){const e=$(id);if(e)e.textContent=v}
function time(s){s=Number(s||0);const h=Math.floor(s/3600),m=Math.floor(s%3600/60),q=s%60;return[h,m,q].map(x=>String(x).padStart(2,'0')).join(':')}
function updateVolumeUI(v){const n=Math.max(0,Math.min(100,Number(v)||0));const input=$('volumeInput');if(input)input.style.setProperty('--volume',n+'%');text('volumeText',n+'%');const b=$('muteBtn');if(b){const muted=n===0;b.textContent=muted?'Unmute':'Mute';b.classList.toggle('muted',muted)}}
let volumeTimer=0;let volumeChanging=false;let volumeRequest=0;
function volumePreview(v){const n=Math.max(0,Math.min(100,Number(v)||0));if(n>0)lastVolume=n;updateVolumeUI(n);volumeChanging=true;clearTimeout(volumeTimer);volumeTimer=setTimeout(()=>sendVolume(n,false),40)}
function toggleMute(){const input=$('volumeInput');if(!input)return;const current=Number(input.value)||0;const target=current>0?0:(lastVolume>0?lastVolume:80);input.value=target;updateVolumeUI(target);setVolume(target)}
async function sendVolume(v,commit){const n=Math.max(0,Math.min(100,Number(v)||0));const requestId=++volumeRequest;try{const body=new URLSearchParams({value:String(n),commit:commit?'1':'0'});const r=await fetch('/api/volume',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body});const d=await r.json();if(commit&&requestId===volumeRequest)toast(d.ok?'Volume '+n+'%':(d.error||'Volume change failed'))}catch(e){if(commit&&requestId===volumeRequest)toast('Volume change failed')}}
function setVolume(v){const n=Math.max(0,Math.min(100,Number(v)||0));if(n>0)lastVolume=n;updateVolumeUI(n);volumeChanging=false;clearTimeout(volumeTimer);sendVolume(n,true)}
function setupDropdown(){
 const configs=[
  [$('modeSelectWrap'),$('modeSelectButton'),$('modeInput')],
  [$('bufferSelectWrap'),$('bufferSelectButton'),$('bufferInput')]
 ];
 configs.forEach(([wrap,btn,native])=>{if(!wrap||!btn||!native)return;const opts=Array.from(wrap.querySelectorAll('.select-option'));function sync(){const selected=opts.find(o=>o.dataset.value===native.value)||opts[0];opts.forEach(o=>o.classList.toggle('selected',o===selected));btn.textContent=selected.textContent}opts.forEach(o=>o.onclick=()=>{native.value=o.dataset.value;sync();wrap.classList.remove('open')});btn.onclick=e=>{e.stopPropagation();wrap.classList.toggle('open')};document.addEventListener('click',e=>{if(!wrap.contains(e.target))wrap.classList.remove('open')});sync()})
}
function apply(d){
 text('state',d.state||'—');text('deviceStatus',d.state||'—');
 const formatActive=!!d.formatValid && (d.state==='Buffering'||d.state==='Streaming');
 const bitsText=formatActive?((d.bits||'—')+' BIT'):'—';const rateText=formatActive?(((Number(d.sampleRate||0)/1000).toFixed(Number(d.sampleRate||0)%1000===0?0:1))+' KHZ'):'—';const channelsText=formatActive?(d.channels===2?'STEREO':'MONO'):'—';
 text('format',formatActive?((d.sampleRate||0)+' Hz · '+(d.bits||0)+'-bit · '+(d.channels===2?'Stereo':'Mono')):'—');text('formatBits',bitsText);text('formatRate',rateText);text('formatChannels',channelsText);
 text('mode',d.mode||'—');text('host',(d.host||'—')+' · '+(d.mode==='HTTP WAV'?d.httpPort:d.tcpPort));text('session',time(d.sessionSeconds));text('underruns',d.underruns||0);text('reconnects',d.reconnects||0);text('lastError',d.lastError||'None');
 const connected=!!d.wifiConnected;const streaming=d.state==='Streaming';const bufferPct=Number(d.bufferPercent||0);text('connectionStatus',connected?(streaming?'STREAMING':'CONNECTED'):'OFFLINE');text('bufferHealth',bufferPct>0?'BUFFER '+bufferPct+'%':'BUFFER READY');text('latencyText',(Number($('bufferInput')?.value||15))+' ms');const cd=$('connectionDot'),bd=$('bufferDot');if(cd)cd.className=connected?'good':'bad';if(bd)bd.className=bufferPct>0?'good':'bad';
 const vol=Math.max(0,Math.min(100,Number(d.volume??80)||0)),vi=$('volumeInput');if(vi&&document.activeElement!==vi){vi.value=vol;updateVolumeUI(vol)}
 text('wifiStatus',d.wifiConnected?'Connected':'Disconnected');text('ssid',d.ssid||'—');text('ip',d.ip||'—');text('rssi',d.wifiConnected?(d.rssi+' dBm'):'—');text('version',d.version||'—');text('heap',d.heap?(Math.round(d.heap/1024)+' KB'):'—');
 text('psram',d.psramTotal?((d.psramFree?Math.round(d.psramFree/(1024*1024))+'MB free / ':'')+Math.round(d.psramTotal/(1024*1024))+' MB total'):'8 MB Octal');
 text('cpuTemp',d.cpuTemp!==undefined&&d.cpuTemp!==null?(Number(d.cpuTemp).toFixed(1)+' °C'):'—');
 const p=Number(d.bufferPercent||0);text('bufferText',p+'% · '+(d.bufferBytes?Math.round(d.bufferBytes/1024)+' KB':'0 KB'));const c=$('deviceStatus');c.className='chip '+(d.state==='Streaming'?'ok':(d.state==='Stopped'||d.state==='Error'||d.state==='WiFi offline'?'bad':(d.state==='Connecting'?'connecting':'')))
}
let pollBusy=false;let pollTimer=0;async function poll(){if(pollBusy)return;pollBusy=true;try{const r=await fetch('/api/status',{cache:'no-store'});if(!r.ok)throw 0;apply(await r.json());if(!cfgLoaded)await loadCfg()}catch(e){text('state','Web lost');text('deviceStatus','Offline');$('deviceStatus').className='chip bad'}finally{pollBusy=false}}function schedulePoll(){clearTimeout(pollTimer);pollTimer=setTimeout(()=>{poll();schedulePoll()},document.visibilityState==='visible'?1000:1500)}
async function loadCfg(){try{const d=await(await fetch('/api/config',{cache:'no-store'})).json();$('hostInput').value=d.host||'';$('tcpInput').value=d.tcpPort||50005;$('httpInput').value=d.httpPort||8080;$('modeInput').value=d.mode||'tcp';$('bufferInput').value=d.bufferMs||15;$('fallbackInput').checked=!!d.autoFallback;$('autoreconnectInput').checked=!!d.autoReconnect;const v=Math.max(0,Math.min(100,Number(d.volume??80)||0));$('volumeInput').value=v;updateVolumeUI(v);setupDropdown();cfgLoaded=true}catch(e){}}
async function act(url){try{const r=await fetch(url,{method:'POST'}),d=await r.json();toast(d.ok?'Done':(d.error||'Failed'));setTimeout(poll,300)}catch(e){toast('Request failed')}}
async function saveCfg(){const body=new URLSearchParams({host:$('hostInput').value.trim(),tcpPort:$('tcpInput').value,httpPort:$('httpInput').value,bufferMs:$('bufferInput').value,mode:$('modeInput').value,autoFallback:$('fallbackInput').checked?'1':'0',autoReconnect:$('autoreconnectInput').checked?'1':'0',oled:'0'});try{const r=await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body});const d=await r.json();toast(d.ok?'Saved':'Save failed');cfgLoaded=false;setTimeout(poll,500)}catch(e){toast('Save failed')}}
function uploadFw(){const f=$('firmware').files[0];if(!f){toast('Choose .bin first');return}if(!confirm('Install '+f.name+'?'))return;const xhr=new XMLHttpRequest(),form=new FormData();form.append('firmware',f);$('otaProg').classList.remove('hidden');$('otaProg').value=0;$('otaTxt').textContent='Uploading…';$('otaBtn').disabled=true;xhr.upload.onprogress=e=>{if(e.lengthComputable){const p=Math.round(e.loaded/e.total*100);$('otaProg').value=p;$('otaTxt').textContent='Uploading '+p+'%'}};xhr.onload=()=>{$('otaBtn').disabled=false;if(xhr.status===200){$('otaProg').value=100;$('otaTxt').textContent='Update accepted. Restarting…';toast('Firmware update successful')}else{$('otaTxt').textContent='Update failed: '+xhr.responseText;toast('OTA failed')}};xhr.onerror=()=>{$('otaBtn').disabled=false;$('otaTxt').textContent='Upload failed';toast('OTA failed')};xhr.open('POST','/api/ota');xhr.send(form)}
const navButtons=Array.from(document.querySelectorAll('nav button'));const tabs=Array.from(document.querySelectorAll('.tab'));function showTab(index,direction=0){if(index<0||index>=navButtons.length)return;navButtons.forEach(x=>x.classList.remove('active'));tabs.forEach(x=>x.classList.remove('active','from-left','from-right'));const b=navButtons[index],panel=$(b.dataset.tab);b.classList.add('active');if(panel){panel.classList.add('active');if(direction<0)panel.classList.add('from-right');else if(direction>0)panel.classList.add('from-left')}window.scrollTo({top:0,behavior:'smooth'})}navButtons.forEach((b,i)=>b.onclick=()=>showTab(i,0));let swipeStartX=0,swipeStartY=0,swipeTracking=false;document.addEventListener('touchstart',e=>{if(e.touches.length!==1)return;const t=e.touches[0],target=e.target;if(target.closest('nav,button,input,select,textarea,a')){swipeTracking=false;return}swipeStartX=t.clientX;swipeStartY=t.clientY;swipeTracking=true},{passive:true});document.addEventListener('touchend',e=>{if(!swipeTracking||e.changedTouches.length!==1)return;swipeTracking=false;const t=e.changedTouches[0],dx=t.clientX-swipeStartX,dy=t.clientY-swipeStartY;if(Math.abs(dx)<55||Math.abs(dx)<Math.abs(dy)*1.35)return;const current=navButtons.findIndex(b=>b.classList.contains('active'));if(current<0)return;const next=dx<0?current+1:current-1;if(next>=0&&next<navButtons.length)showTab(next,dx<0?-1:1)},{passive:true});poll();schedulePoll();document.addEventListener('visibilitychange',schedulePoll);
</script></body></html>)HTML";
}

static bool otaUploadFailed = false; static String otaUploadError;
static void handleFirmwareUpload() {
  HTTPUpload &up = server.upload();
  if (up.status == UPLOAD_FILE_START) { 
    otaUploadFailed = false; otaUploadError = ""; stopRequested = true; stopStreamClient(); ringClear(); endI2S(); setReceiverState(RX_UPDATING); 
    size_t sz = UPDATE_SIZE_UNKNOWN; 
    if (!Update.begin(sz, U_FLASH)) { 
      otaUploadFailed = true; otaUploadError = Update.errorString(); Serial.printf("[OTA] Begin failed: %s\n", otaUploadError.c_str()); 
    } else Serial.printf("[OTA] Start: %s\n", up.filename.c_str()); 
  }
  else if (up.status == UPLOAD_FILE_WRITE) { 
    if (!otaUploadFailed) { 
      size_t w = Update.write(up.buf, up.currentSize); 
      if (w != up.currentSize) { otaUploadFailed = true; otaUploadError = Update.errorString(); Serial.printf("[OTA] Write failed: %s\n", otaUploadError.c_str()); } 
    } 
  }
  else if (up.status == UPLOAD_FILE_END) { 
    if (!otaUploadFailed) { 
      if (!Update.end(true)) { otaUploadFailed = true; otaUploadError = Update.errorString(); Serial.printf("[OTA] End failed: %s\n", otaUploadError.c_str()); } 
      else Serial.printf("[OTA] Success: %u bytes\n", up.totalSize); 
    } 
  }
  else if (up.status == UPLOAD_FILE_ABORTED) { otaUploadFailed = true; otaUploadError = "Upload aborted"; Update.abort(); }
}

static void setupWebServer() {
  server.on("/", HTTP_GET, []{ 
    server.sendHeader("Cache-Control", "public,max-age=300,must-revalidate"); 
    server.sendHeader("Vary", "Accept-Encoding"); 
    server.send(200, "text/html; charset=utf-8", htmlPage()); 
  });
  server.on("/api/status", HTTP_GET, []{ sendJson(200, makeStatusJson()); });
  server.on("/api/config", HTTP_GET, []{ sendJson(200, makeConfigJson()); });
  server.on("/api/config", HTTP_POST, []{
    if (!requirePost()) return;
    String host = server.arg("host"); host.trim();
    if (host.length() == 0 || host.length() > 63) { sendJson(400, "{\"ok\":false,\"error\":\"Invalid host\"}"); return; }
    uint32_t tp = server.arg("tcpPort").toInt(), hp = server.arg("httpPort").toInt();
    if (tp < 1 || tp > 65535 || hp < 1 || hp > 65535) { sendJson(400, "{\"ok\":false,\"error\":\"Invalid port\"}"); return; }
    settings.phoneHost = host; settings.tcpPort = (uint16_t)tp; settings.httpPort = (uint16_t)hp;
    settings.preferredMode = (server.arg("mode") == "http") ? STREAM_MODE_HTTP : STREAM_MODE_TCP;
    uint32_t bm = server.arg("bufferMs").toInt(); if (bm != 15 && bm != 50 && bm != 150 && bm != 300) { sendJson(400, "{\"ok\":false,\"error\":\"Invalid buffer\"}"); return; }
    settings.autoFallback = (server.arg("autoFallback") == "1"); 
    settings.autoReconnect = (server.arg("autoReconnect") == "1"); 
    settings.oledEnabled = false; 
    settings.targetBufferMs = (uint16_t)bm;
    saveSettings(); reconnectStreaming(); sendJson(200, "{\"ok\":true}");
  });
  server.on("/api/volume", HTTP_POST, []{
    if(!requirePost()) return;
    int v = server.arg("value").toInt();
    if(v < 0 || v > 100) { sendJson(400, "{\"ok\":false,\"error\":\"Invalid volume\"}"); return; }
    settings.volumePercent = (uint8_t)v;
    if(server.arg("commit") == "1") preferences.putUChar("volume", settings.volumePercent);
    sendJson(200, "{\"ok\":true,\"volume\":" + String(settings.volumePercent) + "}");
  });
  server.on("/api/stream/start", HTTP_POST, []{ if(!requirePost()) return; startStreaming(); sendJson(200, "{\"ok\":true}"); });
  server.on("/api/stream/stop", HTTP_POST, []{ if(!requirePost()) return; stopStreaming(); sendJson(200, "{\"ok\":true}"); });
  server.on("/api/stream/reconnect", HTTP_POST, []{ if(!requirePost()) return; reconnectStreaming(); sendJson(200, "{\"ok\":true}"); });
  server.on("/api/wifi/reconnect", HTTP_POST, []{ if(!requirePost()) return; WiFi.disconnect(false, false); stopStreamClient(); mdnsStarted = false; lastWifiAttemptMs = 0; connectWifiIfNeeded(); sendJson(200, "{\"ok\":true}"); });
  server.on("/api/system/clear-stats", HTTP_POST, []{ if(!requirePost()) return; stats = RuntimeStats(); sendJson(200, "{\"ok\":true}"); });
  server.on("/api/system/reboot", HTTP_POST, []{ if(!requirePost()) return; sendJson(200, "{\"ok\":true,\"message\":\"Restarting\"}"); delay(250); ESP.restart(); });
  server.on("/api/system/factory-reset", HTTP_POST, []{ if(!requirePost()) return; resetSettings(); sendJson(200, "{\"ok\":true,\"message\":\"Reset; restarting\"}"); delay(250); ESP.restart(); });
  server.on("/api/ota", HTTP_POST, []{ 
    if (otaUploadFailed) { 
      String b = "{\"ok\":false,\"error\":\"" + jsonEscape(otaUploadError) + "\"}"; 
      sendJson(500, b); stopRequested = false; settings.streamEnabled = true; setReceiverState(RX_IDLE); 
    } else { 
      sendJson(200, "{\"ok\":true,\"message\":\"Firmware written; restarting\"}"); delay(700); ESP.restart(); 
    } 
  }, handleFirmwareUpload);
  server.onNotFound([]{ sendJson(404, "{\"ok\":false,\"error\":\"Not found\"}"); });
  server.begin(); Serial.println("[WEB] HTTP server started on port 80");
}

void setup() {
  Serial.begin(115200); delay(400);
  Serial.println(); Serial.println("============================================================");
  Serial.printf("%s v%s (ESP32-S3 N16R8)\n", DEVICE_NAME, FIRMWARE_VERSION);
  Serial.println("Hi-Res 24-bit 96kHz Receiver · Headless (OLED Purged) · 6MB Lock-Free PSRAM");
  Serial.println("Dual-Core FreeRTOS Architecture (Core 0: Network / Core 1: I2S DMA)");
  Serial.println("============================================================");

  // Initialize 6MB Lock-Free PSRAM ring buffer in 8MB Octal PSRAM
  if (psramInit()) {
    Serial.printf("[PSRAM] Initialized successfully. Total PSRAM: %u bytes\n", ESP.getPsramSize());
    audioRing = (uint8_t*)heap_caps_malloc(AUDIO_RING_BYTES, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (audioRing) {
      actualRingBytes = AUDIO_RING_BYTES;
      Serial.printf("[PSRAM] Allocated 6 MB Lock-Free Audio Ring Buffer in Octal PSRAM at %p\n", (void*)audioRing);
    } else {
      actualRingBytes = 4194304; // 4MB fallback
      audioRing = (uint8_t*)heap_caps_malloc(actualRingBytes, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
      Serial.printf("[PSRAM] Allocated 4 MB Audio Ring Buffer in Octal PSRAM at %p\n", (void*)audioRing);
    }
  } else {
    Serial.println("[PSRAM] WARNING: PSRAM init failed! Falling back to 128KB internal SRAM buffer");
    actualRingBytes = 131072;
    audioRing = (uint8_t*)malloc(actualRingBytes);
  }

  loadSettings();
  audioDataSemaphore = xSemaphoreCreateBinary();
  i2sMux = xSemaphoreCreateMutex();
  streamClientMux = xSemaphoreCreateMutex();
  setReceiverState(RX_BOOTING);

  WiFi.persistent(false); WiFi.mode(WIFI_STA); WiFi.setSleep(false); WiFi.setAutoReconnect(true);
  setupWebServer();

  // DUAL-CORE FREERTOS ASSIGNMENT:
  // Core 1: Pinned exclusively to I2S DMA feeder task with highest priority (configMAX_PRIORITIES - 1)
  // Core 0: Pinned to Wi-Fi, stream socket intake, HTTP webserver
  BaseType_t ok1 = xTaskCreatePinnedToCore(playbackTask, "i2sPlayback", 8192, nullptr, configMAX_PRIORITIES - 1, &playbackTaskHandle, 1);
  BaseType_t ok2 = xTaskCreatePinnedToCore(streamTask, "pcmStream", 8192, nullptr, 2, &streamTaskHandle, 0);

  if (ok1 != pdPASS || ok2 != pdPASS) {
    setReceiverState(RX_ERROR, "Task creation failed");
    Serial.println("[RTOS] ERROR: Failed to create pinned tasks on Core 0 / Core 1");
  } else {
    Serial.println("[RTOS] Playback task pinned to Core 1 (Max Priority), Stream task on Core 0");
  }

  connectWifiIfNeeded();
}

void loop() {
  server.handleClient();
  if (WiFi.status() != WL_CONNECTED) {
    mdnsStarted = false;
    if (receiverState != RX_WIFI_OFFLINE && receiverState != RX_WIFI_CONNECTING && receiverState != RX_UPDATING && settings.streamEnabled) {
      Serial.printf("[WIFI] Disconnected (status=%d)\n", (int)WiFi.status());
      setReceiverState(RX_WIFI_OFFLINE);
    }
    connectWifiIfNeeded();
  }
  else beginMdnsIfNeeded();

  if (millis() - lastStatusRefreshMs >= STATUS_REFRESH_MS) {
    lastStatusRefreshMs = millis();
    uint32_t target = targetPrebufferBytes();
    if (receiverState == RX_BUFFERING && bufferStarted && ringSize() >= (target / 2)) {
      setReceiverState(RX_STREAMING);
    }
  }
  delay(2);
}
