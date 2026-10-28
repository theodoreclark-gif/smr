const WebSocket = require('ws');
const fs = require('fs');

const API_KEY = process.env.AISSTREAM_API_KEY;

// Only transponding vessels
const FLEET_MMSIS = [
  "338924170", "366999398", "338924169", "366999311",
  "338925325", "366999428", "368926311", "368389980",
  "366999407", "368926566", "369970895"
];

let positions = {};
try {
  if (fs.existsSync('fleet_positions.json')) {
    positions = JSON.parse(fs.readFileSync('fleet_positions.json', 'utf8'));
  }
} catch (e) {
  console.log("Starting fresh positions file.");
}

const ws = new WebSocket("wss://stream.aisstream.io/v0/stream");

ws.on('open', () => {
  console.log("Connected to AISStream. Subscribing to St. Marys Section fleet...");
  ws.send(JSON.stringify({
    APIKey: API_KEY,
    BoundingBoxes: [[[45.5, -85.5], [47.2, -83.4]]],
    FiltersShipMMSI: FLEET_MMSIS,
    FilterMessageTypes: ["PositionReport"]
  }));

  setTimeout(() => {
    fs.writeFileSync('fleet_positions.json', JSON.stringify(positions, null, 2));
    ws.close();
    console.log("Telemetry cycle saved successfully.");
    process.exit(0);
  }, 35000);
});

ws.on('message', (data) => {
  try {
    const parsed = JSON.parse(data);
    if (parsed.MessageType === 'PositionReport') {
      const meta = parsed.MetaData || {};
      const pos = (parsed.Message && parsed.Message.PositionReport) ? parsed.Message.PositionReport : {};
      const mmsi = String(meta.MMSI || pos.UserID);

      if (pos.Latitude && pos.Longitude) {
        // Tag incoming packets as automated AIS source
        positions[mmsi] = {
          ...(positions[mmsi] || {}),
          lat: pos.Latitude,
          lon: pos.Longitude,
          sog: typeof pos.Sog === 'number' ? pos.Sog : 0.0,
          source: "AIS",
          updated: new Date().toISOString()
        };
        console.log(`Updated AIS fix for ${mmsi}: [${pos.Latitude}, ${pos.Longitude}]`);
      }
    }
  } catch (err) {
    console.warn("Parse warning:", err);
  }
});

ws.on('error', (err) => console.error("WebSocket error:", err));
