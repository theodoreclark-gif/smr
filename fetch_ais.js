const WebSocket = require('ws');
const fs = require('fs');

// Reads your secret API key from GitHub repository secrets
const API_KEY = process.env.AISSTREAM_API_KEY;

// All 13 St. Marys River Section USACE Vessels & MMSIs
const FLEET_MMSIS = [
  "366999398", // NICOLET (Derrick Barge)
  "366999311", // HARVEY (Crane Barge)
  "368926311", // AQUADIGGER (Excavator Barge)
  "366999428", // CHERAW (Tug)
  "338924169", // PEARCE (Tug)
  "338924170", // LAWSON (Tug)
  "338925325", // DETROIT (Tug)
  "366999409", // FREDERICK (Tug)
  "366999314", // WHITEFISH (Tug)
  "366999407", // BUFE (Survey Boat)
  "368389980", // MACKIN (Survey Boat)
  "368926566", // MUNSON (Launch Boat)
  "369970895"  // SAFEBOAT (Launch Boat)
];

// Load existing positions if file exists, or start fresh
let positions = {};
try {
  if (fs.existsSync('fleet_positions.json')) {
    positions = JSON.parse(fs.readFileSync('fleet_positions.json', 'utf8'));
  }
} catch (e) {
  console.log("Creating fresh positions cache.");
}

console.log("Connecting to AISStream.io telemetry stream...");
const ws = new WebSocket("wss://stream.aisstream.io/v0/stream");

ws.on('open', () => {
  console.log("Connected! Subscribing to USACE fleet MMSIs...");
  const subscriptionMessage = {
    APIKey: API_KEY,
    BoundingBoxes: [
      [[45.5, -85.5], [47.2, -83.4]] // St. Marys Corridor, Whitefish Bay & Straits
    ],
    FiltersShipMMSI: FLEET_MMSIS,
    FilterMessageTypes: ["PositionReport"]
  };

  ws.send(JSON.stringify(subscriptionMessage));

  // Listen for 35 seconds to collect fresh position broadcasts, then save & exit
  setTimeout(() => {
    console.log("Saving collected telemetry to fleet_positions.json...");
    fs.writeFileSync('fleet_positions.json', JSON.stringify(positions, null, 2));
    ws.close();
    console.log("Run completed successfully.");
    process.exit(0);
  }, 900000);
});

ws.on('message', (data) => {
  try {
    const parsed = JSON.parse(data);
    if (parsed.MessageType === 'PositionReport') {
      const meta = parsed.MetaData || {};
      const pos = (parsed.Message && parsed.Message.PositionReport) ? parsed.Message.PositionReport : {};
      const mmsi = String(meta.MMSI || pos.UserID);

      if (pos.Latitude && pos.Longitude) {
        positions[mmsi] = {
          lat: pos.Latitude,
          lon: pos.Longitude,
          sog: typeof pos.Sog === 'number' ? pos.Sog : 0.0,
          updated: new Date().toISOString()
        };
        console.log(`Updated fix for MMSI ${mmsi}: Lat ${pos.Latitude}, Lon ${pos.Longitude}, SOG ${pos.Sog}`);
      }
    }
  } catch (err) {
    console.warn("Message parsing warning:", err);
  }
});

ws.on('error', (err) => {
  console.error("WebSocket error:", err);
});
