
const WebSocket = require('ws');
const fs = require('fs');

const API_KEY = process.env.AISSTREAM_API_KEY;

// Only your active vessels with AIS transponders (11 vessels)
const FLEET_MMSIS = [
  "338924170", // LAWSON
  "366999398", // NICOLET
  "338924169", // PEARCE
  "338925325", // DETROIT
  "366999428", // CHERAW
  "368389980", // MACKIN
  "366999407", // BUFE
  "368926566", // MUNSON
  "369970895"  // SAFEBOAT
  // Note: HARVEY, 300 BARGE, AQUADIGGER, and SHADNAW are managed via mobile_updater.html
];

// Load existing positions file so manual updates to non-AIS barges are never wiped out
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
  console.log("Connected! Subscribing to USACE St. Marys Section fleet...");
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
    console.log("Telemetry run completed successfully.");
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
        // Tag incoming packets as automated AIS source with ISO timestamp
        positions[mmsi] = {
          ...(positions[mmsi] || {}),
          lat: pos.Latitude,
          lon: pos.Longitude,
          sog: typeof pos.Sog === 'number' ? pos.Sog : 0.0,
          source: "AIS",
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
