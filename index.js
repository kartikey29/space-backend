const express = require("express");
const cors = require("cors");
const zlib = require("node:zlib");
require("dotenv").config();
const cron = require("node-cron");

const app = express();

app.use(cors());

const {
  EventHubConsumerClient,
  earliestEventPosition,
} = require("@azure/event-hubs");
const { ContainerClient } = require("@azure/storage-blob");
const {
  BlobCheckpointStore,
} = require("@azure/eventhubs-checkpointstore-blob");

const connectionString = process.env.connectionString;
const eventHubName = process.env.eventHubName;
const consumerGroup = process.env.consumerGroup; // name of the default consumer group
const storageConnectionString = process.env.storageConnectionString;
const containerName = process.env.containerName;

app.listen(5000, () => {
  console.log("server started");
});

let containerClient;
let checkpointStore;
let consumerClient;

let subscription;

let eventProcessingStarted = false;

let ProcessedEvents = {};

function main() {
  containerClient = new ContainerClient(storageConnectionString, containerName);
  checkpointStore = new BlobCheckpointStore(containerClient);
  consumerClient = new EventHubConsumerClient(
    consumerGroup,
    connectionString,
    eventHubName,
    checkpointStore
  );

  //   Create a blob container client and a blob checkpoint store using the client.

  // Subscribe to the events, and specify handlers for processing the events and errors.
  subscription = consumerClient.subscribe(
    {
      processEvents: async (events, context) => {
        if (events.length === 0) {
          console.log(
            `No events received within wait time. Waiting for next interval`
          );
          return;
        }

        // console.log(events);
        // let temp = [];
        console.log("event started");
        for (const event of events) {
          const jsonBuffer = zlib.inflateSync(event.body);

          // parse JSON content
          const jsonStr = jsonBuffer.toString("utf-8");
          const content = JSON.parse(jsonStr);
          ProcessedEvents = { ...ProcessedEvents, ...content };
        }
        console.log(Object.keys(ProcessedEvents).length);
        // ProcessedEvents = [...ProcessedEvents, ...temp];

        //Update the checkpoint.
        await context.updateCheckpoint(events[events.length - 1]);
      },

      processError: async (err, context) => {
        console.log(`Error : ${err}`);
      },
    },
    { startPosition: earliestEventPosition }
  );
}

app.get("/start-event-processing", async (req, res) => {
  try {
    if (eventProcessingStarted) {
      return res
        .status(400)
        .json({ error: "Event processing is already running" });
    }
    eventProcessingStarted = true;
    main();
    // eventProcessingStarted = false;
    res.status(200).json({ message: "Event processing started successfully" });
  } catch (error) {
    console.error("Error in event processing:", error);
    // Reset the flag in case of an error
    isEventProcessingRunning = false;
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/stop-event-processing", async (req, res) => {
  try {
    // Check if event processing is running
    if (!eventProcessingStarted) {
      return res.status(400).json({ error: "Event processing is not running" });
    }
    console.log("stopped");
    await subscription.close();
    await consumerClient.close();
    console.log("stopped");
    eventProcessingStarted = false;
    res.status(200).json({ message: "Event processing stopped successfully" });
  } catch (error) {
    console.error("Error while stopping event processing:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/get-data", (req, res) => {
  let now = Math.floor(Date.now() / 1000);

  // Object.keys(ProcessedEvents).map((key) => {
  //   if (key !== "full_count" && key !== "version") {
  //     if (now - ProcessedEvents[key][10] > 86400 + 3 * 60) {
  //       delete ProcessedEvents[key];
  //     }
  //   }
  // });

  return res.status(200).send({ data: ProcessedEvents });
});

cron.schedule("* 3 * * *", () => {
  ProcessedEvents = [];
});
