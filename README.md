# Kafka connect plugin for EventHubs

## Introduction 

The state of the art for replicate data from Kafka and EventHubs is using tools like Mirror Maker or Replicator.
The goal of this repo is an alternative proposal similar to [GCP PubSub connector]: a little connect without 
all the complexity (and features) of the other complex tools: only "copy" events, with at-least-once guaranties.

Some tools like Mirror Maker 2 requires three management topics on both sides. In the context of EventHubs may be
a problem due to the limits of basic or standard tier (a few topics per namespace).

## EventHub Sink  Connector

In addition to the other common configurations used in kafka connects, we need:

```properties
azure.eventhubs.sink.connectionstring=Endpoint=sb://myNamespace.servicebus.windows.net/;SharedAccessKeyName=myAccessKeyName;SharedAccessKey=03+gdcNCLMoCcXzJzBIQoKH3ZG57+TJk+nemRM1v1i4=
azure.eventhubs.sink.hubname=myEventHubName
```

# Future features list

* Sink task using async producer
* Better management for size of event and batch size
* Azure EventHub schema management
* Source task

# Changelog

## [0.1.0] - 2021/03/06

### Added

- Sink task send batch to EventHubs (basic size of event check)







[0.1.0]: https://github.com/dariocazas/eventhub-kafka-connect/releases/tag/0.1.0

[GCP PubSub connector]: https://github.com/GoogleCloudPlatform/pubsub/tree/master/kafka-connector