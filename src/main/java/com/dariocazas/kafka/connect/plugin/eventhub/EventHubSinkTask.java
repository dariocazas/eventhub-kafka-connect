package com.dariocazas.kafka.connect.plugin.eventhub;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class EventHubSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(EventHubSinkTask.class);

    private String finalConnectionString;
    private String eventHubName;

    private EventHubProducerClient producer = null;


    public EventHubSinkTask() {
    }

    @Override
    public String version() {
        return new EventHubsSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        String connectionString = props.get(EventHubsSinkConnector.PROP_CONNECTION_STRING_NAME);
        eventHubName = props.get(EventHubsSinkConnector.PROP_EVENTHUB_NAME);
        finalConnectionString = String.format("%s;EntityPath=%s", connectionString, eventHubName);
        try {
            producer = new EventHubClientBuilder().connectionString(finalConnectionString).buildProducerClient();
        } catch (Exception e) {
            throw new ConnectException("Couldn't create eventhub producer for EventHubSinkTask", e);
        }
        log.info("Using default Event Hubs client for namespace '{}'", producer.getFullyQualifiedNamespace());
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        // This method should prepare data and send it async, and manage the kafka commit checking the async state in flush operation.
        // The actual implementation locking in put as first approach (no flush or stop special management is needed)
        Map<Optional<Object>, List<Object>> partitionKeyGrouped = sinkRecords.stream().collect(Collectors.groupingBy(x -> Optional.ofNullable(((SinkRecord) x).key())));
        for (Map.Entry<Optional<Object>, List<Object>> entry : partitionKeyGrouped.entrySet()) {
            handleBatch((String) entry.getKey().orElseGet(() -> ""), entry.getValue().stream().map(x -> ((SinkRecord) x).value()).collect(Collectors.toList()));
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
    }


    public void handleBatch(String partitionKey, Collection<Object> payloads) {
        try {
            EventDataBatch batch = createBatch(partitionKey);
            for (Object payload : payloads) {
                log.trace("Received record '{}'", payload);
                if (payload == null) {
                    continue;
                }
                EventData eventData = null;
                if (payload instanceof String) {
                    eventData = new EventData((String) payload);
                } else if (payload instanceof byte[]) {
                    eventData = new EventData((byte[]) payload);
                } else {
                    throw new RuntimeEventHubsTaskException("Unsupported type message for this kind object: " + payload);
                }

                if (!batch.tryAdd(eventData)) {
                    if (batch.getCount() > 0) {
                        producer.send(batch);
                        log.debug("Sent batch to {} with {} records", eventHubName, batch.getCount());
                        batch = createBatch(partitionKey);
                    }
                    if (!batch.tryAdd(eventData)) {
                        log.error("Sending event to {}: event data too large: {}", eventHubName, eventData);
                        throw new RuntimeEventHubsTaskException("Event data too large");
                    }
                }
            }
            producer.send(batch);
            log.debug("Sent batch to {} with {} records", eventHubName, batch.getCount());
        } catch (Exception e) {
            throw new RuntimeEventHubsTaskException(e);
        }
    }

    private EventDataBatch createBatch(String partitionKey) {
        CreateBatchOptions op = new CreateBatchOptions();
        if (partitionKey != null && !partitionKey.equals("")) {
            op.setPartitionKey(partitionKey);
        }
        return producer.createBatch(op);
    }

}
