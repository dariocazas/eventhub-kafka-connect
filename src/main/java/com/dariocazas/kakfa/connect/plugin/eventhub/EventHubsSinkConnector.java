package com.dariocazas.kakfa.connect.plugin.eventhub;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventHubsSinkConnector extends SinkConnector {

    private static final String PROP_PREFIX = "azure.eventhubs.sink.";
    public static final String PROP_CONNECTION_STRING_NAME = PROP_PREFIX + "connectionstring";
    public static final String PROP_EVENTHUB_NAME = PROP_PREFIX + "hubname";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PROP_CONNECTION_STRING_NAME, Type.STRING, null, Importance.HIGH, "Connection string to EventHubs, in format `Endpoint=sb://<NAMESPACE>/;SharedAccessKeyName=<KEY_NAME>;SharedAccessKey=<ACCESS_KEY>`")
            .define(PROP_EVENTHUB_NAME, Type.STRING, null, Importance.HIGH, "Topic name (EventHub) in EventHubs namespace");

    private String connectionString;
    private String eventHubName;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        connectionString = parsedConfig.getString(PROP_CONNECTION_STRING_NAME);
        eventHubName = parsedConfig.getString(PROP_EVENTHUB_NAME);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return EventHubSyncSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (connectionString != null)
                config.put(PROP_CONNECTION_STRING_NAME, connectionString);
            if (eventHubName != null)
                config.put(PROP_EVENTHUB_NAME, eventHubName);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSinkConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
