import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class MergeStreams {

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);

        return props;
    }

    public Topology buildTopology(Properties envProps, ObjectMapper objectMapper) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String[] inputTopics = renderArrayOfTopics(envProps.getProperty("input.csv.topic.names"));
        final String destinationMergeTopic = cleanTopicName(envProps.getProperty("output.destination.topic.name"));
        final String comparisonKey = envProps.getProperty("input.conditional.key.name");
        final String comparisonValue = envProps.getProperty("input.conditional.key.value");
        System.out.println("'" + comparisonValue + "' destination Name: '" + destinationMergeTopic + "' Topic Count: " + inputTopics.length);

        KStream<String, String> workerStream0 = builder.stream(inputTopics[0]);
        KStream<String, String> workerStream1 = builder.stream(inputTopics[1]);
        KStream<String, String> dataStream = workerStream0.merge(workerStream1);

        dataStream
                .filter((key, value) -> {
                            try {
                                if ( value.trim().length() > 0 ) {
                                    JsonNode jsonNode = objectMapper.readTree(value);
                                    String fieldValue = jsonNode.get(comparisonKey).asText();
                                    Boolean result = fieldValue.trim().toLowerCase().equals(comparisonValue.trim().toLowerCase());
                                    return result;
                                } else {
                                    return false;
                                }
                            } catch (JsonProcessingException e) {
                                System.out.println("inside catch loop");
                                e.printStackTrace();
                                return false;
                            }
                        })
                .to(destinationMergeTopic, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    public String cleanTopicName ( String incomingTopicName) {
        return incomingTopicName.trim().replaceAll( " ", "").toLowerCase();
    }

    public String[] renderArrayOfTopics ( String csvTopicNames) {
        String[] topicNames = csvTopicNames.split(",");
        for (int i = 0; i < topicNames.length; i++) {
            topicNames[i] = cleanTopicName(topicNames[i]);
        }
        return topicNames;
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        MergeStreams ms = new MergeStreams();
        Properties envProps = ms.loadEnvProperties(args[0]);
        Properties streamProps = ms.buildStreamsProperties(envProps);
        ObjectMapper objectMapper = new ObjectMapper();
        Topology topology = ms.buildTopology(envProps, objectMapper);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
