import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TwitterConsumer {
    private final Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());

    private TwitterConsumer() {}

    public static void main(String[] args) {
        new TwitterConsumer().run();
    }

    private void run() {

    }

    public class TwitterConsumerRunnable implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());

        private String bootstrapServers;
        private String topic;
        private String consumerGroup;
        private CountDownLatch latch;

        private KafkaConsumer<String, String> consumer;

        public TwitterConsumerRunnable (String bootstrapServers,
                                        String topic,
                                        String consumerGroup,
                                        CountDownLatch latch) {

            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.consumerGroup = consumerGroup;
            this.latch = latch;

            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.consumerGroup);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            consumer = new KafkaConsumer<String, String>(properties);

            consumer.subscribe(Arrays.asList(this.topic));
        }

        public void run() {

        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
    private KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();



        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        return consumer;
    }
}
