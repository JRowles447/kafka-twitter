import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "consumer_group_1";
        String topic = "testing_tweets_topic";
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating consumer thread...");

        TwitterConsumerRunnable consumerRunnable = new TwitterConsumerRunnable(bootstrapServers, topic, groupId, latch);

        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook!");
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Consumer application exiting...");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted!", e);
        } finally {
            logger.info("Application is closing...");
        }
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
            try {
                while(true) {
                    ConsumerRecords<String, String> record = consumer.poll(Duration.ofMillis(100L));

                    // TODO send the records to elastic search
                    for (ConsumerRecord<String, String> rec : record) {
                        logger.info("Received record!" + "\n" +
                                "\tKey:         " + rec.key() + "\n" +
                                "\tValue:       " + rec.value() + "\n" +
                                "\tPartition:   " + rec.partition() + "\n" +
                                "\tOffset:      " + rec.offset() + "\n");
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }

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
