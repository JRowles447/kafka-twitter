import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private TwitterProducer() {}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }


    private void run() {
        // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        String topic = "testing_tweets_topic";
        // TODO what should the partitioning scheme look like... round robin might be fine...
        //  for single search term RR is fine, if multiple maybe partition by term contained in tweet

        logger.info("Creating Twitter client...");

        Client twitterClient = createTwitterClient(msgQueue, eventQueue);

        logger.info("Twitter client created successfully, attempting to connect to Twitter? endpoint");

        twitterClient.connect();

        logger.info("Twitter client connected successfully");

        logger.info("Creating Kafka producer...");

        KafkaProducer<String, String> producer = createProducer();


        for(int i = 0; i < 10; i++) {
            // stream messages from queue into Kafka
            try {
                String tweets = msgQueue.poll(5, TimeUnit.SECONDS);

                logger.info("Retrieved the following tweets: " + tweets);

                ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, tweets);

                producer.send(rec, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null){
                            logger.info("Producer sending message:\n" +
                                            "\tTopic:     " + recordMetadata.topic() +"\n" +
                                            "\tPartition: " + recordMetadata.partition() + "\n" +
                                            "\tOffset:    " + recordMetadata.offset() + "\n" +
                                            "\tTimestamp: " + recordMetadata.timestamp() + "\n");

                        } else {
                            logger.error("Exception encountered while producing message", e);
                        }
                    }
                });
            } catch (InterruptedException e) {
                logger.info("Application was interrupted, exiting...");
                e.printStackTrace();
            }
        }

        logger.info("Application completed, exiting...");

    }

     private Client createTwitterClient(BlockingQueue<String> msgQueue, BlockingQueue<Event> eventQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("skyrim");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        // TODO put in the credentials here :)
        Authentication hosebirdAuth = new OAuth1("consumerKey", "consumerSecret", "token", "secret");

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    // create connection credentials and endpoints - hbc
    // create twitter stream that will be read by producer - hbc

    KafkaProducer<String, String> createProducer() {
        logger.info("Creating a producer");

        Properties properties = new Properties();

        // set up the Kafka props here for cluster
        // bootstrap server ip
        // key serializer and value serialize
        // additional configurations: level of acks, replication factor, etc.
        String bootstrapServer = "127.0.0.1:9092";

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }

    // set up the kafka producer to stream from this to Kafka


}
