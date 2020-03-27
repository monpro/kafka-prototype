package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {
    public static void main(String[] args) {
        new Consumer().run();

    }


    private Consumer(){

    }

    private void run(){
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "second_test_group";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        logger.info("Creating the consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(
                latch,
                bootstrapServer,
                groupId,
                topic
        );
        // start the thread
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("application is exited");
        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());


        public ConsumerRunnable(CountDownLatch latch,
                              String bootstrapServer,
                              String groupId,
                              String topic
                              ){
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            consumer = new KafkaConsumer<String, String>(properties);
//            consumer.subscribe(Arrays.asList(topic));

            TopicPartition topicPartition = new TopicPartition(topic, 0);
            long offsetStart = 15L;
            consumer.assign(Arrays.asList(topicPartition));

            consumer.seek(topicPartition, offsetStart);

        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", offset: " + record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal!");
            }finally {
                consumer.close();
                // tell main to exit the consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            //it will stop consumer.poll()
            // throw a WakeUpException
            consumer.wakeup();
        }
    }
}
