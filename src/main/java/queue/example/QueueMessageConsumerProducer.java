package queue.example;

import io.kubemq.sdk.client.KubeMQClient;
import io.kubemq.sdk.queues.QueueMessage;
import io.kubemq.sdk.queues.QueueMessageReceived;
import io.kubemq.sdk.queues.QueueSendResult;
import io.kubemq.sdk.queues.QueuesClient;
import io.kubemq.sdk.queues.QueuesPollRequest;
import io.kubemq.sdk.queues.QueuesPollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class QueueProducer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(QueueProducer.class);
    private final QueuesClient queuesClient;
    private final String queueName;
    private volatile boolean isRunning = true;
    private final CountDownLatch shutdownLatch;

    public QueueProducer(QueuesClient queuesClient, String baseQueueName, int workerNumber, CountDownLatch shutdownLatch) {
        this.queuesClient = queuesClient;
        this.queueName = baseQueueName + "-" + workerNumber;
        this.shutdownLatch = shutdownLatch;
    }

    @Override
    public void run() {
        try {
            log.info("Started producer for queue: {}", queueName);
            while (isRunning) {
                sendMessages();
                sleep(1000); // Wait 1 second between batches
            }
        } catch (Exception e) {
            log.error("Error in producer for queue {}: {}", queueName, e.getMessage());
        } finally {
            shutdownLatch.countDown();
            log.info("Producer stopped for queue: {}", queueName);
        }
    }

    private void sendMessages() {
        try {
            for (int i = 0; i < 100; i++) {
                Map<String, String> tags = new HashMap<>();
                tags.put("tag1", "kubemq");
                tags.put("tag2", "kubemq2");

                QueueMessage message = QueueMessage.builder()
                        .body(("Message " + (i + 1) + " from producer " + queueName).getBytes())
                        .channel(queueName)
                        .metadata("Sample metadata " + (i + 1))
                        .id(UUID.randomUUID().toString())
                        .tags(tags)
                        .build();

                QueueSendResult sendResult = queuesClient.sendQueuesMessage(message);
                log.info("Queue: {} - Sent message {} with ID: {}", queueName, (i + 1), sendResult.getId());
            }
        } catch (Exception e) {
            log.error("Error sending messages to queue {}: {}", queueName, e.getMessage());
            sleep(1000); // Small delay before retry
        }
    }

    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        isRunning = false;
    }
}
class QueueConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(QueueConsumer.class);
    private final QueuesClient queuesClient;
    private final String queueName;
    private volatile boolean isRunning = true;
    private final CountDownLatch shutdownLatch;

    public QueueConsumer(QueuesClient queuesClient, String baseQueueName, int workerNumber, CountDownLatch shutdownLatch) {
        this.queuesClient = queuesClient;
        this.queueName = baseQueueName + "-" + workerNumber;
        this.shutdownLatch = shutdownLatch;
    }

    @Override
    public void run() {
        try {
            log.info("Started consumer for queue: {}", queueName);
            while (isRunning) {
                pollAndProcessMessages();
            }
        } catch (Exception e) {
            log.error("Error in consumer for queue {}: {}", queueName, e.getMessage());
        } finally {
            shutdownLatch.countDown();
            log.info("Consumer stopped for queue: {}", queueName);
        }
    }

    private void pollAndProcessMessages() {
        try {
            QueuesPollRequest request = QueuesPollRequest.builder()
                    .channel(queueName)
                    .pollMaxMessages(100)
                    .pollWaitTimeoutInSeconds(1)
                    .build();
            log.info("Polling messages from queue: {}", queueName);
            QueuesPollResponse response = queuesClient.receiveQueuesMessages(request);
            log.info("Received response: {}", queueName);
            if (response.isError()){
                log.error("Error polling messages from queue {}: {}", queueName, response.getError());
                sleep(1000);
            }else {
                if (response.getMessages() != null) {
                    response.ackAll();
                    for (QueueMessageReceived message : response.getMessages()) {
                        processMessage(message);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error polling messages from queue {}: {}", queueName, e.getMessage());
            sleep(1000); // Small delay before retry
        }
    }

    private void processMessage(QueueMessageReceived message) {
        try {
            log.info("Queue: {} - Received message: {}", queueName, new String(message.getBody()));
           // message.ack();
        } catch (Exception e) {
            log.error("Error processing message: {}", e.getMessage());
        }
    }

    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        isRunning = false;
    }
}

public class QueueMessageConsumerProducer {
    private static final Logger log = LoggerFactory.getLogger(QueueMessageConsumerProducer.class);
    private static final String BASE_QUEUE_NAME = "mytest-channel";
    private final List<Thread> consumerThreads = new ArrayList<>();
    private final List<Thread> producerThreads = new ArrayList<>();
    private final List<QueueConsumer> consumers = new ArrayList<>();
    private final List<QueueProducer> producers = new ArrayList<>();
    private final CountDownLatch consumerShutdownLatch;
    private final CountDownLatch producerShutdownLatch;
    private final QueuesClient queuesClient;

    public QueueMessageConsumerProducer(int numWorkers) {
        this.consumerShutdownLatch = new CountDownLatch(numWorkers);
        this.producerShutdownLatch = new CountDownLatch(numWorkers);
        this.queuesClient = createQueuesClient();

        // Create consumers and producers
        for (int i = 0; i < numWorkers; i++) {
            // Create and store consumer
            QueueConsumer consumer = new QueueConsumer(queuesClient, BASE_QUEUE_NAME, i, consumerShutdownLatch);
            Thread consumerThread = new Thread(consumer);
            consumers.add(consumer);
            consumerThreads.add(consumerThread);

//             Create and store producer
            QueueProducer producer = new QueueProducer(queuesClient, BASE_QUEUE_NAME, i, producerShutdownLatch);
            Thread producerThread = new Thread(producer);
            producers.add(producer);
            producerThreads.add(producerThread);
        }
    }

    private QueuesClient createQueuesClient() {
        return QueuesClient.builder()
                .address("localhost:50000")
                .clientId("kubeMQClientId")
                .logLevel(KubeMQClient.Level.INFO)
                .keepAlive(true)
                .build();
    }

    public void start() {
        log.info("Starting {} queue consumers and producers...", consumerThreads.size());

        // Start consumers
        for (Thread thread : consumerThreads) {
            thread.start();
        }

        // Start producers
        for (Thread thread : producerThreads) {
            thread.start();
        }
    }

    public void shutdown() {
        log.info("Initiating shutdown...");

        // Stop all consumers and producers
        for (QueueConsumer consumer : consumers) {
            consumer.stop();
        }
        for (QueueProducer producer : producers) {
            producer.stop();
        }

        try {
            // Wait for all consumers and producers to complete
            boolean consumersShutdown = consumerShutdownLatch.await(30, TimeUnit.SECONDS);
            boolean producersShutdown = producerShutdownLatch.await(30, TimeUnit.SECONDS);

            if (!consumersShutdown || !producersShutdown) {
                log.warn("Some workers didn't shut down gracefully within timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Shutdown interrupted: {}", e.getMessage());
        }
    }

    public static void main(String[] args) {
        QueueMessageConsumerProducer manager = new QueueMessageConsumerProducer(100);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(manager::shutdown));

        // Start the consumers and producers
        manager.start();

        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Main thread interrupted: {}", e.getMessage());
        }
    }
}