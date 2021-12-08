
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {
    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        String consumerGroup = "user-notification-service";

        System.out.println("Consumidores es parte del grupo " + consumerGroup);

        Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        consumeMessages(SUSPICIOUS_TRANSACTIONS_TOPIC, kafkaConsumer);
    }

    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
        System.out.println("Transaccion rechazada");
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(10000);


            if (consumerRecords.count()==0) {
            }

            consumerRecords.forEach(record -> {
                sendUserNotification(record.value());
            });

            kafkaConsumer.commitAsync();
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(properties);
    }

    private static void sendUserNotification(Transaction transaction) {
        System.out.println(
                String.format("Enviando al usuario %s una notificacion acerca de una transaccion sospechosa de $%.2f en su cuenta " +
                                "procedente de %s",
                        transaction.getUser(),
                        transaction.getAmount(),
                        transaction.getTransactionLocation()));
    }
}
