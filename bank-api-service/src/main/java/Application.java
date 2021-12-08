import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Servicio de API Bancaria
 */
public class Application {
    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";

    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        Producer<String, Transaction> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);

        try {
            processTransactions(new IncomingTransactionsReader(), new UserResidenceDatabase(), kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                           UserResidenceDatabase userResidenceDatabase,
                                           Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {

        while (incomingTransactionsReader.hasNext()) {
            Transaction transaction = incomingTransactionsReader.next();

            String userResidence = userResidenceDatabase.getUserResidence(transaction.getUser());
            String id = transaction.getUser();
            ProducerRecord<String, Transaction> transactionRecord;

            // Si la Transaccion Valida
            if (userResidence.equalsIgnoreCase(transaction.getTransactionLocation())) {

                //Debemos crear el record de Kafka

                transactionRecord = new ProducerRecord<>(VALID_TRANSACTIONS_TOPIC, id, transaction);


            }
            else {
                transactionRecord = new ProducerRecord<>(SUSPICIOUS_TRANSACTIONS_TOPIC, id, transaction);
            }

            if(transactionRecord != null){
                //Enviamos los datos a Kafka y obtenemos el resultado de la transaccion
                RecordMetadata validTranMetadata = kafkaProducer.send(transactionRecord).get();
            }
            System.out.println("Transaccion Registrada");
        }
    }

    public static Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "banking-api-service");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

}
