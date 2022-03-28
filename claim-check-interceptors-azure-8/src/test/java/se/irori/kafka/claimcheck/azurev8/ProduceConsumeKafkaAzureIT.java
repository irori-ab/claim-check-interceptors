package se.irori.kafka.claimcheck.azurev8;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import se.irori.kafka.claimcheck.BaseClaimCheckConfig;
import se.irori.kafka.claimcheck.ClaimCheckDeserializer;
import se.irori.kafka.claimcheck.ClaimCheckSerializer;
import se.irori.kafka.claimcheck.ClaimCheckProducerInterceptor;
import se.irori.kafka.claimcheck.TestUtils;

/**
 * Integration test on the KafkaProducer/KafkaConsumer level, against
 * a realistic Azure backend (Azurite emulator or real backend) and a real Kafka cluster.
 */
public class ProduceConsumeKafkaAzureIT extends AbstractClaimCheckIT {

  KafkaProducer<String, String> producer;
  KafkaConsumer<String, String> consumer;

  HashMap<String, Object> producerConfig;
  HashMap<String, Object> consumerConfig;

  private static final String TOPIC = "my-topic";
  private static final Logger log = LoggerFactory.getLogger(ProduceConsumeKafkaAzureIT.class);

  @Rule
  public final AzuriteContainer azuriteContainer = new AzuriteContainer()
      .withExposedPorts(10000);

  @ClassRule
  public static final KafkaContainer kafkaContainer =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0")); // 2.8.0

  @Before
  public void setUp() {
    producerConfig = new HashMap<>();
    injectConfigFromSystemProperties(producerConfig, azuriteContainer, "producer.");
    // https://github.com/Azure/Azurite#default-storage-account

    // use default value
    // producerConfig.put(CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG, 10);
    producerConfig.putIfAbsent(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG,
        AzureBlobStorageClaimCheckBackendV8.class);
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        kafkaContainer.getBootstrapServers());

    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ClaimCheckSerializer.class);
    producerConfig.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_SERIALIZER_CLASS,
        StringSerializer.class);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        ClaimCheckProducerInterceptor.class.getName());
    producerConfig.put(AzureClaimCheckConfig.Keys.AZURE_CREATE_CONTAINER_IF_NOT_EXISTS,
        true);

    consumerConfig = new HashMap<>();
    injectConfigFromSystemProperties(consumerConfig, azuriteContainer, "consumer.");
    consumerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        kafkaContainer.getBootstrapServers());
    consumerConfig.putIfAbsent(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG,
        AzureBlobStorageClaimCheckBackendV8.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ClaimCheckDeserializer.class);
    consumerConfig.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_DESERIALIZER_CLASS,
        StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");

    producer = new KafkaProducer<>(producerConfig);
    consumer = new KafkaConsumer<>(consumerConfig);

    consumer.subscribe(Collections.singletonList(TOPIC));
  }

  @Test
  public void testKafkaProduceConsume() throws ExecutionException, InterruptedException {
    String key = "myKey";
    String value = TestUtils.getRandomString(1024 * 1024);

    RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(TOPIC, key, value)).get();

    //Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(log);
    //kafkaContainer.followOutput(logConsumer);

    ArrayList<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
    consumer.poll(Duration.ofSeconds(10)).forEach(consumedRecords::add);

    int timeoutS = 60;
    for (int i = 0; i < 60; i++) {
      if (consumer.assignment().size() > 0) {
        break;
      }
      Thread.sleep(1000);
    }
    assertTrue("no partition assignment within timeout " + timeoutS + "s",
        consumer.assignment().size() > 0);

    consumer.poll(Duration.ofSeconds(10)).forEach(consumedRecords::add);

    assertEquals(1, consumedRecords.size());
    ConsumerRecord<String, String> record = consumedRecords.get(0);
    String keyResult = record.key();
    String valueResult = record.value();
    assertEquals(key, keyResult);
    assertEquals(value, valueResult);
  }

}