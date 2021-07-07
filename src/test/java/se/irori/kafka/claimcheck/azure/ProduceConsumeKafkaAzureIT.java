package se.irori.kafka.claimcheck.azure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class ProduceConsumeKafkaAzureIT {

  KafkaProducer<byte[], byte[]> producer;
  KafkaConsumer<byte[], byte[]> consumer;

  HashMap<String,Object> producerConfig;
  HashMap<String,Object> consumerConfig;

  private static final String TOPIC = "my-topic";
  private static final Logger log = LoggerFactory.getLogger(ProduceConsumeKafkaAzureIT.class);

  @Rule
  public AzuriteContainer azuriteContainer = new AzuriteContainer()
      .withExposedPorts(10000);

  /*
  @Rule
  public StrimziKafkaContainer kafkaContainer =
      new StrimziKafkaContainer("0.24.0-kafka-2.8.0");
  */

  @ClassRule
  public static KafkaContainer kafkaContainer =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0")); // 2.8.0

  @Before
  public void setUp() {
    producerConfig = new HashMap<>();
    // https://github.com/Azure/Azurite#default-storage-account
    String token =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    producerConfig.put(AzureBlobClaimCheckProducerInterceptor.AZURE_STORAGE_ACCOUNT_SASTOKEN_CONFIG,
        token);
    String host = "" + azuriteContainer.getContainerIpAddress();
    String account = "devstoreaccount1";
    // http://<local-machine-address>:<port>/<account-name>/<resource-path>
    String endpoint = String.format("http://%s:%d/%s/",
        host,
        azuriteContainer.getMappedPort(10000),
        account);
    String connectionString = "" +
        "DefaultEndpointsProtocol=http;" +
        "AccountName=devstoreaccount1;" +
        "BlobEndpoint=" + endpoint + ";" +
        "AccountKey=" + token + ";";

    producerConfig.put(
        AzureBlobClaimCheckProducerInterceptor.CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG,
        10);
    producerConfig.put(AzureBlobClaimCheckProducerInterceptor.AZURE_STORAGE_ACCOUNT_URL_CONFIG,
        connectionString);
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        kafkaContainer.getBootstrapServers());

    consumerConfig = new HashMap<>(producerConfig);

    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        AzureBlobClaimCheckProducerInterceptor.class.getName());

    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    consumerConfig.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        AzureBlobClaimCheckConsumerInterceptor.class.getName());

    producer = new KafkaProducer<>(producerConfig);
    consumer = new KafkaConsumer<>(consumerConfig);

    consumer.subscribe(Collections.singletonList(TOPIC));
  }

  @Test
  public void testKafkaProduceConsume() throws ExecutionException, InterruptedException {
    String key = "myKey";
    // String value = "01234567890";
    StringBuilder valueBuilder = new StringBuilder();
    while (valueBuilder.length() < 1024*1024) {
      valueBuilder.append(UUID.randomUUID());
    }
    String value = valueBuilder.toString();

    RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(TOPIC,
        key.getBytes(StandardCharsets.UTF_8),
        value.getBytes(StandardCharsets.UTF_8))).get();

    //Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(log);
    //kafkaContainer.followOutput(logConsumer);

    ArrayList<ConsumerRecord<byte[], byte[]>> consumedRecords = new ArrayList<>();
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
    ConsumerRecord<byte[], byte[]> record = consumedRecords.get(0);
    String keyResult = new String(record.key(), StandardCharsets.UTF_8);
    String valueResult = new String(record.value(), StandardCharsets.UTF_8);
    assertEquals(key, keyResult);
    assertEquals(value, valueResult);
  }

}