package se.irori.kafka.claimcheck.azurev8;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static se.irori.kafka.claimcheck.BaseClaimCheckConfig.Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import se.irori.kafka.claimcheck.BaseClaimCheckConfig;
import se.irori.kafka.claimcheck.DeserializingClaimCheckConsumerInterceptor;
import se.irori.kafka.claimcheck.SerializingClaimCheckProducerInterceptor;
import se.irori.kafka.claimcheck.TestUtils;

/**
 * Integration test on the ProducerInterceptor/ConsumerInterceptor level, against
 * a realistic Azure backend (Azurite emulator or real backend).
 */
public class InterceptorsAzureIT extends AbstractClaimCheckIT {

  SerializingClaimCheckProducerInterceptor<String,String> producerInterceptor;
  DeserializingClaimCheckConsumerInterceptor<String,String> consumerInterceptor;
  HashMap<String,Object> producerConfig;
  HashMap<String,Object> consumerConfig;

  private static final String TOPIC = "my-topic";

  @Rule
  public final AzuriteContainer azuriteContainer = new AzuriteContainer()
      .withExposedPorts(10000);

  @Before
  public void setUp() {
    producerConfig = new HashMap<>();
    injectConfigFromSystemProperties(producerConfig, azuriteContainer, "producer.");

    producerConfig.putIfAbsent(
        CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG, 100);
    producerConfig.putIfAbsent(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG,
        AzureBlobStorageClaimCheckBackendV8.class);
    producerConfig.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    consumerConfig = new HashMap<>();
    injectConfigFromSystemProperties(consumerConfig, azuriteContainer, "consumer.");
    consumerConfig.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.putIfAbsent(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG,
        AzureBlobStorageClaimCheckBackendV8.class);

    producerInterceptor = new SerializingClaimCheckProducerInterceptor<>();
    consumerInterceptor = new DeserializingClaimCheckConsumerInterceptor<>();

    producerInterceptor.configure(producerConfig);
    consumerInterceptor.configure(consumerConfig);
  }


  @Test
  public void testProduceToBlob() {
    String key = "myKey";
    String value = TestUtils.getRandomString(200); // large message
    ProducerRecord<String, String> interceptedRecord = producerInterceptor.onSend(
        new ProducerRecord<>(TOPIC, key, value));

    // should be a claim check
    assertTrue(Arrays.stream(interceptedRecord.headers().toArray())
        .anyMatch(r -> r.key()
            .equals(SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK)));

    TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
    ConsumerRecords<String,String> consumeRecords = new ConsumerRecords<>(
        Collections.singletonMap(topicPartition, Collections.singletonList(
            new ConsumerRecord<>(TOPIC, 0, 0L,
                0L, TimestampType.CREATE_TIME,
                0L,
                interceptedRecord.key().length(),
                0,
                interceptedRecord.key(), interceptedRecord.value(), interceptedRecord.headers()))));

    ConsumerRecords<String,String> interceptedRecords =
        consumerInterceptor.onConsume(consumeRecords);

    assertEquals(interceptedRecords.count(), 1);
    ConsumerRecord<String, String> next = interceptedRecords.iterator().next();
    assertEquals(value, next.value());
  }

}