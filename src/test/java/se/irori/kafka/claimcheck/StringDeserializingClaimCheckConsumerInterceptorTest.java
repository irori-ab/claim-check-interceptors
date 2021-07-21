package se.irori.kafka.claimcheck;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class StringDeserializingClaimCheckConsumerInterceptorTest {

  DummyClaimCheckConsumerInterceptor unit;

  @Before
  public void setup() {
    unit = new DummyClaimCheckConsumerInterceptor();
    HashMap<String, Object> config = new HashMap<>();
    config.put(
            AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG, "https://someEndpoint");
    config.put(
            AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG, "value:testSasToken");
    config.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    unit.configure(config);
  }

  @Test
  public void onConsumeLarge() {

    // GIVEN
    ConsumerRecord<String, String> consumerRecord =
        new ConsumerRecord<>("a", 1, 0, "key", "x");
    consumerRecord.headers().add(AbstractClaimCheckProducerInterceptor.HEADER_MESSAGE_IS_CLAIM_CHECK, null);
    ConsumerRecords<String, String> records = new ConsumerRecords<>(
            Collections.singletonMap(new TopicPartition("a", 1),
                    Collections.singletonList(consumerRecord)));

    // WHEN
    ConsumerRecords<String, String> result = unit.onConsume(records);
    ConsumerRecord<String, String> record = result.iterator().next();
    // THEN
    assertEquals(1, result.count());
    assertEquals("0", record.value());
    assertEquals(1, record.partition());
    assertEquals("a", record.topic());
    assertEquals("key", record.key());
    assertEquals(0, record.offset());
    assertEquals(1, unit.getCount());
  }

  @Test
  public void onConsumeSmall() {
    // GIVEN
    ConsumerRecord<String, String> consumerRecord =
        new ConsumerRecord<>("a", 1, 0, "key", "x");
    ConsumerRecords<String, String> records = new ConsumerRecords<>(Collections
        .singletonMap(new TopicPartition("a", 1), Collections.singletonList(consumerRecord)));

    // WHEN
    ConsumerRecords<String, String> result = unit.onConsume(records);

    // THEN
    assertEquals(1, result.count());
    assertEquals("x", result.iterator().next().value());
    assertEquals(0, unit.getCount());
  }

  public static class DummyClaimCheckConsumerInterceptor
      extends DeserializingClaimCheckConsumerInterceptor {

    private int counter = 0;

    public int getCount() {
      return counter;
    }

    @Override
    public byte[] checkOut(ClaimCheck claimCheck) {
      String counterString = (counter++)+"";
      return counterString.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {

    }
  }
}