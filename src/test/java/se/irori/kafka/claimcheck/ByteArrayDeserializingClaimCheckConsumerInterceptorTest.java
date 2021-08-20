package se.irori.kafka.claimcheck;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Before;
import org.junit.Test;
import se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ByteArrayDeserializingClaimCheckConsumerInterceptorTest {

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
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    config.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    unit.configure(config);
  }

  @Test
  public void onConsumeLarge() {

    // GIVEN
    ConsumerRecord<byte[], byte[]> consumerRecord =
        new ConsumerRecord<>("a", 1, 0, "key".getBytes(StandardCharsets.UTF_8), "x".getBytes(StandardCharsets.UTF_8));
    consumerRecord.headers().add(AbstractClaimCheckProducerInterceptor.HEADER_MESSAGE_IS_CLAIM_CHECK, "abc123".getBytes(
        StandardCharsets.UTF_8));
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
            Collections.singletonMap(new TopicPartition("a", 1),
                    Collections.singletonList(consumerRecord)));

    // WHEN
    ConsumerRecords<byte[], byte[]> result = unit.onConsume(records);
    ConsumerRecord<byte[], byte[]> record = result.iterator().next();
    // THEN
    assertEquals(1, result.count());
    assertEquals(0+"", new String(record.value(), StandardCharsets.UTF_8));
    assertEquals(1, record.partition());
    assertEquals("a", record.topic());
    assertEquals("key", new String(record.key(), StandardCharsets.UTF_8));
    assertEquals(0, record.offset());
    assertEquals(1, unit.getCount());
  }

  @Test
  public void onConsumeSmall() {
    // GIVEN
    ConsumerRecord<byte[], byte[]> consumerRecord =
        new ConsumerRecord<>("a", 1, 0, new byte[] {}, "x".getBytes(StandardCharsets.UTF_8));
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Collections
        .singletonMap(new TopicPartition("a", 1), Collections.singletonList(consumerRecord)));

    // WHEN
    ConsumerRecords<byte[], byte[]> result = unit.onConsume(records);

    // THEN
    assertEquals(1, result.count());
    assertEquals("x", new String(result.iterator().next().value(), StandardCharsets.UTF_8));
    assertEquals(0, unit.getCount());
  }

  @Test
  public void onConsumeNull() {
    // GIVEN
    ConsumerRecord<byte[], byte[]> consumerRecord =
            new ConsumerRecord<>("a", 1, 0, new byte[] {}, null);
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Collections
            .singletonMap(new TopicPartition("a", 1), Collections.singletonList(consumerRecord)));

    // WHEN
    ConsumerRecords<byte[], byte[]> result = unit.onConsume(records);

    // THEN
    assertEquals(1, result.count());
    assertNull(result.iterator().next().value());
    assertEquals(0, unit.getCount());
  }

  public static class DummyClaimCheckConsumerInterceptor
      extends DeserializingClaimCheckConsumerInterceptor<byte[],byte[]> {

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