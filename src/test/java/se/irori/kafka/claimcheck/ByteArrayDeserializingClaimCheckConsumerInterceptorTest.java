package se.irori.kafka.claimcheck;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Before;
import org.junit.Test;

public class ByteArrayDeserializingClaimCheckConsumerInterceptorTest {

  DeserializingClaimCheckConsumerInterceptor<byte[],byte[]> unit;

  @Before
  public void setup() {
    unit = new DeserializingClaimCheckConsumerInterceptor<>();
    HashMap<String, Object> config = new HashMap<>();
    config.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    config.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

    FakeClaimCheckBackend.resetCounter();
    config.put(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, FakeClaimCheckBackend.class);

    unit.configure(config);
  }

  @Test
  public void onConsumeLarge() {

    // GIVEN
    ConsumerRecord<byte[], byte[]> consumerRecord =
        new ConsumerRecord<>("a", 1, 0, "key".getBytes(StandardCharsets.UTF_8), "x".getBytes(StandardCharsets.UTF_8));
    consumerRecord.headers().add(SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK, "abc123".getBytes(
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
    assertEquals(1, FakeClaimCheckBackend.getCount());
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
    assertEquals(0, FakeClaimCheckBackend.getCount());
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
    assertEquals(0, FakeClaimCheckBackend.getCount());
  }

}