package se.irori.kafka.claimcheck;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;

public class StringDeserializingClaimCheckConsumerInterceptorTest {

  DeserializingClaimCheckConsumerInterceptor<String,String> unit;

  @Before
  public void setup() {
    unit = new DeserializingClaimCheckConsumerInterceptor<>();


    HashMap<String, Object> config = new HashMap<>();
    config.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    FakeClaimCheckBackend.resetCounter();
    config.put(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, FakeClaimCheckBackend.class);
    unit.configure(config);
  }

  @Test
  public void onConsumeLarge() {

    // GIVEN
    ConsumerRecord<String, String> consumerRecord =
        new ConsumerRecord<>("a", 1, 0, "key", "x");
    consumerRecord.headers().add(SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK, "abc123".getBytes(
        StandardCharsets.UTF_8));
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
    assertEquals(1, FakeClaimCheckBackend.getCount());
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
    assertEquals(0, FakeClaimCheckBackend.getCount());
  }

  @Test
  public void onConsumeNull() {
    // GIVEN
    String nullStr = null;
    ConsumerRecord<String, String> consumerRecord =
            new ConsumerRecord<>("a", 1, 0, "key", nullStr);
    ConsumerRecords<String, String> records = new ConsumerRecords<>(Collections
            .singletonMap(new TopicPartition("a", 1), Collections.singletonList(consumerRecord)));

    // WHEN
    ConsumerRecords<String, String> result = unit.onConsume(records);

    // THEN
    assertEquals(1, result.count());
    assertEquals(nullStr, result.iterator().next().value());
    assertEquals(0, FakeClaimCheckBackend.getCount());
  }

}