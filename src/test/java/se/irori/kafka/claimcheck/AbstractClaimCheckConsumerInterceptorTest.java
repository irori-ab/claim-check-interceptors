package se.irori.kafka.claimcheck;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

public class AbstractClaimCheckConsumerInterceptorTest {

  DummyClaimCheckConsumerInterceptor unit;

  @Before
  public void setup() {
    unit = new DummyClaimCheckConsumerInterceptor();
  }

  @Test
  public void onConsumeLarge() {

    // GIVEN
    ConsumerRecord<byte[], byte[]> consumerRecord =
        new ConsumerRecord<>("a", 1, 0, new byte[] {}, "x".getBytes(StandardCharsets.UTF_8));
    consumerRecord.headers().add(AbstractClaimCheckProducerInterceptor.HEADER_MESSAGE_IS_CLAIM_CHECK, null);
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
            Collections.singletonMap(new TopicPartition("a", 1),
                    Collections.singletonList(consumerRecord)));

    // WHEN
    ConsumerRecords<byte[], byte[]> result = unit.onConsume(records);

    // THEN
    assertEquals(1, result.count());
    assertEquals(0+"", new String(result.iterator().next().value(), StandardCharsets.UTF_8));
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

  public static class DummyClaimCheckConsumerInterceptor
      extends AbstractClaimCheckConsumerInterceptor {

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
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void close() {

    }
  }
}