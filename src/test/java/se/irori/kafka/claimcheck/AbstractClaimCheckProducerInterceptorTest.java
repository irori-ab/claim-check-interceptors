package se.irori.kafka.claimcheck;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;

public class AbstractClaimCheckProducerInterceptorTest {

  DummyClaimCheckProducerInterceptor unit;

  @Before
  public void setup() {
    unit = new DummyClaimCheckProducerInterceptor(); // reset counter
    HashMap<String,Object> config = new HashMap<>();
    config.put(
        AbstractClaimCheckProducerInterceptor.CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG,
        10);
    unit.configure(config);
  }

  @Test
  public void onSendLarge() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    // WHEN sending a record with null key, and body > 10 bytes
    ProducerRecord<byte[], byte[]> producerRecord =
        new ProducerRecord<>("dummyTopic",
            "01234567890".getBytes(StandardCharsets.UTF_8));
    ProducerRecord<byte[], byte[]> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals("0", new ClaimCheck(result.value()).getReference());
    assertEquals(1, unit.getCount());
  }

  @Test
  public void onSendSmall() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    // WHEN sending a record with null key, and body < 10 bytes
    ProducerRecord<byte[], byte[]> producerRecord =
        new ProducerRecord<>("dummyTopic",
            "0123456789".getBytes(StandardCharsets.UTF_8));

    ProducerRecord<byte[], byte[]> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals("0123456789", new String(result.value(), StandardCharsets.UTF_8));
    assertEquals(0, unit.getCount());
  }

  public static class DummyClaimCheckProducerInterceptor
      extends AbstractClaimCheckProducerInterceptor {

    private int counter = 0;

    public int getCount() {
      return counter;
    }

    @Override
    public ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord) {
      return new ClaimCheck((counter++) + "");
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }
  }
}