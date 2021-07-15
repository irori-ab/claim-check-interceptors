package se.irori.kafka.claimcheck;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class SerializingClaimCheckProducerInterceptorTest {

  DummyClaimCheckProducerInterceptor unit;

  @Before
  public void setup() {
    unit = new DummyClaimCheckProducerInterceptor(); // reset counter
    HashMap<String, Object> config = new HashMap<>();
    config.put(
        AzureClaimCheckConfig.Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG,
        10);
    unit.configure(config);
  }

  @Test
  public void onSendLargeByte() {
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
  public void onSendSmallByte() {
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

  @Test
  public void onSendLargeString() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    // WHEN sending a record with null key, and body > 10 bytes
    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("dummyTopic",
                    "01234567890");
    ProducerRecord<String, String> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals("0", new ClaimCheck(result.value()).getReference());
    assertEquals(1, unit.getCount());
  }

  @Test
  public void onSendSmallString() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    // WHEN sending a record with null key, and body < 10 bytes
    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("dummyTopic",
                    "0123456789");

    ProducerRecord<String, String> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals("0123456789", result.value());
    assertEquals(0, unit.getCount());
  }

  public static class DummyClaimCheckProducerInterceptor
      extends SerializingClaimCheckProducerInterceptor {

    private int counter = 0;

    public int getCount() {
      return counter;
    }

    /*
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
    */
  }
}