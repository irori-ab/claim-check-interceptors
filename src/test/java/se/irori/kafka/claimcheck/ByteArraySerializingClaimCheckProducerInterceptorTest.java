package se.irori.kafka.claimcheck;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test the SerializingClaimCheckProducerInterceptor with byte[] datatypes.
 */
public class ByteArraySerializingClaimCheckProducerInterceptorTest {

  SerializingClaimCheckProducerInterceptor<byte[],byte[]> unit;

  @Before
  public void setup() {
    unit = new SerializingClaimCheckProducerInterceptor<>();

    HashMap<String, Object> config = new HashMap<>();
    config.put(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG,
        100); // actual message is payload + some protocol overhead
    config.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    config.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

    FakeClaimCheckBackend.resetCounter();
    config.put(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, FakeClaimCheckBackend.class);

    unit.configure(config);
  }

    @Test
  public void onSendLargeByte() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    // WHEN sending a record with null key, and body > 100 bytes (with margin)
      byte[] body = TestUtils.getRandomBytes(200);
    ProducerRecord<byte[], byte[]> producerRecord =
        new ProducerRecord<>("dummyTopic", body);
    ProducerRecord<byte[], byte[]> result = unit.onSend(producerRecord);

    Header headerResult = result.headers().iterator().next();
    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals("0", new ClaimCheck(headerResult.value()).getReference());
    assertEquals(SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK,
        headerResult.key());
    assertEquals(1, FakeClaimCheckBackend.getCount());
  }

  @Test
  public void onSendSmallByte() {
    // GIVEN the interceptor is configured with max batch limit 100 bytes

    // WHEN sending a record with null key, and body < 100 bytes (with margin)
    byte[] body = TestUtils.getRandomBytes(10);
    ProducerRecord<byte[], byte[]> producerRecord =
        new ProducerRecord<>("dummyTopic", body);

    ProducerRecord<byte[], byte[]> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals(body, result.value());
    assertEquals(0, FakeClaimCheckBackend.getCount());
  }

  @Test
  public void onSendNullByte() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    // WHEN sending a record with null key, and body < 10 bytes
    ProducerRecord<byte[], byte[]> producerRecord =
            new ProducerRecord<>("dummyTopic",
                    null);

    ProducerRecord<byte[], byte[]> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertNull(result.value());
    assertEquals(0, FakeClaimCheckBackend.getCount());
  }

}