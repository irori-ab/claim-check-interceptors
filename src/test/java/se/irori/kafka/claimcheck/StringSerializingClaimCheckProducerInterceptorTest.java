package se.irori.kafka.claimcheck;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test the SerializingClaimCheckProducerInterceptor with String datatypes.
 */
public class StringSerializingClaimCheckProducerInterceptorTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StringSerializingClaimCheckProducerInterceptorTest.class);

  SerializingClaimCheckProducerInterceptor<String,String> unit;

  @Before
  public void setup() {
    unit = new SerializingClaimCheckProducerInterceptor<>();
    HashMap<String, Object> config = new HashMap<>();
    config.put(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG, 200);

    FakeClaimCheckBackend.resetCounter();
    config.put(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, FakeClaimCheckBackend.class);

    config.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    unit.configure(config);
  }

  @Test
  public void onSendLargeString() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    // WHEN sending a record with null key, and body > 100 bytes (with margin)
    String body = TestUtils.getRandomString(200);
    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("dummyTopic", body);
    ProducerRecord<String, String> result = unit.onSend(producerRecord);

    Header headerResult = result.headers().iterator().next();

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals("0", new ClaimCheck(headerResult.value()).getReference());
    assertEquals(SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK,
        headerResult.key());
    assertEquals(1, FakeClaimCheckBackend.getCount());
  }

  @Test
  public void onSendSmallString() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    // WHEN sending a record with null key, and body < 10 bytes
    String body = TestUtils.getRandomString(10);
    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("dummyTopic", body);

    ProducerRecord<String, String> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals(body, result.value());
    assertEquals(0, FakeClaimCheckBackend.getCount());
  }

  @Test
  public void onSendNullString() {
    // GIVEN the interceptor is configured with max limit 10 bytes

    String nullStr = null;
    // WHEN sending a record with null key, and null body
    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("dummyTopic",
                    nullStr);

    ProducerRecord<String, String> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals(nullStr, result.value());
    assertEquals(0, FakeClaimCheckBackend.getCount());
  }


}