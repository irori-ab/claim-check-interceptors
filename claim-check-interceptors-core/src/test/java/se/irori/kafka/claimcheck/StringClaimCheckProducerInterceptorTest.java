package se.irori.kafka.claimcheck;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test the ClaimCheckProducerInterceptor with String datatypes.
 */
public class StringClaimCheckProducerInterceptorTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StringClaimCheckProducerInterceptorTest.class);

  ClaimCheckProducerInterceptor<String,String> unit;

  @Before
  public void setup() {
    unit = new ClaimCheckProducerInterceptor<>();
    HashMap<String, Object> config = new HashMap<>();
    config.put(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG,
        200);

    FakeClaimCheckBackend.reset();
    config.put(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, FakeClaimCheckBackend.class);

    config.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ClaimCheckSerializer.class);
    config.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_SERIALIZER_CLASS,
        StringSerializer.class);

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

    assertTrue(ClaimCheckUtils.isClaimCheck(producerRecord.headers()));

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals("1", new ClaimCheck(ClaimCheckUtils.getClaimCheckRefFromHeader(
        producerRecord.headers())).getReference());
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
    assertFalse(ClaimCheckUtils.isClaimCheck(producerRecord.headers()));
    assertFalse(ClaimCheckUtils.isClaimCheckError(producerRecord.headers()));
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
    assertFalse(ClaimCheckUtils.isClaimCheck(producerRecord.headers()));
    assertFalse(ClaimCheckUtils.isClaimCheckError(producerRecord.headers()));
  }

  @Test
  public void onSendBackendError() {
    // GIVEN the interceptor is configured with max limit 10 bytes
    // GIVEN the fake backend is set to throw errors
    FakeClaimCheckBackend.setErrorModeOn(true);

    // WHEN sending a record with null key, and body > 100 bytes (with margin)
    String body = TestUtils.getRandomString(200);
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>("dummyTopic", body);

    ProducerRecord<String, String> result = unit.onSend(producerRecord);

    // THEN result should be a normal message with claim check error header, we called backend
    assertEquals(body, result.value());
    assertEquals(1, FakeClaimCheckBackend.getCount());
    assertFalse(ClaimCheckUtils.isClaimCheck(producerRecord.headers()));
    assertTrue(ClaimCheckUtils.isClaimCheckError(producerRecord.headers()));

    String stackTrace = ClaimCheckUtils.getClaimCheckErrorStackTraceFromHeader(
        producerRecord.headers());
    assertTrue(stackTrace.contains("RuntimeException"));
    assertTrue(stackTrace.contains("FakeClaimCheckBackend"));
    assertTrue(stackTrace.contains("ClaimCheckProducerInterceptor"));
    assertTrue(stackTrace.contains("Some fake backend exception"));

  }


}