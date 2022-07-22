package se.irori.kafka.claimcheck;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

public class ClaimCheckStreamingProducerInterceptorTest {

  ClaimCheckStreamingProducerInterceptor<String> unit;

  @Before
  public void setup() {
    unit = new ClaimCheckStreamingProducerInterceptor<>();
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
        InputStreamSerializer.class);

    config.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    unit.configure(config);
  }

  @Test
  public void onSendLargeString() {
    // GIVEN the interceptor is configured with max limit 200 bytes

    // WHEN sending a record with null key, and body > 100 bytes (with margin)
    String body = TestUtils.getRandomString(200);
    byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);

    ProducerRecord<String, InputStream> producerRecord =
        new ProducerRecord<>("dummyTopic", new ByteArrayInputStream(bodyBytes));
    ClaimCheckStreamingProducerInterceptor.setPayloadSize(producerRecord.headers(),
        bodyBytes.length);

    ProducerRecord<String, InputStream> result = unit.onSend(producerRecord);

    assertTrue(ClaimCheckUtils.isClaimCheck(producerRecord.headers()));

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    assertEquals("1", new ClaimCheck(ClaimCheckUtils.getClaimCheckRefFromHeader(
        result.headers())).getReference());
    assertEquals(1, FakeClaimCheckBackend.getCount());
    assertFalse(ClaimCheckUtils.isClaimCheckError(result.headers()));
  }

  @Test
  public void onSendSmallString() throws IOException {
    // GIVEN the interceptor is configured with max limit 200 bytes

    // WHEN sending a record with null key, and body < 100 bytes
    String body = TestUtils.getRandomString(10);
    byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);

    ProducerRecord<String, InputStream> producerRecord =
        new ProducerRecord<>("dummyTopic", new ByteArrayInputStream(bodyBytes));
    ClaimCheckStreamingProducerInterceptor.setPayloadSize(producerRecord.headers(),
        bodyBytes.length);

    ProducerRecord<String, InputStream> result = unit.onSend(producerRecord);

    // THEN result should be a claim check reference to the 0 counter value from the dummy impl
    byte[] outputBytes = new byte[bodyBytes.length];
    DataInputStream dis = new DataInputStream(result.value());
    dis.readFully(outputBytes);

    assertEquals(body, new String(outputBytes, StandardCharsets.UTF_8));
    assertEquals(0, FakeClaimCheckBackend.getCount());
    assertFalse(ClaimCheckUtils.isClaimCheck(producerRecord.headers()));
    assertFalse(ClaimCheckUtils.isClaimCheckError(producerRecord.headers()));
  }

  @Test
  public void onSendNullString() {
    // GIVEN the interceptor is configured with max limit 200 bytes

    // WHEN sending a record with null key, and null body
    ProducerRecord<String, InputStream> producerRecord =
        new ProducerRecord<>("dummyTopic",
            null);
    ClaimCheckStreamingProducerInterceptor.setPayloadSize(producerRecord.headers(),
        0);

    ProducerRecord<String, InputStream> result = unit.onSend(producerRecord);

    // THEN result should be a null value that is not a claim check
    assertEquals(null, result.value());
    assertEquals(0, FakeClaimCheckBackend.getCount());
    assertFalse(ClaimCheckUtils.isClaimCheck(result.headers()));
    assertFalse(ClaimCheckUtils.isClaimCheckError(result.headers()));
  }

  @Test
  public void onSendBackendError() throws IOException {
    // GIVEN the interceptor is configured with max limit 200 bytes
    // GIVEN the fake backend is set to throw errors
    FakeClaimCheckBackend.setErrorModeOn(true);

    // WHEN sending a record with null key, and body > 100 bytes (with margin)
    String body = TestUtils.getRandomString(200);
    byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);

    ProducerRecord<String, InputStream> producerRecord =
        new ProducerRecord<>("dummyTopic", new ByteArrayInputStream(bodyBytes));
    ClaimCheckStreamingProducerInterceptor.setPayloadSize(producerRecord.headers(),
        bodyBytes.length);

    ProducerRecord<String, InputStream> result = unit.onSend(producerRecord);

    // THEN result should be a normal message with claim check error header, we called backend
    byte[] outputBytes = new byte[bodyBytes.length];
    DataInputStream dis = new DataInputStream(result.value());
    dis.readFully(outputBytes);

    assertEquals(body, new String(outputBytes, StandardCharsets.UTF_8));
    assertEquals(1, FakeClaimCheckBackend.getCount());
    assertFalse(ClaimCheckUtils.isClaimCheck(result.headers()));
    assertTrue(ClaimCheckUtils.isClaimCheckError(result.headers()));

    String stackTrace = ClaimCheckUtils.getClaimCheckErrorStackTraceFromHeader(
        result.headers());
    assertTrue(stackTrace.contains("RuntimeException"));
    assertTrue(stackTrace.contains("FakeClaimCheckBackend"));
    assertTrue(stackTrace.contains("ClaimCheckStreamingProducerInterceptor"));
    assertTrue(stackTrace.contains("Some fake backend exception"));

  }

  @Test
  public void testPayloadSizeHeaderSerialization() {
    RecordHeaders headers = new RecordHeaders();
    long inputSize = 42;
    ClaimCheckStreamingProducerInterceptor.setPayloadSize(headers, inputSize);
    long outputSize = ClaimCheckStreamingProducerInterceptor.getPayloadSize(headers);

    assertEquals(inputSize, outputSize);
  }

  @Test
  public void testSimpleStreamSerialization() {

  }
}