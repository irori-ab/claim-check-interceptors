package se.irori.kafka.claimcheck;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

public class ClaimCheckWrappingSerializerTest {
  ClaimCheckWrappingSerializer<String> unit;

  @Before
  public void setup() {
    unit = new ClaimCheckWrappingSerializer<>();
    HashMap<String, Object> config = new HashMap<>();

    config.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_SERIALIZER_CLASS,
        StringSerializer.class);
    config.put(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, FakeClaimCheckBackend.class);

    unit.configure(config, false);
  }

  @Test
  public void serializeNonClaimCheck() {
    // GIVEN no headers
    Headers headers = new RecordHeaders();
    String payload = "myPayload";

    // WHEN serializing the record
    byte[] serialized = unit.serialize("my-topic", headers, payload);

    // THEN verify payload is as-is serialized
    assertEquals(payload, new String(serialized, StandardCharsets.UTF_8));
  }

  @Test
  public void serializeClaimCheck() {
    // GIVEN claim check header
    Headers headers = new RecordHeaders().add(
        SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK,
        "https://my.claim.check/reference".getBytes(StandardCharsets.UTF_8));
    String payload = null;

    // WHEN serializing the record
    byte[] serialized = unit.serialize("my-topic", headers, payload);

    // THEN verify payload replaced with empty array
    assertEquals(0, serialized.length);
  }

  @Test
  public void serializeClaimCheckError() {
    // GIVEN claim check header
    Headers headers = new RecordHeaders().add(
        SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK_ERROR,
        "My stack trace".getBytes(StandardCharsets.UTF_8));
    String payload = "myPayload";

    // WHEN serializing the record
    try {
      unit.serialize("my-topic", headers, payload);
      fail("Expected serialization error");
    } catch (Exception e) {
      // THEN verify stacktraced is propagated in rethrown error
      assertTrue(e.getMessage().contains("My stack trace"));
    }
  }

}