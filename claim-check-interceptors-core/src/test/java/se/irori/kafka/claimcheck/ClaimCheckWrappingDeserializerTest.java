package se.irori.kafka.claimcheck;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;

public class ClaimCheckWrappingDeserializerTest {
  ClaimCheckWrappingDeserializer<String> unit;

  @Before
  public void setup() {
    unit = new ClaimCheckWrappingDeserializer<>();
    FakeClaimCheckBackend.reset();

    HashMap<String, Object> config = new HashMap<>();

    config.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_DESERIALIZER_CLASS,
        StringDeserializer.class);

    config.put(BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG,
        FakeClaimCheckBackend.class);
    unit.configure(config, false);
  }

  @Test
  public void deserializeNonClaimCheck() {
    // GIVEN no headers
    Headers headers = new RecordHeaders();
    String payload = "my-payload";

    // WHEN deserializing
    String deserialized =
        unit.deserialize("my-topic", headers, payload.getBytes(StandardCharsets.UTF_8));

    // THEN
    assertEquals(payload, deserialized);
  }

  @Test
  public void deserializeClaimCheck() {
    // GIVEN claim check header
    Headers headers = new RecordHeaders()
        .add(SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK,
            "https://my.claimcheck.com/ref".getBytes(StandardCharsets.UTF_8));

    // WHEN deserializing
    String deserialized =
        unit.deserialize("my-topic", headers, new byte[0]);

    // THEN ensure we get the first fake backend response
    assertEquals("0", deserialized);
  }

  @Test
  public void deserializeClaimCheckError() {
    // GIVEN claim check header, Fake backend set to throw error
    FakeClaimCheckBackend.setErrorModeOn(true);
    Headers headers = new RecordHeaders()
        .add(SerializingClaimCheckProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK,
            "https://my.claimcheck.com/ref".getBytes(StandardCharsets.UTF_8));

    // WHEN deserializing
    try {
      unit.deserialize("my-topic", headers, new byte[0]);
      fail("Expected execption from deserialization");
    } catch (Exception e) {
      // THEN ensure we get the first fake backend response
      assertTrue(e.getMessage().contains("Some fake backend exception"));
    }
  }
}