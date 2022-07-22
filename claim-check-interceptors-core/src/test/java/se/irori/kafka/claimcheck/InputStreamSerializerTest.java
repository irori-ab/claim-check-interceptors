package se.irori.kafka.claimcheck;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class InputStreamSerializerTest {

  InputStreamSerializer unit = new InputStreamSerializer();

  @Test
  public void testSimpleSerialization() {
    // GIVEN some input bytes as an input stream
    String input = "myInput";
    byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(inputBytes);

    // WHEN serializing them with the proper payload size header
    RecordHeaders headers = new RecordHeaders();
    ClaimCheckStreamingProducerInterceptor.setPayloadSize(headers, inputBytes.length);
    byte[] outputBytes = unit.serialize("my-topic", headers, inputStream);

    // THEN the output bytes should be the same as the input
    String output = new String(outputBytes, StandardCharsets.UTF_8);
    assertEquals(input, output);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoHeader() {
    // GIVEN some input bytes as an input stream
    String input = "myInput";
    byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(inputBytes);

    // WHEN serializing them without  payload size header
    RecordHeaders headers = new RecordHeaders();
    byte[] outputBytes = unit.serialize("my-topic", headers, inputStream);

    // THEN expect IllegalArgumentException
  }

  @Test(expected = SerializationException.class)
  public void testTooLongSize() {
    // GIVEN some input bytes as an input stream
    String input = "myInput";
    byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(inputBytes);

    // WHEN serializing them with too Long size header
    RecordHeaders headers = new RecordHeaders();
    headers.add(ClaimCheckStreamingProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK_PAYLOAD_SIZE,
        BigInteger.valueOf(Long.MAX_VALUE).pow(2).toByteArray());
    byte[] outputBytes = unit.serialize("my-topic", headers, inputStream);

    // THEN expect SerializationException
  }
}