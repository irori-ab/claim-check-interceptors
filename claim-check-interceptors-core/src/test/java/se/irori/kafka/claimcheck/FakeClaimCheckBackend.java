package se.irori.kafka.claimcheck;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Fake implementation of {@link ClaimCheckBackend} to support basic unit test verification.
 */
public class FakeClaimCheckBackend
    implements ClaimCheckBackend {

  // note: not thread safe (for basic test usage only)
  /** the number of times a message check in has been attempted since reset */
  private static int counter = 0;

  private static boolean errorModeOn = false;

  /**
   * reset the counter and turn off error mode if set
   */
  public static void reset() {
    counter = 0;
    errorModeOn = false;
  }

  public static int getCount() {
    return counter;
  }

  @Override
  public ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord) {
    counter += 1;
    if (errorModeOn) {
      throw new RuntimeException("Some fake backend exception");
    }
    return new ClaimCheck("" + counter);
  }

  @Override
  public ClaimCheck checkInStreaming(String topic, InputStream payload, long payloadSize) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public byte[] checkOut(ClaimCheck claimCheck) {
    String counterString = (counter++)+"";
    if (errorModeOn) {
      throw new RuntimeException("Some fake backend exception");
    }
    return counterString.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public InputStream checkOutStreaming(ClaimCheck claimCheck) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void configure(Map<String, ?> configs) {

  }

  public static boolean isErrorModeOn() {
    return errorModeOn;
  }

  public static void setErrorModeOn(boolean errorModeOn) {
    FakeClaimCheckBackend.errorModeOn = errorModeOn;
  }
}