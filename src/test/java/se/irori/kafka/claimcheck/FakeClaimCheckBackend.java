package se.irori.kafka.claimcheck;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testcontainers.shaded.org.apache.commons.lang.NotImplementedException;

public class FakeClaimCheckBackend
    implements ClaimCheckBackend {

  // note: not thread safe (for basic test usage only)
  private static int counter = 0;

  public static void resetCounter() {
    counter = 0;
  }

  public static int getCount() {
    return counter;
  }

  @Override
  public ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord) {
    return new ClaimCheck((counter++) + "");
  }

  @Override
  public byte[] checkOut(ClaimCheck claimCheck) {
    String counterString = (counter++)+"";
    return counterString.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}