package se.irori.kafka.claimcheck;


import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Common utility methods for tests.
 */
public final class TestUtils {

  /**
   * Random seed.
   */
  private static final Random RANDOM = new Random(0L);

  private TestUtils() {
  }

  /**
   * Generates random strings of a given length.
   *
   * @param length length of string.
   *
   * @return the random string.
   */
  public static String getRandomString(final int length) {
    // bit ugly, doesn't really need to be performant though
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    Base64.Encoder encoder = Base64.getEncoder();

    return encoder.encodeToString(bytes).substring(0, length);
  }

  /**
   * Generates random byte arrays of a given length.
   *
   * @param length length of array.
   *
   * @return the random byte array.
   */
  public static byte[] getRandomBytes(final int length) {
    // bit ugly, doesn't really need to be performant though
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);
    return bytes;
  }


  /**
   * Create a test streaming producer record.
   *
   * @param topic record topic
   * @param key record key
   * @param value record value
   * @return a ProducerRecord with value as stream, with proper payload size header
   */
  public static ProducerRecord<String, InputStream> streamRecordFromString(
      String topic, String key, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream valueStream = new ByteArrayInputStream(bytes);
    ProducerRecord<String, InputStream> inputRecord =
        new ProducerRecord<>(topic, key, valueStream);
    ClaimCheckStreamingUtils.setPayloadSize(inputRecord.headers(), bytes.length);
    return inputRecord;
  }
}
