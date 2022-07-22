package se.irori.kafka.claimcheck;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

/**
 * Utility methods for Claim Check streaming mode.
 */
public class ClaimCheckStreamingUtils {


  private static LongSerializer payloadSizeSerializer = new LongSerializer();
  private static LongDeserializer payloadSizeDeserializer = new LongDeserializer();

  /**
   * Get the payload size header from a set of message headers.
   *
   * @param headers the header set to process
   * @return the payload size, if header was found
   * @throws IllegalArgumentException if no header entry was found
   */
  public static long getPayloadSize(Headers headers) {
    Header payloadSizeHeader = headers.lastHeader(
        ClaimCheckStreamingProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK_PAYLOAD_SIZE);
    if (payloadSizeHeader == null) {
      throw new IllegalArgumentException("You must supply the '"
          + ClaimCheckStreamingProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK_PAYLOAD_SIZE
          + "' header in streaming mode.");
    }
    return payloadSizeDeserializer.deserialize("dummy", payloadSizeHeader.value());
  }

  /**
   * Set the payload size header on a set of headers.
   *
   * @param headers where to set the header
   * @param payloadSize value to set
   */
  public static void setPayloadSize(Headers headers, long payloadSize) {
    byte[] payloadSizeBytes = payloadSizeSerializer.serialize("dummy", payloadSize);
    headers.add(ClaimCheckStreamingProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK_PAYLOAD_SIZE,
        payloadSizeBytes);
  }

  /**
   * Convert an InputStream to a byte array.
   *
   * @param dataStream input stream
   * @param payloadSize number of bytes in input stream
   * @return a byte array with the fully read stream
   * @throws IOException in case of read errors
   */
  public static byte[] streamToBytes(InputStream dataStream, long payloadSize) throws IOException {
    byte[] data = new byte[(int) payloadSize];
    DataInputStream dis = new DataInputStream(dataStream);
    dis.readFully(data);
    dis.close();
    return data;
  }
  
}
