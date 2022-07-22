package se.irori.kafka.claimcheck;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serialize InputStreams to bytes, for small streaming mode payloads.
 */
public class InputStreamSerializer implements Serializer<InputStream> {
  @Override
  public byte[] serialize(String s, InputStream inputStream) {
    throw new IllegalArgumentException("Need to use Kafka Client library >2.1.0 that passes "
        + "headers to serializer");
  }

  @Override
  public byte[] serialize(String topic, Headers headers, InputStream dataStream) {
    long payloadSize = ClaimCheckStreamingUtils.getPayloadSize(headers);

    byte[] data = new byte[(int) payloadSize];
    DataInputStream dis = new DataInputStream(dataStream);
    try {
      dis.readFully(data);
      dis.close();
      return data;
    } catch (IOException exception) {
      throw new KafkaException("Error reading message payload input stream", exception);
    }
  }
}
