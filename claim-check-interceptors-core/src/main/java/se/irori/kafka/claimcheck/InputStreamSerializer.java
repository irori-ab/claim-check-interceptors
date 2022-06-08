package se.irori.kafka.claimcheck;

import static se.irori.kafka.claimcheck.ClaimCheckStreamingProducerInterceptor.HEADER_MESSAGE_CLAIM_CHECK_PAYLOAD_SIZE;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class InputStreamSerializer implements Serializer<InputStream> {
  @Override
  public byte[] serialize(String s, InputStream inputStream) {
    throw new IllegalArgumentException("Need to use Kafka Client library >2.1.0 that passes "
        + "headers to serializer");
  }

  @Override
  public byte[] serialize(String topic, Headers headers, InputStream dataStream) {
    byte[] longBytes =
        headers.lastHeader(HEADER_MESSAGE_CLAIM_CHECK_PAYLOAD_SIZE).value();
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.put(longBytes);
    long payloadSize = buffer.getLong();

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
