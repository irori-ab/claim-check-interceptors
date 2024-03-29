package se.irori.kafka.claimcheck;

import java.io.InputStream;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;

/**
 * A Claim Check storage backend that can store large messages, issue references to them,
 * and using these references, later retrieve the messages.
 */
public interface ClaimCheckBackend extends Configurable  {

  /**
   * Check in the record in the underlying storage system.
   *
   * <p>This means uploading the payload, collecting some reference (claim check) which
   * can later be used to retrieved the payload.
   *
   * @param largeRecord the message record to check in with the claim check pattern
   * @return a Claim Check representing a reference to the underlying storage system.
   */
  ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord);

  /**
   * Check in a payload in the underlying storage system, from an input stream.
   * @param topic Kafka topic from record
   * @param payload the payload input stream
   * @param payloadSize the size (number of bytes) that will be provided in the stream.
   * @return a Claim Check representing a reference to the underlying storage system.
   */
  ClaimCheck checkInStreaming(String topic, InputStream payload, long payloadSize);

  /**
   * Retrieve a previously stored record, using the Claim Check (reference).
   *
   * @param claimCheck previously issued claim check for this backend
   * @return the message payload previously checked in
   */
  byte[] checkOut(ClaimCheck claimCheck);

  /**
   * Retrieve a previously stored record as a stream, using the Claim Check (reference).
   *
   * @param claimCheck previously issued claim check for this backend
   * @return the message payload previously checked in, as a stream
   */
  InputStream checkOutStreaming(ClaimCheck claimCheck);

  /**
   * Close any resources opened to communicate with the backend.
   */
  default void close() {}
}
