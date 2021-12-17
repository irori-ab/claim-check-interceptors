package se.irori.kafka.claimcheck;

import java.util.Map;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

public class ClaimCheckWrappingSerializer<T> implements Serializer<T> {
  private Serializer<T> valueSerializer;


  /**
   * Configure this class.
   *
   * @param configs configs in key/value pairs
   * @param isKey   whether is for key or value
   */
  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    BaseClaimCheckConfig baseClaimCheckConfig = BaseClaimCheckConfig.validatedConfig(configs);
    if (isKey) {
      throw new IllegalStateException("Should not be used to wrap key serializer, only value");
    }

    this.valueSerializer = baseClaimCheckConfig.getConfiguredInstance(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_SERIALIZER_CLASS, Serializer.class);
    this.valueSerializer.configure(configs, false);
  }

  /**
   * Convert {@code data} into a byte array.
   *
   * @param topic   topic associated with data
   * @param headers headers associated with the record
   * @param data    typed data
   * @return serialized bytes
   */
  @Override
  public byte[] serialize(String topic, Headers headers, T data) {
    if (ClaimCheckUtils.isClaimCheckError(headers)) {
      String error = ClaimCheckUtils.getClaimCheckErrorStackTraceFromHeader(headers);

      throw new KafkaStorageException("Claim check interceptor error detected:\n" + error);
    } else if (ClaimCheckUtils.isClaimCheck(headers)) {
        // we need non-null value to trigger consumer serializer
        return new byte[0];
    } else {
      return valueSerializer.serialize(topic, headers, data);
    }

  }

  /**
   * Convert {@code data} into a byte array.
   *
   * @param topic topic associated with data
   * @param data  typed data
   * @return serialized bytes
   */
  @Override
  public byte[] serialize(String topic, T data) {
    throw new IllegalArgumentException("Need to use Kafka Client library >2.1.0 that passes "
        + "headers to serializer");
  }
}
