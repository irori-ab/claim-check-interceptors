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
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    BaseClaimCheckConfig baseClaimCheckConfig = BaseClaimCheckConfig.validatedConfig(configs);
    if (isKey) {
      throw new IllegalStateException("Cannot wrap key serializer");
    }

    // TODO: validate using CC interceptor?

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
    if (data == null) {
      if (ClaimCheckUtils.isClaimCheck(headers)) {
        // we need non-null value to trigger consumer serializer
        return new byte[0];
      } else {
        // assume claim check interceptor had some error
        // NOTE: this excludes log compaction tombstone usecases for now
        throw new KafkaStorageException("Null message but no claim check headers from "
            + "interceptor, please check logs for interceptor errors");
      }
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
