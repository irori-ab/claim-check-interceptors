package se.irori.kafka.claimcheck;

import static se.irori.kafka.claimcheck.ClaimCheckUtils.getClaimCheckRefFromHeader;
import static se.irori.kafka.claimcheck.ClaimCheckUtils.isClaimCheck;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClaimCheckDeserializer<T> implements Deserializer<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ClaimCheckDeserializer.class);

  private Deserializer<T> valueDeserializer;
  private ClaimCheckBackend claimCheckBackend;

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
      throw new ConfigException("Should not be used to wrap key serializer, only value");
    }

    this.valueDeserializer = baseClaimCheckConfig.getConfiguredInstance(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_WRAPPED_VALUE_DESERIALIZER_CLASS, Deserializer.class);
    this.valueDeserializer.configure(configs, false);

    this.claimCheckBackend = baseClaimCheckConfig.getConfiguredInstance(
        BaseClaimCheckConfig.Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, ClaimCheckBackend.class);
  }

  /**
   * Deserialize a record value from a byte array into a value or object.
   *
   * @param topic topic associated with the data
   * @param data  serialized bytes; may be null; implementations are recommended to handle null by
   *              returning a value or null rather than throwing an exception.
   * @return deserialized typed data; may be null
   */
  @Override
  public T deserialize(String topic, byte[] data) {
    throw new IllegalArgumentException("Need to use Kafka Client library >2.1.0 that passes "
        + "headers to deserializer");
  }

  /**
   * Deserialize a record value from a byte array into a value or object.
   *
   * @param topic   topic associated with the data
   * @param headers headers associated with the record; may be empty.
   * @param data    serialized bytes; may be null; implementations are recommended to handle null
   *                by returning a value or null rather than throwing an exception.
   * @return deserialized typed data; may be null
   */
  @Override
  public T deserialize(String topic, Headers headers, byte[] data) {

    if (isClaimCheck(headers)) {

      ClaimCheck claimCheck = new ClaimCheck(getClaimCheckRefFromHeader(headers));
      LOG.trace("received claim check: topic={}, ref={}",
          topic, claimCheck.getReference());
      T deserializedValue = valueDeserializer
          .deserialize(topic, headers, claimCheckBackend.checkOut(claimCheck));
      LOG.trace("checked out claim check: topic={}, ref={}",
          topic, claimCheck.getReference());
      return deserializedValue;
    } else {
      return valueDeserializer
          .deserialize(topic, headers, data);
    }
  }
}
