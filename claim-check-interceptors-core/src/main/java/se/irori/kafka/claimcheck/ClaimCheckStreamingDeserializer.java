package se.irori.kafka.claimcheck;

import static se.irori.kafka.claimcheck.ClaimCheckUtils.getClaimCheckRefFromHeader;
import static se.irori.kafka.claimcheck.ClaimCheckUtils.isClaimCheck;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes Kafka messages that are potentially Claim Check messages.
 *
 * <p>If messages carry the `message-claim-check` header, the configured Claim Check backend
 * is used to fetch the real payload from the underlying store.
 *
 * <p>The configured wrapped de-serializer is used to de-serialize the message bytes that either
 * come from the claim check backend or directly as Kafka message value.
 *
 */
public class ClaimCheckStreamingDeserializer implements Deserializer<InputStream> {

  private static final Logger LOG = LoggerFactory.getLogger(ClaimCheckStreamingDeserializer.class);

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
  public InputStream deserialize(String topic, byte[] data) {
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
  public InputStream deserialize(String topic, Headers headers, byte[] data) {

    if (isClaimCheck(headers)) {
      ClaimCheck claimCheck = new ClaimCheck(getClaimCheckRefFromHeader(headers));

      LOG.trace("received claim check: topic={}, ref={}",
          topic, claimCheck.getReference());
      InputStream deserializedValue = claimCheckBackend.checkOutStreaming(claimCheck);
      LOG.trace("checked out claim check: topic={}, ref={}",
          topic, claimCheck.getReference());
      return deserializedValue;
    } else {
      return new ByteArrayInputStream(data);
    }
  }
}
