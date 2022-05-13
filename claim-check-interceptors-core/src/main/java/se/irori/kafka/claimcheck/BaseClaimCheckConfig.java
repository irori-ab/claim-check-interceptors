package se.irori.kafka.claimcheck;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;


/**
 * Common configuration for all Claim Check interceptors.
 */
public class BaseClaimCheckConfig extends AbstractConfig {

  // Use default value for "max.request.size" -  The maximum size of a request in bytes.
  // conservatively with a 512 byte buffer
  // default: 1048576 (1024 * 1024)
  public static final int
      CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_DEFAULT = 1048576 - 512;

  public static final String CHECKIN_SIZE_OVER = "The the byte limit where Kafka record batches"
      + " above this size are checked in using the Claim Check backend. *Note*: this applies"
      + " to the uncompressed message batch size. If you want to optimize for more messages "
      + " not being checked in when compression is used, you will need to experiment with "
      + " compression ratios for your specific flow, and then increase this config.";

  public static final String BACKEND_DOCS = "The fully qualified name of the backend "
      + "implementation. E.g. `se.irori.kafka.claimcheck.azure.AzureBlobStorageClaimCheckBackend`";

  public static final String WRAPPED_DESERIALIZER_DOCS = "Set to the normal Kafka Consumer de-serializer"
      + " that would have been used before enabling Claim Check interceptors on the flow.";

  public static final String WRAPPED_SERIALIZER_DOCS = "Set to the normal Kafka Producer serializer"
      + " that would have been used before enabling Claim Check interceptors on the flow.";

  public static final String VALUE_SERIALIZER_DOCS = "Set to"
      + " `se.irori.kafka.claimcheck.ClaimCheckSerializer` for the Producer in a Claim Check"
      + " enabled flow.";

  public static final String INTERCEPTOR_CLASSES_DOCS = "Set to"
      + " `se.irori.kafka.claimcheck.ClaimCheckProducerInterceptor` for the Producer in a Claim Check"
      + " enabled flow.";

  public static final String DESERIALIZER_DOCS = "Set to"
      + " `se.irori.kafka.claimcheck.ClaimCheckDeserializer` for the Consumer in a Claim Check"
      + " enabled flow.";

  public static final String KEY_SERIALIZER_DOCS = "Standard Kafka key.serializer option. Used "
      + " for the calculation of message size to determine if it should be checked in.";

  static ConfigDef buildConfigDef(ConfigDef base) {
    base.define(Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG,
        ConfigDef.Type.LONG,
        CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_DEFAULT,
        ConfigDef.Importance.MEDIUM, CHECKIN_SIZE_OVER);

    base.define(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.MEDIUM, VALUE_SERIALIZER_DOCS);

    base.define(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.MEDIUM, DESERIALIZER_DOCS);

    base.define(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ConfigDef.Type.LIST,
        null,
        ConfigDef.Importance.MEDIUM, INTERCEPTOR_CLASSES_DOCS);

    base.define(Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, ConfigDef.Type.CLASS,
        NO_DEFAULT_VALUE,
        ConfigDef.Importance.MEDIUM, BACKEND_DOCS);

    base.define(Keys.CLAIMCHECK_WRAPPED_VALUE_DESERIALIZER_CLASS, ConfigDef.Type.CLASS,
        null,
        ConfigDef.Importance.MEDIUM, WRAPPED_DESERIALIZER_DOCS);

    base.define(Keys.CLAIMCHECK_WRAPPED_VALUE_SERIALIZER_CLASS, ConfigDef.Type.CLASS,
        null,
        ConfigDef.Importance.MEDIUM, WRAPPED_SERIALIZER_DOCS);

    base.define(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ConfigDef.Type.CLASS,
        null,
        ConfigDef.Importance.MEDIUM, KEY_SERIALIZER_DOCS);

    return base;
  }

  /**
   * Construct, parse and validate a map of configurations.
   *
   * @param originals config options
   * @return a validated configuration object
   * @throws ConfigException if any config option is invalid
   */
  public static BaseClaimCheckConfig validatedConfig(Map<?, ?> originals) {
    BaseClaimCheckConfig baseClaimCheckConfig = new BaseClaimCheckConfig(originals);
    baseClaimCheckConfig.validate();
    return baseClaimCheckConfig;
  }

  BaseClaimCheckConfig(ConfigDef configDef, Map<?, ?> originals) {
    super(buildConfigDef(configDef), originals, false);
  }

  BaseClaimCheckConfig(Map<?, ?> originals) {
    this(new ConfigDef(), originals);
  }

  /**
   * Validate config properties not covered by @see {@link ConfigDef#parse(Map)} which is called
   * in {@link AbstractConfig} constructor.
   *
   * @throws ConfigException if configuration is not valid
   */
  void validate() {
    // assume parse is enough
  }

  /**
   * Config keys for {@link BaseClaimCheckConfig}.
   */
  public static class Keys {
    public static final String CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG
        = "claimcheck.checkin.uncompressed-batch-size.over.bytes";

    public static final String CLAIMCHECK_BACKEND_CLASS_CONFIG
        = "claimcheck.backend.class";

    public static final String CLAIMCHECK_WRAPPED_VALUE_DESERIALIZER_CLASS
        = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
          + ".wrapped.deserializer";

    public static final String CLAIMCHECK_WRAPPED_VALUE_SERIALIZER_CLASS
        = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
        + ".wrapped.serializer";
  }

}
