package se.irori.kafka.claimcheck;

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

  private static ConfigDef buildConfigDef(ConfigDef base) {
    base.define(Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_CONFIG,
        ConfigDef.Type.LONG,
        CLAIMCHECK_CHECKIN_UNCOMPRESSED_BATCH_SIZE_OVER_BYTES_DEFAULT,
        ConfigDef.Importance.MEDIUM, "TODO docs");

    base.define(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.MEDIUM, "TODO docs");

    base.define(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.MEDIUM, "TODO docs");

    base.define(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.MEDIUM, "TODO docs");

    base.define(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.MEDIUM, "TODO docs");

    base.define(Keys.CLAIMCHECK_BACKEND_CLASS_CONFIG, ConfigDef.Type.CLASS,
        null,
        ConfigDef.Importance.MEDIUM, "TODO docs");

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
    BaseClaimCheckConfig baseAzureClaimCheckConfig = new BaseClaimCheckConfig(originals);
    baseAzureClaimCheckConfig.validate();
    return baseAzureClaimCheckConfig;
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
  }

}
