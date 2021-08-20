package se.irori.kafka.claimcheck.azure;

import static se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys.CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG;

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

  // TODO: does this account for headers as well?
  private static final int CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_DEFAULT = 1048588;

  private static ConfigDef buildConfigDef(ConfigDef base) {
    base.define(CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG, ConfigDef.Type.LONG,
        CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_DEFAULT,
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


}
