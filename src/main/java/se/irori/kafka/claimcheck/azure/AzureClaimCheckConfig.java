package se.irori.kafka.claimcheck.azure;

import static se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG;
import static se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG;
import static se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;


/**
 * Configuration for Azure Blob Storage interceptors.
 */
public class AzureClaimCheckConfig extends AbstractConfig {

  private static final List<String> ENDPOINT_CONFIGS =
      Arrays.asList(AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG,
          AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG);

  enum SasTokenFromMechanism {
    VALUE, ENV, FILE
  }

  private SasTokenFromMechanism sasTokenFromMechanism;

  private static ConfigDef buildConfigDef(ConfigDef base) {
    base.define(AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, "TODO docs");

    base.define(AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, "TODO docs");

    base.define(AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, "'path:...', 'env:..', 'value:...'");

    return base;
  }

  SasTokenFromMechanism getSasTokenFromMechanism() {
    String mechanism = getString(AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG);
    return SasTokenFromMechanism.valueOf(mechanism.substring(0,
        mechanism.indexOf(':')).toUpperCase());
  }

  public String getSasTokenFromSpecifier() {
    String mechanism = getString(AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG);
    return mechanism.substring(mechanism.indexOf(':') + 1);
  }

  /**
   * Get the SAS token using the configured mechanism.
   *
   * @return the configured SAS token
   */
  public String getSasToken() {
    switch (getSasTokenFromMechanism()) {
      case ENV:
        return System.getenv(getSasTokenFromSpecifier());
      case FILE:
        try {
          return new String(Files.readAllBytes(Paths.get(getSasTokenFromSpecifier())),
              StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
          throw new ConfigException("Invalid Sas Token file: " + getSasTokenFromSpecifier(), e);
        }
      case VALUE:
        return getSasTokenFromSpecifier();
      default:
        throw new ConfigException("Invalid SasToken specification: "
            + getSasTokenFromMechanism() + " " + getSasTokenFromSpecifier());
    }
  }

  /**
   * Construct, parse and validate a map of configurations.
   *
   * @param originals config options
   * @return a validated configuration object
   * @throws ConfigException if any config option is invalid
   */
  public static AzureClaimCheckConfig validatedConfig(Map<?, ?> originals) {
    AzureClaimCheckConfig azureClaimCheckConfig = new AzureClaimCheckConfig(originals);
    azureClaimCheckConfig.validate();
    return azureClaimCheckConfig;
  }

  private AzureClaimCheckConfig(ConfigDef configDef, Map<?, ?> originals) {
    super(buildConfigDef(configDef), originals, false);
  }

  private AzureClaimCheckConfig(Map<?, ?> originals) {
    this(new ConfigDef(), originals);
  }

  /**
   * Validate config properties not covered by @see {@link ConfigDef#parse(Map)} which is called
   * in {@link AbstractConfig} constructor.
   *
   * @throws org.apache.kafka.common.config.ConfigException if configuration is not valid
   */
  private void validate() {


    List<String> endpointConfigValues =
        ENDPOINT_CONFIGS.stream().map(this::getString)
            .collect(Collectors.toList());

    String connectionString = getString(AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG);

    if (connectionString != null) {
      // connection string based config
      if (endpointConfigValues.stream().anyMatch(Objects::nonNull)) {
        throw new ConfigException("When '" + AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG
            + "' is set, you cannot set any of '" + ENDPOINT_CONFIGS + "'");
      }
    } else {
      // endpoint based config
      if (!endpointConfigValues.stream().allMatch(Objects::nonNull)) {
        throw new ConfigException("When using endpoint based Azure configuration, you must set "
            + "all of " + ENDPOINT_CONFIGS);
      }

      try {
        getSasTokenFromMechanism();
      } catch (IllegalArgumentException e) {
        throw new ConfigException("Invalid Sas token mechanism: "
            + getString(AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG));
      }
    }
  }


  /**
   * Config keys for configuring Claim Check interceptors.
   */
  public static class Keys {

    public static final String AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG
        = "azure.blob.storage.account.endpoint";

    public static final String AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG
        = "azure.blob.storage.account.connectionstring";

    public static final String AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG
        = "azure.blob.storage.account.sastoken.from";

    public static final String CLAIMCHECK_CHECKIN_UNCOMPRESSED_SIZE_OVER_BYTES_CONFIG
        = "claimcheck.checkin.uncompressed-size.over.bytes";

  }
}
