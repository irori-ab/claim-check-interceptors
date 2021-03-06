package se.irori.kafka.claimcheck.azurev8;

import static se.irori.kafka.claimcheck.azurev8.AzureClaimCheckConfig.Keys.AZURE_CREATE_CONTAINER_IF_NOT_EXISTS;
import static se.irori.kafka.claimcheck.azurev8.AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG;
import static se.irori.kafka.claimcheck.azurev8.AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG;
import static se.irori.kafka.claimcheck.azurev8.AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG;

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

  public static final String SERVICE_ENDPOINT_DOCS =
      "Storage account service endpoint. Either use this in combination with `"
          + AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG + "` or instead configure `"
          + AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG + "`";

  public static final String CONNECTION_STRING_DOCS = "Configure a connection string to connect to"
      + " the storage account. See [docs]"
      + "(https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string)";

  public static final String SASTOKEN_FROM_DOCS = "Where to fetch the SAS token credential from."
      + " Valid mechanisms are `path:`, `env:`, or `value:`. Use *path* for specifying a path to a"
      + " file on disk. Use *env* for fetching from an enviroment variable. Or use *value* to"
      + " specify the SAS token value explicitly.";

  public static final String CREATE_CONTAINER_DOCS = "Create the container if it does not exist. "
      + "*Note* this seems to require a SAS token with full account access. This does not work"
      + " well with SAS tokens limited to a specific container (topic)";

  enum SasTokenFromMechanism {
    VALUE, ENV, FILE
  }

  static ConfigDef buildConfigDef(ConfigDef base) {
    base.define(AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM,
        SERVICE_ENDPOINT_DOCS);

    base.define(AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, CONNECTION_STRING_DOCS);

    base.define(AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, SASTOKEN_FROM_DOCS);

    base.define(AZURE_CREATE_CONTAINER_IF_NOT_EXISTS, ConfigDef.Type.BOOLEAN, false,
        ConfigDef.Importance.MEDIUM, CREATE_CONTAINER_DOCS);

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

    public static final String AZURE_CREATE_CONTAINER_IF_NOT_EXISTS
        = "azure.blob.create.container.if.not.exists";

  }
}
