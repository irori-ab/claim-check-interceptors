package se.irori.kafka.claimcheck.azure;

import static se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys.AZURE_CREATE_CONTAINER_IF_NOT_EXISTS;
import static se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG;
import static se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG;
import static se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_IDENTITY_FROM_CONFIG;
import static se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
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

  private static final List<String> CREDENTIAL_CONFIGS =
      Arrays.asList(
          AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG,
          AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG,
          AZURE_STORAGE_ACCOUNT_IDENTITY_FROM_CONFIG);

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

  public static final String IDENTITY_FROM_DOCS = "Where to fetch the Azure identity from."
      + " Valid mechanisms are `default` (DefaultAzureCredential), "
      + " `managed_id_env:` (ManagedIdentityCredential), "
      + " or `managed_id_value:` (ManagedIdentityCredential). "
      + " For `managed_id_*` you supply the client id, either explicity as a value, or with"
      + " the name of an environment variable where to find it. "
      + " See <https://learn.microsoft.com/en-us/java/api/overview/azure/identity-readme?view=azure-java-stable#credential-classes>";

  public static final String CREATE_CONTAINER_DOCS = "Create the container if it does not exist. "
      + "*Note* this seems to require a SAS token with full account access. This does not work"
      + " well with SAS tokens limited to a specific container (topic)";

  enum SasTokenFromMechanism {
    VALUE, ENV, FILE
  }

  enum IdentityFromMechanism {
    DEFAULT, MANAGED_ID_ENV, MANAGED_ID_VALUE
  }

  static ConfigDef buildConfigDef(ConfigDef base) {
    base.define(AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM,
        SERVICE_ENDPOINT_DOCS);

    base.define(AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, CONNECTION_STRING_DOCS);

    base.define(AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, SASTOKEN_FROM_DOCS);

    base.define(AZURE_STORAGE_ACCOUNT_IDENTITY_FROM_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, IDENTITY_FROM_DOCS);

    base.define(AZURE_CREATE_CONTAINER_IF_NOT_EXISTS, ConfigDef.Type.BOOLEAN, false,
        ConfigDef.Importance.MEDIUM, CREATE_CONTAINER_DOCS);

    return base;
  }

  SasTokenFromMechanism getSasTokenFromMechanism() {
    String mechanism = getString(AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG);
    return SasTokenFromMechanism.valueOf(mechanism.substring(0,
        mechanism.indexOf(':')).toUpperCase());
  }

  IdentityFromMechanism getIdentityFromMechanism() {
    String value = getString(AZURE_STORAGE_ACCOUNT_IDENTITY_FROM_CONFIG);
    String mechanism = value.contains(":")
        ? value.substring(0, value.indexOf(':'))
        : value;
    return IdentityFromMechanism.valueOf(mechanism.toUpperCase());
  }

  public String getFromSecondPart(String config) {
    String value = getString(config);
    return value.substring(value.indexOf(':') + 1);
  }

  public String getSasTokenFromSpecifier() {
    return getFromSecondPart(AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG);
  }

  public String getIdentityFromSpecifier() {
    return getFromSecondPart(AZURE_STORAGE_ACCOUNT_IDENTITY_FROM_CONFIG);
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

  public TokenCredential getIdentityCredential() {
    switch (getIdentityFromMechanism()) {
      case DEFAULT:
        return new DefaultAzureCredentialBuilder().build();
      case MANAGED_ID_VALUE:
        return new ManagedIdentityCredentialBuilder()
            .clientId(getIdentityFromSpecifier()).build();
      case MANAGED_ID_ENV:
        return new ManagedIdentityCredentialBuilder()
            .clientId(System.getenv(getIdentityFromSpecifier())).build();
      default:
        throw new ConfigException("Invalid Identity specification: "
            + getIdentityFromMechanism() + " " + getIdentityFromSpecifier());
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
    List<String> setCredentialConfigs = CREDENTIAL_CONFIGS.stream()
        .filter(c -> this.getString(c) != null)
        .collect(Collectors.toList());

    if (setCredentialConfigs.size() != 1) {
      throw new ConfigException("You need to configure exactly one type of credential of ("
          + CREDENTIAL_CONFIGS + "), but found " + setCredentialConfigs.size() + ": "
          + setCredentialConfigs);
    }

    String credentialConfig = setCredentialConfigs.get(0);
    List<String> endpointConfigValues =
        ENDPOINT_CONFIGS.stream().map(this::getString)
            .collect(Collectors.toList());

    switch (credentialConfig) {

      case AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG:
        // connection string based config
        if (endpointConfigValues.stream().anyMatch(Objects::nonNull)) {
          throw new ConfigException("When '" + AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG
              + "' is set, you cannot set any of '" + ENDPOINT_CONFIGS + "'");
        }
        break;
      case AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG:
        if (getString(AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG) == null) {
          throw new ConfigException("When using credential config '" + credentialConfig
              + "' is set, you must set '" + AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG + "'");
        }
        try {
          getSasTokenFromMechanism();
        } catch (IllegalArgumentException e) {
          throw new ConfigException("Invalid Sas token mechanism: "
              + getString(AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG));
        }
        break;
      case AZURE_STORAGE_ACCOUNT_IDENTITY_FROM_CONFIG:
        if (getString(AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG) == null) {
          throw new ConfigException("When using credential config '" + credentialConfig
              + "' is set, you must set '" + AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG + "'");
        }
        try {
          getIdentityFromMechanism();
        } catch (IllegalArgumentException e) {
          throw new ConfigException("Invalid identity mechanism: "
              + getString(AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG));
        }
        break;
      default:
        throw new ConfigException("Unknown credential setting: " + credentialConfig);
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

    public static final String AZURE_STORAGE_ACCOUNT_IDENTITY_FROM_CONFIG
        = "azure.blob.storage.account.identity.from";

    public static final String AZURE_CREATE_CONTAINER_IF_NOT_EXISTS
        = "azure.blob.create.container.if.not.exists";

  }
}
