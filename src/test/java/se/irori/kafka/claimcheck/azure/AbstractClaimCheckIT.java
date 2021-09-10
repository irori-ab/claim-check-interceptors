package se.irori.kafka.claimcheck.azure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.irori.kafka.claimcheck.azure.AzureClaimCheckConfig.Keys;

/**
 * Helper methods for integration tests to inject dynamic config from system properties (typically
 * set via Maven profiles), or defaults.
 */
public class AbstractClaimCheckIT {

  private static final List<String> CONFIG_FROM_SYS_PROPS = Arrays.asList(
      Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG,
      Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG,
      Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG
  );

  private static final Logger LOG = LoggerFactory.getLogger(AbstractClaimCheckIT.class);

  /**
   * Inject config based on system properties (if present), or configure for the Azurite
   * emulator.
   *
   * @param baseConfig config map to inject properties into
   * @param azuriteContainer initialized azurite container
   * @param withPrefix potential prefix prepend when looking up config to look for (eg 'producer_')
   *                   or null for no prefix
   */
  public void injectConfigFromSystemProperties(Map<String, Object> baseConfig,
                                               AzuriteContainer azuriteContainer,
                                               String withPrefix) {
    String prefix = withPrefix == null ? "" : withPrefix;
    for (String k : CONFIG_FROM_SYS_PROPS) {
      String value = System.getProperty(prefix + k);
      if (value != null && !value.trim().isEmpty()) {
        baseConfig.put(k, value);
      }
    }

    if (!baseConfig.containsKey(Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG) &&
        !baseConfig.containsKey(Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG)) {
      // assume Azurite emulator default settings
      // https://github.com/Azure/Azurite#default-storage-account
      LOG.info("defaulting to Azurite client config (injecting for prefix: {})", withPrefix);
      String token =
          "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
      String host = "" + azuriteContainer.getContainerIpAddress();
      String account = "devstoreaccount1";
      // http://<local-machine-address>:<port>/<account-name>/<resource-path>
      String endpoint = String.format("http://%s:%d/%s/",
          host,
          azuriteContainer.getMappedPort(10000),
          account);
      String connectionString = "" +
          "DefaultEndpointsProtocol=http;" +
          "AccountName=devstoreaccount1;" +
          "BlobEndpoint=" + endpoint + ";" +
          "AccountKey=" + token + ";";
      baseConfig.put(Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG,
          connectionString);
    }
  }
}
