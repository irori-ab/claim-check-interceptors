package se.irori.kafka.claimcheck.azurev8;

import static org.junit.Assert.*;

import java.util.HashMap;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import se.irori.kafka.claimcheck.azurev8.AzureClaimCheckConfig.Keys;

/**
 * Unit test AzureClaimCheckConfig for valid config combinations.
 */
public class AzureClaimCheckConfigTest {

  private AzureClaimCheckConfig unit;

  @Test(expected = ConfigException.class)
  public void testErrorOnEmpty() {
    HashMap<String,String> config = new HashMap<>();
    unit = AzureClaimCheckConfig.validatedConfig(config);
  }

  @Test
  public void testEndpointOk() {
    HashMap<String,String> config = new HashMap<>();
    config.put(Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG, "https://someEndpoint");
    config.put(Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG, "value:testSasToken");

    unit = AzureClaimCheckConfig.validatedConfig(config);
  }

  @Test(expected = ConfigException.class)
  public void testEndpointIncompleteError() {
    HashMap<String,String> config = new HashMap<>();
    config.put(Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG, "https://someEndpoint");

    unit = AzureClaimCheckConfig.validatedConfig(config);
  }

  @Test(expected = ConfigException.class)
  public void testEndpointAndConnectionStringError() {
    HashMap<String,String> config = new HashMap<>();
    config.put(Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG, "https://someEndpoint");
    config.put(Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG, "value:testSasToken");
    config.put(Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG, "connString");

    unit = AzureClaimCheckConfig.validatedConfig(config);
  }

  @Test()
  public void testConnectionStringOk() {
    HashMap<String,String> config = new HashMap<>();
    config.put(Keys.AZURE_STORAGE_ACCOUNT_CONNECTION_STRING_CONFIG, "connString");

    unit = AzureClaimCheckConfig.validatedConfig(config);
  }


  @Test
  public void testSasTokenMechanismOk() {
    HashMap<String,String> config = new HashMap<>();
    config.put(Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG, "https://someEndpoint");
    config.put(Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG, "value:testSasToken");

    unit = AzureClaimCheckConfig.validatedConfig(config);

    assertEquals(AzureClaimCheckConfig.SasTokenFromMechanism.VALUE,
        unit.getSasTokenFromMechanism());
    assertEquals("testSasToken", unit.getSasTokenFromSpecifier());
  }

  @Test(expected = ConfigException.class)
  public void testSasTokenMechanismError() {
    HashMap<String,String> config = new HashMap<>();
    config.put(Keys.AZURE_STORAGE_ACCOUNT_ENDPOINT_CONFIG, "https://someEndpoint");
    config.put(Keys.AZURE_STORAGE_ACCOUNT_SASTOKEN_FROM_CONFIG, "blah:testSasToken");

    unit = AzureClaimCheckConfig.validatedConfig(config);
  }

  @Test
  public void generateDocs() {
    ConfigDef configDef = AzureClaimCheckConfig.buildConfigDef(new ConfigDef());
    // RST / markdown hack
    String rst = configDef.toEnrichedRst().replace("``", "`");

    System.out.println("-----");
    System.out.println(rst);
    System.out.println("-----");

    // RST output seems to work fine as markdown
  }

}