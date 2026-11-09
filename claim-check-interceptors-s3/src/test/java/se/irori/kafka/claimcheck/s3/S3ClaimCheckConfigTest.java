package se.irori.kafka.claimcheck.s3;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys;

public class S3ClaimCheckConfigTest {

  @Test
  public void minimalConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put(Keys.S3_BUCKET_CONFIG, "my-bucket");

    S3ClaimCheckConfig s3Config = S3ClaimCheckConfig.validatedConfig(config);

    assertEquals("my-bucket", s3Config.getString(Keys.S3_BUCKET_CONFIG));
    assertEquals("", s3Config.getString(Keys.S3_KEY_PREFIX_CONFIG));
  }

  @Test(expected = ConfigException.class)
  public void bucketRequired() {
    S3ClaimCheckConfig.validatedConfig(new HashMap<>());
  }

  @Test(expected = ConfigException.class)
  public void accessKeyWithoutSecret() {
    Map<String, Object> config = new HashMap<>();
    config.put(Keys.S3_BUCKET_CONFIG, "my-bucket");
    config.put(Keys.S3_ACCESS_KEY_ID_CONFIG, "AKIA...");

    S3ClaimCheckConfig.validatedConfig(config);
  }

  @Test(expected = ConfigException.class)
  public void secretWithoutAccessKey() {
    Map<String, Object> config = new HashMap<>();
    config.put(Keys.S3_BUCKET_CONFIG, "my-bucket");
    config.put(Keys.S3_SECRET_ACCESS_KEY_CONFIG, "secret");

    S3ClaimCheckConfig.validatedConfig(config);
  }

  @Test
  public void generateDocs() {
    ConfigDef configDef = S3ClaimCheckConfig.buildConfigDef(new ConfigDef());
    // RST / markdown hack
    String rst = configDef.toEnrichedRst().replace("``", "`");

    System.out.println("-----");
    System.out.println(rst);
    System.out.println("-----");
  }
}
