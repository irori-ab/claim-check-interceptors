package se.irori.kafka.claimcheck.s3;

import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_ACCESS_KEY_ID_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_BUCKET_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_CHECKSUM_WHEN_REQUIRED_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_CREATE_BUCKET_IF_NOT_EXISTS;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_ENDPOINT_OVERRIDE_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_KEY_PREFIX_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_PATH_STYLE_ACCESS_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_REGION_CONFIG;
import static se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys.S3_SECRET_ACCESS_KEY_CONFIG;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;


/**
 * Configuration for the AWS S3 (or S3 compatible) Claim Check backend.
 */
public class S3ClaimCheckConfig extends AbstractConfig {

  public static final String BUCKET_DOCS = "The S3 bucket to store claim checked payloads in. "
      + "Objects are stored under the key `<prefix><topic>/<uuid>`, so IAM policies can be "
      + "scoped per topic.";

  public static final String REGION_DOCS = "AWS region of the bucket. If not set, the AWS SDK "
      + "default region provider chain is used (e.g. `AWS_REGION` environment variable).";

  public static final String ENDPOINT_OVERRIDE_DOCS = "Override the S3 endpoint URL, e.g. for "
      + "S3 compatible stores such as MinIO, or for testing. Leave unset for AWS S3.";

  public static final String PATH_STYLE_ACCESS_DOCS = "Use path style access "
      + "(`https://host/bucket/key`) instead of virtual hosted style. Often needed for "
      + "S3 compatible stores.";

  public static final String KEY_PREFIX_DOCS = "Optional prefix for all object keys, "
      + "e.g. `claim-check/`.";

  public static final String ACCESS_KEY_ID_DOCS = "Optional static access key id. If not set, "
      + "the AWS SDK default credentials provider chain is used (environment, profile, "
      + "web identity / IRSA, instance profile). Must be set together with `"
      + S3_SECRET_ACCESS_KEY_CONFIG + "`.";

  public static final String SECRET_ACCESS_KEY_DOCS = "Optional static secret access key. See `"
      + S3_ACCESS_KEY_ID_CONFIG + "`.";

  public static final String CREATE_BUCKET_DOCS = "Create the bucket if it does not exist. "
      + "Mostly useful for testing.";

  public static final String CHECKSUM_WHEN_REQUIRED_DOCS = "Only calculate request checksums "
      + "when required by the operation, instead of the AWS SDK default of always. Enable this "
      + "for S3 compatible stores that do not support the newer flexible checksums.";

  static ConfigDef buildConfigDef(ConfigDef base) {
    base.define(S3_BUCKET_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
        new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, BUCKET_DOCS);

    base.define(S3_REGION_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, REGION_DOCS);

    base.define(S3_ENDPOINT_OVERRIDE_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, ENDPOINT_OVERRIDE_DOCS);

    base.define(S3_PATH_STYLE_ACCESS_CONFIG, ConfigDef.Type.BOOLEAN, false,
        ConfigDef.Importance.MEDIUM, PATH_STYLE_ACCESS_DOCS);

    base.define(S3_KEY_PREFIX_CONFIG, ConfigDef.Type.STRING, "",
        ConfigDef.Importance.LOW, KEY_PREFIX_DOCS);

    base.define(S3_ACCESS_KEY_ID_CONFIG, ConfigDef.Type.STRING, null,
        ConfigDef.Importance.MEDIUM, ACCESS_KEY_ID_DOCS);

    base.define(S3_SECRET_ACCESS_KEY_CONFIG, ConfigDef.Type.PASSWORD, null,
        ConfigDef.Importance.MEDIUM, SECRET_ACCESS_KEY_DOCS);

    base.define(S3_CREATE_BUCKET_IF_NOT_EXISTS, ConfigDef.Type.BOOLEAN, false,
        ConfigDef.Importance.LOW, CREATE_BUCKET_DOCS);

    base.define(S3_CHECKSUM_WHEN_REQUIRED_CONFIG, ConfigDef.Type.BOOLEAN, false,
        ConfigDef.Importance.LOW, CHECKSUM_WHEN_REQUIRED_DOCS);

    return base;
  }

  /**
   * Construct, parse and validate a map of configurations.
   *
   * @param originals config options
   * @return a validated configuration object
   * @throws ConfigException if any config option is invalid
   */
  public static S3ClaimCheckConfig validatedConfig(Map<?, ?> originals) {
    S3ClaimCheckConfig s3ClaimCheckConfig = new S3ClaimCheckConfig(originals);
    s3ClaimCheckConfig.validate();
    return s3ClaimCheckConfig;
  }

  private S3ClaimCheckConfig(ConfigDef configDef, Map<?, ?> originals) {
    super(buildConfigDef(configDef), originals, false);
  }

  private S3ClaimCheckConfig(Map<?, ?> originals) {
    this(new ConfigDef(), originals);
  }

  /**
   * Validate config properties not covered by @see {@link ConfigDef#parse(Map)} which is called
   * in {@link AbstractConfig} constructor.
   *
   * @throws ConfigException if configuration is not valid
   */
  private void validate() {
    String accessKeyId = getString(S3_ACCESS_KEY_ID_CONFIG);
    Password secretAccessKey = getPassword(S3_SECRET_ACCESS_KEY_CONFIG);
    if ((accessKeyId == null) != (secretAccessKey == null)) {
      throw new ConfigException("'" + S3_ACCESS_KEY_ID_CONFIG + "' and '"
          + S3_SECRET_ACCESS_KEY_CONFIG + "' must be set together");
    }
  }

  /**
   * Config keys for configuring the S3 Claim Check backend.
   */
  public static class Keys {

    public static final String S3_BUCKET_CONFIG = "s3.bucket";

    public static final String S3_REGION_CONFIG = "s3.region";

    public static final String S3_ENDPOINT_OVERRIDE_CONFIG = "s3.endpoint.override";

    public static final String S3_PATH_STYLE_ACCESS_CONFIG = "s3.path.style.access.enabled";

    public static final String S3_KEY_PREFIX_CONFIG = "s3.key.prefix";

    public static final String S3_ACCESS_KEY_ID_CONFIG = "s3.access.key.id";

    public static final String S3_SECRET_ACCESS_KEY_CONFIG = "s3.secret.access.key";

    public static final String S3_CREATE_BUCKET_IF_NOT_EXISTS = "s3.create.bucket.if.not.exists";

    public static final String S3_CHECKSUM_WHEN_REQUIRED_CONFIG = "s3.checksum.when.required";

  }
}
