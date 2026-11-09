package se.irori.kafka.claimcheck.s3;

import java.util.HashMap;
import java.util.Map;
import org.gaul.s3proxy.junit.S3ProxyRule;
import se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys;

/**
 * Helpers for tests against an in-process S3Proxy (S3 API over HTTP, in-memory storage).
 */
final class S3TestSupport {

  static final String BUCKET = "claim-check-test";

  private S3TestSupport() {
  }

  static S3ProxyRule newS3Proxy() {
    return S3ProxyRule.builder()
        .withBlobStoreProvider("transient")
        .withCredentials("access", "secret")
        .build();
  }

  static Map<String, Object> s3Config(S3ProxyRule s3Proxy) {
    Map<String, Object> config = new HashMap<>();
    config.put(Keys.S3_BUCKET_CONFIG, BUCKET);
    config.put(Keys.S3_REGION_CONFIG, "us-east-1");
    config.put(Keys.S3_ENDPOINT_OVERRIDE_CONFIG, s3Proxy.getUri().toString());
    config.put(Keys.S3_PATH_STYLE_ACCESS_CONFIG, true);
    config.put(Keys.S3_ACCESS_KEY_ID_CONFIG, s3Proxy.getAccessKey());
    config.put(Keys.S3_SECRET_ACCESS_KEY_CONFIG, s3Proxy.getSecretKey());
    config.put(Keys.S3_CREATE_BUCKET_IF_NOT_EXISTS, true);
    return config;
  }
}
