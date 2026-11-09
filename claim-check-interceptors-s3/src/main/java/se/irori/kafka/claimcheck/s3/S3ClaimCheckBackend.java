package se.irori.kafka.claimcheck.s3;

import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.KafkaStorageException;
import se.irori.kafka.claimcheck.ClaimCheck;
import se.irori.kafka.claimcheck.ClaimCheckBackend;
import se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Implementation of the ClaimCheckBackend backed by AWS S3 (or an S3 compatible store), using
 * the AWS SDK v2.
 *
 * <p>Payloads are stored in a single configured bucket, under the key
 * {@code <prefix><topic>/<uuid>}. The claim check reference is {@code s3://<bucket>/<key>}.
 */
public class S3ClaimCheckBackend implements ClaimCheckBackend {

  static final String S3_SCHEME_PREFIX = "s3://";

  /**
   * Max size of a single PutObject upload. Payloads are assumed to stay below this, so
   * multipart upload is deliberately not supported.
   */
  static final long MAX_SINGLE_PUT_BYTES = 5L * 1024 * 1024 * 1024;

  private S3Client s3Client;

  private String bucket;

  private String keyPrefix;

  @Override
  public ClaimCheck checkIn(ProducerRecord<byte[], byte[]> largeRecord) {
    String key = newKey(largeRecord.topic());
    s3Client.putObject(putRequest(key), RequestBody.fromBytes(largeRecord.value()));
    return new ClaimCheck(S3_SCHEME_PREFIX + bucket + "/" + key);
  }

  @Override
  public ClaimCheck checkInStreaming(String topic, InputStream payload, long payloadSize) {
    if (payloadSize > MAX_SINGLE_PUT_BYTES) {
      throw new KafkaStorageException("Payload size " + payloadSize + " exceeds the S3 single "
          + "PUT limit of " + MAX_SINGLE_PUT_BYTES + " bytes, which is the max supported size");
    }
    String key = newKey(topic);
    // streams the payload with a known content length, without buffering it fully in memory
    s3Client.putObject(putRequest(key), RequestBody.fromInputStream(payload, payloadSize));
    return new ClaimCheck(S3_SCHEME_PREFIX + bucket + "/" + key);
  }

  @Override
  public byte[] checkOut(ClaimCheck claimCheck) {
    return s3Client.getObjectAsBytes(getRequest(claimCheck)).asByteArray();
  }

  /**
   * Retrieve the payload as a stream. The GET request is deferred until the stream is first read,
   * so that a consumer poll returning many claim checks does not hold one open HTTP connection
   * per record. Callers must close the returned stream.
   */
  @Override
  public InputStream checkOutStreaming(ClaimCheck claimCheck) {
    GetObjectRequest request = getRequest(claimCheck);
    return new LazyS3InputStream(() -> s3Client.getObject(request));
  }

  private String newKey(String topic) {
    return keyPrefix + topic + "/" + UUID.randomUUID();
  }

  private PutObjectRequest putRequest(String key) {
    return PutObjectRequest.builder().bucket(bucket).key(key).build();
  }

  static GetObjectRequest getRequest(ClaimCheck claimCheck) {
    String reference = claimCheck.getReference();
    int bucketEnd = reference.indexOf('/', S3_SCHEME_PREFIX.length());
    if (!reference.startsWith(S3_SCHEME_PREFIX) || bucketEnd < 0
        || bucketEnd == S3_SCHEME_PREFIX.length() || bucketEnd == reference.length() - 1) {
      throw new KafkaStorageException("Bad S3 claim check reference: " + reference);
    }
    return GetObjectRequest.builder()
        .bucket(reference.substring(S3_SCHEME_PREFIX.length(), bucketEnd))
        .key(reference.substring(bucketEnd + 1))
        .build();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    S3ClaimCheckConfig config = S3ClaimCheckConfig.validatedConfig(configs);

    bucket = config.getString(Keys.S3_BUCKET_CONFIG);
    keyPrefix = config.getString(Keys.S3_KEY_PREFIX_CONFIG);

    S3ClientBuilder builder = S3Client.builder()
        .forcePathStyle(config.getBoolean(Keys.S3_PATH_STYLE_ACCESS_CONFIG));

    String region = config.getString(Keys.S3_REGION_CONFIG);
    if (region != null) {
      builder.region(Region.of(region));
    }
    String endpointOverride = config.getString(Keys.S3_ENDPOINT_OVERRIDE_CONFIG);
    if (endpointOverride != null) {
      builder.endpointOverride(URI.create(endpointOverride));
    }
    String accessKeyId = config.getString(Keys.S3_ACCESS_KEY_ID_CONFIG);
    Password secretAccessKey = config.getPassword(Keys.S3_SECRET_ACCESS_KEY_CONFIG);
    if (accessKeyId != null && secretAccessKey != null) {
      builder.credentialsProvider(StaticCredentialsProvider.create(
          AwsBasicCredentials.create(accessKeyId, secretAccessKey.value())));
    }
    if (config.getBoolean(Keys.S3_CHECKSUM_WHEN_REQUIRED_CONFIG)) {
      builder.requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED);
    }

    s3Client = builder.build();

    if (config.getBoolean(Keys.S3_CREATE_BUCKET_IF_NOT_EXISTS)) {
      createBucketIfNotExists();
    }
  }

  private void createBucketIfNotExists() {
    try {
      s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
    } catch (S3Exception e) {
      if (e.statusCode() != 404) {
        throw e;
      }
      try {
        s3Client.createBucket(b -> b.bucket(bucket));
      } catch (BucketAlreadyOwnedByYouException ignored) {
        // created concurrently, e.g. by a producer and consumer in the same JVM
      }
    }
  }

  @Override
  public void close() {
    if (s3Client != null) {
      s3Client.close();
    }
  }
}
