package se.irori.kafka.claimcheck.s3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import se.irori.kafka.claimcheck.ClaimCheck;
import se.irori.kafka.claimcheck.TestUtils;
import se.irori.kafka.claimcheck.s3.S3ClaimCheckConfig.Keys;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Tests the S3 backend against an in-process S3Proxy.
 */
public class S3ClaimCheckBackendTest {

  @Rule
  public S3ProxyRule s3Proxy = S3TestSupport.newS3Proxy();

  private S3ClaimCheckBackend backend;

  @Before
  public void setUp() {
    Map<String, Object> config = S3TestSupport.s3Config(s3Proxy);
    config.put(Keys.S3_KEY_PREFIX_CONFIG, "cc/");
    backend = new S3ClaimCheckBackend();
    backend.configure(config);
  }

  @After
  public void tearDown() {
    backend.close();
  }

  @Test
  public void checkInCheckOutBytes() {
    byte[] payload = TestUtils.getRandomBytes(2 * 1024 * 1024);

    ClaimCheck claimCheck =
        backend.checkIn(new ProducerRecord<>("my-topic", new byte[0], payload));

    assertTrue(claimCheck.getReference(),
        claimCheck.getReference().startsWith("s3://" + S3TestSupport.BUCKET + "/cc/my-topic/"));
    assertArrayEquals(payload, backend.checkOut(claimCheck));
  }

  @Test
  public void checkInCheckOutStreaming() throws IOException {
    byte[] payload = TestUtils.getRandomBytes(20 * 1024 * 1024);

    ClaimCheck claimCheck = backend.checkInStreaming("my-topic",
        new ByteArrayInputStream(payload), payload.length);

    try (InputStream in = backend.checkOutStreaming(claimCheck)) {
      assertArrayEquals(payload, in.readAllBytes());
    }
  }

  @Test
  public void checkOutStreamingIsLazy() throws IOException {
    ClaimCheck missing = new ClaimCheck("s3://" + S3TestSupport.BUCKET + "/does/not/exist");

    // no request is made until the stream is read
    LazyS3InputStream in = (LazyS3InputStream) backend.checkOutStreaming(missing);
    assertFalse(in.isOpened());

    try {
      in.read();
      fail("expected missing key error on first read");
    } catch (NoSuchKeyException expected) {
      // expected
    } finally {
      in.close();
    }
  }

  @Test
  public void closeBeforeEndOfStream() throws IOException {
    byte[] payload = TestUtils.getRandomBytes(5 * 1024 * 1024);
    ClaimCheck claimCheck = backend.checkInStreaming("my-topic",
        new ByteArrayInputStream(payload), payload.length);

    InputStream in = backend.checkOutStreaming(claimCheck);
    byte[] start = new byte[1024];
    assertEquals(1024, in.readNBytes(start, 0, start.length));
    in.close();

    // the backend (and its connection pool) is still usable after an aborted download
    try (InputStream again = backend.checkOutStreaming(claimCheck)) {
      assertArrayEquals(payload, again.readAllBytes());
    }
  }

  @Test(expected = KafkaStorageException.class)
  public void rejectsPayloadOverSinglePutLimit() {
    backend.checkInStreaming("my-topic", new ByteArrayInputStream(new byte[0]),
        S3ClaimCheckBackend.MAX_SINGLE_PUT_BYTES + 1);
  }

  @Test
  public void parseReference() {
    GetObjectRequest request = S3ClaimCheckBackend.getRequest(
        new ClaimCheck("s3://bucket/prefix/topic/uuid"));
    assertEquals("bucket", request.bucket());
    assertEquals("prefix/topic/uuid", request.key());
  }

  @Test
  public void badReferences() {
    for (String ref : new String[] {"https://bucket/key", "s3://bucket", "s3:///key",
        "s3://bucket/"}) {
      try {
        S3ClaimCheckBackend.getRequest(new ClaimCheck(ref));
        fail("expected error for " + ref);
      } catch (KafkaStorageException expected) {
        // expected
      }
    }
  }
}
