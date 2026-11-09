package se.irori.kafka.claimcheck.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;
import software.amazon.awssdk.core.ResponseInputStream;

/**
 * InputStream that opens the underlying S3 object stream on first use.
 *
 * <p>If closed before the end of the object is reached, the HTTP request is aborted instead of
 * draining the remaining (potentially very large) response body.
 */
class LazyS3InputStream extends InputStream {

  private final Supplier<? extends ResponseInputStream<?>> opener;

  private ResponseInputStream<?> delegate;

  private boolean endOfStream = false;

  private boolean closed = false;

  LazyS3InputStream(Supplier<? extends ResponseInputStream<?>> opener) {
    this.opener = opener;
  }

  private ResponseInputStream<?> delegate() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    if (delegate == null) {
      delegate = opener.get();
    }
    return delegate;
  }

  @Override
  public int read() throws IOException {
    int b = delegate().read();
    if (b < 0) {
      endOfStream = true;
    }
    return b;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    int n = delegate().read(buf, off, len);
    if (n < 0) {
      endOfStream = true;
    }
    return n;
  }

  @Override
  public long skip(long n) throws IOException {
    return delegate().skip(n);
  }

  @Override
  public int available() throws IOException {
    return delegate == null ? 0 : delegate.available();
  }

  /**
   * Whether the underlying S3 request has been made.
   */
  boolean isOpened() {
    return delegate != null;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (delegate != null) {
      if (!endOfStream) {
        delegate.abort();
      }
      delegate.close();
    }
  }
}
