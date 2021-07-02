package se.irori.kafka.claimcheck;

import java.nio.charset.StandardCharsets;

/**
 * Represents a Claim Check in the claim check pattern.
 * <p/>
 * https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html
 */
public class ClaimCheck {
  private String reference;

  public ClaimCheck(String reference) {
    this.reference = reference;
  }

  public ClaimCheck(byte[] serializedReference) {
    this.reference = new String(serializedReference, StandardCharsets.UTF_8);
  }

  public byte[] serialize() {
    return reference.getBytes(StandardCharsets.UTF_8);
  }

  public String getReference() {
    return reference;
  }
}
