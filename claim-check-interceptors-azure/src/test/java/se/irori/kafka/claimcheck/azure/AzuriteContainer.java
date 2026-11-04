package se.irori.kafka.claimcheck.azure;

import org.testcontainers.containers.GenericContainer;

/**
 * Testcontainers definition for the Azurite Azure blob emulator image.
 */
public class AzuriteContainer extends GenericContainer<AzuriteContainer> {

  public AzuriteContainer() {
    super("mcr.microsoft.com/azure-storage/azurite");
    // The image's default command, plus --skipApiVersionCheck: the Azure SDK sends the newest
    // storage API version, which is usually newer than the latest Azurite image supports.
    withCommand("azurite", "-l", "/data",
        "--blobHost", "0.0.0.0", "--queueHost", "0.0.0.0", "--tableHost", "0.0.0.0",
        "--skipApiVersionCheck");
  }
}
