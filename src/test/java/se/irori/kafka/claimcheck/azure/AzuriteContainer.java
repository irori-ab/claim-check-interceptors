package se.irori.kafka.claimcheck.azure;

import org.testcontainers.containers.GenericContainer;

public class AzuriteContainer extends GenericContainer<AzuriteContainer> {

  public AzuriteContainer() {
    super("mcr.microsoft.com/azure-storage/azurite");
  }
}
