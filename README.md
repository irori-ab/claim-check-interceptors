# Claim Check interceptors
Library implementing the Claim Check pattern for use with Kafka and Azure Blob Storage

## Building 

`./mvnw clean install`

## Commits

Commit messages should follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) standard.

## Testcontainers prerequisites
We use [testcontainers](https://www.testcontainers.org) for integration tests against Kafka and an
Azure Blob Storage emulator (Azurite). The tests are run with Maven Failsafe.

See [Docker pre-requisites](https://www.testcontainers.org/supported_docker_environment/)
for running these tests locally.

## Reference Documentation 

- [Azure SDK documentation](https://azuresdkartifacts.blob.core.windows.net/azure-sdk-for-java/index.html)