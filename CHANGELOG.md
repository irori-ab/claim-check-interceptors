# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [unreleased]
### Added
- Azure 12 backend: added `azure.blob.storage.account.identity.from` config option to use Azure identity 
  credentials `DefaultAzureCredential` and `ManagedIdentityCredential` instead of SasToken.

### Changed
- N/A 

## [1.0.0]
### Added
- N/A

### Changed
Functionally same as 0.7.1.

- docs: updated readme with new version in example

## [0.7.1]
### Added
- ci: add spotbugs

### Changed
- fix: bump dependencies (Kafka 3.1.0, Azure )
- docs: generate docs from ConfigDefs, add proper config doc strings
- docs: add back simpler diagram, refer to detailed one in link
- docs: add some missed javadocs
- ci: change mvn release git tag format
  
## [0.7.0] - 2022-05-06
### Changed
- ci: maven profile to release to Maven central (sign, javadocs, sources)
- docs: fix some javadocs formatting so that it generates without error

## [0.6.0] - 2022-03-29
### Added
- feat: BREAKING CHANGE add ClaimCheck(De)serializers that wrap actual (de)serializers for better error handling.
    Require that `ClaimCheckSerializer` is being used with `ClaimCheckProducerInterceptor`.
- docs: add changelog

### Changed
- fix(azure-backend-v8): BREAKING CHANGE rename packages azure => azurev8 distinguish from v12 backend classes. If you
    intend to keep using the v8 backend, please update to
    `claimcheck.backend.class=se.irori.kafka.claimcheck.azurev8.AzureBlobStorageClaimCheckBackendV8`.
- fix: add basic DEBUG/TRACE logging with Slf4j for easier troubleshooting
- fix: avoid using deprecated method removed in Kafka 3.0.0 clients
- fix: use empty byte array as payload when using wrapping serializer, to avoid semantic overload of null with
  log compacted topics.
- fix(azure-backend): only create container if not exists depending on config, default off
- docs: update diagram with serializer/error flows

### Removed
- fix: BREAKING CHANGE remove `DeserializingClaimCheckConsumerInterceptor`, use deserializer `ClaimCheckDeserializer`
  instead, with no consumer interceptor configured.

## [0.5.0] - 2021-10-08
### Added
- feat(azure-backend): Azure Blob Storage SDK v12 backed impl of producer/consumer interceptors
- feat(azure-backend-v8): Azure Blob Storage SDK v8 backed impl of producer/consumer interceptors, for increased
  backward compatibility in different dependency setups
- ci: mvn verify every commit
- test: Testcontainers based integration tests with Azurite and Kafka

[Unreleased]: https://github.com/irori-ab/claim-check-interceptors/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/irori-ab/claim-check-interceptors/compare/v0.7.1...v1.0.0
[0.7.1]: https://github.com/irori-ab/claim-check-interceptors/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/irori-ab/claim-check-interceptors/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/irori-ab/claim-check-interceptors/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/irori-ab/claim-check-interceptors/releases/tag/v0.5.0

