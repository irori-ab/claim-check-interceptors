# AWS S3 claim check backend

Backend storing claim checked payloads in AWS S3, or an S3 compatible store (MinIO, Ceph, ...),
using the AWS SDK for Java v2.

## Usage

```
<dependency>
  <groupId>se.irori.kafka</groupId>
  <artifactId>claim-check-interceptors-s3</artifactId>
  <version>${claim-check.version}</version>
</dependency>
```

```
claimcheck.backend.class=se.irori.kafka.claimcheck.s3.S3ClaimCheckBackend
s3.bucket=my-claim-check-bucket
s3.region=eu-north-1
```

Payloads are stored in a single bucket under the key `<s3.key.prefix><topic>/<uuid>`, and the claim
check reference in the Kafka header is `s3://<bucket>/<key>`. The consumer reads the bucket from the
reference, so the producer and consumer only need to agree on the bucket via configuration.

Credentials come from the AWS SDK
[default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html)
(environment variables, profile, web identity / IRSA / EKS pod identity, instance profile), unless
`s3.access.key.id` and `s3.secret.access.key` are set.

## Streaming mode

Streaming mode (`ClaimCheckStreamingProducerInterceptor` / `ClaimCheckStreamingDeserializer`) is
supported:

- **Producer:** the payload stream is uploaded with a single `PutObject` with the known content
  length from the `message-claim-check-payload-size` header, without buffering the payload in memory.
  The SDK can only rewind the first 128 KiB of a non markable stream, so a retry after more than that
  has been read fails the send (the error is propagated by `ClaimCheckSerializer`). Payloads above
  5 GiB (the single `PutObject` limit) are not supported, and fail before anything is uploaded.
- **Consumer:** the returned `InputStream` only issues the `GetObject` request on first read. This
  avoids holding one open HTTP connection per record for all records returned by a `poll()`. Once
  read, the stream holds a connection from the client's pool (default max 50) until closed, so
  **always close the stream**. Closing it before the end aborts the request, rather than downloading
  the rest of the object.

## Permissions

Restricting producer and consumer per topic, e.g. for topic `my-topic`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ClaimCheckProducer",
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::my-claim-check-bucket/my-topic/*"
    }
  ]
}
```

Consumer: same, with `s3:GetObject`. `s3.create.bucket.if.not.exists` additionally needs
`s3:ListBucket` and `s3:CreateBucket` on the bucket, and is mostly intended for testing.

## Expiry

Use an S3 lifecycle rule to delete old payloads, e.g. after 14 days:

```
aws s3api put-bucket-lifecycle-configuration --bucket my-claim-check-bucket \
  --lifecycle-configuration '{"Rules":[{"ID":"expire-claim-check-messages","Status":"Enabled",
    "Filter":{"Prefix":""},"Expiration":{"Days":14}}]}'
```

## Config reference

`s3.bucket`
The S3 bucket to store claim checked payloads in. Objects are stored under the key `<prefix><topic>/<uuid>`, so IAM policies can be scoped per topic.

* Type: string
* Valid Values: non-empty string
* Importance: high

`s3.access.key.id`
Optional static access key id. If not set, the AWS SDK default credentials provider chain is used (environment, profile, web identity / IRSA, instance profile). Must be set together with `s3.secret.access.key`.

* Type: string
* Default: null
* Importance: medium

`s3.endpoint.override`
Override the S3 endpoint URL, e.g. for S3 compatible stores such as MinIO, or for testing. Leave unset for AWS S3.

* Type: string
* Default: null
* Importance: medium

`s3.path.style.access.enabled`
Use path style access (`https://host/bucket/key`) instead of virtual hosted style. Often needed for S3 compatible stores.

* Type: boolean
* Default: false
* Importance: medium

`s3.region`
AWS region of the bucket. If not set, the AWS SDK default region provider chain is used (e.g. `AWS_REGION` environment variable).

* Type: string
* Default: null
* Importance: medium

`s3.secret.access.key`
Optional static secret access key. See `s3.access.key.id`.

* Type: password
* Default: null
* Importance: medium

`s3.checksum.when.required`
Only calculate request checksums when required by the operation, instead of the AWS SDK default of always. Enable this for S3 compatible stores that do not support the newer flexible checksums.

* Type: boolean
* Default: false
* Importance: low

`s3.create.bucket.if.not.exists`
Create the bucket if it does not exist. Mostly useful for testing.

* Type: boolean
* Default: false
* Importance: low

`s3.key.prefix`
Optional prefix for all object keys, e.g. `claim-check/`.

* Type: string
* Default: ""
* Importance: low



## Tests

The unit tests run against [S3Proxy](https://github.com/gaul/s3proxy), an S3 API server started
in-process with in-memory storage, so no Docker is needed: `mvn test`.

`ProduceConsumeStreamingKafkaS3IT` runs a real KafkaProducer/KafkaConsumer in streaming mode
(`mvn verify`). By default it uses a Kafka Testcontainer (needs Docker) and S3Proxy. You can point it
at existing infrastructure instead:

```
# external Kafka broker, no Docker needed
mvn verify -Dkafka.bootstrap.servers=localhost:9092

# real S3 bucket, credentials from the AWS default provider chain
mvn verify -Ds3.bucket=my-bucket -Ds3.region=eu-north-1
```
