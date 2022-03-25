# SMILE Message Publisher

Spring Boot LIMS SampleMetadata publisher framework
# SMILE Publisher 📘

The SMILE Publisher is one of several components that comprise the SMILE distributed microservices system. It runs as a standalone tool that may publish directly to specific SMILE topics given the appropriate consumer permissions are provided.

### The SMILE Publisher runs in 2 modes:
**1. LIMS Request IDs mode**
  - Option 1: Provide comma-separated list of request ids directly
  - Option 2: Provide a start date and (optionally) an end date to fetch LIMS requests delivered within the specified date range

**2. File reading (or message recovery) mode**
This mode can be run to publish messages to a stream directly from a file with tab-separated values. The file must follow the same structure that the publishing logger file uses. 

File structure:
- Column 1: Date message was received (YYYY/MM/DD)
- Column 2: Message topic
- Column 2: Message contents

## Run

### Custom properties

Make an `application.properties` based on [application.properties.EXAMPLE](src/main/resources/application.properties.EXAMPLE). 

All properties are required with the exception of some NATS connection-specific properties. The following are only required if `nats.tls_channel` is set to `true`:

- `nats.keystore_path` : path to client keystore
- `nats.truststore_path` : path to client truststore
- `nats.key_password` : keystore password
- `nats.store_password` : truststore password

Configure your `log4j.properties` based on [log4j.properties.EXAMPLE](src/main/resources/log4j.properties.EXAMPLE). The default configuration writes to std.out which is standard for container environments.

### Locally

**Requirements:**
- maven 3.6.1
- java 8

Add `application.properties` and `log4j.properties` to the local application resources: `src/main/resources`

Build with 

```
mvn clean install
```

Run with 

```
java -jar target/smile_publisher.jar
```

### With Docker

**Requirements**
- docker

Build image with Docker

```
docker build -t <repo>/<tag>:<version> .
```

Push image to DockerHub 

```
docker push <repo>/<tag>:<version>
```

If the Docker image is built with the properties baked in then simply run with:


```
docker run --name sample-publisher <repo>/<tag>:<version> -jar /publisher/smile_publisher.jar
```

Otherwise use a bind mount to make the local files available to the Docker image and add  `--spring.config.location` to the java arg


```
docker run --mount type=bind,source=<local path to properties files>,target=/publisher/src/main/resources --name sample-publisher <repo>/<tag>:<version> -jar /publisher/smile_publisher.jar --spring.config.location=/publisher/src/main/resources/application.properties
```
