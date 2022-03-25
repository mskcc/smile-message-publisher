FROM maven:3.6.1-jdk-8-slim
RUN mkdir /publisher
ADD . /publisher
WORKDIR /publisher
RUN mvn clean install

# copy jar and set entrypoint
FROM openjdk:8-slim
COPY --from=0 /publisher/target/smile_publisher.jar /publisher/smile_publisher.jar
ENTRYPOINT ["java"]
