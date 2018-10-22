FROM openjdk:8-jdk-alpine
VOLUME /tmp
ARG JAR_FILE
ADD ${JAR_FILE} ContainerIdToImageStreamProcessor.0.0.1.jar
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/ContainerIdToImageStreamProcessor.0.0.1.jar"]
