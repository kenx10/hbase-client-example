FROM openjdk:11

COPY ./target/app-exec.jar .

CMD ["java", "-server", "-Xmx128m", "-jar", "/app-exec.jar"]