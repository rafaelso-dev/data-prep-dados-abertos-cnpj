FROM openjdk:8-jdk-alpine
COPY "target/dados-publicos-cnpj-3.0-SNAPSHOT.jar" "/usr/dados-publicos-cnpj-3.0-SNAPSHOT.jar"
COPY "target/lib" "/usr/lib"
WORKDIR /usr/
ENTRYPOINT ["java", "-jar", "dados-publicos-cnpj-3.0-SNAPSHOT.jar"]
