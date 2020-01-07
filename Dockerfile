FROM openjdk:8-jdk-alpine
COPY "target/dados-publicos-cnpj-2.0-SNAPSHOT.jar" "/usr/dados-publicos-cnpj-2.0-SNAPSHOT.jar"
COPY "target/lib" "/usr/lib"
#VOLUME C:\Users\rsolivei\Documents\Projetos\data-prep-dados-abertos-cnpj\home:/home
WORKDIR /usr/
ENTRYPOINT ["java", "-jar", "dados-publicos-cnpj-2.0-SNAPSHOT.jar"]