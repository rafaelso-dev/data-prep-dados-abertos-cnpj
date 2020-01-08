docker stop meulab
docker rm meulab
docker rmi lab
docker build -t lab .
docker run --name meulab -v snapshotsData:/tmp -v C:\Users\rsolivei\Documents\Projetos\data-prep-dados-abertos-cnpj\home:/home lab