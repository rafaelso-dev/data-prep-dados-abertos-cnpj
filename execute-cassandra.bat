docker rm my-dse
docker run -e DS_LICENSE=accept --name my-dse -v snapshotsData:/config -d datastax/dse-server:6.7.2