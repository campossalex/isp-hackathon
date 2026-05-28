# ISP Streaming Hackathon


## Cookbook and examples  
https://github.com/campossalex/flink_cookbook

## Services 

### Kafka: 
`host.minikube.internal:9092`

### Postgres:
`jdbc:postgresql://host.minikube.internal:5432`   
ssh: sudo -i -u postgres psql

### Fluss:
`coordinator-server-0.coordinator-server-hs.fluss.svc.cluster.local:9124`

### Minio
`s3://data`

Interact with Minio (CLI):
```
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
./mc alias set vvpminio http://localhost:9000 admin password --api S3v4
./mc mb vvpminio/data/my_data
./mc od if=local_file.csv of=vvpminio/data/my_data/file.csv
```

