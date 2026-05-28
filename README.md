# ISP Streaming Hackathon


## Cookbook and examples  
https://github.com/campossalex/flink_cookbook

## Services 

### Kafka: 
`host.minikube.internal:9092`

From ssh: `kubernetes-vm:9092`

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

S3 credentials to add (session cluster or job):

```
s3.access-key: admin
s3.secret-key: password
s3.endpoint: 'http://minio.vvp-system.svc:9000'
s3.path.style.access: 'true'
```

### Enviroments

Group 1: http://ec2-3-79-24-110.eu-central-1.compute.amazonaws.com:8088  
Group 2: http://ec2-3-79-30-161.eu-central-1.compute.amazonaws.com:8088  
Group 3: http://ec2-63-178-228-105.eu-central-1.compute.amazonaws.com:8088  
Group 4: http://ec2-63-179-102-85.eu-central-1.compute.amazonaws.com:8088  
Group 5: http://ec2-63-176-92-17.eu-central-1.compute.amazonaws.com:8088  

