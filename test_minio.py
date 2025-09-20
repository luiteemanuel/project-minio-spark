from minio import Minio
import os

# Conectar ao MinIO
client = Minio(
    'localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin123',
    secure=False
)

# Listar buckets
buckets = client.list_buckets()
print("Buckets criados:")
for bucket in buckets:
    print(f"- {bucket.name}")