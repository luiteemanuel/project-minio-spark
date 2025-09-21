cat > config/settings.py << 'EOF'
import os
from dotenv import load_dotenv

load_dotenv()

# MinIO Settings
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')

# Buckets
RAW_DATA_BUCKET = 'raw-data'
PROCESSED_DATA_BUCKET = 'processed-data'
LOGS_BUCKET = 'logs'

# API Settings
API_KEY = os.getenv('API_KEY')
API_BASE_URL = os.getenv('https://api.coingecko.com')
EOF