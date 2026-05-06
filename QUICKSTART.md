# Quick Start Guide

Get the Real-Time Order Notification System up and running in minutes.

## Prerequisites

- Docker Desktop installed and running
- Python 3.8 or higher
- Git

## Step-by-Step Setup

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/db-update.git
cd db-update
```

### 2. Create Environment File

Copy `.env.example` to `.env` in the project root and replace the placeholder values:

```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=change-me-db-password
DB_NAME=realtime_orders
DB_PORT=3306
KAFKA_BROKERS=localhost:9092
API_KEYS=change-me-user-key
INTERNAL_API_KEYS=change-me-internal-key
CORS_ALLOW_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
APP_ENV=development
REQUIRE_HTTPS=false
```

The backend reads secrets from environment variables, so keep `.env` local and out of source control.

### 3. Start Infrastructure Services

Start MySQL, Kafka, Zookeeper, and Debezium using Docker Compose:

```bash
docker-compose up -d
```

Wait about 30 seconds for all services to initialize.

### 4. Configure Debezium Connector

Register the MySQL connector with Debezium:

```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d @debezium-connector.json
```

**Windows PowerShell users:**

```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors/" -Method Post -ContentType "application/json" -InFile "debezium-connector.json"
```

The connector resolves `database.password` from `DB_PASSWORD`, so you do not need to edit the JSON file with a secret value.

### 5. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 6. Start the FastAPI Backend

In one terminal:

```bash
uvicorn main:app --reload --port 8000
```

Include `X-API-Key` when calling the HTTP API from scripts or clients. Browser WebSocket clients can pass the same key as the `api_key` query parameter.

### 7. Start the Kafka Ingest Worker

In a **second terminal**:

```bash
python consumer.py
```

This worker persists normalized notification events in MySQL first; the FastAPI backend replays and broadcasts them from the queue, which keeps restarts and Kafka replays idempotent.

### 8. Open the Dashboard

Navigate to [http://localhost:8000](http://localhost:8000) in your browser.

## Testing the System

1. Click "Create Test Order" on the dashboard
2. Watch real-time notifications appear instantly
3. Try updating or deleting orders
4. Check both terminals to see the backend and consumer logs

## Troubleshooting

### Services not starting?

```bash
docker-compose down
docker-compose up -d
```

### Debezium connector issues?

Check connector status:

```bash
curl http://localhost:8083/connectors/orders-connector/status
```

### Database connection errors?

Ensure MySQL is running:

```bash
docker ps
```

### Kafka consumer not receiving messages?

Verify Debezium is capturing changes:

```bash
docker logs connect
```

## Architecture Flow

```text
User Action -> MySQL -> Debezium -> Kafka -> Consumer -> FastAPI -> WebSocket -> Dashboard
```

## Next Steps

- Explore the API endpoints at [http://localhost:8000/docs](http://localhost:8000/docs)
- Run the CLI client: `python client.py`
- Check the full README for detailed documentation

## Stopping the Services

```bash
# Stop Python processes with Ctrl+C

# Stop Docker containers
docker-compose down
```
