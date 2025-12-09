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

Create a `.env` file in the project root:

```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=supersecret99
DB_NAME=realtime_orders
DB_PORT=3306
KAFKA_BROKERS=localhost:9092
```

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

### 5. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 6. Start the FastAPI Backend

In one terminal:

```bash
uvicorn main:app --reload --port 8000
```

### 7. Start the Kafka Consumer

In a **second terminal**:

```bash
python consumer.py
```

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

```
User Action → MySQL → Debezium → Kafka → Consumer → FastAPI → WebSocket → Dashboard
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
