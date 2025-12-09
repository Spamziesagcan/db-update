# Real-Time Order Notification System

A real-time system that automatically notifies clients whenever data changes occur in the database using Debezium, Kafka, FastAPI, and WebSockets. This system captures database changes through Change Data Capture (CDC) and ensures connected clients receive instantaneous updates on any inserts, updates, or deletes on the `orders` table without relying on frequent polling.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Problem Statement](#problem-statement)
- [Architecture](#architecture)
- [Technology Choices and Trade-offs](#technology-choices-and-trade-offs)
- [Setup Instructions](#setup-instructions)
- [API Endpoints](#api-endpoints)
- [Testing](#testing)
- [Scalability and Production Improvements](#scalability-and-production-improvements)
- [Technical Discussion and FAQs](#technical-discussion-and-faqs)
- [License](#license)

---

## Project Overview

This project implements a backend service that listens for database changes using Debezium CDC (Change Data Capture) streaming to Kafka and notifies connected clients via WebSockets served by a FastAPI backend. Clients can be either a web dashboard or a CLI script, both receiving real-time updates about changes to orders.

---

## Problem Statement

Build a system where clients automatically receive updates whenever the data in the `orders` MySQL table changes. The system must operate in real-time without relying on frequent client polling and must handle any insert, update, or delete on the `orders` table by pushing those changes to the connected clients automatically.

---

## Architecture

[MySQL DB] → [Debezium Connector] → [Kafka] → [Python Consumer] → [FastAPI] → [WebSocket Manager] → [Connected Clients]

- **MySQL DB:** Hosts the `orders` table. Changes are captured via binlog.
- **Debezium Connector:** Reads MySQL binlog and publishes change events to Kafka topics.
- **Kafka:** Message broker that streams database change events in real-time.
- **Python Consumer:** Consumes Kafka messages and sends them to FastAPI via internal HTTP endpoint.
- **FastAPI Backend:** Manages WebSocket connections and broadcasts notifications.
- **WebSocket Manager:** Distributes notifications to clients in real-time.
- **Clients:** Web dashboard or CLI apps display live updates.

---

## Technology Choices and Trade-offs

### Why this approach?

- **Debezium CDC:** Captures database changes by reading MySQL binlog without adding overhead to write operations.
- **Kafka:** Distributed event streaming platform that provides durability, scalability, and fault tolerance.
- **FastAPI:** Async Python framework with excellent WebSocket support.
- **WebSockets:** Persistent, two-way communication enabling real-time push without client-side polling.
- **aiomysql:** Async MySQL connector for non-blocking database operations.

### Pros

- Real-time updates without client polling.
- No performance impact on database writes (no triggers).
- Scalable and production-ready architecture.
- Event persistence in Kafka prevents data loss.
- Decoupled architecture allows independent scaling of components.
- Can replay events from Kafka for debugging or recovery.

### Cons

- More complex infrastructure (Kafka, Zookeeper, Debezium).
- Higher resource requirements for running multiple services.
- Requires understanding of event streaming concepts.

---

## Setup Instructions

Choose one of the following setup options based on whether you want to use Docker for MySQL or your existing MySQL installation.

### Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ installed
- Git installed

---

## Option A: Using Docker MySQL (Recommended for Quick Start)

This option runs everything in Docker containers including MySQL.

### 1. Fork and Clone

1. **Fork the repository** on GitHub to your own account.

2. **Clone your fork locally:**
   ```bash
   git clone https://github.com/<your-username>/db-update.git
   cd db-update
   ```

### 2. Restore MySQL Service in Docker Compose

Edit `docker-compose.yml` and add the MySQL service back (if removed):
```yaml
services:
  mysql:
    image: mysql:8.0
    container_name: mysql
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: ${DB_PASSWORD}
      MYSQL_DATABASE: ${DB_NAME}
    volumes:
      - ./db_update.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD", "mysqladmin" ,"ping", "-h", "localhost"]
      timeout: 20s
      retries: 10

  zookeeper:
    # ... rest of services
```

### 3. Update Debezium Connector

Edit `debezium-connector.json` and change hostname to `mysql`:
```json
{
  "config": {
    "database.hostname": "mysql",
    ...
  }
}
```

### 4. Create Environment File

Create a `.env` file:
```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=realtime_orders
DB_PORT=3306
KAFKA_BROKERS=localhost:9092
```

### 5. Start All Services

```bash
docker-compose up -d
```

### 6. Configure Debezium Connector

Wait 30 seconds for services to start, then:
```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  localhost:8083/connectors/ -d @debezium-connector.json
```

**Windows PowerShell:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors/" -Method Post -ContentType "application/json" -InFile "debezium-connector.json"
```

### 7. Set Up Python Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 8. Run the Application

**Terminal 1 - FastAPI Backend:**
```bash
uvicorn main:app --reload --port 8000
```

**Terminal 2 - Kafka Consumer:**
```bash
python consumer.py
```

### 9. Access Dashboard

Navigate to [http://localhost:8000](http://localhost:8000)

---

## Option B: Using Existing MySQL Database

This option uses your existing MySQL installation and only runs Kafka/Zookeeper/Debezium in Docker.

### 1. Fork and Clone

1. **Fork the repository** on GitHub to your own account.

2. **Clone your fork locally:**
   ```bash
   git clone https://github.com/<your-username>/db-update.git
   cd db-update
   ```

### 2. Enable MySQL Binary Logging

Debezium requires binlog to be enabled. Check if it's enabled:
```sql
SHOW VARIABLES LIKE 'log_bin';
```

If `OFF`, edit your MySQL configuration file (`my.ini` on Windows, `my.cnf` on Linux/Mac) and add:
```ini
[mysqld]
log-bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
server-id=1
```

Restart MySQL service:
```bash
# Windows
Restart-Service MySQL80

# Linux/Mac
sudo systemctl restart mysql
```

### 3. Create Database and Tables

Run the SQL setup:
```bash
mysql -u root -p < db_update.sql
```

Or manually:
```sql
CREATE DATABASE IF NOT EXISTS realtime_orders;
USE realtime_orders;

CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    status ENUM('pending', 'shipped', 'delivered') DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_updated_at (updated_at)
);
```

### 4. Grant Debezium Permissions

```sql
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'%';
FLUSH PRIVILEGES;
```

### 5. Update Debezium Connector Configuration

Edit `debezium-connector.json` with your MySQL password and set hostname:
```json
{
  "name": "orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "topic.prefix": "dbserver",
    "database.hostname": "host.docker.internal",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "YOUR_MYSQL_PASSWORD",
    ...
  }
}
```

### 6. Create Environment File

Create a `.env` file with your MySQL credentials:
```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=YOUR_MYSQL_PASSWORD
DB_NAME=realtime_orders
DB_PORT=3306
KAFKA_BROKERS=localhost:9092
```

### 7. Start Docker Services (Kafka, Zookeeper, Debezium only)

The `docker-compose.yml` is already configured to exclude MySQL. Start services:
```bash
docker-compose up -d
```

### 8. Configure Debezium Connector

Wait 30 seconds, then:
```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  localhost:8083/connectors/ -d @debezium-connector.json
```

**Windows PowerShell:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors/" -Method Post -ContentType "application/json" -InFile "debezium-connector.json"
```

### 9. Set Up Python Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 10. Run the Application

**Terminal 1 - FastAPI Backend:**
```bash
uvicorn main:app --reload --port 8000
```

**Terminal 2 - Kafka Consumer:**
```bash
python consumer.py
```

### 11. Access Dashboard

Navigate to [http://localhost:8000](http://localhost:8000)

---

## Verification

### Check Debezium Connector Status
```bash
curl http://localhost:8083/connectors/orders-connector/status
```

**PowerShell:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors/orders-connector/status"
```

### Check Docker Containers
```bash
docker ps
```

You should see: `zookeeper`, `kafka`, and `connect` (and optionally `mysql` if using Option A).

---

## API Endpoints

| Method | Endpoint         | Description                  |
|--------|------------------|------------------------------|
| GET    | `/`              | Serves the web dashboard     |
| WS     | `/ws`            | WebSocket endpoint for notifications |
| POST   | `/api/orders`    | Create a new order           |
| PUT    | `/api/orders/{id}` | Update an existing order     |
| DELETE | `/api/orders/{id}` | Delete an order              |

---

## Testing

### Manual Tests

1. Open the web dashboard and observe the connection status.
2. Use the buttons to insert, update, or delete orders.
3. Changes should appear instantly in the dashboard.
4. Run the CLI client and verify it receives notifications.

### Database Trigger Testing

Run SQL commands directly:

INSERT INTO orders (customer_name, product_name, status) VALUES ('TestUser', 'ProductA', 'pending');
UPDATE orders SET status = 'shipped' WHERE id = 1;
DELETE FROM orders WHERE id = 1;

All changes trigger corresponding notifications to the clients.

### Load Testing (Optional)

Simulate multiple WebSocket clients to ensure backend handles connections properly.

---

## Scalability and Production Improvements

### Current Architecture Benefits

- **Debezium CDC:** Already captures changes from binlog without database overhead.
- **Kafka:** Provides durability and prevents message loss if the backend crashes.
- **Event Sourcing:** All database changes are persisted in Kafka for replay and debugging.

### Recommended Enhancements

1. **Horizontal Scaling:**
   - Deploy multiple FastAPI backend instances behind a load balancer.
   - Use Redis Pub/Sub or Kafka consumer groups to coordinate message distribution across instances.
   - Implement sticky sessions for WebSocket connections.

2. **Kafka Optimizations:**
   - Configure partitioning strategy for better load distribution.
   - Enable message compression to reduce network bandwidth.
   - Tune consumer settings for optimal throughput.

3. **Monitoring & Alerting:**
   - Add observability on connection counts, message delivery, and errors.
   - Monitor Kafka lag and consumer throughput.
   - Set up alerts for connector failures or consumer lag.

4. **High Availability:**
   - Run Kafka and Zookeeper in clustered mode.
   - Deploy multiple Debezium connector instances with failover.
   - Implement circuit breakers for external service calls.

---

## Technical Discussion and FAQs

### Why not use polling on the client?

Polling clients create unnecessary network traffic and add latency depending on the polling interval. WebSockets provide instant, server-pushed updates reducing resource consumption and improving user experience.

### How would you scale to 10,000+ clients?

- Distribute WebSocket connections across multiple backend instances with load balancing.
- Use Kafka consumer groups to parallelize message processing.
- Implement Redis Pub/Sub for cross-instance WebSocket broadcasting.
- Deploy Kafka in clustered mode for high throughput.
- Use sticky sessions or connection affinity for WebSocket routing.

### What if a client disconnects?

- WebSocket manager cleans up disconnected clients to free resources.
- Clients implement reconnection logic with exponential backoff.
- Kafka retains messages based on retention policy, allowing clients to catch up on missed events.

### Why use Debezium instead of MySQL triggers?

Debezium reads the MySQL binlog asynchronously, capturing all changes without adding overhead to database write operations. This approach is more performant, scalable, and production-ready compared to triggers, which execute synchronously with each write operation and can slow down the database.

---

## License

This project is open source and free to use under the MIT License.

---

Thank you for reviewing this project. For questions or suggestions, please contact meor check the source repository.
