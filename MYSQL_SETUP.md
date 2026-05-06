# MySQL Setup for Existing Database

Since you're using an existing MySQL instance instead of the Docker MySQL container, follow these steps:

## 1. Enable Binary Logging

Debezium requires MySQL binary logging to be enabled. 

### Check if binlog is enabled:
```sql
SHOW VARIABLES LIKE 'log_bin';
```

If it shows `OFF`, you need to enable it.

### Enable binlog (Windows):

1. Find your MySQL configuration file (usually `my.ini` in the MySQL installation directory)
2. Add these lines under `[mysqld]` section:
   ```ini
   [mysqld]
   log-bin=mysql-bin
   binlog_format=ROW
   binlog_row_image=FULL
   server-id=1
   ```
3. Restart MySQL service:
   ```powershell
   Restart-Service MySQL80
   ```
   (Replace `MySQL80` with your MySQL service name)

## 2. Create the Database and Tables

Run the SQL setup script:

```powershell
mysql -u root -p < db_update.sql
```

Or manually create:

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

## 3. Update Debezium Connector Configuration

The connector resolves `database.password` from the `DB_PASSWORD` environment variable, so keep the JSON file free of secrets and inject the password at runtime:

```json
{
  "name": "orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "topic.prefix": "dbserver",
    "database.hostname": "host.docker.internal",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "${env:DB_PASSWORD}",
    "database.allowPublicKeyRetrieval": "true",
    "database.server.id": "1",
    "database.include.list": "realtime_orders",
    "table.include.list": "realtime_orders.orders",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "dbhistory.orders"
  }
}
```

**Important**: Set `DB_PASSWORD` in your environment or secret manager before registering the connector.

## 4. Update .env File

Update your `.env` file to match your MySQL configuration:

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

## 5. Grant Necessary Permissions

Debezium needs specific permissions:

```sql
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'%';
FLUSH PRIVILEGES;
```

## 6. Start Docker Services

Now start only Kafka, Zookeeper, and Debezium:

```powershell
docker-compose up -d
```

## 7. Verify Setup

Check that binlog is enabled:
```sql
SHOW MASTER STATUS;
```

You should see the binary log file and position.

## Troubleshooting

### Debezium can't connect to MySQL?
- Ensure MySQL is listening on `0.0.0.0` or `localhost`
- Check Windows Firewall isn't blocking port 3306
- Verify `host.docker.internal` resolves correctly in Docker

### "Access denied" error?
- Verify the `DB_PASSWORD` environment variable matches your MySQL password
- Ensure the user has proper permissions (see step 5)

### Binlog format error?
- Ensure `binlog_format=ROW` in MySQL config
- Restart MySQL after configuration changes
