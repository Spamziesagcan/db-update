import asyncio
import json
import logging
import os
import signal
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
import uuid

import pymysql
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, validator
import aiomysql
from aiomysql import Pool

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class Config:
    """Application configuration with validation"""
    DB_HOST: str = os.getenv('DB_HOST', 'localhost')
    DB_USER: str = os.getenv('DB_USER', 'root')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD', '')
    DB_NAME: str = os.getenv('DB_NAME', 'realtime_orders')
    DB_PORT: int = int(os.getenv('DB_PORT', 3306))
    
    # Connection pool settings
    DB_POOL_MIN_SIZE: int = int(os.getenv('DB_POOL_MIN_SIZE', 5))
    DB_POOL_MAX_SIZE: int = int(os.getenv('DB_POOL_MAX_SIZE', 20))
    
    # Notification polling settings
    POLL_INTERVAL: float = float(os.getenv('POLL_INTERVAL', 0.1))
    MAX_BATCH_SIZE: int = int(os.getenv('MAX_BATCH_SIZE', 100))
    
    # WebSocket settings
    HEARTBEAT_INTERVAL: int = int(os.getenv('HEARTBEAT_INTERVAL', 30))
    CONNECTION_TIMEOUT: int = int(os.getenv('CONNECTION_TIMEOUT', 300))

config = Config()

class ConnectionManager:
    """Enhanced WebSocket connection manager with health monitoring"""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.connection_metadata: Dict[str, dict] = {}
        
    async def connect(self, websocket: WebSocket, client_id: Optional[str] = None) -> str:
        """Connect a WebSocket client with unique ID and metadata"""
        await websocket.accept()
        
        if not client_id:
            client_id = str(uuid.uuid4())
        
        self.active_connections[client_id] = websocket
        self.connection_metadata[client_id] = {
            'connected_at': datetime.now(timezone.utc),
            'last_ping': datetime.now(timezone.utc),
            'message_count': 0
        }
        
        logger.info(f"Client {client_id} connected. Total connections: {len(self.active_connections)}")
        return client_id
    
    def disconnect(self, client_id: str):
        """Disconnect a WebSocket client"""
        if client_id in self.active_connections:
            del self.active_connections[client_id]
            del self.connection_metadata[client_id]
            logger.info(f"Client {client_id} disconnected. Total connections: {len(self.active_connections)}")
    
    async def send_to_client(self, client_id: str, message: dict) -> bool:
        """Send message to specific client with error handling"""
        if client_id not in self.active_connections:
            return False
        
        try:
            websocket = self.active_connections[client_id]
            await websocket.send_text(json.dumps(message, default=str))
            self.connection_metadata[client_id]['message_count'] += 1
            return True
        except Exception as e:
            logger.error(f"Error sending message to client {client_id}: {e}")
            self.disconnect(client_id)
            return False
    
    async def broadcast(self, message: dict, exclude_clients: Set[str] = None) -> int:
        """Broadcast message to all connected clients with delivery tracking"""
        if not self.active_connections:
            return 0
        
        exclude_clients = exclude_clients or set()
        successful_sends = 0
        failed_clients = set()
        
        message_str = json.dumps(message, default=str)
        
        for client_id, websocket in self.active_connections.copy().items():
            if client_id in exclude_clients:
                continue
                
            try:
                await websocket.send_text(message_str)
                self.connection_metadata[client_id]['message_count'] += 1
                successful_sends += 1
            except Exception as e:
                logger.error(f"Error broadcasting to client {client_id}: {e}")
                failed_clients.add(client_id)
        
        # Clean up failed connections
        for client_id in failed_clients:
            self.disconnect(client_id)
        
        if successful_sends > 0:
            logger.debug(f"Broadcast successful to {successful_sends} clients")
        
        return successful_sends
    
    def get_connection_stats(self) -> dict:
        """Get connection statistics"""
        return {
            'total_connections': len(self.active_connections),
            'connections': {
                client_id: {
                    'connected_duration': (datetime.now(timezone.utc) - meta['connected_at']).total_seconds(),
                    'message_count': meta['message_count'],
                    'last_ping': meta['last_ping'].isoformat()
                }
                for client_id, meta in self.connection_metadata.items()
            }
        }
    
    async def cleanup_stale_connections(self):
        """Remove stale connections that haven't pinged recently"""
        cutoff_time = datetime.now(timezone.utc).timestamp() - config.CONNECTION_TIMEOUT
        stale_clients = []
        
        for client_id, meta in self.connection_metadata.items():
            if meta['last_ping'].timestamp() < cutoff_time:
                stale_clients.append(client_id)
        
        for client_id in stale_clients:
            logger.info(f"Removing stale connection: {client_id}")
            self.disconnect(client_id)

# Global connection manager
manager = ConnectionManager()

class DatabasePool:
    """Async database connection pool manager"""
    
    def __init__(self):
        self.pool: Optional[Pool] = None
    
    async def create_pool(self):
        """Create database connection pool"""
        try:
            self.pool = await aiomysql.create_pool(
                host=config.DB_HOST,
                port=config.DB_PORT,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                db=config.DB_NAME,
                minsize=config.DB_POOL_MIN_SIZE,
                maxsize=config.DB_POOL_MAX_SIZE,
                autocommit=True,
                charset='utf8mb4'
            )
            logger.info(f"Database pool created: {config.DB_POOL_MIN_SIZE}-{config.DB_POOL_MAX_SIZE} connections")
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise
    
    async def close_pool(self):
        """Close database connection pool"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("Database pool closed")
    
    def get_pool(self) -> Pool:
        """Get database pool"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        return self.pool

# Global database pool
db_pool = DatabasePool()

class DatabaseNotificationListener:
    """Enhanced database notification listener with batching and retry logic"""
    
    def __init__(self):
        self.last_processed_id = 0
        self.running = False
        self.retry_count = 0
        self.max_retries = 5
        self.batch_processor_task = None
        self.notification_queue = asyncio.Queue()
    
    async def initialize(self):
        """Initialize the listener by getting the last processed ID"""
        try:
            pool = db_pool.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("SELECT MAX(id) as max_id FROM order_notifications")
                    result = await cursor.fetchone()
                    self.last_processed_id = result['max_id'] or 0
                    logger.info(f"Initialized listener with last_processed_id: {self.last_processed_id}")
        except Exception as e:
            logger.error(f"Failed to initialize listener: {e}")
            raise
    
    async def start_listening(self):
        """Start the notification listener with batch processing"""
        self.running = True
        logger.info("Starting database notification listener")
        
        # Start batch processor
        self.batch_processor_task = asyncio.create_task(self._batch_processor())
        
        while self.running:
            try:
                await self._poll_notifications()
                self.retry_count = 0  # Reset retry count on successful poll
                await asyncio.sleep(config.POLL_INTERVAL)
            except Exception as e:
                self.retry_count += 1
                logger.error(f"Error in notification listener (attempt {self.retry_count}): {e}")
                
                if self.retry_count >= self.max_retries:
                    logger.critical("Max retries reached. Stopping notification listener.")
                    break
                
                # Exponential backoff
                wait_time = min(2 ** self.retry_count, 30)
                await asyncio.sleep(wait_time)
    
    async def _poll_notifications(self):
        """Poll for new notifications with batching"""
        try:
            pool = db_pool.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM order_notifications 
                        WHERE id > %s 
                        ORDER BY id ASC 
                        LIMIT %s
                    """, (self.last_processed_id, config.MAX_BATCH_SIZE))
                    
                    notifications = await cursor.fetchall()
                    
                    for notification in notifications:
                        await self.notification_queue.put(notification)
                        self.last_processed_id = notification['id']
        
        except Exception as e:
            logger.error(f"Database polling error: {e}")
            raise
    
    async def _batch_processor(self):
        """Process notifications in batches for better performance"""
        batch = []
        batch_timeout = 0.05  # 50ms batch timeout
        
        while self.running:
            try:
                # Collect notifications for batch processing
                try:
                    notification = await asyncio.wait_for(
                        self.notification_queue.get(), timeout=batch_timeout
                    )
                    batch.append(notification)
                    
                    # Process batch if it reaches optimal size or queue is empty
                    if len(batch) >= 10 or self.notification_queue.empty():
                        await self._process_notification_batch(batch)
                        batch.clear()
                        
                except asyncio.TimeoutError:
                    # Process any pending notifications in batch
                    if batch:
                        await self._process_notification_batch(batch)
                        batch.clear()
                        
            except Exception as e:
                logger.error(f"Error in batch processor: {e}")
                await asyncio.sleep(1)
    
    async def _process_notification_batch(self, notifications: List[dict]):
        """Process a batch of notifications"""
        if not notifications:
            return
            
        try:
            messages = []
            for notification in notifications:
                message = await self._create_notification_message(notification)
                if message:
                    messages.append(message)
            
            # Broadcast all messages
            for message in messages:
                await manager.broadcast(message)
                
            logger.debug(f"Processed batch of {len(messages)} notifications")
            
        except Exception as e:
            logger.error(f"Error processing notification batch: {e}")
    
    async def _create_notification_message(self, notification: dict) -> Optional[dict]:
        """Create a formatted notification message"""
        try:
            old_data = json.loads(notification['old_data']) if notification['old_data'] else None
            new_data = json.loads(notification['new_data']) if notification['new_data'] else None
            
            message = {
                'event_type': 'order_change',
                'action': notification['action'],
                'order_id': notification['order_id'],
                'timestamp': notification['created_at'].isoformat(),
                'old_data': old_data,
                'new_data': new_data,
                'change_id': notification['id']  # Add unique change identifier
            }
            
            return message
            
        except Exception as e:
            logger.error(f"Error creating notification message: {e}")
            return None
    
    def stop(self):
        """Stop the notification listener"""
        self.running = False
        if self.batch_processor_task:
            self.batch_processor_task.cancel()
        logger.info("Database notification listener stopped")

# Global notification listener
notification_listener = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Enhanced application lifecycle management"""
    global notification_listener
    
    # Startup
    try:
        logger.info("Starting application...")
        
        # Initialize database pool
        await db_pool.create_pool()
        
        # Test database connection
        await test_database_connection()
        
        # Initialize and start notification listener
        notification_listener = DatabaseNotificationListener()
        await notification_listener.initialize()
        listener_task = asyncio.create_task(notification_listener.start_listening())
        
        # Start connection cleanup task
        cleanup_task = asyncio.create_task(periodic_cleanup())
        
        logger.info("Application startup complete")
        
        yield
        
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise
    
    # Shutdown
    finally:
        logger.info("Shutting down application...")
        
        if notification_listener:
            notification_listener.stop()
        
        # Cancel background tasks
        for task in [listener_task, cleanup_task]:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Close database pool
        await db_pool.close_pool()
        
        logger.info("Application shutdown complete")

async def test_database_connection():
    """Test database connection and create tables if needed"""
    try:
        pool = db_pool.get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Test connection
                await cursor.execute("SELECT 1")
                
                # Create tables if they don't exist
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        customer_name VARCHAR(255) NOT NULL,
                        product_name VARCHAR(255) NOT NULL,
                        status ENUM('pending', 'shipped', 'delivered') DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_status (status),
                        INDEX idx_updated_at (updated_at)
                    )
                """)
                
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS order_notifications (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        order_id INT NOT NULL,
                        action ENUM('INSERT', 'UPDATE', 'DELETE') NOT NULL,
                        old_data JSON,
                        new_data JSON,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_id_created (id, created_at),
                        INDEX idx_order_id (order_id)
                    )
                """)
                
                logger.info("Database connection successful and tables verified")
                
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        raise

async def periodic_cleanup():
    """Periodic cleanup of stale connections and old notifications"""
    while True:
        try:
            await asyncio.sleep(300)  # Run every 5 minutes
            
            # Cleanup stale WebSocket connections
            await manager.cleanup_stale_connections()
            
            # Cleanup old notifications (keep last 10000)
            pool = db_pool.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        DELETE FROM order_notifications 
                        WHERE id < (
                            SELECT id FROM (
                                SELECT id FROM order_notifications 
                                ORDER BY id DESC LIMIT 1 OFFSET 10000
                            ) AS subquery
                        )
                    """)
                    
                    if cursor.rowcount > 0:
                        logger.info(f"Cleaned up {cursor.rowcount} old notifications")
                        
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")

# FastAPI app with enhanced configuration
app = FastAPI(
    title="Real-time Order Notifications System",
    description="High-performance real-time order tracking with WebSocket notifications",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models with validation
class OrderCreate(BaseModel):
    customer_name: str = Field(..., min_length=1, max_length=255)
    product_name: str = Field(..., min_length=1, max_length=255)
    status: str = Field(default="pending")
    
    @validator('status')
    def validate_status(cls, v):
        if v not in ['pending', 'shipped', 'delivered']:
            raise ValueError('Status must be one of: pending, shipped, delivered')
        return v

class OrderUpdate(BaseModel):
    status: str = Field(...)
    
    @validator('status')
    def validate_status(cls, v):
        if v not in ['pending', 'shipped', 'delivered']:
            raise ValueError('Status must be one of: pending, shipped, delivered')
        return v

class OrderResponse(BaseModel):
    id: int
    customer_name: str
    product_name: str
    status: str
    created_at: datetime
    updated_at: datetime

# WebSocket endpoint with enhanced connection handling
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    client_id = None
    try:
        client_id = await manager.connect(websocket)
        
        # Send initial connection confirmation
        await manager.send_to_client(client_id, {
            'event_type': 'connection_established',
            'client_id': client_id,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
        while True:
            try:
                # Wait for client messages with timeout
                data = await asyncio.wait_for(websocket.receive_text(), timeout=config.HEARTBEAT_INTERVAL * 2)
                
                if data == "ping":
                    await websocket.send_text("pong")
                    manager.connection_metadata[client_id]['last_ping'] = datetime.now(timezone.utc)
                elif data.startswith("subscribe:"):
                    # Handle subscription to specific order IDs (future enhancement)
                    pass
                    
            except asyncio.TimeoutError:
                # Send ping to check if connection is alive
                try:
                    await websocket.send_text("ping")
                except:
                    break
                    
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error for client {client_id}: {e}")
    finally:
        if client_id:
            manager.disconnect(client_id)

# Enhanced REST API endpoints
@app.post("/api/orders", response_model=dict)
async def create_order(order: OrderCreate):
    """Create a new order"""
    try:
        pool = db_pool.get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    INSERT INTO orders (customer_name, product_name, status)
                    VALUES (%s, %s, %s)
                """, (order.customer_name, order.product_name, order.status))
                
                order_id = cursor.lastrowid
                
                logger.info(f"Order {order_id} created successfully")
                return {
                    "success": True,
                    "message": "Order created successfully",
                    "order_id": order_id
                }
                
    except Exception as e:
        logger.error(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/orders/{order_id}", response_model=dict)
async def update_order(order_id: int, order: OrderUpdate):
    """Update an existing order"""
    try:
        pool = db_pool.get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Check if order exists
                await cursor.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
                existing_order = await cursor.fetchone()
                
                if not existing_order:
                    raise HTTPException(status_code=404, detail="Order not found")
                
                # Update order
                await cursor.execute("""
                    UPDATE orders SET status = %s, updated_at = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (order.status, order_id))
                
                logger.info(f"Order {order_id} updated successfully")
                return {
                    "success": True,
                    "message": "Order updated successfully",
                    "order_id": order_id
                }
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating order {order_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/orders/{order_id}", response_model=dict)
async def delete_order(order_id: int):
    """Delete an order"""
    try:
        pool = db_pool.get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Check if order exists
                await cursor.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
                existing_order = await cursor.fetchone()
                
                if not existing_order:
                    raise HTTPException(status_code=404, detail="Order not found")
                
                # Delete order
                await cursor.execute("DELETE FROM orders WHERE id = %s", (order_id,))
                
                logger.info(f"Order {order_id} deleted successfully")
                return {
                    "success": True,
                    "message": "Order deleted successfully",
                    "order_id": order_id
                }
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting order {order_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/orders", response_model=List[OrderResponse])
async def get_orders(limit: int = 50, offset: int = 0):
    """Get list of orders with pagination"""
    try:
        pool = db_pool.get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT * FROM orders 
                    ORDER BY created_at DESC 
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                
                orders = await cursor.fetchall()
                return orders
                
    except Exception as e:
        logger.error(f"Error fetching orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats", response_model=dict)
async def get_system_stats():
    """Get system statistics"""
    try:
        connection_stats = manager.get_connection_stats()
        
        pool = db_pool.get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Get order counts
                await cursor.execute("""
                    SELECT status, COUNT(*) as count 
                    FROM orders 
                    GROUP BY status
                """)
                order_stats = {row['status']: row['count'] for row in await cursor.fetchall()}
                
                # Get notification count
                await cursor.execute("SELECT COUNT(*) as count FROM order_notifications")
                notification_count = (await cursor.fetchone())['count']
        
        return {
            "connections": connection_stats,
            "orders": order_stats,
            "total_notifications": notification_count,
            "last_processed_notification_id": notification_listener.last_processed_id if notification_listener else 0
        }
        
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def get_dashboard():
    """Enhanced dashboard with better UI and features"""
    return HTMLResponse(content="""
<!DOCTYPE html>
<html>
<head>
    <title>Real-time Order Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
        }
        .container { 
            max-width: 1200px; 
            margin: 0 auto; 
            padding: 20px; 
            background: rgba(255,255,255,0.95);
            min-height: 100vh;
            box-shadow: 0 0 30px rgba(0,0,0,0.1);
        }
        .header { 
            text-align: center; 
            margin-bottom: 30px; 
            background: linear-gradient(45deg, #667eea, #764ba2);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        .stats-grid { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 20px; 
            margin-bottom: 30px; 
        }
        .stat-card { 
            background: white; 
            padding: 20px; 
            border-radius: 12px; 
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
            border-left: 4px solid #667eea;
        }
        .stat-card:hover { transform: translateY(-5px); }
        .stat-value { font-size: 2rem; font-weight: bold; color: #667eea; }
        .stat-label { color: #666; font-size: 0.9rem; margin-top: 5px; }
        .controls { 
            display: flex; 
            gap: 15px; 
            margin-bottom: 30px; 
            flex-wrap: wrap;
        }
        .btn { 
            background: linear-gradient(45deg, #667eea, #764ba2); 
            color: white; 
            border: none; 
            padding: 12px 24px; 
            border-radius: 25px; 
            cursor: pointer; 
            transition: all 0.3s ease;
            font-weight: 600;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
        }
        .btn:hover { 
            transform: translateY(-2px);
            box-shadow: 0 6px 25px rgba(102, 126, 234, 0.4);
        }
        .connection-status { 
            position: fixed; 
            top: 20px; 
            right: 20px; 
            padding: 10px 20px; 
            border-radius: 25px;
            font-weight: 600;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            z-index: 1000;
        }
        .connected { background: linear-gradient(45deg, #4CAF50, #45a049); color: white; }
        .disconnected { background: linear-gradient(45deg, #f44336, #d32f2f); color: white; }
        .events-container { 
            background: white; 
            border-radius: 12px; 
            padding: 20px; 
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        }
        .event { 
            padding: 15px; 
            margin: 10px 0; 
            border-radius: 8px; 
            border-left: 4px solid #333; 
            background: #f8f9fa;
            transition: all 0.3s ease;
            animation: slideIn 0.5s ease;
        }
        @keyframes slideIn {
            from { opacity: 0; transform: translateX(-20px); }
            to { opacity: 1; transform: translateX(0); }
        }
        .event:hover { transform: scale(1.02); }
        .insert { border-left-color: #28a745; background: linear-gradient(135deg, #d4edda, #c3e6cb); }
        .update { border-left-color: #ffc107; background: linear-gradient(135deg, #fff3cd, #ffeaa7); }
        .delete { border-left-color: #dc3545; background: linear-gradient(135deg, #f8d7da, #f5c6cb); }
        .event-header { font-weight: bold; font-size: 1.1rem; margin-bottom: 5px; }
        .event-time { font-size: 0.85rem; color: #666; }
        .event-details { margin-top: 10px; font-size: 0.95rem; }
        .filter-controls { 
            margin-bottom: 20px;
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }
        .filter-btn {
            background: white;
            border: 2px solid #667eea;
            color: #667eea;
            padding: 8px 16px;
            border-radius: 20px;
            cursor: pointer;
            transition: all 0.3s ease;
        }
        .filter-btn.active, .filter-btn:hover {
            background: #667eea;
            color: white;
        }
        .clear-btn {
            background: #f44336;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 20px;
            cursor: pointer;
            transition: all 0.3s ease;
        }
        .clear-btn:hover {
            background: #d32f2f;
        }
        .no-events {
            text-align: center;
            color: #666;
            padding: 40px;
            font-style: italic;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1 class="header">Real-time Order Dashboard v2.0</h1>
        
        <div id="connection-status" class="connection-status disconnected">
            Disconnected
        </div>
        
        <!-- Statistics Dashboard -->
        <div class="stats-grid" id="stats-grid">
            <div class="stat-card">
                <div class="stat-value" id="total-connections">0</div>
                <div class="stat-label">Active Connections</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="total-orders">0</div>
                <div class="stat-label">Total Orders</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="pending-orders">0</div>
                <div class="stat-label">Pending Orders</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="total-notifications">0</div>
                <div class="stat-label">Notifications Processed</div>
            </div>
        </div>
        
        <!-- Control Buttons -->
        <div class="controls">
            <button class="btn" onclick="createTestOrder()">Create Random Order</button>
            <button class="btn" onclick="createBatchOrders()">Create 5 Orders</button>
            <button class="btn" onclick="updateRandomOrder()">Update Random Order</button>
            <button class="btn" onclick="deleteRandomOrder()">Delete Random Order</button>
            <button class="btn" onclick="loadOrderHistory()">Load Order History</button>
        </div>
        
        <!-- Event Filters -->
        <div class="events-container">
            <h3>Live Events</h3>
            <div class="filter-controls">
                <button class="filter-btn active" data-filter="all">All Events</button>
                <button class="filter-btn" data-filter="insert">New Orders</button>
                <button class="filter-btn" data-filter="update">Updates</button>
                <button class="filter-btn" data-filter="delete">Deletions</button>
                <button class="clear-btn" onclick="clearEvents()">Clear Events</button>
            </div>
            
            <div id="events">
                <div class="no-events">No events yet. Start by creating some test orders!</div>
            </div>
        </div>
    </div>

    <script>
        // Global variables
        let ws = null;
        let connectionAttempts = 0;
        let maxReconnectAttempts = 10;
        let currentFilter = 'all';
        let eventCount = 0;
        let orderIds = new Set();
        
        // DOM elements
        const eventsDiv = document.getElementById('events');
        const statusDiv = document.getElementById('connection-status');
        
        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {
            setupEventFilters();
            loadStats();
            connect();
            
            // Update stats every 10 seconds
            setInterval(loadStats, 10000);
        });
        
        function setupEventFilters() {
            const filterButtons = document.querySelectorAll('.filter-btn');
            filterButtons.forEach(btn => {
                btn.addEventListener('click', function() {
                    // Remove active class from all buttons
                    filterButtons.forEach(b => b.classList.remove('active'));
                    
                    // Add active class to clicked button
                    this.classList.add('active');
                    
                    // Update filter
                    currentFilter = this.getAttribute('data-filter');
                    filterEvents();
                });
            });
        }
        
        function connect() {
            if (connectionAttempts >= maxReconnectAttempts) {
                updateConnectionStatus('Max reconnection attempts reached', false);
                return;
            }
            
            connectionAttempts++;
            ws = new WebSocket('ws://localhost:8000/ws');
            
            ws.onopen = function() {
                updateConnectionStatus('Connected', true);
                connectionAttempts = 0;
                console.log('WebSocket connected');
            };
            
            ws.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    
                    if (data.event_type === 'connection_established') {
                        console.log('Connection established with client ID:', data.client_id);
                        return;
                    }
                    
                    if (data.event_type === 'order_change') {
                        displayEvent(data);
                        updateEventCount();
                    }
                } catch (e) {
                    console.error('Error parsing message:', e);
                }
            };
            
            ws.onclose = function() {
                updateConnectionStatus('Disconnected', false);
                console.log('WebSocket disconnected');
                
                // Auto-reconnect with exponential backoff
                if (connectionAttempts < maxReconnectAttempts) {
                    const delay = Math.min(1000 * Math.pow(2, connectionAttempts), 30000);
                    setTimeout(connect, delay);
                }
            };
            
            ws.onerror = function(error) {
                console.error('WebSocket error:', error);
                updateConnectionStatus('Connection Error', false);
            };
        }
        
        function updateConnectionStatus(message, connected) {
            statusDiv.textContent = message;
            statusDiv.className = connected ? 
                'connection-status connected' : 
                'connection-status disconnected';
        }
        
        function displayEvent(data) {
            // Remove "no events" message if present
            const noEventsMsg = eventsDiv.querySelector('.no-events');
            if (noEventsMsg) {
                noEventsMsg.remove();
            }
            
            eventCount++;
            const eventDiv = document.createElement('div');
            eventDiv.className = `event ${data.action.toLowerCase()}`;
            eventDiv.setAttribute('data-action', data.action.toLowerCase());
            
            // Create event content with enhanced details
            let content = `
                <div class="event-header">
                    ${getActionEmoji(data.action)} ${data.action} - Order #${data.order_id}
                    <span style="float: right; color: #666;">#${eventCount}</span>
                </div>
                <div class="event-time">${formatTimestamp(data.timestamp)}</div>
                <div class="event-details">
            `;
            
            if (data.action === 'INSERT' && data.new_data) {
                content += `
                    <strong>New Order Created:</strong><br>
                    Customer: ${data.new_data.customer_name}<br>
                    Product: ${data.new_data.product_name}<br>
                    Status: <span style="color: #28a745; font-weight: bold;">${data.new_data.status}</span>
                `;
                orderIds.add(data.order_id);
            } else if (data.action === 'UPDATE' && data.old_data && data.new_data) {
                content += `
                    <strong>Order Status Updated:</strong><br>
                    Customer: ${data.new_data.customer_name}<br>
                    Product: ${data.new_data.product_name}<br>
                    Status: <span style="color: #dc3545;">${data.old_data.status}</span> → 
                    <span style="color: #28a745; font-weight: bold;">${data.new_data.status}</span>
                `;
            } else if (data.action === 'DELETE' && data.old_data) {
                content += `
                    <strong>Order Deleted:</strong><br>
                    Customer: ${data.old_data.customer_name}<br>
                    Product: ${data.old_data.product_name}<br>
                    Final Status: <span style="color: #dc3545;">${data.old_data.status}</span>
                `;
                orderIds.delete(data.order_id);
            }
            
            content += '</div>';
            eventDiv.innerHTML = content;
            
            // Insert at the beginning
            eventsDiv.insertBefore(eventDiv, eventsDiv.firstChild);
            
            // Apply current filter
            filterEvents();
            
            // Keep only last 100 events for performance
            const events = eventsDiv.querySelectorAll('.event');
            if (events.length > 100) {
                for (let i = 100; i < events.length; i++) {
                    events[i].remove();
                }
            }
        }
        
        function getActionEmoji(action) {
            switch(action) {
                case 'INSERT': return '➕';
                case 'UPDATE': return '🔄';
                case 'DELETE': return '❌';
                default: return '📝';
            }
        }
        
        function formatTimestamp(timestamp) {
            const date = new Date(timestamp);
            return date.toLocaleString();
        }
        
        function filterEvents() {
            const events = eventsDiv.querySelectorAll('.event');
            
            events.forEach(event => {
                const action = event.getAttribute('data-action');
                
                if (currentFilter === 'all' || currentFilter === action) {
                    event.style.display = 'block';
                } else {
                    event.style.display = 'none';
                }
            });
        }
        
        function clearEvents() {
            eventsDiv.innerHTML = '<div class="no-events">Events cleared. Create some orders to see new events!</div>';
            eventCount = 0;
        }
        
        function updateEventCount() {
            // This could be used to update a counter if needed
        }
        
        async function loadStats() {
            try {
                const response = await fetch('/api/stats');
                const stats = await response.json();
                
                // Update connection stats
                document.getElementById('total-connections').textContent = 
                    stats.connections.total_connections;
                
                // Update order stats
                const totalOrders = Object.values(stats.orders).reduce((a, b) => a + b, 0);
                document.getElementById('total-orders').textContent = totalOrders;
                document.getElementById('pending-orders').textContent = stats.orders.pending || 0;
                document.getElementById('total-notifications').textContent = stats.total_notifications;
                
            } catch (error) {
                console.error('Error loading stats:', error);
            }
        }
        
        // Enhanced order creation functions
        async function createTestOrder() {
            const customers = ['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson', 'Eva Brown'];
            const products = ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Monitor', 'Keyboard', 'Mouse'];
            const statuses = ['pending', 'shipped', 'delivered'];
            
            try {
                const response = await fetch('/api/orders', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        customer_name: customers[Math.floor(Math.random() * customers.length)],
                        product_name: products[Math.floor(Math.random() * products.length)],
                        status: statuses[Math.floor(Math.random() * statuses.length)]
                    })
                });
                
                const result = await response.json();
                if (result.success) {
                    console.log('Order created:', result.order_id);
                }
            } catch (error) {
                console.error('Error creating order:', error);
            }
        }
        
        async function createBatchOrders() {
            for (let i = 0; i < 5; i++) {
                await createTestOrder();
                // Small delay to see individual notifications
                await new Promise(resolve => setTimeout(resolve, 200));
            }
        }
        
        async function updateRandomOrder() {
            if (orderIds.size === 0) {
                alert('No orders available to update. Create some orders first.');
                return;
            }
            
            const ordersArray = Array.from(orderIds);
            const randomOrderId = ordersArray[Math.floor(Math.random() * ordersArray.length)];
            const statuses = ['pending', 'shipped', 'delivered'];
            const randomStatus = statuses[Math.floor(Math.random() * statuses.length)];
            
            try {
                const response = await fetch(`/api/orders/${randomOrderId}`, {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({status: randomStatus})
                });
                
                const result = await response.json();
                if (result.success) {
                    console.log('Order updated:', randomOrderId);
                }
            } catch (error) {
                console.error('Error updating order:', error);
            }
        }
        
        async function deleteRandomOrder() {
            if (orderIds.size === 0) {
                alert('No orders available to delete. Create some orders first.');
                return;
            }
            
            const ordersArray = Array.from(orderIds);
            const randomOrderId = ordersArray[Math.floor(Math.random() * ordersArray.length)];
            
            try {
                const response = await fetch(`/api/orders/${randomOrderId}`, {
                    method: 'DELETE'
                });
                
                const result = await response.json();
                if (result.success) {
                    console.log('Order deleted:', randomOrderId);
                }
            } catch (error) {
                console.error('Error deleting order:', error);
            }
        }
        
        async function loadOrderHistory() {
            try {
                const response = await fetch('/api/orders?limit=20');
                const orders = await response.json();
                
                console.log('Order History:', orders);
                alert(`Loaded ${orders.length} orders. Check console for details.`);
                
                // Update order IDs set
                orders.forEach(order => orderIds.add(order.id));
                
            } catch (error) {
                console.error('Error loading order history:', error);
            }
        }
        
        // Send heartbeat every 30 seconds
        setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send('ping');
            }
        }, 30000);
        
        // Handle page visibility changes
        document.addEventListener('visibilitychange', function() {
            if (document.hidden) {
                console.log('Page hidden - reducing activity');
            } else {
                console.log('Page visible - resuming normal activity');
                loadStats(); // Refresh stats when page becomes visible
            }
        });
    </script>
</body>
</html>
    """)