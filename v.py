import asyncio
import json
import logging
import os
import signal
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Any
import uuid
from dataclasses import dataclass, asdict
from enum import Enum
import hashlib
import hmac

import pymysql
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, validator
import aiomysql
from aiomysql import Pool
import redis.asyncio as redis
from prometheus_client import Counter, Histogram, Gauge, generate_latest
import structlog

# Enhanced structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Metrics
NOTIFICATION_COUNTER = Counter('notifications_total', 'Total notifications sent', ['action', 'channel'])
CONNECTION_GAUGE = Gauge('websocket_connections_total', 'Active WebSocket connections')
MESSAGE_HISTOGRAM = Histogram('message_processing_seconds', 'Message processing time')
ORDER_COUNTER = Counter('orders_total', 'Total orders processed', ['action'])

class OrderStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"

class NotificationChannel(str, Enum):
    WEBSOCKET = "websocket"
    WEBHOOK = "webhook"
    EMAIL = "email"
    SMS = "sms"

@dataclass
class NotificationRule:
    """Rules for when to send notifications"""
    id: str
    name: str
    conditions: Dict[str, Any]
    channels: List[NotificationChannel]
    recipients: List[str]
    active: bool = True
    created_at: datetime = None
    
    def matches(self, event_data: Dict[str, Any]) -> bool:
        """Check if notification rule matches the event data"""
        for field, condition in self.conditions.items():
            if field not in event_data:
                continue
            
            value = event_data[field]
            
            if isinstance(condition, dict):
                if 'equals' in condition and value != condition['equals']:
                    return False
                if 'in' in condition and value not in condition['in']:
                    return False
                if 'regex' in condition:
                    import re
                    if not re.match(condition['regex'], str(value)):
                        return False
            elif value != condition:
                return False
        
        return True

class Config:
    """Enhanced application configuration"""
    # Database settings
    DB_HOST: str = os.getenv('DB_HOST', 'localhost')
    DB_USER: str = os.getenv('DB_USER', 'root')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD', '')
    DB_NAME: str = os.getenv('DB_NAME', 'realtime_orders')
    DB_PORT: int = int(os.getenv('DB_PORT', 3306))
    DB_POOL_MIN_SIZE: int = int(os.getenv('DB_POOL_MIN_SIZE', 5))
    DB_POOL_MAX_SIZE: int = int(os.getenv('DB_POOL_MAX_SIZE', 20))
    
    # Redis settings for caching and pub/sub
    REDIS_URL: str = os.getenv('REDIS_URL', 'redis://localhost:6379')
    REDIS_POOL_SIZE: int = int(os.getenv('REDIS_POOL_SIZE', 10))
    
    # Notification settings
    POLL_INTERVAL: float = float(os.getenv('POLL_INTERVAL', 0.1))
    MAX_BATCH_SIZE: int = int(os.getenv('MAX_BATCH_SIZE', 100))
    NOTIFICATION_RETRY_ATTEMPTS: int = int(os.getenv('NOTIFICATION_RETRY_ATTEMPTS', 3))
    
    # WebSocket settings
    HEARTBEAT_INTERVAL: int = int(os.getenv('HEARTBEAT_INTERVAL', 30))
    CONNECTION_TIMEOUT: int = int(os.getenv('CONNECTION_TIMEOUT', 300))
    MAX_CONNECTIONS_PER_CLIENT: int = int(os.getenv('MAX_CONNECTIONS_PER_CLIENT', 5))
    
    # Security settings
    JWT_SECRET: str = os.getenv('JWT_SECRET', 'your-secret-key-change-in-production')
    API_RATE_LIMIT: int = int(os.getenv('API_RATE_LIMIT', 100))
    
    # Webhook settings
    WEBHOOK_TIMEOUT: int = int(os.getenv('WEBHOOK_TIMEOUT', 10))
    WEBHOOK_RETRY_ATTEMPTS: int = int(os.getenv('WEBHOOK_RETRY_ATTEMPTS', 3))
    
    # Performance settings
    ENABLE_COMPRESSION: bool = os.getenv('ENABLE_COMPRESSION', 'true').lower() == 'true'
    ENABLE_METRICS: bool = os.getenv('ENABLE_METRICS', 'true').lower() == 'true'
    CACHE_TTL: int = int(os.getenv('CACHE_TTL', 300))  # 5 minutes

config = Config()

class RateLimiter:
    """Simple rate limiter using Redis"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
    
    async def is_allowed(self, key: str, limit: int, window: int = 60) -> bool:
        """Check if request is within rate limit"""
        pipe = self.redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, window)
        results = await pipe.execute()
        
        return results[0] <= limit

class EnhancedConnectionManager:
    """Enhanced WebSocket connection manager with advanced features"""
    
    def __init__(self, redis_client: redis.Redis = None):
        self.active_connections: Dict[str, Dict[str, Any]] = {}
        self.client_subscriptions: Dict[str, Set[str]] = {}  # Client to order IDs
        self.order_subscribers: Dict[str, Set[str]] = {}    # Order ID to clients
        self.rate_limiter = RateLimiter(redis_client) if redis_client else None
        self.redis = redis_client
        
    async def connect(self, websocket: WebSocket, client_id: Optional[str] = None,
                     auth_token: Optional[str] = None) -> str:
        """Connect a WebSocket client with authentication and rate limiting"""
        await websocket.accept()
        
        if not client_id:
            client_id = str(uuid.uuid4())
        
        # Rate limiting check
        if self.rate_limiter:
            client_ip = websocket.client.host if websocket.client else "unknown"
            if not await self.rate_limiter.is_allowed(f"conn:{client_ip}", 
                                                    config.MAX_CONNECTIONS_PER_CLIENT):
                await websocket.close(code=1008, reason="Rate limit exceeded")
                raise HTTPException(status_code=429, detail="Too many connections")
        
        self.active_connections[client_id] = {
            'websocket': websocket,
            'connected_at': datetime.now(timezone.utc),
            'last_ping': datetime.now(timezone.utc),
            'message_count': 0,
            'auth_token': auth_token,
            'ip_address': websocket.client.host if websocket.client else None,
            'user_agent': None  # Could be extracted from headers
        }
        
        self.client_subscriptions[client_id] = set()
        CONNECTION_GAUGE.inc()
        
        logger.info("client_connected", client_id=client_id, 
                   total_connections=len(self.active_connections))
        return client_id
    
    def disconnect(self, client_id: str):
        """Disconnect a WebSocket client and clean up subscriptions"""
        if client_id in self.active_connections:
            # Clean up subscriptions
            if client_id in self.client_subscriptions:
                for order_id in self.client_subscriptions[client_id]:
                    if order_id in self.order_subscribers:
                        self.order_subscribers[order_id].discard(client_id)
                        if not self.order_subscribers[order_id]:
                            del self.order_subscribers[order_id]
                del self.client_subscriptions[client_id]
            
            del self.active_connections[client_id]
            CONNECTION_GAUGE.dec()
            
            logger.info("client_disconnected", client_id=client_id,
                       total_connections=len(self.active_connections))
    
    async def subscribe_to_order(self, client_id: str, order_id: str):
        """Subscribe client to specific order notifications"""
        if client_id not in self.client_subscriptions:
            return False
        
        self.client_subscriptions[client_id].add(order_id)
        
        if order_id not in self.order_subscribers:
            self.order_subscribers[order_id] = set()
        self.order_subscribers[order_id].add(client_id)
        
        logger.info("client_subscribed", client_id=client_id, order_id=order_id)
        return True
    
    async def unsubscribe_from_order(self, client_id: str, order_id: str):
        """Unsubscribe client from specific order notifications"""
        if client_id in self.client_subscriptions:
            self.client_subscriptions[client_id].discard(order_id)
        
        if order_id in self.order_subscribers:
            self.order_subscribers[order_id].discard(client_id)
            if not self.order_subscribers[order_id]:
                del self.order_subscribers[order_id]
        
        logger.info("client_unsubscribed", client_id=client_id, order_id=order_id)
    
    async def send_to_client(self, client_id: str, message: dict, 
                           retry_attempts: int = 1) -> bool:
        """Send message to specific client with retry logic"""
        if client_id not in self.active_connections:
            return False
        
        connection_info = self.active_connections[client_id]
        websocket = connection_info['websocket']
        
        for attempt in range(retry_attempts):
            try:
                message_str = json.dumps(message, default=str)
                
                # Compress message if enabled and size is significant
                if config.ENABLE_COMPRESSION and len(message_str) > 1024:
                    import gzip
                    compressed = gzip.compress(message_str.encode())
                    if len(compressed) < len(message_str):
                        await websocket.send_bytes(compressed)
                    else:
                        await websocket.send_text(message_str)
                else:
                    await websocket.send_text(message_str)
                
                connection_info['message_count'] += 1
                return True
                
            except Exception as e:
                logger.error("send_error", client_id=client_id, attempt=attempt + 1, error=str(e))
                if attempt == retry_attempts - 1:
                    self.disconnect(client_id)
                    return False
                await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
        
        return False
    
    async def broadcast_to_order_subscribers(self, order_id: str, message: dict) -> int:
        """Broadcast message to all clients subscribed to a specific order"""
        if order_id not in self.order_subscribers:
            return 0
        
        subscribers = self.order_subscribers[order_id].copy()
        successful_sends = 0
        
        tasks = []
        for client_id in subscribers:
            task = self.send_to_client(client_id, message)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful_sends = sum(1 for result in results if result is True)
        
        return successful_sends
    
    async def broadcast(self, message: dict, exclude_clients: Set[str] = None, 
                       filter_func=None) -> int:
        """Enhanced broadcast with filtering and performance optimization"""
        if not self.active_connections:
            return 0
        
        exclude_clients = exclude_clients or set()
        successful_sends = 0
        
        # Prepare message once
        message_str = json.dumps(message, default=str)
        
        # Use asyncio.gather for concurrent sending
        tasks = []
        for client_id, conn_info in self.active_connections.copy().items():
            if client_id in exclude_clients:
                continue
            
            # Apply custom filter if provided
            if filter_func and not filter_func(client_id, conn_info):
                continue
            
            task = self._send_prepared_message(client_id, message_str)
            tasks.append(task)
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful_sends = sum(1 for result in results if result is True)
        
        NOTIFICATION_COUNTER.labels(action=message.get('action', 'unknown'), 
                                  channel='websocket').inc(successful_sends)
        
        return successful_sends
    
    async def _send_prepared_message(self, client_id: str, message_str: str) -> bool:
        """Send pre-serialized message to client"""
        if client_id not in self.active_connections:
            return False
        
        try:
            websocket = self.active_connections[client_id]['websocket']
            await websocket.send_text(message_str)
            self.active_connections[client_id]['message_count'] += 1
            return True
        except Exception as e:
            logger.error("broadcast_send_error", client_id=client_id, error=str(e))
            self.disconnect(client_id)
            return False
    
    def get_connection_stats(self) -> dict:
        """Get comprehensive connection statistics"""
        stats = {
            'total_connections': len(self.active_connections),
            'total_subscriptions': sum(len(subs) for subs in self.client_subscriptions.values()),
            'orders_with_subscribers': len(self.order_subscribers),
            'connections': {}
        }
        
        for client_id, conn_info in self.active_connections.items():
            duration = (datetime.now(timezone.utc) - conn_info['connected_at']).total_seconds()
            stats['connections'][client_id] = {
                'connected_duration': duration,
                'message_count': conn_info['message_count'],
                'last_ping': conn_info['last_ping'].isoformat(),
                'subscriptions': len(self.client_subscriptions.get(client_id, set())),
                'ip_address': conn_info.get('ip_address')
            }
        
        return stats

# Global instances
manager = EnhancedConnectionManager()
db_pool = None
redis_client = None

class NotificationRuleManager:
    """Manage notification rules and routing"""
    
    def __init__(self):
        self.rules: Dict[str, NotificationRule] = {}
        self.webhook_session = None
    
    async def load_rules_from_db(self):
        """Load notification rules from database"""
        if not db_pool:
            return
        
        try:
            pool = db_pool.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("SELECT * FROM notification_rules WHERE active = 1")
                    rules = await cursor.fetchall()
                    
                    for rule_data in rules:
                        rule = NotificationRule(
                            id=rule_data['id'],
                            name=rule_data['name'],
                            conditions=json.loads(rule_data['conditions']),
                            channels=[NotificationChannel(ch) for ch in json.loads(rule_data['channels'])],
                            recipients=json.loads(rule_data['recipients']),
                            active=rule_data['active'],
                            created_at=rule_data['created_at']
                        )
                        self.rules[rule.id] = rule
                        
            logger.info("notification_rules_loaded", count=len(self.rules))
        except Exception as e:
            logger.error("error_loading_notification_rules", error=str(e))
    
    async def process_event(self, event_data: Dict[str, Any]):
        """Process event against all notification rules"""
        matching_rules = []
        
        for rule in self.rules.values():
            if rule.active and rule.matches(event_data):
                matching_rules.append(rule)
        
        if not matching_rules:
            return
        
        # Process notifications for each matching rule
        tasks = []
        for rule in matching_rules:
            for channel in rule.channels:
                task = self._send_notification(rule, channel, event_data)
                tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_notification(self, rule: NotificationRule, channel: NotificationChannel, 
                               event_data: Dict[str, Any]):
        """Send notification via specific channel"""
        try:
            if channel == NotificationChannel.WEBSOCKET:
                await self._send_websocket_notification(rule, event_data)
            elif channel == NotificationChannel.WEBHOOK:
                await self._send_webhook_notification(rule, event_data)
            elif channel == NotificationChannel.EMAIL:
                await self._send_email_notification(rule, event_data)
            elif channel == NotificationChannel.SMS:
                await self._send_sms_notification(rule, event_data)
            
            NOTIFICATION_COUNTER.labels(
                action=event_data.get('action', 'unknown'),
                channel=channel.value
            ).inc()
            
        except Exception as e:
            logger.error("notification_send_error", rule_id=rule.id, 
                        channel=channel.value, error=str(e))
    
    async def _send_websocket_notification(self, rule: NotificationRule, event_data: Dict[str, Any]):
        """Send WebSocket notification"""
        message = {
            'event_type': 'order_change',
            'rule_id': rule.id,
            'rule_name': rule.name,
            **event_data
        }
        
        # Send to all connected clients or specific subscribers
        if 'order_id' in event_data:
            await manager.broadcast_to_order_subscribers(str(event_data['order_id']), message)
        else:
            await manager.broadcast(message)
    
    async def _send_webhook_notification(self, rule: NotificationRule, event_data: Dict[str, Any]):
        """Send webhook notification"""
        if not self.webhook_session:
            import aiohttp
            self.webhook_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=config.WEBHOOK_TIMEOUT)
            )
        
        payload = {
            'rule_id': rule.id,
            'rule_name': rule.name,
            'event_data': event_data,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        for recipient in rule.recipients:
            if not recipient.startswith('http'):
                continue
            
            for attempt in range(config.WEBHOOK_RETRY_ATTEMPTS):
                try:
                    async with self.webhook_session.post(recipient, json=payload) as response:
                        if response.status < 400:
                            break
                except Exception as e:
                    if attempt == config.WEBHOOK_RETRY_ATTEMPTS - 1:
                        logger.error("webhook_failed", url=recipient, error=str(e))
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
    
    async def _send_email_notification(self, rule: NotificationRule, event_data: Dict[str, Any]):
        """Send email notification (placeholder - implement with your email service)"""
        # Implement email sending logic here
        logger.info("email_notification_placeholder", rule_id=rule.id, recipients=rule.recipients)
    
    async def _send_sms_notification(self, rule: NotificationRule, event_data: Dict[str, Any]):
        """Send SMS notification (placeholder - implement with your SMS service)"""
        # Implement SMS sending logic here
        logger.info("sms_notification_placeholder", rule_id=rule.id, recipients=rule.recipients)

notification_rule_manager = NotificationRuleManager()

class EnhancedDatabaseNotificationListener:
    """Enhanced database notification listener with Redis pub/sub and advanced features"""
    
    def __init__(self):
        self.last_processed_id = 0
        self.running = False
        self.retry_count = 0
        self.max_retries = 5
        self.batch_processor_task = None
        self.notification_queue = asyncio.Queue(maxsize=1000)
        self.dead_letter_queue = asyncio.Queue(maxsize=100)
        
    async def initialize(self):
        """Initialize the listener"""
        try:
            # Get last processed ID from Redis cache first, then database
            if redis_client:
                cached_id = await redis_client.get("last_processed_notification_id")
                if cached_id:
                    self.last_processed_id = int(cached_id)
                    logger.info("loaded_last_processed_id_from_cache", id=self.last_processed_id)
                    return
            
            # Fallback to database
            pool = db_pool.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("SELECT MAX(id) as max_id FROM order_notifications")
                    result = await cursor.fetchone()
                    self.last_processed_id = result['max_id'] or 0
                    
                    # Cache in Redis
                    if redis_client:
                        await redis_client.set("last_processed_notification_id", 
                                             self.last_processed_id, ex=3600)
                    
                    logger.info("initialized_listener", last_processed_id=self.last_processed_id)
        except Exception as e:
            logger.error("listener_initialization_error", error=str(e))
            raise
    
    async def start_listening(self):
        """Start the notification listener"""
        self.running = True
        logger.info("starting_notification_listener")
        
        # Start batch processor
        self.batch_processor_task = asyncio.create_task(self._batch_processor())
        
        # Start dead letter queue processor
        dlq_processor_task = asyncio.create_task(self._dead_letter_processor())
        
        try:
            while self.running:
                try:
                    await self._poll_notifications()
                    self.retry_count = 0
                    await asyncio.sleep(config.POLL_INTERVAL)
                except Exception as e:
                    self.retry_count += 1
                    logger.error("polling_error", attempt=self.retry_count, error=str(e))
                    
                    if self.retry_count >= self.max_retries:
                        logger.critical("max_retries_reached")
                        break
                    
                    wait_time = min(2 ** self.retry_count, 30)
                    await asyncio.sleep(wait_time)
        finally:
            dlq_processor_task.cancel()
    
    async def _poll_notifications(self):
        """Poll for new notifications with enhanced error handling"""
        with MESSAGE_HISTOGRAM.time():
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
                            try:
                                await asyncio.wait_for(
                                    self.notification_queue.put(notification),
                                    timeout=1.0
                                )
                                self.last_processed_id = notification['id']
                            except asyncio.TimeoutError:
                                # Queue is full, add to dead letter queue
                                try:
                                    self.dead_letter_queue.put_nowait(notification)
                                except asyncio.QueueFull:
                                    logger.warning("dead_letter_queue_full", 
                                                 notification_id=notification['id'])
                        
                        # Update cache periodically
                        if redis_client and notifications:
                            await redis_client.set("last_processed_notification_id", 
                                                 self.last_processed_id, ex=3600)
            
            except Exception as e:
                logger.error("database_polling_error", error=str(e))
                raise
    
    async def _batch_processor(self):
        """Process notifications in optimized batches"""
        batch = []
        batch_timeout = 0.05
        
        while self.running:
            try:
                try:
                    notification = await asyncio.wait_for(
                        self.notification_queue.get(), timeout=batch_timeout
                    )
                    batch.append(notification)
                    
                    # Process when batch is optimal size or queue is empty
                    if len(batch) >= 10 or self.notification_queue.empty():
                        await self._process_notification_batch(batch)
                        batch.clear()
                        
                except asyncio.TimeoutError:
                    if batch:
                        await self._process_notification_batch(batch)
                        batch.clear()
                        
            except Exception as e:
                logger.error("batch_processor_error", error=str(e))
                await asyncio.sleep(1)
    
    async def _dead_letter_processor(self):
        """Process failed notifications from dead letter queue"""
        while self.running:
            try:
                notification = await asyncio.wait_for(
                    self.dead_letter_queue.get(), timeout=5.0
                )
                
                # Retry processing with delay
                await asyncio.sleep(1)
                await self._process_single_notification(notification)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("dead_letter_processor_error", error=str(e))
                await asyncio.sleep(1)
    
    async def _process_notification_batch(self, notifications: List[dict]):
        """Process a batch of notifications with parallel execution"""
        if not notifications:
            return
        
        tasks = []
        for notification in notifications:
            task = self._process_single_notification(notification)
            tasks.append(task)
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.debug("batch_processed", count=len(notifications))
        except Exception as e:
            logger.error("batch_processing_error", error=str(e))
    
    async def _process_single_notification(self, notification: dict):
        """Process a single notification"""
        try:
            message = await self._create_notification_message(notification)
            if not message:
                return
            
            # Send via WebSocket
            await manager.broadcast(message)
            
            # Process through notification rules
            await notification_rule_manager.process_event(message)
            
            # Publish to Redis for other services
            if redis_client:
                await redis_client.publish('order_notifications', json.dumps(message, default=str))
            
        except Exception as e:
            logger.error("single_notification_error", 
                        notification_id=notification.get('id'), error=str(e))
    
    async def _create_notification_message(self, notification: dict) -> Optional[dict]:
        """Create enhanced notification message with validation"""
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
                'change_id': notification['id'],
                'server_id': os.getenv('SERVER_ID', 'server-1'),  # For multi-server setups
                'version': '2.0'
            }
            
            # Add change summary for easy filtering
            if old_data and new_data:
                changed_fields = []
                for key in new_data:
                    if key in old_data and old_data[key] != new_data[key]:
                        changed_fields.append(key)
                message['changed_fields'] = changed_fields
            
            return message
            
        except Exception as e:
            logger.error("message_creation_error", error=str(e))
            return None
    
    def stop(self):
        """Stop the notification listener"""
        self.running = False
        if self.batch_processor_task:
            self.batch_processor_task.cancel()
        logger.info("notification_listener_stopped")

# Global listener instance
notification_listener = None