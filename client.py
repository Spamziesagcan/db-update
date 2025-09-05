import asyncio
import websockets
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def client():
    uri = "ws://localhost:8000/ws"
    
    try:
        async with websockets.connect(uri) as websocket:
            logger.info("Connected to WebSocket server")
            
            # Send heartbeat every 30 seconds
            heartbeat_task = asyncio.create_task(heartbeat(websocket))
            
            try:
                async for message in websocket:
                    if message == "pong":
                        logger.debug("Received heartbeat response")
                        continue
                    
                    try:
                        data = json.loads(message)
                        print(f"\n🔔 NOTIFICATION RECEIVED:")
                        print(f"   Action: {data['action']}")
                        print(f"   Order ID: {data['order_id']}")
                        print(f"   Time: {data['timestamp']}")
                        
                        if data['action'] == 'INSERT':
                            print(f"   ➕ New Order: {data['new_data']['customer_name']} - {data['new_data']['product_name']} ({data['new_data']['status']})")
                        elif data['action'] == 'UPDATE':
                            print(f"   🔄 Status Update: {data['old_data']['status']} → {data['new_data']['status']}")
                        elif data['action'] == 'DELETE':
                            print(f"   ❌ Deleted Order: {data['old_data']['customer_name']} - {data['old_data']['product_name']}")
                        
                        print("-" * 50)
                    
                    except json.JSONDecodeError:
                        logger.error("Received invalid JSON message")
            
            finally:
                heartbeat_task.cancel()
    
    except Exception as e:
        logger.error(f"Connection error: {e}")

async def heartbeat(websocket):
    """Send periodic heartbeat to keep connection alive"""
    while True:
        try:
            await asyncio.sleep(30)
            await websocket.send("ping")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")
            break

if __name__ == "__main__":
    print("🚀 Starting Real-time Order Notification Client")
    print("   Connecting to ws://localhost:8000/ws")
    print("   Press Ctrl+C to exit\n")
    
    try:
        asyncio.run(client())
    except KeyboardInterrupt:
        print("\n👋 Client disconnected")