import { useState, useEffect, useRef } from 'react';
import { OrderEvent } from '@/types';

export default function useOrderWebSocket(url: string) {
    const [isConnected, setIsConnected] = useState(false);
    const [lastEvent, setLastEvent] = useState<OrderEvent | null>(null);
    const ws = useRef<WebSocket | null>(null);
    const reconnectAttempts = useRef(0);
    
    useEffect(() => {
        const connect = () => {
            if (reconnectAttempts.current > 10) {
                console.error("Max reconnect attempts reached.");
                return;
            }

            const socket = new WebSocket(url);
            ws.current = socket;

            socket.onopen = () => {
                console.log('WebSocket connected');
                setIsConnected(true);
                reconnectAttempts.current = 0; // Reset on successful connection
                 // Send heartbeat ping every 30 seconds
                setInterval(() => {
                    if (socket.readyState === WebSocket.OPEN) {
                        socket.send('ping');
                    }
                }, 30000);
            };

            socket.onclose = () => {
                console.log('WebSocket disconnected');
                setIsConnected(false);
                // Exponential backoff for reconnection
                const delay = Math.min(1000 * (2 ** reconnectAttempts.current), 30000);
                setTimeout(connect, delay);
                reconnectAttempts.current++;
            };

            socket.onmessage = (event) => {
                // If the server sends a ping, respond with a pong and do nothing else.
                if (event.data === 'ping') {
                    if (socket.readyState === WebSocket.OPEN) {
                        socket.send('pong');
                    }
                    return;
                }

                // If the server sends a pong (in response to our own ping), ignore it.
                if (event.data === 'pong') {
                    return;
                }

                // Otherwise, try to parse the message as JSON.
                try {
                    const data = JSON.parse(event.data);
                    if (data.event_type === 'order_change') {
                        setLastEvent(data as OrderEvent);
                    }
                } catch (error) {
                    console.error('Failed to parse message:', error, 'Received data:', event.data);
                }
            };

            socket.onerror = (error) => {
                console.error('WebSocket error:', error);
                socket.close();
            };
        };

        connect();

        return () => {
            if (ws.current) {
                ws.current.close();
            }
        };
    }, [url]);

    return { isConnected, lastEvent };
}