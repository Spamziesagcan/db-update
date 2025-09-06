'use client';

import { useState, useEffect } from 'react';
import { Stats, OrderEvent } from '@/types';
import useOrderWebSocket from '@/hooks/useOrderWebSocket';
import { getStats, createOrder, updateRandomOrder, deleteRandomOrder, getOrders } from '@/services/api';
import StatsGrid from './StatsGrid';
import Controls from './Controls';
import EventsFeed from './EventsFeed';
import ConnectionStatus from './ConnectionStatus';

const API_BASE_URL = "http://localhost:8000/api";
const WS_URL = "ws://localhost:8000/ws";

export default function Dashboard() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [orderIds, setOrderIds] = useState<number[]>([]);
  
  const { isConnected, lastEvent } = useOrderWebSocket(WS_URL);

  const fetchInitialData = async () => {
    try {
      console.log("Fetching initial data from API...");
      const statsData = await getStats(API_BASE_URL);
      setStats(statsData);
      const orders = await getOrders(API_BASE_URL);
      setOrderIds(orders.map(o => o.id));
      console.log("Initial data fetched successfully.", { statsData, orders });
    } catch (error) {
      console.error("Failed to fetch initial data:", error);
    }
  };

  // 1. Fetch initial data ONLY ONCE when the component mounts.
  useEffect(() => {
    fetchInitialData();
  }, []); // Empty dependency array means this runs only once.

  // 2. Use a separate useEffect to handle real-time updates.
  useEffect(() => {
    // If there's no new event, do nothing.
    if (!lastEvent) return;

    console.log("🚀 Event received, starting state update:", { lastEvent });

    // Update the stats locally for an instant UI response.
    setStats(prevStats => {
      if (!prevStats) {
        console.warn("   - Skipping stats update because previous stats are null.");
        return null;
      }
      
      console.log("   - Stats BEFORE update:", JSON.parse(JSON.stringify(prevStats)));

      // Create a deep copy to safely modify.
      const newStats = { 
        ...prevStats,
        orders: { ...prevStats.orders } 
      };

      if (lastEvent.action === 'INSERT' && lastEvent.new_data) {
        const status = lastEvent.new_data.status;
        newStats.orders[status] = (newStats.orders[status] || 0) + 1;
      } else if (lastEvent.action === 'DELETE' && lastEvent.old_data) {
        const status = lastEvent.old_data.status;
        if (newStats.orders[status] > 0) {
          newStats.orders[status] -= 1;
        }
      } else if (lastEvent.action === 'UPDATE' && lastEvent.old_data && lastEvent.new_data) {
        const oldStatus = lastEvent.old_data.status;
        const newStatus = lastEvent.new_data.status;
        if (newStats.orders[oldStatus] > 0) {
          newStats.orders[oldStatus] -= 1;
        }
        newStats.orders[newStatus] = (newStats.orders[newStatus] || 0) + 1;
      }
      
      newStats.total_notifications = prevStats.total_notifications + 1;

      console.log("   - Stats AFTER update:", JSON.parse(JSON.stringify(newStats)));
      
      return newStats;
    });

    // Update our local list of order IDs for the other buttons.
    if (lastEvent.action === 'INSERT') {
      setOrderIds(prev => [...prev, lastEvent.order_id]);
    } else if (lastEvent.action === 'DELETE') {
      setOrderIds(prev => prev.filter(id => id !== lastEvent.order_id));
    }
    
  }, [lastEvent]);

  const handleCreateOrder = async (batch = 1) => {
    for (let i = 0; i < batch; i++) {
      await createOrder(API_BASE_URL);
      if(batch > 1) await new Promise(r => setTimeout(r, 100));
    }
  };

  const handleUpdateOrder = async () => {
    if (orderIds.length === 0) {
      alert('No orders to update. Please create an order first.');
      return;
    }
    await updateRandomOrder(API_BASE_URL, orderIds);
  };

  const handleDeleteOrder = async () => {
     if (orderIds.length === 0) {
        alert('No orders to delete. Please create an order first.');
        return;
    }
    await deleteRandomOrder(API_BASE_URL, orderIds);
  };

  return (
    <>
      <ConnectionStatus isConnected={isConnected} />
      
      {stats && <StatsGrid stats={stats} />}
      
      <Controls 
        onCreateOrder={handleCreateOrder}
        onUpdateOrder={handleUpdateOrder}
        onDeleteOrder={handleDeleteOrder}
        onLoadHistory={fetchInitialData} // Re-use the fetcher for the button
      />
      
      <EventsFeed lastEvent={lastEvent} />
    </>
  );
}