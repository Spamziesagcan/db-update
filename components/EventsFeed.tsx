'use client';

import { useState, useEffect, useMemo } from 'react';
import { OrderEvent } from '@/types';
import { motion, AnimatePresence } from 'framer-motion';
import { PlusCircle, RefreshCw, XCircle, Trash } from 'lucide-react';
import clsx from 'clsx';

const eventConfig = {
  INSERT: {
    icon: <PlusCircle className="text-emerald-400" />,
    title: 'New Order Created',
    borderColor: 'border-emerald-500',
  },
  UPDATE: {
    icon: <RefreshCw className="text-amber-400" />,
    title: 'Order Status Updated',
    borderColor: 'border-amber-500',
  },
  DELETE: {
    icon: <XCircle className="text-red-400" />,
    title: 'Order Deleted',
    borderColor: 'border-red-500',
  },
};

const EventCard = ({ event }: { event: OrderEvent }) => {
    const config = eventConfig[event.action];
    
    return (
        <motion.div
            layout
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, x: -20, transition: { duration: 0.2 } }}
            className={clsx(
                'p-4 mb-3 bg-slate-800/50 backdrop-blur-sm border border-slate-700 rounded-lg shadow-lg',
                'border-l-4',
                config.borderColor
            )}
        >
            <div className="flex items-start justify-between">
                <div className="flex items-center gap-3">
                    {config.icon}
                    <div className="font-bold text-slate-100">{config.title} #{event.order_id}</div>
                </div>
                <div className="text-xs text-slate-500">{new Date(event.timestamp).toLocaleTimeString()}</div>
            </div>
            <div className="mt-2 pl-8 text-sm text-slate-400">
                {event.action === 'INSERT' && event.new_data && (
                    <p>Customer <span className="font-medium text-slate-200">{event.new_data.customer_name}</span> ordered <span className="font-medium text-slate-200">{event.new_data.product_name}</span>.</p>
                )}
                {event.action === 'UPDATE' && event.old_data && event.new_data && (
                    <p>Status changed from <span className="font-semibold text-amber-400">{event.old_data.status}</span> → <span className="font-semibold text-emerald-400">{event.new_data.status}</span>.</p>
                )}
                {event.action === 'DELETE' && event.old_data && (
                     <p>Order for customer <span className="font-medium text-slate-200">{event.old_data.customer_name}</span> was removed.</p>
                )}
            </div>
        </motion.div>
    );
};

interface EventsFeedProps {
    lastEvent: OrderEvent | null;
}

export default function EventsFeed({ lastEvent }: EventsFeedProps) {
    const [events, setEvents] = useState<OrderEvent[]>([]);
    const [filter, setFilter] = useState<'ALL' | 'INSERT' | 'UPDATE' | 'DELETE'>('ALL');

    useEffect(() => {
        if (lastEvent) {
            setEvents(prevEvents => [lastEvent, ...prevEvents].slice(0, 100)); // Keep last 100 events
        }
    }, [lastEvent]);

    const filteredEvents = useMemo(() => {
        if (filter === 'ALL') return events;
        return events.filter(event => event.action === filter);
    }, [events, filter]);

    return (
        <div className="bg-slate-800/30 backdrop-blur-sm border border-slate-700 p-6 rounded-xl">
            <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-4">
                <h2 className="text-2xl font-bold text-slate-50 mb-3 sm:mb-0">Live Events</h2>
                <div className="flex flex-wrap items-center gap-2">
                    {(['ALL', 'INSERT', 'UPDATE', 'DELETE'] as const).map(f => (
                        <button key={f} onClick={() => setFilter(f)} className={clsx("px-4 py-1.5 text-sm font-semibold rounded-full transition-colors", filter === f ? 'bg-emerald-500 text-slate-900' : 'bg-slate-700/50 text-slate-300 hover:bg-slate-700')}>
                            {f.charAt(0) + f.slice(1).toLowerCase()}
                        </button>
                    ))}
                     <button onClick={() => setEvents([])} className="p-2 text-sm font-semibold rounded-full transition-colors bg-red-500/20 text-red-400 hover:bg-red-500/40">
                        <Trash size={16} />
                    </button>
                </div>
            </div>
            
            <div className="h-[32rem] overflow-y-auto pr-2 -mr-2">
                 <AnimatePresence>
                    {filteredEvents.length > 0 ? (
                        filteredEvents.map(event => <EventCard key={event.change_id} event={event} />)
                    ) : (
                        <motion.div 
                            initial={{ opacity: 0 }}
                            animate={{ opacity: 1 }}
                            className="text-center text-slate-500 py-20"
                        >
                            <p>Awaiting new order events...</p>
                        </motion.div>
                    )}
                </AnimatePresence>
            </div>
        </div>
    );
}