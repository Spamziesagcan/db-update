import { Stats } from '@/types';
import { Users, Package, Clock, Bell } from 'lucide-react';
import StatsCard from './StatsCard';

interface StatsGridProps {
  stats: Stats;
}

export default function StatsGrid({ stats }: StatsGridProps) {
    const totalOrders = Object.values(stats.orders).reduce((sum, count) => sum + count, 0);

    return (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
            <StatsCard
                label="Active Connections"
                value={stats.connections.total_connections}
                icon={<Users className="text-emerald-400" size={28}/>}
            />
            <StatsCard
                label="Total Orders"
                value={totalOrders}
                icon={<Package className="text-sky-400" size={28} />}
            />
            <StatsCard
                label="Pending Orders"
                value={stats.orders.pending || 0}
                icon={<Clock className="text-amber-400" size={28} />}
            />
            <StatsCard
                label="Notifications Sent"
                value={stats.total_notifications}
                icon={<Bell className="text-violet-400" size={28} />}
            />
        </div>
    );
}