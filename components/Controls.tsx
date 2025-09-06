import { Plus, Edit, Trash2, History } from 'lucide-react';
import { ReactNode } from 'react';

const ControlButton = ({ onClick, children }: { onClick: () => void; children: ReactNode }) => (
    <button
      onClick={onClick}
      className="flex items-center justify-center gap-2 px-5 py-2.5 font-semibold text-slate-50 bg-slate-800/50 backdrop-blur-sm border border-slate-700 rounded-lg shadow-lg hover:bg-slate-700/60 hover:border-slate-600 transition-all duration-200"
    >
      {children}
    </button>
);

interface ControlsProps {
    onCreateOrder: (batch?: number) => void;
    onUpdateOrder: () => void;
    onDeleteOrder: () => void;
    onLoadHistory: () => void;
}

export default function Controls({ onCreateOrder, onUpdateOrder, onDeleteOrder, onLoadHistory }: ControlsProps) {
    return (
        <div className="bg-slate-800/30 backdrop-blur-sm border border-slate-700 p-4 rounded-xl mb-8">
            <div className="flex flex-wrap items-center gap-4">
                <ControlButton onClick={() => onCreateOrder(1)}><Plus size={18} /> Create Order</ControlButton>
                <ControlButton onClick={() => onCreateOrder(5)}><Plus size={18} /> Create 5</ControlButton>
                <ControlButton onClick={onUpdateOrder}><Edit size={18} /> Update Random</ControlButton>
                <ControlButton onClick={onDeleteOrder}><Trash2 size={18} /> Delete Random</ControlButton>
                <ControlButton onClick={onLoadHistory}><History size={18} /> Reload Orders</ControlButton>
            </div>
        </div>
    );
}