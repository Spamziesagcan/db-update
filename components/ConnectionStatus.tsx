import clsx from 'clsx';

interface ConnectionStatusProps {
    isConnected: boolean;
}

export default function ConnectionStatus({ isConnected }: ConnectionStatusProps) {
    return (
        <div className="fixed top-6 right-6 z-50">
            <div className={clsx(
                'px-4 py-2 rounded-full text-sm font-semibold shadow-lg flex items-center gap-2.5 transition-all',
                isConnected ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400 animate-pulse'
            )}>
                <span className={clsx(
                    'h-2.5 w-2.5 rounded-full',
                    isConnected ? 'bg-emerald-400' : 'bg-red-400'
                )}></span>
                {isConnected ? 'Connected' : 'Connecting...'}
            </div>
        </div>
    );
}