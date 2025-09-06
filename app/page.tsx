import Dashboard from "@/components/Dashboard";

export default function Home() {
  return (
    <main className="min-h-screen p-4 sm:p-6 lg:p-8 flex flex-col items-center">
      <div className="w-full max-w-7xl">
        <header className="text-center mb-10">
          <h1 className="text-4xl sm:text-5xl font-bold text-slate-50 tracking-tight">
            Real-time Order Dashboard
          </h1>
          <p className="text-slate-400 mt-3 text-lg">
            Live updates from the order processing system
          </p>
        </header>
        
        <Dashboard />
      </div>
    </main>
  );
}