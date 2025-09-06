import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Real-time Order Dashboard",
  description: "A real-time dashboard for monitoring order notifications.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${inter.className} relative`}>
        {/* Aurora Background Effect */}
        <div className="absolute top-0 left-0 -z-10 h-full w-full">
            <div className="relative h-full w-full">
                <div className="absolute -top-40 -right-40 h-[400px] w-[400px] rounded-full bg-emerald-500/30 blur-[150px]"></div>
                <div className="absolute -bottom-40 -left-40 h-[400px] w-[400px] rounded-full bg-sky-500/30 blur-[150px]"></div>
            </div>
        </div>
        
        {children}
      </body>
    </html>
  );
}