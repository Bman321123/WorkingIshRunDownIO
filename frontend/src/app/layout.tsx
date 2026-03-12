import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Arb Dashboard",
  description: "Lightning-fast arbitrage dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="min-h-screen">
          <div className="mx-auto max-w-6xl px-4 py-6">{children}</div>
        </div>
      </body>
    </html>
  );
}

