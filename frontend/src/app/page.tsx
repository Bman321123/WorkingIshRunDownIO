"use client";

import { useEffect, useMemo, useState } from "react";
import type { Arb, RawLine } from "@/lib/types";
import { ArbCard } from "@/components/ArbCard";

const DEFAULT_BASE = "http://127.0.0.1:3030";

const SPORTS = ["NCAAF", "NFL", "MLB", "NBA", "NCAAB", "NHL", "NCAAWB", "MMA"] as const;
type Sport = (typeof SPORTS)[number];

type ScanNowResponse = {
  ok: boolean;
  sports?: string[];
  lastScanMs?: number;
  count?: number;
  arbs?: Arb[];
  lines?: RawLine[];
  error?: string;
  dataAge?: number;
  dpRemaining?: string;
};

export default function HomePage() {
  const baseUrl = process.env.NEXT_PUBLIC_ARBS_URL || DEFAULT_BASE;
  const arbsUrl = `${baseUrl.replace(/\/$/, "")}/arbs`;
  const scanUrl = `${baseUrl.replace(/\/$/, "")}/scan-now`;

  const [selectedSports, setSelectedSports] = useState<Set<Sport>>(() => new Set(SPORTS));
  const [arbs, setArbs] = useState<Arb[]>([]);
  const [lines, setLines] = useState<RawLine[]>([]);
  const [status, setStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [scanStatus, setScanStatus] = useState<"idle" | "scanning" | "error">("idle");
  const [error, setError] = useState<string | null>(null);
  const [lastScannedMs, setLastScannedMs] = useState<number | null>(null);
  const [dataAge, setDataAge] = useState<number | null>(null);
  const [dpRemaining, setDpRemaining] = useState<string | null>(null);

  const [activeTab, setActiveTab] = useState<"arbs" | "lines">("arbs");
  const [expandedBooks, setExpandedBooks] = useState<Set<string>>(new Set());

  const count = arbs.length;
  const header = useMemo(() => {
    if (status === "loading") return "Loading…";
    if (status === "error") return "Disconnected";
    return "Cached";
  }, [status]);

  // Initial load: fetch cached arbs once
  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        setStatus("loading");
        const resp = await fetch(arbsUrl, { cache: "no-store" });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        if (!mounted) return;
        setArbs(Array.isArray(data.arbs) ? data.arbs : Array.isArray(data) ? data : []);
        setLines(Array.isArray(data.lines) ? data.lines : []);
        if (typeof data.dataAge === "number") setDataAge(data.dataAge);
        if (data.dpRemaining) setDpRemaining(data.dpRemaining);
        setStatus("ok");
        setError(null);
      } catch (e: any) {
        if (!mounted) return;
        setStatus("error");
        setError(e?.message || "Failed to load");
      }
    })();
    return () => {
      mounted = false;
    };
  }, [arbsUrl]);

  async function scanNow() {
    const sports = Array.from(selectedSports);
    if (sports.length === 0) return;
    try {
      setScanStatus("scanning");
      setError(null);
      const resp = await fetch(scanUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sports }),
      });
      const payload = (await resp.json()) as ScanNowResponse;
      if (!resp.ok || !payload?.ok) {
        throw new Error(payload?.error || `HTTP ${resp.status}`);
      }
      setArbs(Array.isArray(payload.arbs) ? payload.arbs : []);
      setLines(Array.isArray(payload.lines) ? payload.lines : []);
      setLastScannedMs(payload.lastScanMs ?? Date.now());
      if (typeof payload.dataAge === "number") setDataAge(payload.dataAge);
      if (payload.dpRemaining) setDpRemaining(payload.dpRemaining);
      setScanStatus("idle");
    } catch (e: any) {
      setScanStatus("error");
      setError(e?.message || "Scan failed");
    }
  }

  return (
    <main className="space-y-5">
      <header className="flex flex-col gap-3 rounded-2xl border border-zinc-800 bg-zinc-900/30 px-5 py-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="text-sm font-semibold text-zinc-100">Arbitrage Dashboard</div>
            <div className="mt-0.5 text-xs text-zinc-400">
              Source: <span className="font-mono text-zinc-300">{baseUrl}</span>
            </div>
          </div>

          <div className="flex items-center gap-4">
            {dataAge !== null && (
              <div className="text-right">
                <div className="text-xs text-zinc-400">Data Age</div>
                <div className={[
                  "text-sm font-semibold",
                  dataAge > 300 ? "text-red-400" : dataAge > 120 ? "text-amber-300" : "text-emerald-300",
                ].join(" ")}>
                  {dataAge < 60 ? `${dataAge}s` : `${Math.floor(dataAge / 60)}m ${dataAge % 60}s`}
                </div>
              </div>
            )}
            {dpRemaining && (
              <div className="text-right">
                <div className="text-xs text-zinc-400">DP Left</div>
                <div className="text-sm font-semibold text-zinc-200">{dpRemaining}</div>
              </div>
            )}
            <div className="text-right">
              <div className="text-xs text-zinc-400">Status</div>
              <div className="text-sm font-semibold text-zinc-100">{header}</div>
            </div>
          </div>
        </div>

        {dataAge !== null && dataAge > 300 && (
          <div className="rounded-lg bg-red-500/10 border border-red-500/30 px-3 py-2 text-xs text-red-300">
            ⚠ Data is {Math.floor(dataAge / 60)}+ minutes old. Odds may have changed. Click &quot;Scan Lines&quot; to refresh.
          </div>
        )}

        <div className="flex flex-wrap items-center gap-2">
          {SPORTS.map((s) => {
            const active = selectedSports.has(s);
            return (
              <button
                key={s}
                type="button"
                onClick={() => {
                  setSelectedSports((prev) => {
                    const next = new Set(prev);
                    if (next.has(s)) next.delete(s);
                    else next.add(s);
                    return next;
                  });
                }}
                className={[
                  "rounded-full px-3 py-1 text-xs font-semibold transition",
                  active
                    ? "bg-emerald-500/15 text-emerald-200 ring-1 ring-emerald-500/40"
                    : "bg-zinc-800/60 text-zinc-300 ring-1 ring-zinc-700",
                ].join(" ")}
              >
                {s}
              </button>
            );
          })}
          <div className="flex-1" />
          <button
            type="button"
            disabled={selectedSports.size === 0 || scanStatus === "scanning"}
            onClick={scanNow}
            className={[
              "inline-flex items-center gap-2 rounded-xl px-4 py-2 text-sm font-semibold",
              "bg-emerald-500 text-zinc-950 hover:bg-emerald-400 disabled:cursor-not-allowed disabled:opacity-60",
            ].join(" ")}
          >
            {scanStatus === "scanning" ? (
              <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-zinc-900/40 border-t-zinc-950" />
            ) : null}
            Scan Lines
          </button>
        </div>

        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="text-xs text-zinc-400">
            Opportunities: <span className="font-semibold text-zinc-200">{count}</span>
          </div>
          <div className="text-xs text-zinc-400">
            Last scanned:{" "}
            <span className="font-semibold text-zinc-200">
              {lastScannedMs ? new Date(lastScannedMs).toLocaleTimeString() : "—"}
            </span>
          </div>
        </div>

        {status === "error" || scanStatus === "error" ? (
          <div className="text-xs text-rose-300">
            Error: {error}. Make sure `/Users/seniortech/therundownioV1/server.py` is running on port 3030.
          </div>
        ) : null}

        <div className="flex gap-4 border-b border-zinc-800 pt-2">
          <button
            onClick={() => setActiveTab("arbs")}
            className={`pb-2 text-sm font-semibold transition border-b-2 ${
              activeTab === "arbs" ? "border-emerald-500 text-emerald-400" : "border-transparent text-zinc-400 hover:text-zinc-200"
            }`}
          >
            Arbitrage Opportunities
          </button>
          <button
            onClick={() => setActiveTab("lines")}
            className={`pb-2 text-sm font-semibold transition border-b-2 ${
              activeTab === "lines" ? "border-emerald-500 text-emerald-400" : "border-transparent text-zinc-400 hover:text-zinc-200"
            }`}
          >
            Raw Book Lines
          </button>
        </div>
      </header>
        
      {activeTab === "arbs" ? (
        <section className="grid grid-cols-1 gap-5">
          {arbs.map((arb, idx) => (
            <ArbCard key={`${arb.game}-${arb.market_kind}-${arb.line_label ?? ""}-${idx}`} arb={arb} />
          ))}
        </section>
      ) : (
        <section className="space-y-4">
          {Array.from(new Set(lines.map(l => l.book))).sort().map(bookName => {
            const isExpanded = expandedBooks.has(bookName);
            const bookLines = lines.filter(l => l.book === bookName);
            return (
              <div key={bookName} className="rounded-2xl border border-zinc-800 bg-zinc-900/40 shadow-card overflow-hidden">
                <button 
                  onClick={() => setExpandedBooks(prev => {
                    const next = new Set(prev);
                    if (next.has(bookName)) next.delete(bookName);
                    else next.add(bookName);
                    return next;
                  })}
                  className="w-full flex items-center justify-between px-5 py-4 bg-zinc-800/20 hover:bg-zinc-800/40 transition"
                >
                  <div className="font-semibold text-zinc-100 flex items-center gap-3">
                    <span className="text-lg">{bookName}</span>
                    <span className="text-xs font-mono text-zinc-400 bg-zinc-900 px-2 py-0.5 rounded-full">{bookLines.length} lines pulled</span>
                  </div>
                  <div className="text-zinc-400">{isExpanded ? "▲ Hide" : "▼ Show"}</div>
                </button>
                {isExpanded && (
                  <div className="p-5 border-t border-zinc-800">
                    <table className="w-full text-left text-sm text-zinc-300">
                      <thead>
                        <tr className="border-b border-zinc-800 text-zinc-400">
                          <th className="font-medium pb-2">Time</th>
                          <th className="font-medium pb-2">Sport</th>
                          <th className="font-medium pb-2">Game</th>
                          <th className="font-medium pb-2">Market</th>
                          <th className="font-medium pb-2">Side</th>
                          <th className="font-medium text-right pb-2">American Odds</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-zinc-800/50">
                        {bookLines.map((l, i) => (
                          <tr key={i} className="hover:bg-zinc-800/30 transition group">
                           <td className="py-2.5 px-2 text-xs font-mono text-zinc-500 whitespace-nowrap">
                             {l.updated_at ? new Date(l.updated_at).toLocaleTimeString() : 'N/A'}
                           </td>
                           <td className="py-2.5 px-2 font-semibold text-zinc-200">{l.sport}</td>
                           <td className="py-2.5 px-2 text-zinc-200">{l.game}</td>
                           <td className="py-2.5 px-2">
                             <span className="inline-flex bg-zinc-800 rounded px-1.5 py-0.5 text-xs text-zinc-300 capitalize mix-blend-screen">{l.market_kind} {l.line_label}</span>
                           </td>
                           <td className="py-2.5 px-2 font-medium">{l.side}</td>
                           <td className={`py-2.5 px-2 text-right font-bold ${l.odds_am > 0 ? "text-emerald-400" : "text-amber-300"}`}>{l.odds_am > 0 ? `+${l.odds_am}` : l.odds_am}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            );
          })}
        </section>
      )}
    </main>
  );
}

