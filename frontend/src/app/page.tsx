"use client";

import { useEffect, useMemo, useState } from "react";
import type { Arb } from "@/lib/types";
import { ArbCard } from "@/components/ArbCard";

const DEFAULT_BASE = "http://localhost:8080";

const SPORTS = ["NCAAF", "NFL", "MLB", "NBA", "NCAAB", "NHL", "NCAAWB", "MMA"] as const;
type Sport = (typeof SPORTS)[number];

type ScanNowResponse = {
  ok: boolean;
  sports?: string[];
  lastScanMs?: number;
  count?: number;
  arbs?: Arb[];
  error?: string;
};

export default function HomePage() {
  const baseUrl = process.env.NEXT_PUBLIC_ARBS_URL || DEFAULT_BASE;
  const arbsUrl = `${baseUrl.replace(/\/$/, "")}/arbs`;
  const scanUrl = `${baseUrl.replace(/\/$/, "")}/scan-now`;

  const [selectedSports, setSelectedSports] = useState<Set<Sport>>(() => new Set(SPORTS));
  const [arbs, setArbs] = useState<Arb[]>([]);
  const [status, setStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [scanStatus, setScanStatus] = useState<"idle" | "scanning" | "error">("idle");
  const [error, setError] = useState<string | null>(null);
  const [lastScannedMs, setLastScannedMs] = useState<number | null>(null);

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
        const data = (await resp.json()) as Arb[];
        if (!mounted) return;
        setArbs(Array.isArray(data) ? data : []);
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
      setLastScannedMs(payload.lastScanMs ?? Date.now());
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

          <div className="text-right">
            <div className="text-xs text-zinc-400">Status</div>
            <div className="text-sm font-semibold text-zinc-100">{header}</div>
          </div>
        </div>

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
            Error: {error}. Make sure `/Users/seniortech/therundownioV1/server.py` is running on port 8080.
          </div>
        ) : null}
      </header>

      <section className="grid grid-cols-1 gap-5">
        {arbs.map((arb, idx) => (
          <ArbCard key={`${arb.game}-${arb.market_kind}-${arb.line_label ?? ""}-${idx}`} arb={arb} />
        ))}
      </section>
    </main>
  );
}

