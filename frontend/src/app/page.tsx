"use client";

import { useEffect, useMemo, useState } from "react";
import type { Arb, RawLine, BestLine } from "@/lib/types";
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
  bestLines?: BestLine[];
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
  const [bestLines, setBestLines] = useState<BestLine[]>([]);
  const [status, setStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [scanStatus, setScanStatus] = useState<"idle" | "scanning" | "error">("idle");
  const [error, setError] = useState<string | null>(null);
  const [lastScannedMs, setLastScannedMs] = useState<number | null>(null);
  const [dataAge, setDataAge] = useState<number | null>(null);
  const [dpRemaining, setDpRemaining] = useState<string | null>(null);

  const [activeTab, setActiveTab] = useState<"arbs" | "lines" | "best">("arbs");
  const [expandedBooks, setExpandedBooks] = useState<Set<string>>(new Set());
  const [openDrawers, setOpenDrawers] = useState<Set<string>>(new Set());

  const toggleDrawer = (drawerKey: string) =>
    setOpenDrawers((prev) => {
      const next = new Set(prev);
      next.has(drawerKey) ? next.delete(drawerKey) : next.add(drawerKey);
      return next;
    });

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
        setBestLines(Array.isArray(data.bestLines) ? data.bestLines : []);
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
      setBestLines(Array.isArray(payload.bestLines) ? payload.bestLines : []);
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
          <button
            onClick={() => setActiveTab("best")}
            className={`pb-2 text-sm font-semibold transition border-b-2 ${
              activeTab === "best"
                ? "border-emerald-500 text-emerald-400"
                : "border-transparent text-zinc-400 hover:text-zinc-200"
            }`}
          >
            Best Lines (Odds Shopping)
          </button>
        </div>
      </header>

      {activeTab === "arbs" && (
        <section className="grid grid-cols-1 gap-5">
          {arbs.map((arb, idx) => (
            <ArbCard key={`${arb.game}-${arb.market_kind}-${arb.line_label ?? ""}-${idx}`} arb={arb} />
          ))}
        </section>
      )}

      {activeTab === "lines" && (
        <section className="space-y-4">
          {Array.from(new Set(lines.map((l) => l.book)))
            .sort()
            .map((bookName) => {
              const isExpanded = expandedBooks.has(bookName);
              const bookLines = lines.filter((l) => l.book === bookName);
              return (
                <div
                  key={bookName}
                  className="rounded-2xl border border-zinc-800 bg-zinc-900/40 shadow-card overflow-hidden"
                >
                  <button
                    onClick={() =>
                      setExpandedBooks((prev) => {
                        const next = new Set(prev);
                        if (next.has(bookName)) next.delete(bookName);
                        else next.add(bookName);
                        return next;
                      })
                    }
                    className="w-full flex items-center justify-between px-5 py-4 bg-zinc-800/20 hover:bg-zinc-800/40 transition"
                  >
                    <div className="font-semibold text-zinc-100 flex items-center gap-3">
                      <span className="text-lg">{bookName}</span>
                      <span className="text-xs font-mono text-zinc-400 bg-zinc-900 px-2 py-0.5 rounded-full">
                        {bookLines.length} lines pulled
                      </span>
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
                                {l.updated_at ? new Date(l.updated_at).toLocaleTimeString() : "N/A"}
                              </td>
                              <td className="py-2.5 px-2 font-semibold text-zinc-200">{l.sport}</td>
                              <td className="py-2.5 px-2 text-zinc-200">{l.game}</td>
                              <td className="py-2.5 px-2">
                                <span className="inline-flex bg-zinc-800 rounded px-1.5 py-0.5 text-xs text-zinc-300 capitalize mix-blend-screen">
                                  {l.market_kind} {l.line_label}
                                </span>
                              </td>
                              <td className="py-2.5 px-2 font-medium">{l.side}</td>
                              <td
                                className={`py-2.5 px-2 text-right font-bold ${
                                  l.odds_am > 0 ? "text-emerald-400" : "text-amber-300"
                                }`}
                              >
                                {l.odds_am > 0 ? `+${l.odds_am}` : l.odds_am}
                              </td>
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

      {activeTab === "best" && (
        <section className="space-y-4">
          {bestLines.length === 0 ? (
            <div className="rounded-2xl border border-zinc-800 bg-zinc-900/40 px-5 py-4 text-sm text-zinc-300">
              No eligible markets found for the current scan. Try scanning again or adjusting sports selection.
            </div>
          ) : (
            Array.from(
              bestLines.reduce((acc, bl) => {
                const key = `${bl.sport}::${bl.game}`;
                const list = acc.get(key) ?? [];
                list.push(bl);
                acc.set(key, list);
                return acc;
              }, new Map<string, BestLine[]>())
            ).map(([key, linesForEvent]) => {
              const [sport, game] = key.split("::");
              const ml = linesForEvent.filter((l) => l.type === "moneyline");
              const spreads = linesForEvent.filter((l) => l.type === "spread");
              const totals = linesForEvent.filter((l) => l.type === "total");

              const spreadKey = `spread::${key}`;
              const totalKey = `total::${key}`;
              const spreadsOpen = openDrawers.has(spreadKey);
              const totalsOpen = openDrawers.has(totalKey);

              const shortBook = (name: string) => {
                const lower = name.toLowerCase();
                if (lower.includes("fanduel")) return "FD";
                if (lower.includes("draftk")) return "DK";
                if (lower.includes("betmgm") || lower.includes("mgm")) return "BMG";
                if (lower.includes("kalshi")) return "Kalshi";
                if (lower.includes("hard rock") || lower.includes("hardrock")) return "HRB";
                return name;
              };
              const formatOdds = (v: number | undefined | null) => {
                if (v == null) return "—";
                return v > 0 ? `+${v}` : `${v}`;
              };
              const oddsColor = (v: number | undefined | null) =>
                v == null ? "text-zinc-500" : v > 0 ? "text-emerald-400" : "text-red-400";
              const amToDec = (am: number) =>
                am >= 100 ? am / 100 + 1 : am <= -100 ? 100 / Math.abs(am) + 1 : 0;
              const pairValue = (oddsA: number, oddsB: number) => {
                const decA = amToDec(oddsA);
                const decB = amToDec(oddsB);
                if (decA <= 1 || decB <= 1) return null;
                return Math.round((1 - (1 / decA + 1 / decB)) * 10000) / 100;
              };
              const valueBadge = (val: number | null) => {
                if (val == null) return null;
                const positive = val >= 0;
                return (
                  <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-bold font-mono ${
                    positive ? "bg-emerald-500/15 text-emerald-300" : "bg-red-500/15 text-red-300"
                  }`}>
                    {positive ? "+" : ""}{val.toFixed(1)}%
                  </span>
                );
              };

              const homeName = ml[0]?.home_team ?? spreads.find((s) => s.side === "home")?.team ?? "Home";
              const awayName = ml[0]?.away_team ?? spreads.find((s) => s.side === "away")?.team ?? "Away";

              const spreadPairs = (() => {
                const byAbs = new Map<number, BestLine[]>();
                for (const s of spreads) {
                  if (s.line == null || !s.pick?.book || !s.side) continue;
                  const abs = Math.abs(s.line);
                  const list = byAbs.get(abs) ?? [];
                  list.push(s);
                  byAbs.set(abs, list);
                }
                return Array.from(byAbs.entries()).sort(([a], [b]) => a - b);
              })();

              return (
                <div
                  key={key}
                  className="rounded-2xl border border-zinc-800 bg-zinc-900/40 shadow-card overflow-hidden"
                >
                  <div className="px-5 py-4">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <div className="text-xs uppercase tracking-wide text-zinc-500">{sport}</div>
                        <div className="text-base font-semibold text-zinc-100">{game}</div>
                      </div>
                    </div>

                    {ml.length > 0 && ml.map((bl, idx) => {
                      if (!bl.home || !bl.away || !bl.home_team || !bl.away_team) return null;
                      if (bl.home.odds_am == null || bl.away.odds_am == null) return null;
                      const mlVal = pairValue(bl.home.odds_am, bl.away.odds_am);
                      return (
                        <div key={`ml-${idx}`} className="mt-3">
                          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                            <div className="flex items-center justify-between rounded-xl bg-zinc-800/50 px-4 py-3">
                              <div>
                                <div className="text-[10px] uppercase tracking-wider text-zinc-500">Home</div>
                                <div className="text-sm font-bold text-zinc-100">{bl.home_team}</div>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className={`font-mono text-lg font-bold ${oddsColor(bl.home.odds_am)}`}>
                                  {formatOdds(bl.home.odds_am)}
                                </span>
                                <span className="rounded-full bg-zinc-700 px-2 py-0.5 text-xs font-semibold text-zinc-200">
                                  {shortBook(bl.home.book)}
                                </span>
                              </div>
                            </div>
                            <div className="flex items-center justify-between rounded-xl bg-zinc-800/50 px-4 py-3">
                              <div>
                                <div className="text-[10px] uppercase tracking-wider text-zinc-500">Away</div>
                                <div className="text-sm font-bold text-zinc-100">{bl.away_team}</div>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className={`font-mono text-lg font-bold ${oddsColor(bl.away.odds_am)}`}>
                                  {formatOdds(bl.away.odds_am)}
                                </span>
                                <span className="rounded-full bg-zinc-700 px-2 py-0.5 text-xs font-semibold text-zinc-200">
                                  {shortBook(bl.away.book)}
                                </span>
                              </div>
                            </div>
                          </div>
                          {mlVal != null && (
                            <div className="mt-1.5 flex items-center gap-2 text-[10px] text-zinc-500">
                              <span>Both-sides value:</span> {valueBadge(mlVal)}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>

                  {(spreads.length > 0 || totals.length > 0) && (
                    <div className="flex border-t border-zinc-800">
                      {spreads.length > 0 && (
                        <button
                          type="button"
                          onClick={() => toggleDrawer(spreadKey)}
                          className={[
                            "flex-1 flex items-center justify-center gap-2 px-4 py-2.5 text-xs font-semibold transition",
                            spreadsOpen
                              ? "bg-zinc-800/60 text-emerald-300"
                              : "bg-zinc-900/20 text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/30",
                          ].join(" ")}
                        >
                          <span>{spreadsOpen ? "▾" : "▸"}</span>
                          Spreads
                          <span className="font-mono text-[10px] text-zinc-500">({spreadPairs.length})</span>
                        </button>
                      )}
                      {totals.length > 0 && (
                        <button
                          type="button"
                          onClick={() => toggleDrawer(totalKey)}
                          className={[
                            "flex-1 flex items-center justify-center gap-2 px-4 py-2.5 text-xs font-semibold transition",
                            spreads.length > 0 ? "border-l border-zinc-800" : "",
                            totalsOpen
                              ? "bg-zinc-800/60 text-emerald-300"
                              : "bg-zinc-900/20 text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/30",
                          ].join(" ")}
                        >
                          <span>{totalsOpen ? "▾" : "▸"}</span>
                          Totals
                          <span className="font-mono text-[10px] text-zinc-500">({totals.length})</span>
                        </button>
                      )}
                    </div>
                  )}

                  {spreadsOpen && spreadPairs.length > 0 && (
                    <div className="border-t border-zinc-800 px-5 py-3 space-y-3">
                      {spreadPairs.map(([absLine, entries]) => {
                        const homeNeg = entries.find((e) => e.side === "home" && (e.line ?? 0) < 0);
                        const awayPos = entries.find((e) => e.side === "away" && (e.line ?? 0) > 0);
                        const homePos = entries.find((e) => e.side === "home" && (e.line ?? 0) > 0);
                        const awayNeg = entries.find((e) => e.side === "away" && (e.line ?? 0) < 0);

                        const pairA = homeNeg && awayPos;
                        const pairB = homePos && awayNeg;
                        if (!pairA && !pairB) return null;
                        const valA = pairA ? pairValue(homeNeg!.pick!.odds_am, awayPos!.pick!.odds_am) : null;
                        const valB = pairB ? pairValue(homePos!.pick!.odds_am, awayNeg!.pick!.odds_am) : null;

                        return (
                          <div key={`sp-${absLine}`} className="space-y-1.5">
                            <div className="text-[10px] uppercase tracking-wider text-zinc-500 font-semibold">
                              Spread {absLine}
                            </div>

                            {pairA && (
                              <div>
                                <div className="grid grid-cols-1 gap-1.5 sm:grid-cols-2">
                                  <div className="flex items-center justify-between rounded-lg bg-zinc-800/40 px-3 py-1.5">
                                    <span className="text-xs text-zinc-300">
                                      {homeName}{" "}
                                      <span className={`font-mono ${oddsColor(homeNeg!.line)}`}>{formatOdds(homeNeg!.line)}</span>
                                    </span>
                                    <div className="flex items-center gap-1.5">
                                      <span className={`font-mono text-sm font-semibold ${oddsColor(homeNeg!.pick!.odds_am)}`}>
                                        {formatOdds(homeNeg!.pick!.odds_am)}
                                      </span>
                                      <span className="rounded-full bg-zinc-700 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-200">
                                        {shortBook(homeNeg!.pick!.book)}
                                      </span>
                                    </div>
                                  </div>
                                  <div className="flex items-center justify-between rounded-lg bg-zinc-800/40 px-3 py-1.5">
                                    <span className="text-xs text-zinc-300">
                                      {awayName}{" "}
                                      <span className={`font-mono ${oddsColor(awayPos!.line)}`}>{formatOdds(awayPos!.line)}</span>
                                    </span>
                                    <div className="flex items-center gap-1.5">
                                      <span className={`font-mono text-sm font-semibold ${oddsColor(awayPos!.pick!.odds_am)}`}>
                                        {formatOdds(awayPos!.pick!.odds_am)}
                                      </span>
                                      <span className="rounded-full bg-zinc-700 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-200">
                                        {shortBook(awayPos!.pick!.book)}
                                      </span>
                                    </div>
                                  </div>
                                </div>
                                {valA != null && (
                                  <div className="mt-1 flex items-center gap-1.5 pl-1 text-[10px] text-zinc-500">
                                    Value: {valueBadge(valA)}
                                  </div>
                                )}
                              </div>
                            )}

                            {pairB && (
                              <div>
                                <div className="grid grid-cols-1 gap-1.5 sm:grid-cols-2">
                                  <div className="flex items-center justify-between rounded-lg bg-zinc-800/40 px-3 py-1.5">
                                    <span className="text-xs text-zinc-300">
                                      {homeName}{" "}
                                      <span className={`font-mono ${oddsColor(homePos!.line)}`}>{formatOdds(homePos!.line)}</span>
                                    </span>
                                    <div className="flex items-center gap-1.5">
                                      <span className={`font-mono text-sm font-semibold ${oddsColor(homePos!.pick!.odds_am)}`}>
                                        {formatOdds(homePos!.pick!.odds_am)}
                                      </span>
                                      <span className="rounded-full bg-zinc-700 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-200">
                                        {shortBook(homePos!.pick!.book)}
                                      </span>
                                    </div>
                                  </div>
                                  <div className="flex items-center justify-between rounded-lg bg-zinc-800/40 px-3 py-1.5">
                                    <span className="text-xs text-zinc-300">
                                      {awayName}{" "}
                                      <span className={`font-mono ${oddsColor(awayNeg!.line)}`}>{formatOdds(awayNeg!.line)}</span>
                                    </span>
                                    <div className="flex items-center gap-1.5">
                                      <span className={`font-mono text-sm font-semibold ${oddsColor(awayNeg!.pick!.odds_am)}`}>
                                        {formatOdds(awayNeg!.pick!.odds_am)}
                                      </span>
                                      <span className="rounded-full bg-zinc-700 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-200">
                                        {shortBook(awayNeg!.pick!.book)}
                                      </span>
                                    </div>
                                  </div>
                                </div>
                                {valB != null && (
                                  <div className="mt-1 flex items-center gap-1.5 pl-1 text-[10px] text-zinc-500">
                                    Value: {valueBadge(valB)}
                                  </div>
                                )}
                              </div>
                            )}

                            {pairA && pairB && (
                              <div className="border-b border-zinc-800/40 mt-1.5" />
                            )}
                          </div>
                        );
                      })}
                    </div>
                  )}

                  {totalsOpen && totals.length > 0 && (
                    <div className="border-t border-zinc-800 px-5 py-3 space-y-1.5">
                      {totals
                        .slice()
                        .sort((a, b) => (a.line ?? 0) - (b.line ?? 0))
                        .map((bl, idx) => {
                          if (!bl.over?.book || !bl.under?.book) return null;
                          const tVal = pairValue(bl.over.odds_am, bl.under.odds_am);
                          return (
                            <div
                              key={`total-${bl.line}-${idx}`}
                              className="flex items-center justify-between rounded-lg bg-zinc-800/40 px-3 py-1.5"
                            >
                              <span className="text-xs font-mono text-zinc-400">
                                {bl.line != null ? `Total ${bl.line}` : "Total"}
                              </span>
                              <div className="flex items-center gap-3 text-xs">
                                {tVal != null && valueBadge(tVal)}
                                <div className="flex items-center gap-1.5">
                                  <span className="text-[10px] text-zinc-500">O</span>
                                  <span className={`font-mono ${oddsColor(bl.over.odds_am)}`}>
                                    {formatOdds(bl.over.odds_am)}
                                  </span>
                                  <span className="rounded-full bg-zinc-700 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-200">
                                    {shortBook(bl.over.book)}
                                  </span>
                                </div>
                                <div className="flex items-center gap-1.5">
                                  <span className="text-[10px] text-zinc-500">U</span>
                                  <span className={`font-mono ${oddsColor(bl.under.odds_am)}`}>
                                    {formatOdds(bl.under.odds_am)}
                                  </span>
                                  <span className="rounded-full bg-zinc-700 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-200">
                                    {shortBook(bl.under.book)}
                                  </span>
                                </div>
                              </div>
                            </div>
                          );
                        })}
                    </div>
                  )}
                </div>
              );
            })
          )}
        </section>
      )}
    </main>
  );
}

