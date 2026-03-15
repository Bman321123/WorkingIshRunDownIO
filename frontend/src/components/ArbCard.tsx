import Image from "next/image";
import type { Arb } from "@/lib/types";
import { bookLogoPath } from "@/lib/books";
import { espnTeamLogoUrl, initials, parseMatchup } from "@/lib/teams";
import { buildDeepLink } from "@/lib/deeplinks";

function formatProfit(p: number): string {
  const sign = p > 0 ? "+" : "";
  return `${sign}${p.toFixed(3)}%`;
}

function formatOddsAm(o: number): string {
  if (typeof o !== "number" || !Number.isFinite(o)) return "—";
  return o > 0 ? `+${o}` : `${o}`;
}

export function ArbCard({ arb }: { arb: Arb }) {
  const { away, home } = parseMatchup(arb.game);
  const league = arb.sport || "NBA";
  const awayLogo = espnTeamLogoUrl(league, away);
  const homeLogo = espnTeamLogoUrl(league, home);

  return (
    <div className="rounded-2xl border border-zinc-800 bg-zinc-900/40 shadow-card">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-zinc-800 px-5 py-4">
        <div className="flex items-center gap-3">
          <span className="inline-flex items-center rounded-full bg-zinc-800/70 px-2.5 py-1 text-xs font-semibold text-zinc-200">
            {arb.sport}
          </span>
          <span className="text-sm text-zinc-300">
            {arb.market_kind === "ml"
              ? "Moneyline"
              : arb.market_kind === "spread"
                ? `Spread ${arb.line_label ?? ""}`
                : arb.market_kind === "total"
                  ? `Total ${arb.line_label ?? ""}`
                  : arb.market_kind}
          </span>
        </div>

        <div className="flex items-center gap-3">
          <div className="text-right">
            <div className="text-xs text-zinc-400">Profit</div>
            <div className="text-lg font-bold tracking-tight text-emerald-300">{formatProfit(arb.profit)}</div>
          </div>
        </div>
      </div>

      <div className="px-5 py-4">
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="relative h-9 w-9 overflow-hidden rounded-full bg-zinc-800">
              {awayLogo ? (
                <Image src={awayLogo} alt={away} fill sizes="36px" />
              ) : (
                <div className="grid h-full w-full place-items-center text-xs font-bold text-zinc-200">
                  {initials(away)}
                </div>
              )}
            </div>
            <div>
              <div className="text-sm font-semibold leading-tight text-zinc-100">{away}</div>
              <div className="text-xs text-zinc-400">@</div>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <div>
              <div className="text-sm font-semibold leading-tight text-zinc-100 text-right">{home}</div>
              <div className="text-xs text-zinc-400 text-right">Home</div>
            </div>
            <div className="relative h-9 w-9 overflow-hidden rounded-full bg-zinc-800">
              {homeLogo ? (
                <Image src={homeLogo} alt={home} fill sizes="36px" />
              ) : (
                <div className="grid h-full w-full place-items-center text-xs font-bold text-zinc-200">
                  {initials(home)}
                </div>
              )}
            </div>
          </div>
        </div>

        <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
          <LegBlock
            label="Leg A"
            side={arb.side_a}
            book={arb.book_a}
            odds={arb.odds_a_am}
            stake={arb.stake_a}
            game={arb.game}
          />
          <LegBlock
            label="Leg B"
            side={arb.side_b}
            book={arb.book_b}
            odds={arb.odds_b_am}
            stake={arb.stake_b}
            game={arb.game}
            danger={arb.same_book === true}
          />
        </div>

        {arb.same_book ? (
          <div className="mt-3 text-xs text-amber-300/90">
            Note: both legs are from the same book. Treat as low-confidence / likely unbettable.
          </div>
        ) : null}
      </div>
    </div>
  );
}

function LegBlock(props: {
  label: string;
  side: string;
  book: string;
  odds: number;
  stake: number;
  game: string;
  danger?: boolean;
}) {
  const logo = bookLogoPath(props.book);
  const { home } = parseMatchup(props.game);
  const deepLink = buildDeepLink(props.book, home);

  return (
    <div
      className={[
        "rounded-xl border bg-zinc-950/40 p-4",
        props.danger ? "border-amber-600/50" : "border-zinc-800",
      ].join(" ")}
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xs font-semibold text-zinc-400">{props.label}</div>
          <div className="mt-0.5 text-sm font-semibold text-zinc-100">{props.side}</div>
          <a
            href={deepLink}
            target="_blank"
            rel="noopener noreferrer"
            className="mt-1 inline-flex items-center gap-1.5 text-xs text-emerald-400 hover:text-emerald-300 transition group"
          >
            {props.book}
            <svg
              xmlns="http://www.w3.org/2000/svg"
              viewBox="0 0 16 16"
              fill="currentColor"
              className="h-3 w-3 opacity-0 group-hover:opacity-100 transition-opacity"
            >
              <path
                fillRule="evenodd"
                d="M4.22 11.78a.75.75 0 0 1 0-1.06L9.44 5.5H5.75a.75.75 0 0 1 0-1.5h5.5a.75.75 0 0 1 .75.75v5.5a.75.75 0 0 1-1.5 0V6.56l-5.22 5.22a.75.75 0 0 1-1.06 0Z"
                clipRule="evenodd"
              />
            </svg>
          </a>
        </div>

        <a href={deepLink} target="_blank" rel="noopener noreferrer">
          <div className="relative h-8 w-8 overflow-hidden rounded-md bg-zinc-900 hover:ring-2 hover:ring-emerald-500/50 transition">
            <Image src={logo} alt={props.book} fill sizes="32px" />
          </div>
        </a>
      </div>

      <div className="mt-3 flex items-end justify-between">
        <div className="text-2xl font-extrabold tracking-tight text-zinc-50">
          {formatOddsAm(props.odds)}
        </div>
        <div className="text-right">
          <div className="text-[11px] text-zinc-400">Stake</div>
          <div className="text-sm font-semibold text-zinc-100">${props.stake.toFixed(2)}</div>
        </div>
      </div>
    </div>
  );
}
