export type Arb = {
  sport: string;
  game: string;
  market_kind: "ml" | "spread" | "total" | string;
  line_label?: string;

  side_a: string;
  book_a: string;
  odds_a_am: number;
  updated_at_a?: string | number | null;

  side_b: string;
  book_b: string;
  odds_b_am: number;
  updated_at_b?: string | number | null;

  profit: number;
  stake_a: number;
  stake_b: number;

  same_book?: boolean;
  fresh_age_s?: number | null;
  stale_age_s?: number | null;
};

export type RawLine = {
  sport: string;
  game: string;
  market_kind: "ml" | "spread" | "total" | string;
  line_label?: string;
  side: string;
  book: string;
  odds_am: number;
  updated_at?: string | number | null;
};
