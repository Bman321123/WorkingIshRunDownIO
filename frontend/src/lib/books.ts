export type BookKey = "draftkings" | "fanduel" | "betmgm" | "unknown";

export function normalizeBookName(name: string): BookKey {
  const n = (name || "").toLowerCase().replace(/\s+/g, "");
  if (n.includes("draft")) return "draftkings";
  if (n.includes("fan")) return "fanduel";
  if (n.includes("betmgm") || n.includes("mgm")) return "betmgm";
  return "unknown";
}

export function bookLogoPath(bookName: string): string {
  const key = normalizeBookName(bookName);
  if (key === "draftkings") return "/books/draftkings.svg";
  if (key === "fanduel") return "/books/fanduel.svg";
  if (key === "betmgm") return "/books/betmgm.svg";
  return "/books/book.svg";
}

