export type BookKey = "draftkings" | "fanduel" | "pinnacle" | "unknown";

export function normalizeBookName(name: string): BookKey {
  const n = (name || "").toLowerCase();
  if (n.includes("draft")) return "draftkings";
  if (n.includes("fan")) return "fanduel";
  if (n.includes("pinn")) return "pinnacle";
  return "unknown";
}

export function bookLogoPath(bookName: string): string {
  const key = normalizeBookName(bookName);
  if (key === "draftkings") return "/books/draftkings.svg";
  if (key === "fanduel") return "/books/fanduel.svg";
  if (key === "pinnacle") return "/books/pinnacle.svg";
  return "/books/book.svg";
}

