type League = "NBA";

// ESPN team IDs (NBA). Extend as needed.
const NBA_TEAM_ID_BY_NAME: Record<string, string> = {
  "atlanta hawks": "1",
  "boston celtics": "2",
  "brooklyn nets": "17",
  "charlotte hornets": "30",
  "chicago bulls": "4",
  "cleveland cavaliers": "5",
  "dallas mavericks": "6",
  "denver nuggets": "7",
  "detroit pistons": "8",
  "golden state warriors": "9",
  "houston rockets": "10",
  "indiana pacers": "11",
  "la clippers": "12",
  "los angeles clippers": "12",
  "la lakers": "13",
  "los angeles lakers": "13",
  "memphis grizzlies": "29",
  "miami heat": "14",
  "milwaukee bucks": "15",
  "minnesota timberwolves": "16",
  "new orleans pelicans": "3",
  "new york knicks": "18",
  "oklahoma city thunder": "25",
  "orlando magic": "19",
  "philadelphia 76ers": "20",
  "phoenix suns": "21",
  "portland trail blazers": "22",
  "sacramento kings": "23",
  "san antonio spurs": "24",
  "toronto raptors": "28",
  "utah jazz": "26",
  "washington wizards": "27",
};

function normalizeTeamName(name: string): string {
  return (name || "")
    .toLowerCase()
    .replace(/\./g, "")
    .replace(/\s+/g, " ")
    .trim();
}

export function parseMatchup(game: string): { away: string; home: string } {
  const parts = (game || "").split(" @ ");
  if (parts.length === 2) return { away: parts[0].trim(), home: parts[1].trim() };
  return { away: game || "Away", home: "Home" };
}

export function espnTeamLogoUrl(league: League, teamName: string): string | null {
  if (league !== "NBA") return null;
  const id = NBA_TEAM_ID_BY_NAME[normalizeTeamName(teamName)];
  if (!id) return null;
  return `https://a.espncdn.com/i/teamlogos/nba/500/${id}.png`;
}

export function initials(name: string): string {
  const words = normalizeTeamName(name).split(" ").filter(Boolean);
  const letters = words.slice(0, 2).map((w) => w[0]?.toUpperCase());
  return letters.join("") || "?";
}

