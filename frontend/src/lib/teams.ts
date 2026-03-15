type League = "NBA" | "NHL" | "NFL" | "MLB" | "NCAAB" | "NCAAF" | "MMA" | "NCAAWB" | string;

// ESPN team IDs (NBA).
const NBA_TEAM_ID: Record<string, string> = {
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

// ESPN team abbreviations (NHL).
const NHL_TEAM_ABBREV: Record<string, string> = {
  "anaheim ducks": "ANA",
  "arizona coyotes": "ARI",
  "utah hockey club": "UTA",
  "utah mammoth": "UTA",
  "boston bruins": "BOS",
  "buffalo sabres": "BUF",
  "calgary flames": "CGY",
  "carolina hurricanes": "CAR",
  "chicago blackhawks": "CHI",
  "colorado avalanche": "COL",
  "columbus blue jackets": "CBJ",
  "dallas stars": "DAL",
  "detroit red wings": "DET",
  "edmonton oilers": "EDM",
  "florida panthers": "FLA",
  "los angeles kings": "LA",
  "minnesota wild": "MIN",
  "montreal canadiens": "MTL",
  "nashville predators": "NSH",
  "new jersey devils": "NJ",
  "new york islanders": "NYI",
  "new york rangers": "NYR",
  "ottawa senators": "OTT",
  "philadelphia flyers": "PHI",
  "pittsburgh penguins": "PIT",
  "san jose sharks": "SJ",
  "seattle kraken": "SEA",
  "st. louis blues": "STL",
  "st louis blues": "STL",
  "tampa bay lightning": "TB",
  "toronto maple leafs": "TOR",
  "vancouver canucks": "VAN",
  "vegas golden knights": "VGK",
  "washington capitals": "WSH",
  "winnipeg jets": "WPG",
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

export function espnTeamLogoUrl(league: string, teamName: string): string | null {
  const norm = normalizeTeamName(teamName);

  if (league === "NBA") {
    const id = NBA_TEAM_ID[norm];
    if (!id) return null;
    return `https://a.espncdn.com/i/teamlogos/nba/500/${id}.png`;
  }

  if (league === "NHL") {
    const abbrev = NHL_TEAM_ABBREV[norm];
    if (!abbrev) return null;
    return `https://a.espncdn.com/i/teamlogos/nhl/500/${abbrev.toLowerCase()}.png`;
  }

  return null;
}

export function initials(name: string): string {
  const words = normalizeTeamName(name).split(" ").filter(Boolean);
  const letters = words.slice(0, 2).map((w) => w[0]?.toUpperCase());
  return letters.join("") || "?";
}
