# On-Demand Bovada + TheRundown Integration

## Backend setup

1. Install Python dependencies:

```bash
python3 -m pip install -r requirements.txt
```

2. Set TheRundown API key (if not already in environment):

```bash
export THERUNDOWN_API_KEY="your_key_here"
```

3. Run the backend service:

```bash
python3 server.py
```

The backend runs on `http://127.0.0.1:3030` and exposes:
- `POST /scan-now` (manual trigger from UI)
- `GET /arbs` (latest cached payload)
- `GET /health`

`ENABLE_AUTO_SCAN` is disabled, so there is no continuous polling loop.

## Frontend setup

1. Install dependencies:

```bash
cd frontend
npm install
```

2. (Optional) override backend URL:

```bash
export NEXT_PUBLIC_ARBS_URL="http://127.0.0.1:3030"
```

3. Start Next.js:

```bash
npm run dev
```

Open the UI and click **Scan Lines** to trigger a single on-demand scan.

## Response additions

`POST /scan-now` now includes:
- `matchedGames`: intersection between Bovada and TheRundown games after 3-tier matching.
- `bovadaError`: set when Bovada fails/times out (TheRundown result still returns).

## Matching strategy

1. Normalize team names (lowercase, punctuation stripped, whitespace collapsed).
2. Apply alias dictionary (e.g. `mia -> miami heat`, `tb -> tampa bay buccaneers`).
3. Apply RapidFuzz token matching with threshold `>= 80`.

The threshold `80` balances tolerance for abbreviations/naming variance while avoiding weak cross-team matches.
