# Product Rules — All Properties

These rules govern every consumer-facing page across all properties.
They were learned over 8 months of building with AI and validated on
March 14, 2026 when a real customer (WSOP bracelet holder) converted
in a live betting session. Every rule exists because AI got it wrong
and a human had to correct it.

Paste this document into any AI thread before building. It is the
quality floor, not a suggestion list.

---

## Part 1: Universal Rules (Apply Everywhere)

### The "No AI Smell" Test

AI-built pages have a recognizable aesthetic. The consumer doesn't know
why it feels off, but they bounce. These are the tells:

**Banned vocabulary:**
maximize, optimize, leverage, streamline, execute flawlessly, elevate,
unlock, empower, cutting-edge, next-level, game-changing, revolutionize,
seamlessly, holistic, robust, scalable, innovative, state-of-the-art,
harness, paradigm, synergy, actionable insights, deep dive, end-to-end

**Copy test:** Read it out loud to yourself. If it sounds like a LinkedIn
post or a SaaS landing page, rewrite it. The voice should sound like
you're explaining it to someone sitting across from you at a bar.

**Banned design patterns:**
- Eyebrow pills ("NEW" badges, category tags above headlines)
- Stagger animations on page load
- Grain/noise overlays
- Gradient mesh backgrounds
- CSS-only geometric "illustrations"
- Purple-to-blue gradients
- Card grids with identical structure repeating 6+ times
- Decorative SVG blobs or waves

**Identity test:** Remove the logo and the data. Could you tell which
site made this page? If it looks like every other AI-generated landing
page, it's generic. Start over.

### Never Expose the Infrastructure

The consumer never sees how it's built. This applies to every property.

- No RPC names, table names, column names, or model names on any page
- No "AI-powered," "machine learning," or "our algorithm"
- No "pipeline," "cron," "degraded," "stale," or any ops language
- No backend field names used as UI labels ("endorsement_timeline_days"
  is a column; "How long it takes" is what a person says)
- No data visibility tiers shown to the user (PUBLIC/PROPRIETARY is a
  backend WHERE clause, not a column)

The intelligence is invisible. The output just appears correct.
The product works. That's all the user needs to know.

### Never Frame AI as a Feature

The AI is the engine, not the product. The product is the answer.

- Don't say "AI-powered analysis" — show the analysis
- Don't say "our model predicts" — show the record and let the user decide
- Don't say "machine learning identifies" — show what was identified
- The word "AI" appears once in the product descriptor ("Live AI Analysis")
  and nowhere else in consumer copy

### Photography Rules

Every consumer page needs real photography. Not stock. Not AI-generated.
Not CSS art.

- Photography must be geography-specific or context-specific
- A state licensing page gets a photo OF that state
- A team page gets the team logo (nominative fair use, industry standard)
- Generic stock (handshake photos, diverse-team-at-whiteboard) is banned
- If no real photo exists yet, the page ships without one and gets flagged.
  A missing photo is better than a fake one.

### Typography

Typography carries the brand. Default fonts signal "I didn't try."

**Banned fonts:** Inter, Roboto, Poppins, Arial, system-ui defaults

**Rule:** Pick a serif or a distinctive sans-serif and commit. The font
choice should feel intentional. If you swapped it for Inter and nothing
felt different, you picked wrong.

- Serif headlines + sans-serif body, OR a single distinctive sans-serif
- System fonts are acceptable for data-heavy tables (SF Pro, -apple-system)
  where readability at small sizes matters more than character
- Monospace for all numbers in tables (SF Mono, Menlo, JetBrains Mono)

### Layout

- 8pt spacing grid, 4pt baseline for typography
- One accent color per property, used sparingly. Earned, not decorative
- White or warm off-white backgrounds. No dark mode on consumer pages
- Max content width: 640px for editorial, 920px for data pages
- Mobile-first: if it doesn't work on a phone held in one hand, it doesn't ship

### The Quality Gate (Every Page, Every Property)

Before any page goes live:

1. **Screenshot test:** Would a stranger screenshot this and send it to
   someone? If no, the page is not ready.
2. **AI smell test:** Can you tell AI built this? If yes, find what gave
   it away and fix it.
3. **Say it out loud test:** Read the copy to yourself. If it sounds
   weird spoken aloud, rewrite it.
4. **Photo test:** Is there a real photograph or team logo? If no, the
   page is incomplete.
5. **5-second test:** Can the user get the answer in 5 seconds? If they
   have to read three paragraphs first, restructure.

---

## Part 2: The Drip (thedrip.to)

### Trust Model

Covers won by showing the bettor the math and letting him decide.
That was the innovation — records, percentages, verify it yourself.
The Drip inherits that trust model at higher resolution: team-level,
home/road split, anchored against tonight's posted line, full game log.

The bettor does the math himself in 5 seconds. That's why he trusts it.

### What Goes on Consumer Pages

1. Team name + logo
2. Tonight's line at the top, anchored as the reference point
3. A table: Date, Opponent, Total/Score, Record, Percent
4. The gap between the team's average and tonight's line
5. Nothing else.

### The Line Anchor (Replaces Confidence Scores)

Instead of scoring confidence, show three numbers:
- The team's rolling average (last 10 or season)
- Tonight's posted line
- The gap between them

"Average is 251.9. Line is 244.5. That's 7.4 below."
The bettor connects the dots. No opinion needed.

### Prohibited Surfaces

- Confidence scores — we surface trends, not predictions
- Edge ratings — the bettor calculates his own edge
- Line movement boards — commodity, Action Network does this
- Referee signal boards — internal intelligence layer
- Intel queues — internal prioritization
- Any ops monitoring on consumer pages

### Plain English Transforms

Database text must be transformed before rendering:

| RPC Output | Consumer Sees |
|---|---|
| 1H BTTS NO | No 1st Half BTTS |
| CORNERS UNDER 10.2 | Under 10 Corners |
| CORNERS OVER 9.8 | Over 10 Corners |
| CARDS UNDER 4.1 | Under 4 Cards |
| 2H GOALS > 0 | Goal Scored in 2nd Half |
| LATE GOAL RESISTANT | No Goal After 75' |
| UNDER VS LINE | Under the Total |
| OVER VS LINE | Over the Total |
| DOG COVER | Covers as Underdog |
| FAV COVER | Covers as Favorite |

Rule: ALL CAPS, underscores, or decimal thresholds = database language.
Transform before rendering. Always.

### Record Format

Derive from sample + hit_rate. sample=10, hit_rate=90 → "9-1"
The bettor verifies by counting. The record is the proof.

### SEO Quality Gates

Five gates before a page goes live:
1. Minimum 5 games with line data
2. Last game within 21 days
3. Directional signal ≥ 65%
4. League whitelist (NBA, NHL, EPL, Serie A, La Liga, Bundesliga, Ligue 1, NCAAB, MLS)
5. Sample badge: STRONG (8+), SOLID (6-7), MIN (5)

Standalone SEO pages: SOLID or STRONG only (6+ games).
Source views: v_page_worthy_ou_trends, v_page_worthy_ats_trends

### Product Tiers

**FREE:** Daily picks sheet + all SEO data pages (team trends, league profiles)
- The retention loop. Pages rank, bettor finds them, data is legible.

**PAID ($200/mo):** /live — real-time in-game answers
- Cash-out EV, hold rates by lead size, live line anchoring
- Powered by live_context_snapshots (121K+ and growing)
- This is where the AI replaces Kofi in the text thread

### The Consumer

Not dumb. Thinks in expected value. Watches ESPN Gamecast on his phone,
not TV. One hand free. At a poker room or a bar. His questions:
- "Is this an over play?"
- "Should I cash out?"
- "Is this worth the risk?"

Every surface answers one of these.

---

## Part 3: State Licensing Reference (statelicensingreference.com)

### Voice

Recruiter explaining it to a clinician over coffee. Warm, direct,
no regulatory jargon unless quoting the board directly.

### Page Structure

- Hero image of the specific state from state_hero_images
- Board names, fees, timelines from state_licensing_requirements — never hardcoded
- "Last updated" and "Sources verified" visible on every page
- Trust through transparency, not through claiming authority
- Source citations link to the primary board URL, not aggregator sites
- Every data point traceable to its source

### Prohibited

- Hardcoded licensing data (everything comes from the database)
- Generic stock photography (each state gets its own image)
- Regulatory language used casually (quote it when citing the board, plain English otherwise)
- Aggregator citations (link the board directly)

---

## Part 4: perdiem.fyi

### Scope

417 static licensing pages across 8 allied health specialties.
Programmatic SEO — each page generated from structured data.

### Rules

- Same voice as State Licensing Reference: recruiter to clinician
- Every page needs the state hero image
- Licensing steps presented as a checklist, not a wall of text
- Fees, timelines, and requirements from the database, never hardcoded
- "Last verified" date visible

---

## Part 5: wagelevel.fyi

### Scope

Live H-1B wage level calculator. 62,726 OFLC rows. Early GSC traction.

### Rules

- The calculator IS the page. No explanatory essays above the fold.
- Input: job title + location. Output: wage levels 1-4 with context.
- Source citation: OFLC data, linked directly
- Canonical fix applied (www vs non-www redirect resolved)

---

## Part 6: Break Room (future)

### Voice

Nurse-to-nurse. Never recruiter-to-nurse.

- Zero recruiter fingerprints — no agency names, no "your recruiter can help"
- Patterns, not instructions: "Here's how this works" not "You should do this"
- The reader should feel like they're getting advice from a colleague
  who's been through it, not from someone who wants to place them

---

## Part 7: trust.health (future)

### Architecture

- Guide tab: pure information, zero affiliate links, zero disclosure needed
- Discounts tab: all affiliate links live here, user opts in by clicking the tab
- Recruiter copy button is the distribution mechanism

### Rule

The product markets itself through utility. If the information is useful
enough that a recruiter copies the link and sends it to a clinician,
the product is working. No paid distribution needed.

---

## Part 8: The AI Failure Pattern

Every AI — Claude, ChatGPT, Codex, Copilot — has the same failure mode:

1. It surfaces everything the database can compute
2. It adds monitoring/ops language to consumer pages
3. It invents scoring systems that layer opinions on facts
4. It uses internal field names instead of plain English
5. It builds for the schema instead of for the customer
6. It adds decoration to compensate for lack of content
7. It defaults to generic fonts, gradients, and card grids
8. It writes copy that sounds like a press release

The translation from AI output to product is the moat.
The discipline is knowing what to delete.

If you built something and the consumer understood it instantly
without explanation, ship it. If you have to explain what a column
means or what a badge indicates, the AI added something that
doesn't belong. Find it and remove it.

---

*Document version: March 14, 2026. Validated in production with
real customer, real money, real conversion. Update only when a new
product rule is discovered through customer interaction, not through
internal assumption.*
