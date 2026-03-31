const { CACHE, handleOptions, sendError, sendJson, setApiHeaders } = require("./_lib/http");

const DEFAULT_MATCHES = [
  {
    match_id: "b193b51f-cbed-4398-a297-237dd3322607",
    league_id: "eng.1",
    home_team: "Arsenal",
    away_team: "Manchester City",
    status: "scheduled",
    label: "EPL · Arsenal vs Man City",
  },
  {
    match_id: "b42fe447-b2b1-485f-ae6d-1559ee2b57c7",
    league_id: "fifa.world",
    home_team: "United States",
    away_team: "Mexico",
    status: "scheduled",
    label: "WC26 · USA vs Mexico",
  },
  {
    match_id: "d6742e61-2457-43fd-aa3f-e61f6a76c7af",
    league_id: "nba",
    home_team: "Los Angeles Lakers",
    away_team: "Boston Celtics",
    status: "scheduled",
    label: "NBA · Lakers vs Celtics",
  },
  {
    match_id: "c94d7e01-333d-41cd-a67d-cc0285fa7f28",
    league_id: "fifa.world",
    home_team: "England",
    away_team: "Brazil",
    status: "scheduled",
    label: "WC26 · England vs Brazil",
  },
];

module.exports = async function handler(req, res) {
  if (handleOptions(req, res, CACHE.DEMO)) {
    return;
  }

  setApiHeaders(res, CACHE.DEMO);

  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return sendError(res, 405, "METHOD_NOT_ALLOWED", "Use GET for this endpoint.", CACHE.DEMO);
  }

  return sendJson(
    res,
    200,
    {
      source: "published_samples",
      updatedAt: new Date().toISOString(),
      rows: DEFAULT_MATCHES,
    },
    CACHE.DEMO,
  );
};
