BEGIN;
SET LOCAL search_path = public;

DO $$
DECLARE
    v_seeded_at timestamptz := timezone('utc', now());
    v_snapshot_at timestamptz := '2026-03-12T14:30:00Z';

    v_league_id uuid;
    v_home_team_id uuid;
    v_away_team_id uuid;
    v_venue_id uuid;
    v_match_id uuid;

    v_home_form_id uuid := gen_random_uuid();
    v_away_form_id uuid := gen_random_uuid();
    v_h2h_id uuid := gen_random_uuid();
    v_home_injury_impact_id uuid := gen_random_uuid();
    v_away_injury_impact_id uuid := gen_random_uuid();
    v_market_consensus_id uuid := gen_random_uuid();
    v_market_alt_id uuid := gen_random_uuid();
    v_prediction_market_id uuid := gen_random_uuid();
    v_valuation_id uuid := gen_random_uuid();

    v_saka_injury_id uuid := gen_random_uuid();
    v_jesus_injury_id uuid := gen_random_uuid();
    v_kdb_injury_id uuid := gen_random_uuid();
    v_stones_injury_id uuid := gen_random_uuid();

    v_odegaard_context_id uuid := gen_random_uuid();
    v_saka_context_id uuid := gen_random_uuid();
    v_haaland_context_id uuid := gen_random_uuid();
    v_foden_context_id uuid := gen_random_uuid();

    v_home_trend_id uuid := gen_random_uuid();
    v_away_trend_id uuid := gen_random_uuid();
BEGIN
    SELECT id
      INTO v_league_id
      FROM leagues
     WHERE slug = 'eng.1'
     LIMIT 1;

    IF v_league_id IS NULL THEN
        v_league_id := gen_random_uuid();
        INSERT INTO leagues (
            id,
            slug,
            name,
            sport,
            season,
            country,
            created_at,
            updated_at
        )
        VALUES (
            v_league_id,
            'eng.1',
            'Premier League',
            'soccer',
            '2025-26',
            'England',
            v_seeded_at,
            v_seeded_at
        );
    END IF;

    SELECT id
      INTO v_home_team_id
      FROM teams
     WHERE canonical_name = 'Arsenal'
     LIMIT 1;

    IF v_home_team_id IS NULL THEN
        v_home_team_id := gen_random_uuid();
        INSERT INTO teams (
            id,
            league_id,
            sport,
            name,
            canonical_name,
            code,
            country,
            city,
            conference,
            division,
            created_at,
            updated_at
        )
        VALUES (
            v_home_team_id,
            v_league_id,
            'soccer',
            'Arsenal',
            'Arsenal',
            'ARS',
            'England',
            'London',
            NULL,
            NULL,
            v_seeded_at,
            v_seeded_at
        );
    END IF;

    SELECT id
      INTO v_away_team_id
      FROM teams
     WHERE canonical_name = 'Manchester City'
     LIMIT 1;

    IF v_away_team_id IS NULL THEN
        v_away_team_id := gen_random_uuid();
        INSERT INTO teams (
            id,
            league_id,
            sport,
            name,
            canonical_name,
            code,
            country,
            city,
            conference,
            division,
            created_at,
            updated_at
        )
        VALUES (
            v_away_team_id,
            v_league_id,
            'soccer',
            'Manchester City',
            'Manchester City',
            'MCI',
            'England',
            'Manchester',
            NULL,
            NULL,
            v_seeded_at,
            v_seeded_at
        );
    END IF;

    INSERT INTO team_mappings (id, team_id, provider, provider_team_id, provider_team_name, created_at, updated_at)
    SELECT gen_random_uuid(), v_home_team_id, m.provider, m.provider_team_id, m.provider_team_name, v_seeded_at, v_seeded_at
      FROM (
            VALUES
                ('espn', '359', 'Arsenal'),
                ('odds_api', 'arsenal', 'Arsenal'),
                ('sofascore', '42', 'Arsenal')
           ) AS m(provider, provider_team_id, provider_team_name)
     WHERE NOT EXISTS (
            SELECT 1
              FROM team_mappings tm
             WHERE tm.team_id = v_home_team_id
               AND tm.provider = m.provider
        );

    INSERT INTO team_mappings (id, team_id, provider, provider_team_id, provider_team_name, created_at, updated_at)
    SELECT gen_random_uuid(), v_away_team_id, m.provider, m.provider_team_id, m.provider_team_name, v_seeded_at, v_seeded_at
      FROM (
            VALUES
                ('espn', '382', 'Manchester City'),
                ('odds_api', 'manchester_city', 'Manchester City'),
                ('sofascore', '17', 'Manchester City')
           ) AS m(provider, provider_team_id, provider_team_name)
     WHERE NOT EXISTS (
            SELECT 1
              FROM team_mappings tm
             WHERE tm.team_id = v_away_team_id
               AND tm.provider = m.provider
        );

    SELECT id
      INTO v_venue_id
      FROM venues
     WHERE name = 'Emirates Stadium'
       AND city = 'London'
     LIMIT 1;

    IF v_venue_id IS NULL THEN
        v_venue_id := gen_random_uuid();
        INSERT INTO venues (
            id,
            name,
            city,
            country,
            capacity,
            surface,
            indoor,
            created_at,
            updated_at
        )
        VALUES (
            v_venue_id,
            'Emirates Stadium',
            'London',
            'England',
            60704,
            'grass',
            false,
            v_seeded_at,
            v_seeded_at
        );
    END IF;

    SELECT id
      INTO v_match_id
      FROM matches
     WHERE league_id = v_league_id
       AND home_team_id = v_home_team_id
       AND away_team_id = v_away_team_id
       AND start_time = '2026-03-15T16:30:00Z'::timestamptz
     LIMIT 1;

    IF v_match_id IS NULL THEN
        v_match_id := gen_random_uuid();
        INSERT INTO matches (
            id,
            league_id,
            sport,
            season,
            matchday,
            stage,
            status,
            start_time,
            venue_id,
            home_team_id,
            away_team_id,
            external_ids,
            created_at,
            updated_at
        )
        VALUES (
            v_match_id,
            v_league_id,
            'soccer',
            '2025-26',
            29,
            'regular_season',
            'scheduled',
            '2026-03-15T16:30:00Z'::timestamptz,
            v_venue_id,
            v_home_team_id,
            v_away_team_id,
            jsonb_build_object(
                'espn', '401835812',
                'odds_api', 'arsenal-vs-manchester-city-2026-03-15',
                'sofascore', '12900341'
            ),
            v_seeded_at,
            v_seeded_at
        );
    END IF;

    DELETE FROM valuation WHERE match_id = v_match_id;
    DELETE FROM prediction_markets WHERE match_id = v_match_id;
    DELETE FROM market_odds WHERE match_id = v_match_id;
    DELETE FROM team_trends WHERE match_id = v_match_id;
    DELETE FROM player_context WHERE match_id = v_match_id;
    DELETE FROM team_injury_impact WHERE match_id = v_match_id;
    DELETE FROM injury_reports WHERE match_id = v_match_id;
    DELETE FROM head_to_head WHERE match_id = v_match_id;
    DELETE FROM team_form WHERE match_id = v_match_id;

    INSERT INTO market_odds (
        id,
        match_id,
        source,
        market_type,
        spread,
        spread_juice,
        total,
        total_juice,
        home_ml,
        away_ml,
        draw_ml,
        snapshot_at,
        book_odds,
        created_at
    )
    VALUES
        (
            v_market_consensus_id,
            v_match_id,
            'consensus',
            'full_game',
            -0.25,
            -108,
            3.0,
            -110,
            155,
            170,
            240,
            v_snapshot_at,
            jsonb_build_array(
                jsonb_build_object('source', 'Pinnacle', 'spread', -0.25, 'total', 3.0, 'home_ml', 152, 'away_ml', 175),
                jsonb_build_object('source', 'FanDuel', 'spread', -0.5, 'total', 3.0, 'home_ml', 150, 'away_ml', 180),
                jsonb_build_object('source', 'DraftKings', 'spread', -0.25, 'total', 3.0, 'home_ml', 158, 'away_ml', 168)
            ),
            v_seeded_at
        ),
        (
            v_market_alt_id,
            v_match_id,
            'odds_api',
            'full_game',
            -0.25,
            -105,
            2.75,
            -112,
            160,
            172,
            235,
            v_snapshot_at - interval '20 minutes',
            jsonb_build_array(
                jsonb_build_object('source', 'Bet365', 'spread', -0.25, 'total', 2.75, 'home_ml', 160, 'away_ml', 172)
            ),
            v_seeded_at
        );

    INSERT INTO prediction_markets (
        id,
        match_id,
        source,
        home_win_prob,
        draw_prob,
        away_win_prob,
        volume_usd,
        fetched_at,
        created_at
    )
    VALUES (
        v_prediction_market_id,
        v_match_id,
        'polymarket',
        0.41,
        0.25,
        0.34,
        182450.00,
        v_snapshot_at,
        v_seeded_at
    );

    INSERT INTO valuation (
        id,
        match_id,
        model_name,
        fair_line,
        market_line,
        delta,
        has_model,
        generated_at,
        created_at
    )
    VALUES (
        v_valuation_id,
        v_match_id,
        'sportsync-v1-soccer',
        148,
        155,
        -7,
        true,
        v_snapshot_at,
        v_seeded_at
    );

    INSERT INTO team_form (
        id,
        match_id,
        team_id,
        snapshot_at,
        last_5,
        last_10_record,
        ats_last_10,
        ats_season,
        over_under_pct,
        avg_points_scored,
        avg_points_allowed,
        home_record,
        away_record,
        rest_days,
        fatigue_score,
        situation,
        created_at
    )
    VALUES
        (
            v_home_form_id,
            v_match_id,
            v_home_team_id,
            v_snapshot_at,
            'WWDWW',
            '7-2-1',
            0.60,
            0.58,
            0.56,
            2.00,
            0.90,
            '11-2-1',
            '8-3-2',
            6,
            14,
            'Top-of-table home match after full training week',
            v_seeded_at
        ),
        (
            v_away_form_id,
            v_match_id,
            v_away_team_id,
            v_snapshot_at,
            'WWLWD',
            '6-2-2',
            0.50,
            0.54,
            0.62,
            2.20,
            1.10,
            '10-2-2',
            '8-4-1',
            4,
            22,
            'Third road match in nine days',
            v_seeded_at
        );

    INSERT INTO head_to_head (
        id,
        match_id,
        home_team_id,
        away_team_id,
        total_meetings,
        home_wins,
        away_wins,
        draws,
        recent_matches,
        computed_at,
        created_at
    )
    VALUES (
        v_h2h_id,
        v_match_id,
        v_home_team_id,
        v_away_team_id,
        12,
        3,
        6,
        3,
        jsonb_build_array(
            jsonb_build_object('date', '2025-11-09', 'home_team', 'Manchester City', 'away_team', 'Arsenal', 'home_score', 1, 'away_score', 1, 'venue', 'Etihad Stadium'),
            jsonb_build_object('date', '2025-04-27', 'home_team', 'Arsenal', 'away_team', 'Manchester City', 'home_score', 2, 'away_score', 1, 'venue', 'Emirates Stadium'),
            jsonb_build_object('date', '2024-09-22', 'home_team', 'Manchester City', 'away_team', 'Arsenal', 'home_score', 2, 'away_score', 2, 'venue', 'Etihad Stadium'),
            jsonb_build_object('date', '2024-03-31', 'home_team', 'Manchester City', 'away_team', 'Arsenal', 'home_score', 0, 'away_score', 0, 'venue', 'Etihad Stadium'),
            jsonb_build_object('date', '2023-10-08', 'home_team', 'Arsenal', 'away_team', 'Manchester City', 'home_score', 1, 'away_score', 0, 'venue', 'Emirates Stadium')
        ),
        v_snapshot_at,
        v_seeded_at
    );

    INSERT INTO injury_reports (
        id,
        match_id,
        team_id,
        player_name,
        position,
        status,
        injury,
        impact,
        expected_return,
        reported_at,
        notes,
        created_at
    )
    VALUES
        (
            v_saka_injury_id,
            v_match_id,
            v_home_team_id,
            'Bukayo Saka',
            'RW',
            'probable',
            'Hamstring tightness',
            'high',
            '2026-03-15',
            v_snapshot_at,
            'Managed minutes in training but expected to start on the right wing.',
            v_seeded_at
        ),
        (
            v_jesus_injury_id,
            v_match_id,
            v_home_team_id,
            'Gabriel Jesus',
            'FW',
            'out',
            'Knee rehabilitation',
            'medium',
            '2026-03-29',
            v_snapshot_at,
            'Still not cleared for full-contact sessions.',
            v_seeded_at
        ),
        (
            v_kdb_injury_id,
            v_match_id,
            v_away_team_id,
            'Kevin De Bruyne',
            'AM',
            'questionable',
            'Groin soreness',
            'high',
            '2026-03-15',
            v_snapshot_at,
            'Late fitness test after limited Thursday session.',
            v_seeded_at
        ),
        (
            v_stones_injury_id,
            v_match_id,
            v_away_team_id,
            'John Stones',
            'CB',
            'out',
            'Thigh strain',
            'medium',
            '2026-03-22',
            v_snapshot_at,
            'Not expected back until after the international break.',
            v_seeded_at
        );

    INSERT INTO team_injury_impact (
        id,
        match_id,
        team_id,
        impact_score,
        summary,
        generated_at,
        created_at
    )
    VALUES
        (
            v_home_injury_impact_id,
            v_match_id,
            v_home_team_id,
            5.1,
            'Saka is trending toward available, but Arsenal still lose depth with Gabriel Jesus unavailable.',
            v_snapshot_at,
            v_seeded_at
        ),
        (
            v_away_injury_impact_id,
            v_match_id,
            v_away_team_id,
            6.4,
            'De Bruyne is a late call and Stones being out weakens City''s build-out and set-piece defending.',
            v_snapshot_at,
            v_seeded_at
        );

    INSERT INTO player_context (
        id,
        match_id,
        team_id,
        player_name,
        position,
        status,
        stats,
        prop_market,
        prop_line,
        over_price,
        under_price,
        notes,
        snapshot_at,
        created_at
    )
    VALUES
        (
            v_odegaard_context_id,
            v_match_id,
            v_home_team_id,
            'Martin Odegaard',
            'AM',
            'active',
            jsonb_build_object('goals', 9, 'assists', 11, 'shots_on_target_per_90', 1.1, 'chances_created_per_90', 3.0),
            'shots_on_target',
            1.5,
            115,
            -140,
            'Primary set-piece taker with 14 direct shot assists in his last eight starts.',
            v_snapshot_at,
            v_seeded_at
        ),
        (
            v_saka_context_id,
            v_match_id,
            v_home_team_id,
            'Bukayo Saka',
            'RW',
            'probable',
            jsonb_build_object('goals', 13, 'assists', 8, 'shots_on_target_per_90', 1.7, 'fouls_drawn_per_90', 2.8),
            'anytime_goal_scorer',
            0.5,
            185,
            -230,
            'If he starts, he remains Arsenal''s highest-leverage isolation attacker.',
            v_snapshot_at,
            v_seeded_at
        ),
        (
            v_haaland_context_id,
            v_match_id,
            v_away_team_id,
            'Erling Haaland',
            'ST',
            'active',
            jsonb_build_object('goals', 24, 'shots_per_90', 4.8, 'shots_on_target_per_90', 2.3, 'xg_per_90', 0.94),
            'shots_on_target',
            2.5,
            120,
            -145,
            'Arsenal have limited central volume well, but Haaland still leads the league in box touches.',
            v_snapshot_at,
            v_seeded_at
        ),
        (
            v_foden_context_id,
            v_match_id,
            v_away_team_id,
            'Phil Foden',
            'LW',
            'active',
            jsonb_build_object('goals', 15, 'assists', 9, 'key_passes_per_90', 2.4, 'progressive_carries_per_90', 5.1),
            'shots',
            2.5,
            -105,
            -115,
            'Likely secondary creator if De Bruyne is limited or ruled out.',
            v_snapshot_at,
            v_seeded_at
        );

    INSERT INTO team_trends (
        id,
        match_id,
        team_id,
        summary,
        trend_values,
        generated_at,
        created_at
    )
    VALUES
        (
            v_home_trend_id,
            v_match_id,
            v_home_team_id,
            'Arsenal are unbeaten in seven straight home league matches and have scored first in five of their last six at the Emirates.',
            jsonb_build_object(
                'home_unbeaten_streak', 7,
                'scored_first_last_6', 5,
                'clean_sheets_last_5_home', 3,
                'corners_won_avg_last_5', 6.4
            ),
            v_snapshot_at,
            v_seeded_at
        ),
        (
            v_away_trend_id,
            v_match_id,
            v_away_team_id,
            'Manchester City have gone over 2.5 goals in six of their last eight league matches and generated 1.9+ xG in four straight road fixtures.',
            jsonb_build_object(
                'over_2_5_last_8', 6,
                'road_xg_streak', 4,
                'away_goals_avg_last_5', 2.0,
                'second_half_goal_rate', 0.68
            ),
            v_snapshot_at,
            v_seeded_at
        );

    RAISE NOTICE 'Seeded EPL fixture Arsenal vs Manchester City with match_id=%', v_match_id;
END
$$ LANGUAGE plpgsql;

COMMIT;
