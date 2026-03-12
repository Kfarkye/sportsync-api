BEGIN;
SET LOCAL search_path = public;

DO $$
DECLARE
    v_seeded_at timestamptz := timezone('utc', now());
    v_snapshot_at timestamptz := '2026-03-12T18:00:00Z';

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

    v_lebron_injury_id uuid := gen_random_uuid();
    v_hachimura_injury_id uuid := gen_random_uuid();
    v_porzingis_injury_id uuid := gen_random_uuid();
    v_holiday_injury_id uuid := gen_random_uuid();

    v_luka_context_id uuid := gen_random_uuid();
    v_lebron_context_id uuid := gen_random_uuid();
    v_tatum_context_id uuid := gen_random_uuid();
    v_brown_context_id uuid := gen_random_uuid();

    v_home_trend_id uuid := gen_random_uuid();
    v_away_trend_id uuid := gen_random_uuid();
BEGIN
    SELECT id
      INTO v_league_id
      FROM leagues
     WHERE slug = 'nba'
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
            'nba',
            'National Basketball Association',
            'basketball',
            '2025-26',
            'United States',
            v_seeded_at,
            v_seeded_at
        );
    END IF;

    SELECT id
      INTO v_home_team_id
      FROM teams
     WHERE canonical_name = 'Los Angeles Lakers'
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
            'basketball',
            'Los Angeles Lakers',
            'Los Angeles Lakers',
            'LAL',
            'United States',
            'Los Angeles',
            'West',
            'Pacific',
            v_seeded_at,
            v_seeded_at
        );
    END IF;

    SELECT id
      INTO v_away_team_id
      FROM teams
     WHERE canonical_name = 'Boston Celtics'
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
            'basketball',
            'Boston Celtics',
            'Boston Celtics',
            'BOS',
            'United States',
            'Boston',
            'East',
            'Atlantic',
            v_seeded_at,
            v_seeded_at
        );
    END IF;

    INSERT INTO team_mappings (id, team_id, provider, provider_team_id, provider_team_name, created_at, updated_at)
    SELECT gen_random_uuid(), v_home_team_id, m.provider, m.provider_team_id, m.provider_team_name, v_seeded_at, v_seeded_at
      FROM (
            VALUES
                ('espn', '13', 'Los Angeles Lakers'),
                ('odds_api', 'los_angeles_lakers', 'Los Angeles Lakers'),
                ('sofascore', '3421', 'Los Angeles Lakers')
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
                ('espn', '2', 'Boston Celtics'),
                ('odds_api', 'boston_celtics', 'Boston Celtics'),
                ('sofascore', '3422', 'Boston Celtics')
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
     WHERE name = 'Crypto.com Arena'
       AND city = 'Los Angeles'
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
            'Crypto.com Arena',
            'Los Angeles',
            'United States',
            19060,
            'hardwood',
            true,
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
       AND start_time = '2026-03-15T02:30:00Z'::timestamptz
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
            'basketball',
            '2025-26',
            68,
            'regular_season',
            'scheduled',
            '2026-03-15T02:30:00Z'::timestamptz,
            v_venue_id,
            v_home_team_id,
            v_away_team_id,
            jsonb_build_object(
                'espn', '401704118',
                'odds_api', 'los-angeles-lakers-vs-boston-celtics-2026-03-14',
                'sofascore', '12877055'
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
            4.5,
            -110,
            232.5,
            -108,
            160,
            -190,
            NULL,
            v_snapshot_at,
            jsonb_build_array(
                jsonb_build_object('source', 'Pinnacle', 'spread', 4.5, 'total', 232.5, 'home_ml', 162, 'away_ml', -192),
                jsonb_build_object('source', 'FanDuel', 'spread', 5.0, 'total', 233.5, 'home_ml', 168, 'away_ml', -198),
                jsonb_build_object('source', 'DraftKings', 'spread', 4.5, 'total', 232.0, 'home_ml', 160, 'away_ml', -190)
            ),
            v_seeded_at
        ),
        (
            v_market_alt_id,
            v_match_id,
            'odds_api',
            'full_game',
            4.0,
            -112,
            231.5,
            -110,
            158,
            -185,
            NULL,
            v_snapshot_at - interval '15 minutes',
            jsonb_build_array(
                jsonb_build_object('source', 'Caesars', 'spread', 4.0, 'total', 231.5, 'home_ml', 158, 'away_ml', -185)
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
        0.38,
        NULL,
        0.62,
        264810.00,
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
        'sportsync-v1-nba',
        -4.1,
        -4.5,
        0.4,
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
            'WWLWW',
            '7-3',
            0.60,
            0.53,
            0.57,
            118.4,
            112.1,
            '24-11',
            '18-15',
            2,
            24,
            'Second game in four nights but back home after a short road swing',
            v_seeded_at
        ),
        (
            v_away_form_id,
            v_match_id,
            v_away_team_id,
            v_snapshot_at,
            'WWWLW',
            '8-2',
            0.70,
            0.59,
            0.52,
            121.8,
            109.7,
            '26-8',
            '21-12',
            1,
            31,
            'Final stop of a four-game Western Conference trip',
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
        10,
        4,
        6,
        0,
        jsonb_build_array(
            jsonb_build_object('date', '2025-12-25', 'home_team', 'Boston Celtics', 'away_team', 'Los Angeles Lakers', 'home_score', 121, 'away_score', 116, 'venue', 'TD Garden'),
            jsonb_build_object('date', '2025-02-01', 'home_team', 'Los Angeles Lakers', 'away_team', 'Boston Celtics', 'home_score', 118, 'away_score', 112, 'venue', 'Crypto.com Arena'),
            jsonb_build_object('date', '2024-07-15', 'home_team', 'Boston Celtics', 'away_team', 'Los Angeles Lakers', 'home_score', 114, 'away_score', 108, 'venue', 'TD Garden'),
            jsonb_build_object('date', '2024-02-01', 'home_team', 'Boston Celtics', 'away_team', 'Los Angeles Lakers', 'home_score', 105, 'away_score', 101, 'venue', 'TD Garden'),
            jsonb_build_object('date', '2023-12-25', 'home_team', 'Los Angeles Lakers', 'away_team', 'Boston Celtics', 'home_score', 122, 'away_score', 120, 'venue', 'Crypto.com Arena')
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
            v_lebron_injury_id,
            v_match_id,
            v_home_team_id,
            'LeBron James',
            'F',
            'probable',
            'Left ankle management',
            'high',
            '2026-03-14',
            v_snapshot_at,
            'Listed probable after shootaround and expected to play normal starter minutes.',
            v_seeded_at
        ),
        (
            v_hachimura_injury_id,
            v_match_id,
            v_home_team_id,
            'Rui Hachimura',
            'F',
            'questionable',
            'Right calf soreness',
            'medium',
            '2026-03-17',
            v_snapshot_at,
            'Game-time decision after missing the prior practice session.',
            v_seeded_at
        ),
        (
            v_porzingis_injury_id,
            v_match_id,
            v_away_team_id,
            'Kristaps Porzingis',
            'C',
            'questionable',
            'Hamstring tightness',
            'high',
            '2026-03-16',
            v_snapshot_at,
            'Status tied to pregame movement testing.',
            v_seeded_at
        ),
        (
            v_holiday_injury_id,
            v_match_id,
            v_away_team_id,
            'Jrue Holiday',
            'G',
            'probable',
            'Right elbow bruise',
            'medium',
            '2026-03-14',
            v_snapshot_at,
            'Expected to be available despite a wrap on the shooting arm.',
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
            4.8,
            'LeBron is expected to suit up, but Hachimura''s status affects the Lakers'' wing size against Boston''s forwards.',
            v_snapshot_at,
            v_seeded_at
        ),
        (
            v_away_injury_impact_id,
            v_match_id,
            v_away_team_id,
            6.1,
            'Porzingis is the swing status for Boston because his spacing and rim protection materially alter both sides of the ball.',
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
            v_luka_context_id,
            v_match_id,
            v_home_team_id,
            'Luka Doncic',
            'G',
            'active',
            jsonb_build_object('points_per_game', 31.2, 'assists_per_game', 8.7, 'rebounds_per_game', 8.4, 'usage_rate', 34.9),
            'points',
            31.5,
            -108,
            -112,
            'Primary half-court engine and most likely Laker to bend Boston''s drop coverage with deep pull-ups.',
            v_snapshot_at,
            v_seeded_at
        ),
        (
            v_lebron_context_id,
            v_match_id,
            v_home_team_id,
            'LeBron James',
            'F',
            'probable',
            jsonb_build_object('points_per_game', 25.1, 'assists_per_game', 7.9, 'rebounds_per_game', 7.4, 'minutes_last_5', 34.2),
            'assists',
            7.5,
            -102,
            -118,
            'If active, the Lakers still lean on him to punish switches and trigger transition offense.',
            v_snapshot_at,
            v_seeded_at
        ),
        (
            v_tatum_context_id,
            v_match_id,
            v_away_team_id,
            'Jayson Tatum',
            'F',
            'active',
            jsonb_build_object('points_per_game', 29.6, 'rebounds_per_game', 8.8, 'assists_per_game', 5.1, 'three_point_attempts_per_game', 9.4),
            'points',
            29.5,
            -110,
            -110,
            'Boston''s first-option scorer with favorable volume if the Lakers show single coverage.',
            v_snapshot_at,
            v_seeded_at
        ),
        (
            v_brown_context_id,
            v_match_id,
            v_away_team_id,
            'Jaylen Brown',
            'G/F',
            'active',
            jsonb_build_object('points_per_game', 25.4, 'rebounds_per_game', 6.1, 'steals_per_game', 1.4, 'transition_points_per_game', 5.7),
            'points',
            24.5,
            -115,
            -105,
            'Brown''s downhill pressure is especially important if Porzingis is limited.',
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
            'The Lakers have covered in five of their last seven home games and rank top five in transition frequency over the last two weeks.',
            jsonb_build_object(
                'covers_last_7_home', 5,
                'transition_frequency_rank_last_14_days', 4,
                'paint_points_avg_last_5', 54.6,
                'free_throw_rate_last_5', 0.241
            ),
            v_snapshot_at,
            v_seeded_at
        ),
        (
            v_away_trend_id,
            v_match_id,
            v_away_team_id,
            'Boston has won eight of ten overall, posted a 40% team three-point clip over the last five, and remains elite late in close games.',
            jsonb_build_object(
                'wins_last_10', 8,
                'team_three_point_pct_last_5', 0.401,
                'clutch_net_rating_rank', 2,
                'road_off_rating_last_5', 120.8
            ),
            v_snapshot_at,
            v_seeded_at
        );

    RAISE NOTICE 'Seeded NBA fixture Lakers vs Celtics with match_id=%', v_match_id;
END
$$ LANGUAGE plpgsql;

COMMIT;
