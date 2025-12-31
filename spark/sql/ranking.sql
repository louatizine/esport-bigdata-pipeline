-- Ranking SQL Queries
-- Advanced rankings using window functions

-- Top players by KDA (with ranking)
CREATE OR REPLACE TEMP VIEW top_players_by_kda AS
SELECT
    player_id,
    summoner_name,
    current_team_name,
    role,
    kda_ratio,
    total_games,
    win_rate,
    ROW_NUMBER() OVER (ORDER BY kda_ratio DESC, total_games DESC) as kda_rank,
    RANK() OVER (ORDER BY kda_ratio DESC) as kda_rank_tie,
    DENSE_RANK() OVER (ORDER BY kda_ratio DESC) as kda_dense_rank,
    NTILE(10) OVER (ORDER BY kda_ratio DESC) as kda_decile
FROM players
WHERE total_games >= 10
    AND active = true
    AND kda_ratio IS NOT NULL;

-- Top players by role
CREATE OR REPLACE TEMP VIEW top_players_by_role AS
SELECT
    role,
    player_id,
    summoner_name,
    current_team_name,
    kda_ratio,
    win_rate,
    total_games,
    ROW_NUMBER() OVER (PARTITION BY role ORDER BY kda_ratio DESC, win_rate DESC) as role_rank,
    RANK() OVER (PARTITION BY role ORDER BY kda_ratio DESC) as role_rank_tie
FROM players
WHERE role IS NOT NULL
    AND total_games >= 10
    AND active = true;

-- Top teams by win rate
CREATE OR REPLACE TEMP VIEW team_rankings AS
WITH team_stats AS (
    SELECT
        team_name,
        total_matches,
        total_wins,
        total_losses,
        win_rate,
        avg_kills_per_game,
        avg_gold_per_game
    FROM team_performance
    WHERE total_matches >= 5
)
SELECT
    team_name,
    total_matches,
    total_wins,
    win_rate,
    avg_kills_per_game,
    avg_gold_per_game,
    ROW_NUMBER() OVER (ORDER BY win_rate DESC, total_wins DESC) as overall_rank,
    RANK() OVER (ORDER BY win_rate DESC) as win_rate_rank,
    ROW_NUMBER() OVER (ORDER BY avg_kills_per_game DESC) as kills_rank,
    NTILE(4) OVER (ORDER BY win_rate DESC) as tier
FROM team_stats;

-- Player performance percentiles
CREATE OR REPLACE TEMP VIEW player_percentiles AS
SELECT
    player_id,
    summoner_name,
    role,
    kda_ratio,
    win_rate,
    avg_kills,
    avg_cs_per_min,
    PERCENT_RANK() OVER (ORDER BY kda_ratio) as kda_percentile,
    PERCENT_RANK() OVER (ORDER BY win_rate) as win_rate_percentile,
    PERCENT_RANK() OVER (ORDER BY avg_kills) as kills_percentile,
    PERCENT_RANK() OVER (ORDER BY avg_cs_per_min) as cs_percentile,
    PERCENT_RANK() OVER (PARTITION BY role ORDER BY kda_ratio) as role_kda_percentile
FROM players
WHERE total_games >= 10
    AND active = true;

-- Running totals for tournaments
CREATE OR REPLACE TEMP VIEW tournament_running_totals AS
SELECT
    tournament_name,
    match_date,
    total_matches,
    SUM(total_matches) OVER (
        PARTITION BY tournament_name
        ORDER BY match_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_matches,
    AVG(avg_duration) OVER (
        PARTITION BY tournament_name
        ORDER BY match_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_avg_duration_7days
FROM daily_match_volume
WHERE tournament_name IS NOT NULL;

-- Player momentum (recent form ranking)
CREATE OR REPLACE TEMP VIEW player_momentum AS
SELECT
    player_id,
    summoner_name,
    role,
    recent_form,
    total_games,
    win_rate,
    kda_ratio,
    CASE
        WHEN recent_form LIKE 'W%' THEN LENGTH(recent_form) - LENGTH(REPLACE(recent_form, 'W', ''))
        ELSE 0
    END as recent_wins,
    CASE
        WHEN recent_form LIKE 'L%' THEN LENGTH(recent_form) - LENGTH(REPLACE(recent_form, 'L', ''))
        ELSE 0
    END as recent_losses,
    ROW_NUMBER() OVER (
        PARTITION BY role
        ORDER BY
            CASE WHEN recent_form LIKE 'W%'
                THEN LENGTH(recent_form) - LENGTH(REPLACE(recent_form, 'W', ''))
                ELSE 0 END DESC,
            kda_ratio DESC
    ) as momentum_rank
FROM players
WHERE recent_form IS NOT NULL
    AND active = true;

-- Best performing players by multiple criteria (composite score)
CREATE OR REPLACE TEMP VIEW composite_player_rankings AS
WITH normalized_stats AS (
    SELECT
        player_id,
        summoner_name,
        current_team_name,
        role,
        total_games,
        -- Normalize each metric to 0-100 scale
        (kda_ratio - MIN(kda_ratio) OVER ()) * 100.0 /
            NULLIF((MAX(kda_ratio) OVER () - MIN(kda_ratio) OVER ()), 0) as norm_kda,
        (win_rate - MIN(win_rate) OVER ()) * 100.0 /
            NULLIF((MAX(win_rate) OVER () - MIN(win_rate) OVER ()), 0) as norm_win_rate,
        (avg_cs_per_min - MIN(avg_cs_per_min) OVER ()) * 100.0 /
            NULLIF((MAX(avg_cs_per_min) OVER () - MIN(avg_cs_per_min) OVER ()), 0) as norm_cs,
        (avg_gold_per_min - MIN(avg_gold_per_min) OVER ()) * 100.0 /
            NULLIF((MAX(avg_gold_per_min) OVER () - MIN(avg_gold_per_min) OVER ()), 0) as norm_gold
    FROM players
    WHERE total_games >= 15
        AND active = true
)
SELECT
    player_id,
    summoner_name,
    current_team_name,
    role,
    total_games,
    -- Weighted composite score (weights can be adjusted)
    ROUND(
        (norm_kda * 0.35 +
         norm_win_rate * 0.35 +
         norm_cs * 0.15 +
         norm_gold * 0.15), 2
    ) as composite_score,
    ROW_NUMBER() OVER (ORDER BY
        (norm_kda * 0.35 +
         norm_win_rate * 0.35 +
         norm_cs * 0.15 +
         norm_gold * 0.15) DESC
    ) as overall_rank,
    ROW_NUMBER() OVER (PARTITION BY role ORDER BY
        (norm_kda * 0.35 +
         norm_win_rate * 0.35 +
         norm_cs * 0.15 +
         norm_gold * 0.15) DESC
    ) as role_rank
FROM normalized_stats;

-- Lag/Lead analysis for player performance trends
CREATE OR REPLACE TEMP VIEW player_performance_trends AS
SELECT
    player_id,
    summoner_name,
    role,
    total_games,
    win_rate,
    kda_ratio,
    LAG(win_rate, 1) OVER (PARTITION BY player_id ORDER BY profile_updated_at) as prev_win_rate,
    LEAD(win_rate, 1) OVER (PARTITION BY player_id ORDER BY profile_updated_at) as next_win_rate,
    win_rate - LAG(win_rate, 1) OVER (PARTITION BY player_id ORDER BY profile_updated_at) as win_rate_change
FROM players
WHERE total_games >= 5
ORDER BY player_id, profile_updated_at;
