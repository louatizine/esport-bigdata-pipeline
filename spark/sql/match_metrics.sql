-- Match Analytics SQL Queries
-- Aggregations for match-level metrics

-- Total matches by status
CREATE OR REPLACE TEMP VIEW matches_by_status AS
SELECT
    status,
    COUNT(*) as total_matches,
    COUNT(DISTINCT tournament_name) as unique_tournaments,
    COUNT(DISTINCT CONCAT(team_1_name, '|', team_2_name)) as unique_matchups
FROM matches
GROUP BY status;

-- Average game duration by status
CREATE OR REPLACE TEMP VIEW avg_duration_by_status AS
SELECT
    status,
    AVG(match_duration) as avg_duration_seconds,
    AVG(duration_minutes) as avg_duration_minutes,
    MIN(match_duration) as min_duration_seconds,
    MAX(match_duration) as max_duration_seconds,
    PERCENTILE(match_duration, 0.5) as median_duration_seconds,
    COUNT(*) as total_matches
FROM matches
WHERE match_duration IS NOT NULL
GROUP BY status;

-- Team performance metrics
CREATE OR REPLACE TEMP VIEW team_performance AS
SELECT
    team_name,
    COUNT(*) as total_matches,
    SUM(wins) as total_wins,
    SUM(losses) as total_losses,
    ROUND(SUM(wins) * 100.0 / COUNT(*), 2) as win_rate,
    SUM(total_kills) as total_kills,
    SUM(total_gold) as total_gold,
    AVG(total_kills) as avg_kills_per_game,
    AVG(total_gold) as avg_gold_per_game
FROM (
    SELECT
        team_1_name as team_name,
        CASE WHEN winner_name = team_1_name THEN 1 ELSE 0 END as wins,
        CASE WHEN winner_name = team_1_name THEN 0 ELSE 1 END as losses,
        team_1_kills as total_kills,
        team_1_gold as total_gold
    FROM matches
    WHERE status = 'finished'

    UNION ALL

    SELECT
        team_2_name as team_name,
        CASE WHEN winner_name = team_2_name THEN 1 ELSE 0 END as wins,
        CASE WHEN winner_name = team_2_name THEN 0 ELSE 1 END as losses,
        team_2_kills as total_kills,
        team_2_gold as total_gold
    FROM matches
    WHERE status = 'finished'
) team_stats
GROUP BY team_name;

-- Tournament statistics
CREATE OR REPLACE TEMP VIEW tournament_stats AS
SELECT
    tournament_name,
    COUNT(*) as total_matches,
    COUNT(DISTINCT winner_name) as unique_winners,
    AVG(match_duration) as avg_duration_seconds,
    SUM(team_1_kills + team_2_kills) as total_kills,
    MIN(started_at) as tournament_start,
    MAX(finished_at) as tournament_end
FROM matches
WHERE tournament_name IS NOT NULL
    AND status = 'finished'
GROUP BY tournament_name;

-- Daily match volume
CREATE OR REPLACE TEMP VIEW daily_match_volume AS
SELECT
    DATE(started_at) as match_date,
    COUNT(*) as total_matches,
    COUNT(DISTINCT tournament_name) as tournaments,
    SUM(CASE WHEN status = 'finished' THEN 1 ELSE 0 END) as completed_matches,
    SUM(CASE WHEN status = 'live' THEN 1 ELSE 0 END) as live_matches,
    AVG(match_duration) as avg_duration
FROM matches
WHERE started_at IS NOT NULL
GROUP BY DATE(started_at)
ORDER BY match_date DESC;

-- Kill statistics
CREATE OR REPLACE TEMP VIEW kill_statistics AS
SELECT
    status,
    AVG(team_1_kills + team_2_kills) as avg_total_kills,
    AVG(ABS(team_1_kills - team_2_kills)) as avg_kill_difference,
    MAX(team_1_kills + team_2_kills) as max_total_kills,
    PERCENTILE(team_1_kills + team_2_kills, 0.5) as median_total_kills
FROM matches
WHERE team_1_kills IS NOT NULL
    AND team_2_kills IS NOT NULL
GROUP BY status;

-- Gold efficiency (kills per gold)
CREATE OR REPLACE TEMP VIEW gold_efficiency AS
SELECT
    winner_name as team_name,
    COUNT(*) as matches_won,
    AVG(CASE
        WHEN winner_name = team_1_name THEN team_1_kills * 1000.0 / NULLIF(team_1_gold, 0)
        WHEN winner_name = team_2_name THEN team_2_kills * 1000.0 / NULLIF(team_2_gold, 0)
    END) as avg_kills_per_1k_gold,
    AVG(CASE
        WHEN winner_name = team_1_name THEN team_1_gold
        WHEN winner_name = team_2_name THEN team_2_gold
    END) as avg_winning_gold
FROM matches
WHERE status = 'finished'
    AND winner_name IS NOT NULL
    AND team_1_gold > 0
    AND team_2_gold > 0
GROUP BY winner_name;
