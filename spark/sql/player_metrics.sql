-- Player Analytics SQL Queries
-- Aggregations for player-level metrics

-- Player win rate and statistics
CREATE OR REPLACE TEMP VIEW player_win_rate AS
SELECT
    player_id,
    summoner_name,
    current_team_name,
    role,
    total_games,
    total_wins,
    total_losses,
    ROUND(total_wins * 100.0 / NULLIF(total_games, 0), 2) as win_rate_pct,
    ROUND((total_wins - total_losses) * 1.0 / NULLIF(total_games, 0), 3) as win_loss_ratio,
    active,
    status
FROM players
WHERE total_games > 0;

-- Player KDA metrics
CREATE OR REPLACE TEMP VIEW player_kda_metrics AS
SELECT
    player_id,
    summoner_name,
    current_team_name,
    role,
    avg_kills,
    avg_deaths,
    avg_assists,
    ROUND((avg_kills + avg_assists) / NULLIF(avg_deaths, 0), 2) as kda_ratio,
    ROUND(avg_kills / NULLIF(total_games, 0), 2) as kills_per_game,
    ROUND(avg_deaths / NULLIF(total_games, 0), 2) as deaths_per_game,
    ROUND(avg_assists / NULLIF(total_games, 0), 2) as assists_per_game,
    total_games
FROM players
WHERE total_games > 0;

-- Player efficiency metrics
CREATE OR REPLACE TEMP VIEW player_efficiency AS
SELECT
    player_id,
    summoner_name,
    role,
    avg_cs_per_min,
    avg_gold_per_min,
    ROUND(avg_gold_per_min / NULLIF(avg_cs_per_min, 0), 2) as gold_per_cs,
    total_games,
    win_rate,
    status
FROM players
WHERE total_games > 0
    AND avg_cs_per_min > 0;

-- Active players by role
CREATE OR REPLACE TEMP VIEW active_players_by_role AS
SELECT
    role,
    COUNT(*) as total_players,
    COUNT(CASE WHEN active = true THEN 1 END) as active_players,
    COUNT(CASE WHEN active = false THEN 1 END) as inactive_players,
    AVG(total_games) as avg_games_per_player,
    AVG(win_rate) as avg_win_rate
FROM players
WHERE role IS NOT NULL
GROUP BY role;

-- Team composition
CREATE OR REPLACE TEMP VIEW team_composition AS
SELECT
    current_team_name as team_name,
    COUNT(*) as total_players,
    COUNT(CASE WHEN active = true THEN 1 END) as active_players,
    SUM(total_games) as team_total_games,
    AVG(win_rate) as avg_team_win_rate,
    AVG(kda_ratio) as avg_team_kda,
    COUNT(DISTINCT role) as unique_roles
FROM players
WHERE current_team_name IS NOT NULL
GROUP BY current_team_name;

-- High performers (top tier players)
CREATE OR REPLACE TEMP VIEW high_performers AS
SELECT
    player_id,
    summoner_name,
    current_team_name,
    role,
    total_games,
    win_rate,
    kda_ratio,
    avg_kills,
    avg_deaths,
    avg_assists,
    avg_cs_per_min,
    avg_gold_per_min
FROM players
WHERE total_games >= 10  -- Minimum games threshold
    AND active = true
    AND win_rate >= 50
    AND kda_ratio >= 2.0
ORDER BY kda_ratio DESC, win_rate DESC;

-- Player activity over time
CREATE OR REPLACE TEMP VIEW player_activity AS
SELECT
    player_id,
    summoner_name,
    role,
    total_games,
    last_match_date,
    DATEDIFF(CURRENT_DATE(), last_match_date) as days_since_last_match,
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), last_match_date) <= 7 THEN 'Very Active'
        WHEN DATEDIFF(CURRENT_DATE(), last_match_date) <= 30 THEN 'Active'
        WHEN DATEDIFF(CURRENT_DATE(), last_match_date) <= 90 THEN 'Inactive'
        ELSE 'Very Inactive'
    END as activity_level,
    active,
    status
FROM players
WHERE last_match_date IS NOT NULL;

-- Role performance comparison
CREATE OR REPLACE TEMP VIEW role_performance AS
SELECT
    role,
    COUNT(*) as total_players,
    AVG(win_rate) as avg_win_rate,
    AVG(kda_ratio) as avg_kda,
    AVG(avg_kills) as avg_kills,
    AVG(avg_deaths) as avg_deaths,
    AVG(avg_assists) as avg_assists,
    AVG(avg_cs_per_min) as avg_cs_per_min,
    AVG(avg_gold_per_min) as avg_gold_per_min,
    PERCENTILE(kda_ratio, 0.5) as median_kda,
    MAX(kda_ratio) as max_kda
FROM players
WHERE role IS NOT NULL
    AND total_games >= 5
GROUP BY role;

-- Nationality statistics
CREATE OR REPLACE TEMP VIEW nationality_stats AS
SELECT
    nationality,
    COUNT(*) as total_players,
    COUNT(CASE WHEN active = true THEN 1 END) as active_players,
    AVG(total_games) as avg_games,
    AVG(win_rate) as avg_win_rate,
    AVG(kda_ratio) as avg_kda
FROM players
WHERE nationality IS NOT NULL
GROUP BY nationality
ORDER BY total_players DESC;
