-- PostgreSQL Schema for Esports Analytics Platform
-- Phase 5: Storage & BI Integration

-- Drop existing tables (cascade to drop views that depend on them)
DROP TABLE IF EXISTS matches_analytics CASCADE;
DROP TABLE IF EXISTS player_analytics CASCADE;
DROP TABLE IF EXISTS team_rankings CASCADE;
DROP TABLE IF EXISTS match_team_performance CASCADE;
DROP TABLE IF EXISTS player_role_stats CASCADE;

-- ============================================================================
-- MATCHES ANALYTICS TABLE
-- ============================================================================
CREATE TABLE matches_analytics (
    match_id VARCHAR(255) PRIMARY KEY,
    tournament_name VARCHAR(255),
    status VARCHAR(50),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    match_duration INTEGER,
    duration_minutes NUMERIC(10, 2),

    -- Team 1
    team_1_name VARCHAR(255),
    team_1_kills INTEGER,
    team_1_deaths INTEGER,
    team_1_assists INTEGER,
    team_1_gold BIGINT,
    team_1_towers INTEGER,

    -- Team 2
    team_2_name VARCHAR(255),
    team_2_kills INTEGER,
    team_2_deaths INTEGER,
    team_2_assists INTEGER,
    team_2_gold BIGINT,
    team_2_towers INTEGER,

    -- Winner
    winner_name VARCHAR(255),

    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for matches_analytics
CREATE INDEX idx_matches_tournament ON matches_analytics(tournament_name);
CREATE INDEX idx_matches_status ON matches_analytics(status);
CREATE INDEX idx_matches_started_at ON matches_analytics(started_at);
CREATE INDEX idx_matches_winner ON matches_analytics(winner_name);
CREATE INDEX idx_matches_team1 ON matches_analytics(team_1_name);
CREATE INDEX idx_matches_team2 ON matches_analytics(team_2_name);

-- ============================================================================
-- PLAYER ANALYTICS TABLE
-- ============================================================================
CREATE TABLE player_analytics (
    player_id VARCHAR(255) PRIMARY KEY,
    summoner_name VARCHAR(255) NOT NULL,
    current_team_name VARCHAR(255),
    role VARCHAR(50),
    status VARCHAR(50),

    -- Game statistics
    total_games INTEGER,
    total_wins INTEGER,
    total_losses INTEGER,
    win_rate NUMERIC(5, 2),

    -- Performance metrics
    kda_ratio NUMERIC(10, 2),
    avg_kills NUMERIC(10, 2),
    avg_deaths NUMERIC(10, 2),
    avg_assists NUMERIC(10, 2),
    avg_cs_per_min NUMERIC(10, 2),
    avg_gold_per_min NUMERIC(10, 2),

    -- Recent form
    recent_form VARCHAR(50),

    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for player_analytics
CREATE INDEX idx_player_team ON player_analytics(current_team_name);
CREATE INDEX idx_player_role ON player_analytics(role);
CREATE INDEX idx_player_status ON player_analytics(status);
CREATE INDEX idx_player_winrate ON player_analytics(win_rate DESC);
CREATE INDEX idx_player_kda ON player_analytics(kda_ratio DESC);
CREATE INDEX idx_player_summoner ON player_analytics(summoner_name);

-- ============================================================================
-- TEAM RANKINGS TABLE
-- ============================================================================
CREATE TABLE team_rankings (
    team_name VARCHAR(255) PRIMARY KEY,
    team_rank INTEGER,
    player_count INTEGER,
    avg_win_rate NUMERIC(5, 2),
    avg_kda NUMERIC(10, 2),
    avg_kills NUMERIC(10, 2),
    avg_deaths NUMERIC(10, 2),
    avg_assists NUMERIC(10, 2),
    total_team_games INTEGER,

    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for team_rankings
CREATE INDEX idx_team_rank ON team_rankings(team_rank);
CREATE INDEX idx_team_winrate ON team_rankings(avg_win_rate DESC);
CREATE INDEX idx_team_kda ON team_rankings(avg_kda DESC);

-- ============================================================================
-- MATCH TEAM PERFORMANCE TABLE (Aggregated)
-- ============================================================================
CREATE TABLE match_team_performance (
    id SERIAL PRIMARY KEY,
    team_name VARCHAR(255) NOT NULL,
    matches_played INTEGER,
    total_wins INTEGER,
    total_losses INTEGER,
    win_rate NUMERIC(5, 2),
    avg_kills NUMERIC(10, 2),
    avg_deaths NUMERIC(10, 2),
    avg_gold BIGINT,
    avg_towers INTEGER,

    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(team_name)
);

-- Indexes for match_team_performance
CREATE INDEX idx_mtp_team ON match_team_performance(team_name);
CREATE INDEX idx_mtp_winrate ON match_team_performance(win_rate DESC);

-- ============================================================================
-- PLAYER ROLE STATS TABLE (Aggregated by Role)
-- ============================================================================
CREATE TABLE player_role_stats (
    id SERIAL PRIMARY KEY,
    role VARCHAR(50) NOT NULL,
    player_count INTEGER,
    active_players INTEGER,
    avg_win_rate NUMERIC(5, 2),
    avg_kda NUMERIC(10, 2),
    avg_kills NUMERIC(10, 2),
    avg_deaths NUMERIC(10, 2),
    avg_assists NUMERIC(10, 2),
    avg_cs_per_min NUMERIC(10, 2),
    avg_gold_per_min NUMERIC(10, 2),

    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(role)
);

-- Indexes for player_role_stats
CREATE INDEX idx_prs_role ON player_role_stats(role);

-- ============================================================================
-- BI-OPTIMIZED VIEWS
-- ============================================================================

-- View: Active Players with Team Performance
CREATE OR REPLACE VIEW vw_active_players AS
SELECT
    p.player_id,
    p.summoner_name,
    p.current_team_name,
    p.role,
    p.total_games,
    p.total_wins,
    p.win_rate,
    p.kda_ratio,
    p.avg_kills,
    p.avg_deaths,
    p.avg_assists,
    p.avg_cs_per_min,
    p.avg_gold_per_min,
    t.team_rank,
    t.avg_win_rate as team_avg_win_rate
FROM player_analytics p
LEFT JOIN team_rankings t ON p.current_team_name = t.team_name
WHERE p.status = 'active'
ORDER BY p.kda_ratio DESC;

-- View: Top Players by KDA (min 10 games)
CREATE OR REPLACE VIEW vw_top_players_kda AS
SELECT
    player_id,
    summoner_name,
    current_team_name,
    role,
    total_games,
    kda_ratio,
    win_rate,
    avg_kills,
    avg_deaths,
    avg_assists,
    ROW_NUMBER() OVER (ORDER BY kda_ratio DESC) as rank
FROM player_analytics
WHERE total_games >= 10
  AND status = 'active'
  AND kda_ratio IS NOT NULL
ORDER BY kda_ratio DESC
LIMIT 100;

-- View: Top Players by Win Rate (min 10 games)
CREATE OR REPLACE VIEW vw_top_players_winrate AS
SELECT
    player_id,
    summoner_name,
    current_team_name,
    role,
    total_games,
    total_wins,
    win_rate,
    kda_ratio,
    ROW_NUMBER() OVER (ORDER BY win_rate DESC, total_wins DESC) as rank
FROM player_analytics
WHERE total_games >= 10
  AND status = 'active'
  AND win_rate IS NOT NULL
ORDER BY win_rate DESC, total_wins DESC
LIMIT 100;

-- View: Match Statistics by Status
CREATE OR REPLACE VIEW vw_match_stats_by_status AS
SELECT
    status,
    COUNT(*) as total_matches,
    COUNT(DISTINCT tournament_name) as unique_tournaments,
    AVG(duration_minutes) as avg_duration_minutes,
    AVG(team_1_kills + team_2_kills) as avg_total_kills,
    AVG(team_1_gold + team_2_gold) as avg_total_gold
FROM matches_analytics
WHERE status IS NOT NULL
GROUP BY status
ORDER BY total_matches DESC;

-- View: Tournament Statistics
CREATE OR REPLACE VIEW vw_tournament_stats AS
SELECT
    tournament_name,
    COUNT(*) as total_matches,
    COUNT(CASE WHEN status = 'finished' THEN 1 END) as completed_matches,
    AVG(duration_minutes) as avg_duration,
    AVG(team_1_kills + team_2_kills) as avg_kills_per_match
FROM matches_analytics
WHERE tournament_name IS NOT NULL
GROUP BY tournament_name
ORDER BY total_matches DESC;

-- View: Team Performance Summary
CREATE OR REPLACE VIEW vw_team_performance AS
SELECT
    team_name,
    matches_played,
    total_wins,
    total_losses,
    win_rate,
    avg_kills,
    avg_gold,
    avg_towers,
    ROW_NUMBER() OVER (ORDER BY win_rate DESC, total_wins DESC) as rank
FROM match_team_performance
WHERE matches_played >= 5
ORDER BY win_rate DESC, total_wins DESC;

-- View: Role Performance Comparison
CREATE OR REPLACE VIEW vw_role_comparison AS
SELECT
    role,
    player_count,
    active_players,
    avg_win_rate,
    avg_kda,
    avg_kills,
    avg_deaths,
    avg_assists,
    avg_cs_per_min,
    avg_gold_per_min
FROM player_role_stats
ORDER BY avg_kda DESC;

-- View: Daily Match Volume (for time series)
CREATE OR REPLACE VIEW vw_daily_matches AS
SELECT
    DATE(started_at) as match_date,
    COUNT(*) as matches_count,
    COUNT(DISTINCT tournament_name) as tournaments_count,
    AVG(duration_minutes) as avg_duration
FROM matches_analytics
WHERE started_at IS NOT NULL
GROUP BY DATE(started_at)
ORDER BY match_date DESC;

-- View: Player KPIs (Key Performance Indicators)
CREATE OR REPLACE VIEW vw_player_kpis AS
SELECT
    COUNT(DISTINCT player_id) as total_players,
    COUNT(DISTINCT CASE WHEN status = 'active' THEN player_id END) as active_players,
    AVG(win_rate) as overall_avg_win_rate,
    AVG(kda_ratio) as overall_avg_kda,
    MAX(kda_ratio) as max_kda,
    MAX(win_rate) as max_win_rate
FROM player_analytics;

-- View: Match KPIs
CREATE OR REPLACE VIEW vw_match_kpis AS
SELECT
    COUNT(DISTINCT match_id) as total_matches,
    COUNT(DISTINCT tournament_name) as total_tournaments,
    COUNT(DISTINCT winner_name) as unique_winners,
    AVG(duration_minutes) as avg_match_duration,
    SUM(team_1_kills + team_2_kills) as total_kills
FROM matches_analytics;

-- ============================================================================
-- GRAFANA-COMPATIBLE TIME SERIES VIEW
-- ============================================================================
CREATE OR REPLACE VIEW vw_grafana_time_series AS
SELECT
    started_at as time,
    tournament_name as metric,
    duration_minutes as value,
    status,
    winner_name
FROM matches_analytics
WHERE started_at IS NOT NULL
ORDER BY started_at DESC;

-- ============================================================================
-- FUNCTIONS & TRIGGERS
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers for auto-updating updated_at
CREATE TRIGGER update_matches_analytics_updated_at
    BEFORE UPDATE ON matches_analytics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_analytics_updated_at
    BEFORE UPDATE ON player_analytics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_team_rankings_updated_at
    BEFORE UPDATE ON team_rankings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_match_team_performance_updated_at
    BEFORE UPDATE ON match_team_performance
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_role_stats_updated_at
    BEFORE UPDATE ON player_role_stats
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE matches_analytics IS 'Match-level analytics data from Spark processing';
COMMENT ON TABLE player_analytics IS 'Player performance metrics and statistics';
COMMENT ON TABLE team_rankings IS 'Team rankings based on player performance';
COMMENT ON TABLE match_team_performance IS 'Aggregated team performance from matches';
COMMENT ON TABLE player_role_stats IS 'Aggregated statistics by player role';

COMMENT ON VIEW vw_active_players IS 'Active players with team performance metrics';
COMMENT ON VIEW vw_top_players_kda IS 'Top 100 players ranked by KDA (min 10 games)';
COMMENT ON VIEW vw_top_players_winrate IS 'Top 100 players ranked by win rate (min 10 games)';
COMMENT ON VIEW vw_grafana_time_series IS 'Time series data formatted for Grafana dashboards';

-- ============================================================================
-- GRANT PERMISSIONS (adjust as needed)
-- ============================================================================

-- Grant read-only access for BI tools
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO bi_user;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA public TO bi_user;

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
