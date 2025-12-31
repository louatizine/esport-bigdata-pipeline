"""
Esports Analytics Dashboard - Streamlit Application
Phase 5: BI Integration
"""

import os
import sys
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine


# Page configuration
st.set_page_config(
    page_title="Esports Analytics Dashboard",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Database connection
@st.cache_resource
def get_db_connection():
    """Create PostgreSQL database connection."""
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DATABASE", "esports_analytics")
    postgres_user = os.getenv("POSTGRES_USER", "postgres")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres")

    connection_string = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"

    try:
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None


# Data loading functions
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_player_kpis(_engine):
    """Load player KPIs."""
    query = "SELECT * FROM vw_player_kpis"
    return pd.read_sql(query, _engine)


@st.cache_data(ttl=300)
def load_match_kpis(_engine):
    """Load match KPIs."""
    query = "SELECT * FROM vw_match_kpis"
    return pd.read_sql(query, _engine)


@st.cache_data(ttl=300)
def load_top_players_kda(_engine, limit=100):
    """Load top players by KDA."""
    query = f"SELECT * FROM vw_top_players_kda LIMIT {limit}"
    return pd.read_sql(query, _engine)


@st.cache_data(ttl=300)
def load_top_players_winrate(_engine, limit=100):
    """Load top players by win rate."""
    query = f"SELECT * FROM vw_top_players_winrate LIMIT {limit}"
    return pd.read_sql(query, _engine)


@st.cache_data(ttl=300)
def load_team_performance(_engine):
    """Load team performance."""
    query = "SELECT * FROM vw_team_performance"
    return pd.read_sql(query, _engine)


@st.cache_data(ttl=300)
def load_role_comparison(_engine):
    """Load role comparison."""
    query = "SELECT * FROM vw_role_comparison"
    return pd.read_sql(query, _engine)


@st.cache_data(ttl=300)
def load_tournament_stats(_engine):
    """Load tournament statistics."""
    query = "SELECT * FROM vw_tournament_stats"
    return pd.read_sql(query, _engine)


@st.cache_data(ttl=300)
def load_active_players(_engine, role=None, team=None):
    """Load active players with optional filters."""
    query = "SELECT * FROM vw_active_players WHERE 1=1"

    if role and role != "All":
        query += f" AND role = '{role}'"

    if team and team != "All":
        query += f" AND current_team_name = '{team}'"

    query += " ORDER BY kda_ratio DESC"

    return pd.read_sql(query, _engine)


# Main dashboard
def main():
    """Main dashboard application."""

    # Sidebar
    st.sidebar.title("🎮 Esports Analytics")
    st.sidebar.markdown("---")

    # Navigation
    page = st.sidebar.radio(
        "Navigation",
        ["Overview", "Player Analytics", "Team Analytics",
            "Match Analytics", "Rankings"]
    )

    st.sidebar.markdown("---")
    st.sidebar.info(
        "**Data Source:** PostgreSQL\n\n"
        "**Refresh:** Every 5 minutes\n\n"
        "**Phase:** 5 - BI Integration"
    )

    # Get database connection
    engine = get_db_connection()

    if not engine:
        st.error("❌ Cannot connect to database. Please check your configuration.")
        return

    # Route to selected page
    if page == "Overview":
        show_overview_page(engine)
    elif page == "Player Analytics":
        show_player_analytics_page(engine)
    elif page == "Team Analytics":
        show_team_analytics_page(engine)
    elif page == "Match Analytics":
        show_match_analytics_page(engine)
    elif page == "Rankings":
        show_rankings_page(engine)


def show_overview_page(engine):
    """Display overview page with KPIs."""
    st.title("📊 Dashboard Overview")
    st.markdown("### Key Performance Indicators")

    # Load KPIs
    try:
        player_kpis = load_player_kpis(engine)
        match_kpis = load_match_kpis(engine)

        # Display KPIs in columns
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Players",
                f"{player_kpis['total_players'].values[0]:,}",
                delta=f"{player_kpis['active_players'].values[0]:,} active"
            )

        with col2:
            st.metric(
                "Total Matches",
                f"{match_kpis['total_matches'].values[0]:,}",
                delta=f"{match_kpis['total_tournaments'].values[0]:,} tournaments"
            )

        with col3:
            st.metric(
                "Avg Win Rate",
                f"{player_kpis['overall_avg_win_rate'].values[0]:.1f}%"
            )

        with col4:
            st.metric(
                "Avg Match Duration",
                f"{match_kpis['avg_match_duration'].values[0]:.1f} min"
            )

        st.markdown("---")

        # Top performers
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 🏆 Top 10 Players by KDA")
            top_kda = load_top_players_kda(engine, limit=10)

            # Display table
            display_df = top_kda[[
                'rank', 'summoner_name', 'current_team_name', 'role', 'kda_ratio', 'win_rate']].copy()
            display_df.columns = ['Rank', 'Player',
                                  'Team', 'Role', 'KDA', 'Win Rate %']
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        with col2:
            st.markdown("### 📈 Top 10 Players by Win Rate")
            top_wr = load_top_players_winrate(engine, limit=10)

            # Display table
            display_df = top_wr[['rank', 'summoner_name',
                                 'current_team_name', 'total_games', 'win_rate']].copy()
            display_df.columns = ['Rank', 'Player',
                                  'Team', 'Games', 'Win Rate %']
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Role comparison chart
        st.markdown("### 📊 Performance by Role")
        role_data = load_role_comparison(engine)

        fig = go.Figure()
        fig.add_trace(go.Bar(
            name='Avg KDA',
            x=role_data['role'],
            y=role_data['avg_kda'],
            marker_color='indianred'
        ))
        fig.add_trace(go.Bar(
            name='Avg Win Rate',
            x=role_data['role'],
            y=role_data['avg_win_rate'],
            marker_color='lightsalmon'
        ))

        fig.update_layout(
            barmode='group',
            xaxis_title="Role",
            yaxis_title="Value",
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading overview data: {e}")


def show_player_analytics_page(engine):
    """Display player analytics page."""
    st.title("👤 Player Analytics")

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        role_filter = st.selectbox(
            "Filter by Role",
            ["All", "Top", "Jungle", "Mid", "ADC", "Support"]
        )

    with col2:
        # Get unique teams
        teams_query = "SELECT DISTINCT current_team_name FROM player_analytics WHERE current_team_name IS NOT NULL ORDER BY current_team_name"
        teams = pd.read_sql(teams_query, engine)
        team_filter = st.selectbox(
            "Filter by Team",
            ["All"] + teams['current_team_name'].tolist()
        )

    with col3:
        limit = st.selectbox(
            "Show top",
            [10, 25, 50, 100],
            index=1
        )

    # Load filtered data
    try:
        players_df = load_active_players(
            engine,
            role=role_filter if role_filter != "All" else None,
            team=team_filter if team_filter != "All" else None
        )

        # Limit results
        players_df = players_df.head(limit)

        st.markdown(f"### Showing {len(players_df)} players")

        # Display metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Avg KDA", f"{players_df['kda_ratio'].mean():.2f}")

        with col2:
            st.metric("Avg Win Rate", f"{players_df['win_rate'].mean():.1f}%")

        with col3:
            st.metric("Avg Games", f"{players_df['total_games'].mean():.0f}")

        # Data table
        st.markdown("### Player Statistics")
        display_df = players_df[[
            'summoner_name', 'current_team_name', 'role',
            'total_games', 'win_rate', 'kda_ratio',
            'avg_kills', 'avg_deaths', 'avg_assists'
        ]].copy()

        display_df.columns = [
            'Player', 'Team', 'Role', 'Games', 'Win Rate %',
            'KDA', 'Avg Kills', 'Avg Deaths', 'Avg Assists'
        ]

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Scatter plot: KDA vs Win Rate
        st.markdown("### KDA vs Win Rate")
        fig = px.scatter(
            players_df,
            x='kda_ratio',
            y='win_rate',
            color='role',
            size='total_games',
            hover_data=['summoner_name', 'current_team_name'],
            labels={
                'kda_ratio': 'KDA Ratio',
                'win_rate': 'Win Rate (%)',
                'role': 'Role',
                'total_games': 'Total Games'
            },
            title="Player Performance Distribution"
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading player data: {e}")


def show_team_analytics_page(engine):
    """Display team analytics page."""
    st.title("🏆 Team Analytics")

    try:
        team_data = load_team_performance(engine)

        # Top teams
        st.markdown("### Top 20 Teams by Win Rate")

        top_teams = team_data.head(20)

        # Display table
        display_df = top_teams[[
            'rank', 'team_name', 'matches_played', 'total_wins',
            'win_rate', 'avg_kills', 'avg_gold'
        ]].copy()

        display_df.columns = [
            'Rank', 'Team', 'Matches', 'Wins', 'Win Rate %',
            'Avg Kills', 'Avg Gold'
        ]

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Win rate bar chart
        st.markdown("### Win Rate Comparison")
        fig = px.bar(
            top_teams.head(15),
            x='team_name',
            y='win_rate',
            color='win_rate',
            labels={'team_name': 'Team', 'win_rate': 'Win Rate (%)'},
            color_continuous_scale='RdYlGn'
        )

        fig.update_layout(xaxis_tickangle=-45, height=500)
        st.plotly_chart(fig, use_container_width=True)

        # Team stats comparison
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Avg Kills per Game")
            fig = px.bar(
                top_teams.head(10),
                x='team_name',
                y='avg_kills',
                labels={'team_name': 'Team', 'avg_kills': 'Avg Kills'}
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("### Avg Gold per Game")
            fig = px.bar(
                top_teams.head(10),
                x='team_name',
                y='avg_gold',
                labels={'team_name': 'Team', 'avg_gold': 'Avg Gold'}
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading team data: {e}")


def show_match_analytics_page(engine):
    """Display match analytics page."""
    st.title("⚔️ Match Analytics")

    try:
        tournament_data = load_tournament_stats(engine)

        # Tournament stats
        st.markdown("### Tournament Statistics")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Tournaments", len(tournament_data))

        with col2:
            st.metric("Total Matches",
                      f"{tournament_data['total_matches'].sum():,}")

        with col3:
            st.metric("Avg Match Duration",
                      f"{tournament_data['avg_duration'].mean():.1f} min")

        # Top tournaments
        st.markdown("### Top Tournaments by Match Count")

        top_tournaments = tournament_data.head(15)

        display_df = top_tournaments[[
            'tournament_name', 'total_matches', 'completed_matches',
            'avg_duration', 'avg_kills_per_match'
        ]].copy()

        display_df.columns = [
            'Tournament', 'Total Matches', 'Completed',
            'Avg Duration (min)', 'Avg Kills'
        ]

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Match count by tournament
        st.markdown("### Match Distribution")
        fig = px.bar(
            top_tournaments.head(10),
            x='tournament_name',
            y='total_matches',
            labels={'tournament_name': 'Tournament',
                    'total_matches': 'Total Matches'},
            color='total_matches',
            color_continuous_scale='Blues'
        )

        fig.update_layout(xaxis_tickangle=-45, height=500)
        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading match data: {e}")


def show_rankings_page(engine):
    """Display rankings page."""
    st.title("🏅 Rankings")

    # Tabs for different rankings
    tab1, tab2 = st.tabs(["KDA Rankings", "Win Rate Rankings"])

    with tab1:
        st.markdown("### Top 50 Players by KDA")

        try:
            top_kda = load_top_players_kda(engine, limit=50)

            # Display table
            display_df = top_kda[[
                'rank', 'summoner_name', 'current_team_name', 'role',
                'total_games', 'kda_ratio', 'win_rate',
                'avg_kills', 'avg_deaths', 'avg_assists'
            ]].copy()

            display_df.columns = [
                'Rank', 'Player', 'Team', 'Role', 'Games',
                'KDA', 'Win Rate %', 'Kills', 'Deaths', 'Assists'
            ]

            st.dataframe(display_df, use_container_width=True, hide_index=True)

        except Exception as e:
            st.error(f"Error loading KDA rankings: {e}")

    with tab2:
        st.markdown("### Top 50 Players by Win Rate")

        try:
            top_wr = load_top_players_winrate(engine, limit=50)

            # Display table
            display_df = top_wr[[
                'rank', 'summoner_name', 'current_team_name', 'role',
                'total_games', 'total_wins', 'win_rate', 'kda_ratio'
            ]].copy()

            display_df.columns = [
                'Rank', 'Player', 'Team', 'Role', 'Games',
                'Wins', 'Win Rate %', 'KDA'
            ]

            st.dataframe(display_df, use_container_width=True, hide_index=True)

        except Exception as e:
            st.error(f"Error loading win rate rankings: {e}")


if __name__ == "__main__":
    main()
