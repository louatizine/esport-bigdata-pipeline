"""
Esports Analytics Dashboard - Real-time Kafka Data Viewer
Displays match data from Kafka topics
"""

import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Page configuration
st.set_page_config(
    page_title="Esports Analytics - Kafka Live",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Styling
st.markdown("""
    <style>
    .big-font {
        font-size:30px !important;
        font-weight: bold;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
    """, unsafe_allow_html=True)


def load_matches_from_kafka(max_messages=100):
    """Load match data from Kafka topic with retry logic."""
    matches = []
    message_count = 0

    max_retries = 3
    retry_delay = 2  # seconds

    for attempt in range(max_retries):
        try:
            # Create fresh consumer each time (not cached)
            consumer = KafkaConsumer(
                'esport-matches',
                bootstrap_servers=os.getenv('KAFKA_BROKER', 'localhost:9092'),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=5000,  # 5 second timeout
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            # Connection successful, break retry loop
            break

        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                st.warning(
                    f"⏳ Kafka not ready, retrying in {retry_delay}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                st.error(
                    "❌ Cannot connect to Kafka. Please ensure Kafka is running:")
                st.code("docker-compose --profile core up -d", language="bash")
                return pd.DataFrame()
        except Exception as e:
            st.error(f"❌ Unexpected error connecting to Kafka: {e}")
            return pd.DataFrame()

    try:
        for message in consumer:
            if message_count >= max_messages:
                break

            data = message.value

            # Skip test messages
            if data.get('test') or data.get('metadata', {}).get('matchId', '').startswith('TEST'):
                continue

            # Parse Riot API match data
            metadata = data.get('metadata', {})
            info = data.get('info', {})

            match_record = {
                'match_id': metadata.get('matchId', 'N/A'),
                'game_id': info.get('gameId', 'N/A'),
                'game_mode': info.get('gameMode', 'N/A'),
                'game_type': info.get('gameType', 'N/A'),
                'game_duration': info.get('gameDuration', 0),
                'game_version': info.get('gameVersion', 'N/A'),
                'map_id': info.get('mapId', 'N/A'),
                'participants_count': len(info.get('participants', [])),
                'game_start': datetime.fromtimestamp(info.get('gameStartTimestamp', 0) / 1000) if info.get('gameStartTimestamp') else None,
                'game_end': datetime.fromtimestamp(info.get('gameEndTimestamp', 0) / 1000) if info.get('gameEndTimestamp') else None,
            }

            # Extract participant stats
            participants = info.get('participants', [])
            if participants:
                total_kills = sum(p.get('kills', 0) for p in participants)
                total_deaths = sum(p.get('deaths', 0) for p in participants)
                total_assists = sum(p.get('assists', 0) for p in participants)

                match_record.update({
                    'total_kills': total_kills,
                    'total_deaths': total_deaths,
                    'total_assists': total_assists,
                    'avg_kda': round((total_kills + total_assists) / max(total_deaths, 1), 2)
                })

            matches.append(match_record)
            message_count += 1

        consumer.close()

    except Exception as e:
        st.error(f"❌ Error connecting to Kafka: {e}")
        return pd.DataFrame()

    return pd.DataFrame(matches)


# Main app
def main():
    st.title("🎮 Esports Analytics Dashboard")
    st.markdown("### Real-time Data from Kafka")

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        max_messages = st.slider("Max messages to load", 10, 200, 100)

        st.markdown("---")
        st.markdown("### 📊 Data Source")
        st.info("**Kafka Topic:** esport-matches")
        st.info(f"**Broker:** {os.getenv('KAFKA_BROKER', 'localhost:9092')}")

        if st.button("🔄 Refresh Data", width='stretch'):
            st.cache_data.clear()
            st.rerun()

    # Load data
    with st.spinner("Loading data from Kafka..."):
        df = load_matches_from_kafka(max_messages)

    if df.empty:
        st.warning("⚠️ No match data found in Kafka topic")
        st.info(
            "Run the Riot API producer to ingest data: `python src/ingestion/riot_producer.py`")
        return

    # Key metrics
    st.markdown("### 📈 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Matches", len(df))

    with col2:
        avg_duration = df['game_duration'].mean() / 60
        st.metric("Avg Duration", f"{avg_duration:.1f} min")

    with col3:
        total_participants = df['participants_count'].sum()
        st.metric("Total Players", total_participants)

    with col4:
        if 'avg_kda' in df.columns:
            avg_kda = df['avg_kda'].mean()
            st.metric("Avg KDA", f"{avg_kda:.2f}")

    # Data table
    st.markdown("### 📋 Match Data")

    # Display columns
    display_cols = ['match_id', 'game_mode', 'game_duration', 'participants_count',
                    'total_kills', 'total_deaths', 'game_version']

    available_cols = [col for col in display_cols if col in df.columns]
    st.dataframe(df[available_cols], width='stretch', height=300)

    # Visualizations
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🎯 Game Modes Distribution")
        if 'game_mode' in df.columns:
            mode_counts = df['game_mode'].value_counts()
            fig = px.pie(
                values=mode_counts.values,
                names=mode_counts.index,
                title="Matches by Game Mode"
            )
            st.plotly_chart(fig, width='stretch')

    with col2:
        st.markdown("### ⏱️ Match Duration Distribution")
        if 'game_duration' in df.columns:
            df['duration_minutes'] = df['game_duration'] / 60
            fig = px.histogram(
                df,
                x='duration_minutes',
                nbins=20,
                title="Match Duration (minutes)",
                labels={'duration_minutes': 'Duration (min)'}
            )
            st.plotly_chart(fig, width="stretch")

    # Timeline view
    if 'game_start' in df.columns and df['game_start'].notna().any():
        st.markdown("### 📅 Matches Timeline")
        df_timeline = df.dropna(subset=['game_start']).copy()
        df_timeline['duration_minutes'] = df_timeline['game_duration'] / 60

        fig = px.scatter(
            df_timeline,
            x='game_start',
            y='duration_minutes',
            color='game_mode',
            size='participants_count',
            hover_data=['match_id', 'total_kills'],
            title="Matches Over Time"
        )
        st.plotly_chart(fig, width="stretch")

    # Raw data viewer (expandable)
    with st.expander("🔍 View Raw Data"):
        st.json(df.head(5).to_dict('records'))

    # Footer
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
