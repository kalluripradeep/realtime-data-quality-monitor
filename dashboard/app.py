"""
Real-Time Data Quality Dashboard
"""
import streamlit as st
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import time
import config

st.set_page_config(
    page_title="Data Quality Monitor",
    page_icon="📊",
    layout="wide"
)

st.markdown("""
    <style>
    .big-metric { font-size: 3rem; font-weight: bold; margin: 0; }
    .metric-label { font-size: 1rem; color: #666; }
    </style>
""", unsafe_allow_html=True)


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        database=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD
    )


def get_latest_stats():
    """Get the most recent quality statistics."""
    conn = get_db_connection()
    query = """
        SELECT 
            total_records, clean_records, issues_found,
            completeness_score, timeliness_score, accuracy_score, overall_score
        FROM quality_stats
        ORDER BY created_at DESC LIMIT 1
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df.iloc[0] if not df.empty else None


def get_historical_stats(minutes=60):
    """Get historical quality statistics."""
    conn = get_db_connection()
    query = f"""
        SELECT window_start, overall_score, completeness_score, 
               timeliness_score, accuracy_score, clean_records, issues_found
        FROM quality_stats
        WHERE window_start >= NOW() - INTERVAL '{minutes} minutes'
        ORDER BY window_start ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def create_gauge(value, title):
    """Create gauge chart."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 20}},
        gauge={
            'axis': {'range': [None, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 50], 'color': '#ffcccc'},
                {'range': [50, 75], 'color': '#fff4cc'},
                {'range': [75, 90], 'color': '#d4edda'},
                {'range': [90, 100], 'color': '#c3e6cb'}
            ],
            'threshold': {'line': {'color': "red", 'width': 4}, 'value': 90}
        }
    ))
    fig.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20))
    return fig


# Main Dashboard
st.title("🎯 Real-Time Data Quality Monitor")
st.markdown("---")

# Auto-refresh
if st.sidebar.checkbox("Auto-refresh", value=True):
    refresh_rate = st.sidebar.slider("Refresh rate (seconds)", 5, 60, 5)
    st.sidebar.info(f"Auto-refreshing every {refresh_rate} seconds")
    time.sleep(refresh_rate)
    st.rerun()

try:
    latest_stats = get_latest_stats()
    
    if latest_stats is not None:
        # Top Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            score = latest_stats['overall_score']
            color = "🟢" if score >= 90 else "🟡" if score >= 75 else "🔴"
            st.markdown('<p class="metric-label">Overall Quality Score</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="big-metric">{color} {score:.1f}%</p>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<p class="metric-label">Total Records</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="big-metric">{latest_stats["total_records"]:,}</p>', unsafe_allow_html=True)
        
        with col3:
            clean_pct = (latest_stats['clean_records'] / latest_stats['total_records'] * 100)
            st.markdown('<p class="metric-label">Clean Records</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="big-metric">✅ {clean_pct:.1f}%</p>', unsafe_allow_html=True)
        
        with col4:
            issue_pct = (latest_stats['issues_found'] / latest_stats['total_records'] * 100)
            st.markdown('<p class="metric-label">Issues Found</p>', unsafe_allow_html=True)
            st.markdown(f'<p class="big-metric">⚠️ {issue_pct:.1f}%</p>', unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Quality Gauges
        st.subheader("📊 Quality Dimensions")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.plotly_chart(create_gauge(latest_stats['completeness_score'], "Completeness"), use_container_width=True)
        with col2:
            st.plotly_chart(create_gauge(latest_stats['timeliness_score'], "Timeliness"), use_container_width=True)
        with col3:
            st.plotly_chart(create_gauge(latest_stats['accuracy_score'], "Accuracy"), use_container_width=True)
        
        st.markdown("---")
        
        # Historical Trends
        st.subheader("📈 Quality Trends")
        history_minutes = st.slider("Time window (minutes)", 15, 120, 60)
        historical_data = get_historical_stats(history_minutes)
        
        if not historical_data.empty:
            # Overall score trend
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=historical_data['window_start'],
                y=historical_data['overall_score'],
                mode='lines+markers',
                name='Overall Score',
                line=dict(color='#1f77b4', width=3)
            ))
            fig.update_layout(
                title="Overall Quality Score Over Time",
                xaxis_title="Time",
                yaxis_title="Score (%)",
                yaxis=dict(range=[0, 100]),
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Dimensions comparison
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=historical_data['window_start'], y=historical_data['completeness_score'], mode='lines', name='Completeness'))
            fig.add_trace(go.Scatter(x=historical_data['window_start'], y=historical_data['timeliness_score'], mode='lines', name='Timeliness'))
            fig.add_trace(go.Scatter(x=historical_data['window_start'], y=historical_data['accuracy_score'], mode='lines', name='Accuracy'))
            fig.update_layout(
                title="Quality Dimensions Comparison",
                xaxis_title="Time",
                yaxis_title="Score (%)",
                yaxis=dict(range=[0, 100]),
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Issues vs Clean
            fig = go.Figure()
            fig.add_trace(go.Bar(x=historical_data['window_start'], y=historical_data['clean_records'], name='Clean', marker_color='lightgreen'))
            fig.add_trace(go.Bar(x=historical_data['window_start'], y=historical_data['issues_found'], name='Issues', marker_color='lightcoral'))
            fig.update_layout(title="Clean vs Problematic Records", xaxis_title="Time", yaxis_title="Count", barmode='stack', height=400)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⏳ Waiting for data...")
        
except Exception as e:
    st.error(f"❌ Error: {e}")

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Dashboard Info")
st.sidebar.info(f"**Refresh:** {config.REFRESH_INTERVAL}s\n**Database:** {config.POSTGRES_HOST}")
