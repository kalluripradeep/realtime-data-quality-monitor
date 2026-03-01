"""
Enhanced Real-Time Data Quality Monitor Dashboard
Shows all 6 quality dimensions with API integration
"""
import streamlit as st
import psycopg2
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
import requests
import os

# Page config
st.set_page_config(
    page_title="Data Quality Monitor", 
    page_icon="📊", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stMetric {
        background-color: white;
        padding: 15px;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.title("🎯 Real-Time Data Quality Monitor")
st.markdown("**Monitoring 6 Quality Dimensions in Real-Time**")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("⚙️ Settings")
    
    # Data source selection
    data_source = st.radio(
        "Data Source",
        ["Database (PostgreSQL)", "API (FastAPI)"],
        index=0
    )
    
    # Time range filter
    time_range = st.selectbox(
        "Time Range",
        ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days"],
        index=2
    )
    
    # Auto-refresh
    auto_refresh = st.checkbox("Auto-refresh (10s)", value=False)
    if auto_refresh:
        st.info("Dashboard will refresh every 10 seconds")
    
    # Manual refresh button
    if st.button("🔄 Refresh Now"):
        st.rerun()

# Database connection
def get_db_connection():
    try:
        # Try Streamlit secrets first, then env vars
        try:
            host = st.secrets["DB_HOST"]
            port = st.secrets["DB_PORT"]
            database = st.secrets["DB_NAME"]
            user = st.secrets["DB_USER"]
            password = st.secrets["DB_PASSWORD"]
        except:
            host = os.environ.get("DB_HOST", "postgres")
            port = os.environ.get("DB_PORT", "5432")
            database = os.environ.get("DB_NAME", "postgres")
            user = os.environ.get("DB_USER", "postgres")
            password = os.environ.get("DB_PASSWORD", "")
        
        conn = psycopg2.connect(
            host=host, port=port, database=database,
            user=user, password=password, sslmode="require"
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# API connection
def get_api_data(endpoint):
    try:
        response = requests.get(f"http://quality-api:8000{endpoint}", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API error: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"API connection failed: {e}")
        return None

# Convert time range to hours
time_range_hours = {
    "Last Hour": 1,
    "Last 6 Hours": 6,
    "Last 24 Hours": 24,
    "Last 7 Days": 168
}
hours = time_range_hours[time_range]

try:
    # Get data based on source selection
    if data_source == "API (FastAPI)":
        # Use API
        health_data = get_api_data("/health")
        latest_metrics = get_api_data("/metrics/latest")
        dimensions_data = get_api_data("/metrics/dimensions")
        
        if health_data and latest_metrics:
            # Overview metrics from API
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "📦 Recent Metrics", 
                    f"{health_data['recent_metrics_count']:,}",
                    delta="Last 5 minutes"
                )
            with col2:
                if health_data['latest_window']:
                    st.metric(
                        "📋 Latest Window", 
                        f"{health_data['latest_window']['records']:,} orders"
                    )
            with col3:
                if health_data['latest_window']:
                    st.metric(
                        "⭐ Quality Score", 
                        f"{health_data['latest_window']['quality_score']:.2f}%"
                    )
            with col4:
                status_color = "🟢" if health_data['status'] == 'healthy' else "🔴"
                st.metric(
                    "🔌 System Status", 
                    f"{status_color} {health_data['status'].title()}"
                )
    else:
        # Use Database
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            
            # Overview metrics
            cursor.execute("""
                SELECT 
                    SUM(total_records) as total_orders,
                    SUM(issues_found) as total_issues,
                    ROUND(AVG(overall_score), 2) as avg_score,
                    ROUND(EXTRACT(EPOCH FROM (MAX(window_end) - MIN(window_start)))/3600, 1) as uptime_hours
                FROM quality_stats
                WHERE window_end > NOW() - INTERVAL '%s hours'
            """, (hours,))
            
            stats = cursor.fetchone()
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📦 Total Orders", f"{stats[0]:,}")
            with col2:
                st.metric("🔍 Issues Detected", f"{stats[1]:,}")
            with col3:
                st.metric("⭐ Quality Score", f"{stats[2]}%")
            with col4:
                st.metric("⏱️ Uptime", f"{stats[3]:.1f} hours")
    
    st.markdown("---")
    
    # Section: 6 Quality Dimensions
    st.subheader("📊 Quality Dimensions - Real-Time Scores")
    
    if data_source == "API (FastAPI)" and latest_metrics:
        # Get dimension scores from API
        dimensions = {}
        for dim in latest_metrics['dimensions']:
            name = dim['metric_name'].replace('_score', '').title()
            dimensions[name] = dim['avg_score']
        
        # Create 6 columns for 6 dimensions
        cols = st.columns(6)
        
        dimension_order = ['Completeness', 'Timeliness', 'Accuracy', 'Consistency', 'Uniqueness', 'Validity']
        dimension_icons = ['✅', '⏱️', '🎯', '🔄', '🆔', '✔️']
        
        for idx, (dim_name, icon) in enumerate(zip(dimension_order, dimension_icons)):
            with cols[idx]:
                score = dimensions.get(dim_name, 0)
                
                # Color based on score
                if score >= 95:
                    color = "green"
                elif score >= 85:
                    color = "orange"
                else:
                    color = "red"
                
                st.metric(
                    f"{icon} {dim_name}",
                    f"{score:.1f}%",
                    delta=None
                )
                
                # Mini gauge
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=score,
                    gauge={
                        'axis': {'range': [0, 100]},
                        'bar': {'color': color},
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': 90
                        }
                    }
                ))
                fig.update_layout(height=150, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)
    
    else:
        # Get from database
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            
            # Get latest scores for all 6 dimensions from quality_metrics
            cursor.execute("""
                SELECT 
                    metric_name,
                    AVG(metric_value) as avg_score
                FROM quality_metrics
                WHERE timestamp > NOW() - INTERVAL '5 minutes'
                GROUP BY metric_name
                ORDER BY metric_name
            """)
            
            metrics_data = cursor.fetchall()
            dimensions = {row[0].replace('_score', '').title(): row[1] for row in metrics_data}
            
            # Create 6 columns
            cols = st.columns(6)
            
            dimension_order = ['Completeness', 'Timeliness', 'Accuracy', 'Consistency', 'Uniqueness', 'Validity']
            dimension_icons = ['✅', '⏱️', '🎯', '🔄', '🆔', '✔️']
            
            for idx, (dim_name, icon) in enumerate(zip(dimension_order, dimension_icons)):
                with cols[idx]:
                    score = dimensions.get(dim_name, 0)
                    
                    # Color based on score
                    if score >= 95:
                        color = "green"
                    elif score >= 85:
                        color = "orange"
                    else:
                        color = "red"
                    
                    st.metric(
                        f"{icon} {dim_name}",
                        f"{score:.1f}%"
                    )
                    
                    # Mini gauge
                    fig = go.Figure(go.Indicator(
                        mode="gauge+number",
                        value=score,
                        gauge={
                            'axis': {'range': [0, 100]},
                            'bar': {'color': color},
                            'threshold': {
                                'line': {'color': "red", 'width': 4},
                                'thickness': 0.75,
                                'value': 90
                            }
                        }
                    ))
                    fig.update_layout(height=150, margin=dict(l=20, r=20, t=20, b=20))
                    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Section: Historical Trends
    st.subheader("📈 Historical Quality Trends")
    
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        
        # Get historical data
        cursor.execute("""
            SELECT 
                window_end,
                completeness_score,
                timeliness_score,
                accuracy_score,
                overall_score
            FROM quality_stats
            WHERE window_end > NOW() - INTERVAL '%s hours'
            ORDER BY window_end ASC
        """, (hours,))
        
        history_data = cursor.fetchall()
        
        if history_data:
            df = pd.DataFrame(history_data, columns=[
                'Time', 'Completeness', 'Timeliness', 'Accuracy', 'Overall'
            ])
            
            # Line chart
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df['Time'], y=df['Completeness'],
                name='Completeness', mode='lines+markers'
            ))
            fig.add_trace(go.Scatter(
                x=df['Time'], y=df['Timeliness'],
                name='Timeliness', mode='lines+markers'
            ))
            fig.add_trace(go.Scatter(
                x=df['Time'], y=df['Accuracy'],
                name='Accuracy', mode='lines+markers'
            ))
            fig.add_trace(go.Scatter(
                x=df['Time'], y=df['Overall'],
                name='Overall', mode='lines+markers', line=dict(width=3)
            ))
            
            fig.update_layout(
                title=f"Quality Scores Over Time ({time_range})",
                xaxis_title="Time",
                yaxis_title="Quality Score (%)",
                height=400,
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"quality_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    st.markdown("---")
    
    # Section: Issue Distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🚨 Issue Severity Distribution")
        
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT severity, COUNT(*) as count
                FROM quality_issues
                WHERE detected_at > NOW() - INTERVAL '%s hours'
                GROUP BY severity
                ORDER BY 
                    CASE severity
                        WHEN 'critical' THEN 1
                        WHEN 'high' THEN 2
                        WHEN 'medium' THEN 3
                        WHEN 'low' THEN 4
                    END
            """, (hours,))
            
            severity_data = cursor.fetchall()
            
            if severity_data:
                df_severity = pd.DataFrame(severity_data, columns=['Severity', 'Count'])
                
                colors = {
                    'critical': '#dc3545',
                    'high': '#fd7e14',
                    'medium': '#ffc107',
                    'low': '#28a745'
                }
                
                fig = px.pie(
                    df_severity, 
                    values='Count', 
                    names='Severity',
                    color='Severity',
                    color_discrete_map=colors,
                    title=f"Issues by Severity ({time_range})"
                )
                
                st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📋 Recent Issues")
        
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    order_id,
                    issue_type,
                    severity,
                    detected_at
                FROM quality_issues
                WHERE detected_at > NOW() - INTERVAL '1 hour'
                ORDER BY detected_at DESC
                LIMIT 10
            """)
            
            recent_issues = cursor.fetchall()
            
            if recent_issues:
                df_issues = pd.DataFrame(recent_issues, columns=[
                    'Order ID', 'Issue Type', 'Severity', 'Detected At'
                ])
                
                # Style the dataframe
                st.dataframe(
                    df_issues,
                    use_container_width=True,
                    height=400
                )
            else:
                st.success("✅ No issues in the last hour!")
    
    st.markdown("---")
    
    # Footer
    st.caption(f"📊 Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption("🔗 [GitHub Repository](https://github.com/kalluripradeep/realtime-data-quality-monitor) | 🌐 [API Docs](http://localhost:8000/docs)")

except Exception as e:
    st.error(f"❌ Error: {str(e)}")
    st.info("Make sure all services are running: `docker compose ps`")

# Auto-refresh logic
if auto_refresh:
    import time
    time.sleep(10)
    st.rerun()