import streamlit as st
import psycopg2
import plotly.graph_objects as go

st.set_page_config(page_title="Data Quality Monitor", page_icon="📊", layout="wide")

st.title("🎯 Real-Time Data Quality Monitor")
st.markdown("---")

try:
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="data_quality",
        user="admin",
        password="admin123"
    )
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            SUM(total_records) as total_orders,
            SUM(issues_found) as total_issues,
            ROUND(AVG(overall_score), 2) as avg_score,
            ROUND(EXTRACT(EPOCH FROM (MAX(window_end) - MIN(window_start)))/3600, 1) as uptime_hours
        FROM quality_stats
    """)
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
    
    st.subheader("📊 Quality Dimensions")
    cursor.execute("""
        SELECT completeness_score, timeliness_score, accuracy_score
        FROM quality_stats
        ORDER BY created_at DESC
        LIMIT 1
    """)
    dims = cursor.fetchone()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=dims[0],
            title={'text': "Completeness"},
            gauge={'axis': {'range': [0, 100]}, 'bar': {'color': "blue"}}
        ))
        fig.update_layout(height=250)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=dims[1],
            title={'text': "Timeliness"},
            gauge={'axis': {'range': [0, 100]}, 'bar': {'color': "green"}}
        ))
        fig.update_layout(height=250)
        st.plotly_chart(fig, use_container_width=True)
    
    with col3:
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=dims[2],
            title={'text': "Accuracy"},
            gauge={'axis': {'range': [0, 100]}, 'bar': {'color': "orange"}}
        ))
        fig.update_layout(height=250)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    st.subheader("🚨 Issues by Severity")
    cursor.execute("""
        SELECT severity, COUNT(*) as count
        FROM quality_issues
        GROUP BY severity
        ORDER BY count DESC
    """)
    issues = cursor.fetchall()
    
    col1, col2 = st.columns(2)
    
    with col1:
        for severity, count in issues:
            icon = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}.get(severity, "⚪")
            st.metric(f"{icon} {severity.capitalize()}", f"{count:,}")
    
    with col2:
        labels = [s[0].capitalize() for s in issues]
        values = [s[1] for s in issues]
        fig = go.Figure(data=[go.Pie(labels=labels, values=values)])
        fig.update_layout(height=300, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    
    cursor.close()
    conn.close()
    
    if st.sidebar.checkbox("Auto-refresh", value=False):
        import time
        time.sleep(10)
        st.rerun()
    
except Exception as e:
    st.error(f"❌ Error: {e}")

st.sidebar.markdown("---")
st.sidebar.info("Real-Time Data Quality Monitor")