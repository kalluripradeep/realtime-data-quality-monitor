"""
Enhanced Real-Time Data Quality Monitor Dashboard
Shows all 6 quality dimensions
"""
import streamlit as st
import psycopg2
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Data Quality Monitor", page_icon="📊", layout="wide")
st.title("🎯 Real-Time Data Quality Monitor")
st.markdown("**Monitoring 6 Quality Dimensions in Real-Time**")
st.markdown("---")

def get_db_connection():
    try:
        return psycopg2.connect(
            host="postgres", port="5432", database="data_quality",
            user="admin", password="admin123"
        )
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

try:
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        
        # Overview metrics
        cursor.execute("""
            SELECT SUM(total_records), SUM(issues_found),
                   ROUND(AVG(overall_score), 2),
                   ROUND(EXTRACT(EPOCH FROM (MAX(window_end) - MIN(window_start)))/3600, 1)
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
        st.subheader("📊 Quality Dimensions - Real-Time Scores (Last 5 Minutes)")
        
        # Get all 6 dimensions
        cursor.execute("""
            SELECT metric_name, ROUND(AVG(metric_value), 1)
            FROM quality_metrics
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
            GROUP BY metric_name
        """)
        
        dimensions = {row[0].replace('_score', '').title(): row[1] for row in cursor.fetchall()}
        
        cols = st.columns(6)
        dim_order = ['Completeness', 'Timeliness', 'Accuracy', 'Consistency', 'Uniqueness', 'Validity']
        dim_icons = ['✅', '⏱️', '🎯', '🔄', '🆔', '✔️']
        
        for idx, (name, icon) in enumerate(zip(dim_order, dim_icons)):
            with cols[idx]:
                score = dimensions.get(name, 0)
                color = "green" if score >= 95 else ("orange" if score >= 85 else "red")
                
                st.metric(f"{icon} {name}", f"{score:.1f}%")
                
                fig = go.Figure(go.Indicator(
                    mode="gauge+number", value=score,
                    gauge={'axis': {'range': [0, 100]}, 'bar': {'color': color},
                           'threshold': {'line': {'color': "red", 'width': 4}, 'value': 90}}
                ))
                fig.update_layout(height=150, margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig, use_container_width=True, key=f"g_{name}")
        
        st.markdown("---")
        st.subheader("📈 Historical Quality Trends (Last 24 Hours)")
        
        cursor.execute("""
            SELECT window_end, completeness_score, timeliness_score, accuracy_score, overall_score
            FROM quality_stats
            WHERE window_end > NOW() - INTERVAL '24 hours'
            ORDER BY window_end
        """)
        
        history = cursor.fetchall()
        if history:
            df = pd.DataFrame(history, columns=['Time', 'Completeness', 'Timeliness', 'Accuracy', 'Overall'])
            
            fig = go.Figure()
            for col in ['Completeness', 'Timeliness', 'Accuracy']:
                fig.add_trace(go.Scatter(x=df['Time'], y=df[col], name=col, mode='lines+markers'))
            fig.add_trace(go.Scatter(x=df['Time'], y=df['Overall'], name='Overall', 
                                    mode='lines+markers', line=dict(width=3)))
            
            fig.update_layout(title="Quality Scores", xaxis_title="Time", 
                            yaxis_title="Score (%)", height=400, hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
            
            st.download_button("📥 Download CSV", df.to_csv(index=False),
                             f"quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
        
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚨 Issue Severity (24h)")
            cursor.execute("""
                SELECT severity, COUNT(*)
                FROM quality_issues
                WHERE detected_at > NOW() - INTERVAL '24 hours'
                GROUP BY severity
            """)
            sev_data = cursor.fetchall()
            if sev_data:
                df_sev = pd.DataFrame(sev_data, columns=['Severity', 'Count'])
                colors = {'critical': '#dc3545', 'high': '#fd7e14', 'medium': '#ffc107', 'low': '#28a745'}
                fig = px.pie(df_sev, values='Count', names='Severity', color='Severity',
                           color_discrete_map=colors)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("📋 Recent Issues (1h)")
            cursor.execute("""
                SELECT order_id, issue_type, severity, detected_at
                FROM quality_issues
                WHERE detected_at > NOW() - INTERVAL '1 hour'
                ORDER BY detected_at DESC LIMIT 10
            """)
            issues = cursor.fetchall()
            if issues:
                df_iss = pd.DataFrame(issues, columns=['Order ID', 'Issue', 'Severity', 'Time'])
                st.dataframe(df_iss, use_container_width=True, height=400)
            else:
                st.success("✅ No issues!")
        
        st.markdown("---")
        col1, col2 = st.columns([3, 1])
        with col1:
            st.caption(f"📊 Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        with col2:
            if st.button("🔄 Refresh"):
                st.rerun()
        
        cursor.close()
        conn.close()

except Exception as e:
    st.error(f"❌ Error: {str(e)}")
