"""
FastAPI service for Real-Time Data Quality Monitor.
Exposes quality metrics, statistics, and health information via REST API.
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Initialize FastAPI app
app = FastAPI(
    title="Data Quality Monitor API",
    description="REST API for real-time data quality metrics and monitoring",
    version="1.0.0"
)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'data_quality'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'admin123')
}


def get_db_connection():
    """Create database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")


@app.get("/")
async def root():
    """API information and available endpoints."""
    return {
        "service": "Data Quality Monitor API",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "health": "/health",
            "latest_metrics": "/metrics/latest",
            "metrics_history": "/metrics/history?hours=24",
            "dimensions": "/metrics/dimensions",
            "recent_issues": "/issues/recent?limit=100",
            "window_stats": "/stats/window?limit=10"
        },
        "documentation": "/docs"
    }


@app.get("/health")
async def health_check():
    """System health check."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check database connection
        cursor.execute("SELECT 1")
        
        # Check if data is being written (last 5 minutes)
        cursor.execute("""
            SELECT COUNT(*) as recent_count
            FROM quality_metrics
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
        """)
        recent_count = cursor.fetchone()[0]
        
        # Check latest window
        cursor.execute("""
            SELECT window_end, total_records, overall_score
            FROM quality_stats
            ORDER BY window_end DESC
            LIMIT 1
        """)
        latest_window = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        is_healthy = recent_count > 0
        
        return {
            "status": "healthy" if is_healthy else "degraded",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "connected",
            "recent_metrics_count": recent_count,
            "latest_window": {
                "time": latest_window[0].isoformat() if latest_window else None,
                "records": latest_window[1] if latest_window else 0,
                "quality_score": float(latest_window[2]) if latest_window else 0
            } if latest_window else None,
            "data_flowing": is_healthy
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            }
        )


@app.get("/metrics/latest")
async def get_latest_metrics():
    """Get the latest quality scores for all dimensions."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get latest scores for each dimension (last 5 minutes average)
        cursor.execute("""
            SELECT 
                metric_name,
                COUNT(*) as sample_count,
                ROUND(AVG(metric_value), 2) as avg_score,
                ROUND(MIN(metric_value), 2) as min_score,
                ROUND(MAX(metric_value), 2) as max_score,
                MAX(timestamp) as last_updated
            FROM quality_metrics
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
            GROUP BY metric_name
            ORDER BY metric_name
        """)
        
        metrics = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "window": "last_5_minutes",
            "dimensions": [dict(row) for row in metrics],
            "dimension_count": len(metrics)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/history")
async def get_metrics_history(
    hours: int = Query(default=24, ge=1, le=168, description="Hours of history (1-168)")
):
    """Get historical quality metrics over time."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                window_end,
                total_records,
                clean_records,
                issues_found,
                completeness_score,
                timeliness_score,
                accuracy_score,
                overall_score
            FROM quality_stats
            WHERE window_end > NOW() - INTERVAL '%s hours'
            ORDER BY window_end DESC
        """, (hours,))
        
        history = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "hours": hours,
            "window_count": len(history),
            "windows": [dict(row) for row in history]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/dimensions")
async def get_dimensions_breakdown():
    """Get detailed breakdown of all 6 quality dimensions."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get 24-hour stats for each dimension
        cursor.execute("""
            SELECT 
                metric_name,
                COUNT(*) as total_checks,
                ROUND(AVG(metric_value), 2) as avg_score,
                ROUND(MIN(metric_value), 2) as min_score,
                ROUND(MAX(metric_value), 2) as max_score,
                ROUND(STDDEV(metric_value), 2) as std_deviation,
                COUNT(CASE WHEN metric_value < 50 THEN 1 END) as critical_count,
                COUNT(CASE WHEN metric_value >= 50 AND metric_value < 80 THEN 1 END) as warning_count,
                COUNT(CASE WHEN metric_value >= 80 THEN 1 END) as good_count
            FROM quality_metrics
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY metric_name
            ORDER BY metric_name
        """)
        
        dimensions = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "period": "last_24_hours",
            "dimensions": [dict(row) for row in dimensions]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/issues/recent")
async def get_recent_issues(
    limit: int = Query(default=100, ge=1, le=1000, description="Number of issues to return"),
    severity: Optional[str] = Query(default=None, description="Filter by severity: critical, high, medium, low")
):
    """Get recent quality issues."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                id,
                order_id,
                issue_type,
                severity,
                issue_description,
                detected_at
            FROM quality_issues
            WHERE 1=1
        """
        params = []
        
        if severity:
            query += " AND severity = %s"
            params.append(severity.lower())
        
        query += " ORDER BY detected_at DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        issues = cursor.fetchall()
        
        # Get issue summary
        cursor.execute("""
            SELECT 
                severity,
                COUNT(*) as count
            FROM quality_issues
            WHERE detected_at > NOW() - INTERVAL '24 hours'
            GROUP BY severity
            ORDER BY 
                CASE severity
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'medium' THEN 3
                    WHEN 'low' THEN 4
                END
        """)
        summary = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "limit": limit,
            "severity_filter": severity,
            "issue_count": len(issues),
            "issues": [dict(row) for row in issues],
            "summary_24h": [dict(row) for row in summary]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats/window")
async def get_window_stats(
    limit: int = Query(default=10, ge=1, le=100, description="Number of windows to return")
):
    """Get statistics for recent processing windows."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                window_start,
                window_end,
                total_records,
                clean_records,
                issues_found,
                completeness_score,
                timeliness_score,
                accuracy_score,
                overall_score,
                EXTRACT(EPOCH FROM (window_end - window_start)) as window_duration_seconds
            FROM quality_stats
            ORDER BY window_end DESC
            LIMIT %s
        """, (limit,))
        
        windows = cursor.fetchall()
        
        # Calculate aggregates
        if windows:
            total_records = sum(int(w['total_records']) for w in windows)
            total_clean = sum(int(w['clean_records']) for w in windows)
            total_issues = sum(int(w['issues_found']) for w in windows)
            avg_quality = sum(float(w['overall_score']) for w in windows) / len(windows)
            clean_pct = (float(total_clean) / float(total_records) * 100.0) if total_records > 0 else 0.0
        else:
            total_records = total_clean = total_issues = 0
            avg_quality = 0.0
            clean_pct = 0.0
        
        cursor.close()
        conn.close()
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "window_count": len(windows),
            "aggregate": {
                "total_records": total_records,
                "clean_records": total_clean,
                "issues_found": total_issues,
                "average_quality_score": round(avg_quality, 2),
                "clean_percentage": round(clean_pct, 2)
            },
            "windows": [dict(row) for row in windows]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)