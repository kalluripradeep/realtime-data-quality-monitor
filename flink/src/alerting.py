"""
Alerting module for data quality monitoring.
Handles logging alerts and email notifications.
"""
import logging
from datetime import datetime
from typing import Dict
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AlertManager:
    """Manages quality alerts via logging and email"""
    
    def __init__(self, 
                 quality_threshold: float = 90.0,
                 issue_rate_threshold: float = 40.0,
                 critical_issue_threshold: int = 100,
                 email_enabled: bool = False,
                 email_to: str = None,
                 email_from: str = None,
                 smtp_server: str = "smtp.gmail.com",
                 smtp_port: int = 587,
                 smtp_password: str = None,
                 alert_log_dir: str = "/app/logs"):
        
        self.quality_threshold = quality_threshold
        self.issue_rate_threshold = issue_rate_threshold
        self.critical_issue_threshold = critical_issue_threshold
        self.email_enabled = email_enabled
        self.email_to = email_to
        self.email_from = email_from
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_password = smtp_password
        
        # Create logs directory if it doesn't exist
        os.makedirs(alert_log_dir, exist_ok=True)
        alert_log_file = os.path.join(alert_log_dir, "alerts.log")
        
        # Set up alert file logging
        self.alert_logger = logging.getLogger('quality_alerts')
        self.alert_logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(alert_log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        self.alert_logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter('🚨 ALERT: %(message)s')
        )
        self.alert_logger.addHandler(console_handler)
        
        logger.info(f"AlertManager initialized - Quality threshold: {quality_threshold}%")
        logger.info(f"Alert log file: {alert_log_file}")
        logger.info(f"Email alerts: {'ENABLED' if email_enabled else 'DISABLED'}")
    
    def check_quality_score(self, quality_percentage: float, window_stats: Dict):
        """Check if quality score is below threshold"""
        if quality_percentage < self.quality_threshold:
            severity = "CRITICAL" if quality_percentage < 80 else "WARNING"
            
            message = (
                f"{severity}: Quality score dropped to {quality_percentage:.2f}% "
                f"(threshold: {self.quality_threshold}%)\n"
                f"Window stats: {window_stats['total_records']} records processed, "
                f"{window_stats['clean_records']} clean, "
                f"{window_stats['issues_found']} with issues"
            )
            
            # Log based on severity
            if severity == "CRITICAL":
                self.alert_logger.error(message)
            else:
                self.alert_logger.warning(message)
            
            # Send email if enabled
            if self.email_enabled:
                self._send_email(
                    subject=f"🚨 Data Quality Alert - {severity}",
                    body=self._format_email_body(message, window_stats)
                )
    
    def check_high_issue_rate(self, issues_found: int, total_records: int):
        """Alert if issue rate is abnormally high"""
        if total_records == 0:
            return
        
        issue_rate = (issues_found / total_records) * 100
        
        # Alert if above threshold
        if issue_rate > self.issue_rate_threshold:
            message = (
                f"WARNING: High issue rate detected: {issue_rate:.1f}% "
                f"(threshold: {self.issue_rate_threshold}%)\n"
                f"Found {issues_found} issues in {total_records} records"
            )
            
            self.alert_logger.warning(message)
            
            if self.email_enabled:
                self._send_email(
                    subject="⚠️ High Data Quality Issue Rate",
                    body=message
                )
    
    def check_critical_issues(self, critical_count: int):
        """Alert on critical severity issues"""
        if critical_count > self.critical_issue_threshold:
            message = (
                f"CRITICAL: {critical_count} critical severity issues detected "
                f"(threshold: {self.critical_issue_threshold})\n"
                f"These are records with overall quality score < 50%"
            )
            
            self.alert_logger.error(message)
            
            if self.email_enabled:
                self._send_email(
                    subject="🔴 Critical Data Quality Issues",
                    body=message
                )
    
    def log_system_health(self, is_healthy: bool, message: str = ""):
        """Log system health status"""
        if not is_healthy:
            alert_msg = f"SYSTEM HEALTH: UNHEALTHY - {message}"
            self.alert_logger.error(alert_msg)
            
            if self.email_enabled:
                self._send_email(
                    subject="🔥 Data Quality Monitor - System Unhealthy",
                    body=alert_msg
                )
        else:
            logger.info(f"System health check: HEALTHY - {message}")
    
    def log_info(self, message: str):
        """Log informational message"""
        self.alert_logger.info(message)
    
    def _format_email_body(self, alert_message: str, window_stats: Dict = None) -> str:
        """Format a detailed email body"""
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        
        body = f"""
Data Quality Alert
==================
Time: {timestamp}

{alert_message}

"""
        
        if window_stats:
            total = window_stats.get('total_records', 0)
            clean = window_stats.get('clean_records', 0)
            issues = window_stats.get('issues_found', 0)
            critical = window_stats.get('critical_issues', 0)
            
            clean_pct = (clean / total * 100) if total > 0 else 0
            issue_pct = (issues / total * 100) if total > 0 else 0
            
            body += f"""
Detailed Statistics:
-------------------
Total Records: {total}
Clean Records: {clean} ({clean_pct:.1f}%)
Records with Issues: {issues} ({issue_pct:.1f}%)
Critical Issues: {critical}

"""
        
        body += """
---
Real-Time Data Quality Monitor
https://github.com/kalluripradeep/realtime-data-quality-monitor
"""
        
        return body
    
    def _send_email(self, subject: str, body: str):
        """Send email alert"""
        if not self.email_to or not self.email_from:
            logger.debug("Email not configured, skipping email alert")
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_from
            msg['To'] = self.email_to
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Connect to SMTP server
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            
            if self.smtp_password:
                server.login(self.email_from, self.smtp_password)
            
            server.send_message(msg)
            server.quit()
            
            logger.info(f"✅ Email alert sent to {self.email_to}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send email alert: {e}")


# Test the alert manager
if __name__ == "__main__":
    # Test with logging only (no email)
    alert_mgr = AlertManager(
        quality_threshold=90.0,
        email_enabled=False
    )
    
    # Test quality score alert
    print("\n🧪 Testing quality score alert...")
    alert_mgr.check_quality_score(85.5, {
        'total_records': 100,
        'clean_records': 70,
        'issues_found': 30,
        'critical_issues': 5
    })
    
    # Test high issue rate
    print("\n🧪 Testing high issue rate alert...")
    alert_mgr.check_high_issue_rate(45, 100)
    
    # Test critical issues
    print("\n🧪 Testing critical issues alert...")
    alert_mgr.check_critical_issues(150)
    
    # Test system health
    print("\n🧪 Testing system health alert...")
    alert_mgr.log_system_health(False, "Database connection failed")
    
    print("\n✅ Alert tests complete! Check /app/logs/alerts.log for output")