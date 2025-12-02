"""
Notification Service for Smart Uploader Platform
Handles email notifications for process status, errors, and success reports
"""

import smtplib
import asyncio
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import json
import jinja2
from jinja2 import Template
import pandas as pd

logger = logging.getLogger(__name__)

class NotificationLevel(Enum):
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class NotificationType(Enum):
    DAILY_REPORT = "daily_report"
    ERROR_ALERT = "error_alert"
    SUCCESS_NOTIFICATION = "success_notification"
    SFTP_STATUS = "sftp_status"
    UPLOAD_COMPLETE = "upload_complete"
    MAPPING_ISSUES = "mapping_issues"
    SYSTEM_HEALTH = "system_health"

@dataclass
class NotificationRecipient:
    name: str
    email: str
    notification_types: List[NotificationType]
    level_threshold: NotificationLevel = NotificationLevel.INFO

@dataclass
class ProcessingSummary:
    process_name: str
    start_time: datetime
    end_time: datetime
    total_files: int
    successful_files: int
    failed_files: int
    total_records: int
    successful_records: int
    failed_records: int
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    performance_metrics: Dict[str, Any]

class NotificationService:
    """
    Enterprise notification service for automated status reporting
    """
    
    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        username: str,
        password: str,
        sender_name: str = "Smart Uploader Platform",
        sender_email: str = None,
        use_tls: bool = True
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.sender_name = sender_name
        self.sender_email = sender_email or username
        self.use_tls = use_tls
        
        self.recipients = {}
        self.notification_history = []
        
        # Load email templates
        self._load_templates()
        
        logger.info(f"Notification service initialized for {sender_email}")
    
    def _load_templates(self):
        """Load email templates for different notification types"""
        
        # HTML template for daily reports
        self.daily_report_template = Template("""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .header { background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px; }
                .success { color: #28a745; }
                .warning { color: #ffc107; }
                .error { color: #dc3545; }
                .metric { display: inline-block; margin: 10px; padding: 10px; background-color: #e9ecef; border-radius: 5px; }
                .table { border-collapse: collapse; width: 100%; margin: 20px 0; }
                .table th, .table td { border: 1px solid #ddd; padding: 12px; text-align: left; }
                .table th { background-color: #f8f9fa; font-weight: bold; }
                .footer { margin-top: 30px; font-size: 12px; color: #6c757d; }
            </style>
        </head>
        <body>
            <div class="header">
                <h2>📊 Smart Uploader - Daily Processing Report</h2>
                <p><strong>Report Date:</strong> {{ report_date }}</p>
                <p><strong>Report Period:</strong> {{ period_start }} to {{ period_end }}</p>
            </div>
            
            <div class="summary">
                <h3>📋 Processing Summary</h3>
                <div class="metric">
                    <strong>Total Files:</strong> {{ summary.total_files }}
                </div>
                <div class="metric success">
                    <strong>Successful:</strong> {{ summary.successful_files }}
                </div>
                <div class="metric error">
                    <strong>Failed:</strong> {{ summary.failed_files }}
                </div>
                <div class="metric">
                    <strong>Success Rate:</strong> {{ success_rate }}%
                </div>
            </div>
            
            <div class="records">
                <h3>📈 Records Processed</h3>
                <div class="metric">
                    <strong>Total Records:</strong> {{ "{:,}".format(summary.total_records) }}
                </div>
                <div class="metric success">
                    <strong>Successful:</strong> {{ "{:,}".format(summary.successful_records) }}
                </div>
                <div class="metric error">
                    <strong>Failed:</strong> {{ "{:,}".format(summary.failed_records) }}
                </div>
            </div>
            
            {% if summary.performance_metrics %}
            <div class="performance">
                <h3>⚡ Performance Metrics</h3>
                <div class="metric">
                    <strong>Processing Time:</strong> {{ summary.performance_metrics.get('total_time_minutes', 'N/A') }} minutes
                </div>
                <div class="metric">
                    <strong>Records/Second:</strong> {{ summary.performance_metrics.get('records_per_second', 'N/A') }}
                </div>
                <div class="metric">
                    <strong>Average File Size:</strong> {{ summary.performance_metrics.get('avg_file_size_mb', 'N/A') }} MB
                </div>
            </div>
            {% endif %}
            
            {% if summary.errors %}
            <div class="errors">
                <h3 class="error">🚨 Errors Encountered</h3>
                <table class="table">
                    <tr>
                        <th>Time</th>
                        <th>File/Process</th>
                        <th>Error Type</th>
                        <th>Description</th>
                    </tr>
                    {% for error in summary.errors %}
                    <tr>
                        <td>{{ error.timestamp }}</td>
                        <td>{{ error.source }}</td>
                        <td>{{ error.error_type }}</td>
                        <td>{{ error.description }}</td>
                    </tr>
                    {% endfor %}
                </table>
            </div>
            {% endif %}
            
            {% if summary.warnings %}
            <div class="warnings">
                <h3 class="warning">⚠️ Warnings</h3>
                <table class="table">
                    <tr>
                        <th>Time</th>
                        <th>Source</th>
                        <th>Warning</th>
                    </tr>
                    {% for warning in summary.warnings %}
                    <tr>
                        <td>{{ warning.timestamp }}</td>
                        <td>{{ warning.source }}</td>
                        <td>{{ warning.description }}</td>
                    </tr>
                    {% endfor %}
                </table>
            </div>
            {% endif %}
            
            <div class="footer">
                <p>This report was automatically generated by Smart Uploader Platform</p>
                <p>Report generated at: {{ generation_time }}</p>
            </div>
        </body>
        </html>
        """)
        
        # Simple text template for error alerts
        self.error_alert_template = Template("""
🚨 SMART UPLOADER ALERT - {{ alert_level }}

Process: {{ process_name }}
Time: {{ timestamp }}
Error Type: {{ error_type }}

Description:
{{ description }}

{% if details %}
Details:
{{ details }}
{% endif %}

{% if suggested_action %}
Suggested Action:
{{ suggested_action }}
{% endif %}

---
Smart Uploader Platform
Alert generated at: {{ generation_time }}
        """)
        
        # Success notification template
        self.success_template = Template("""
✅ SMART UPLOADER SUCCESS

Process: {{ process_name }}
Completed: {{ timestamp }}

Summary:
- Files Processed: {{ files_processed }}
- Records Uploaded: {{ records_uploaded }}
- Processing Time: {{ processing_time }}
- Success Rate: {{ success_rate }}%

{% if highlights %}
Highlights:
{% for highlight in highlights %}
• {{ highlight }}
{% endfor %}
{% endif %}

---
Smart Uploader Platform
        """)
    
    def add_recipient(self, recipient: NotificationRecipient):
        """Add notification recipient"""
        
        self.recipients[recipient.email] = recipient
        logger.info(f"Added notification recipient: {recipient.email}")
    
    def remove_recipient(self, email: str):
        """Remove notification recipient"""
        
        if email in self.recipients:
            del self.recipients[email]
            logger.info(f"Removed notification recipient: {email}")
    
    async def send_daily_report(
        self,
        summary: ProcessingSummary,
        recipients: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Send comprehensive daily processing report
        """
        
        try:
            # Calculate additional metrics
            success_rate = (
                (summary.successful_files / summary.total_files * 100) 
                if summary.total_files > 0 else 0
            )
            
            # Prepare template data
            template_data = {
                'summary': summary,
                'report_date': datetime.now().strftime('%Y-%m-%d'),
                'period_start': summary.start_time.strftime('%Y-%m-%d %H:%M'),
                'period_end': summary.end_time.strftime('%Y-%m-%d %H:%M'),
                'success_rate': f"{success_rate:.1f}",
                'generation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Generate email content
            html_content = self.daily_report_template.render(**template_data)
            
            # Prepare subject
            status_icon = "✅" if summary.failed_files == 0 else ("⚠️" if summary.failed_files < summary.total_files else "🚨")
            subject = f"{status_icon} Daily Report - {summary.total_files} files, {success_rate:.1f}% success"
            
            # Get recipients
            target_recipients = self._get_recipients_for_notification(
                NotificationType.DAILY_REPORT,
                recipients
            )
            
            # Send emails
            results = await self._send_email_batch(
                recipients=target_recipients,
                subject=subject,
                html_content=html_content,
                notification_type=NotificationType.DAILY_REPORT
            )
            
            return {
                "success": True,
                "recipients_count": len(target_recipients),
                "delivery_results": results
            }
            
        except Exception as e:
            logger.error(f"Failed to send daily report: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    async def send_error_alert(
        self,
        process_name: str,
        error_type: str,
        description: str,
        details: Optional[str] = None,
        suggested_action: Optional[str] = None,
        alert_level: NotificationLevel = NotificationLevel.ERROR,
        recipients: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Send immediate error alert notification
        """
        
        try:
            # Prepare template data
            template_data = {
                'process_name': process_name,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'error_type': error_type,
                'description': description,
                'details': details,
                'suggested_action': suggested_action,
                'alert_level': alert_level.value.upper(),
                'generation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Generate email content
            text_content = self.error_alert_template.render(**template_data)
            
            # Prepare subject with urgency indicator
            urgency_icons = {
                NotificationLevel.WARNING: "⚠️",
                NotificationLevel.ERROR: "🚨",
                NotificationLevel.CRITICAL: "🔥"
            }
            
            icon = urgency_icons.get(alert_level, "🚨")
            subject = f"{icon} {alert_level.value.upper()}: {process_name} - {error_type}"
            
            # Get recipients based on alert level
            target_recipients = self._get_recipients_for_notification(
                NotificationType.ERROR_ALERT,
                recipients,
                min_level=alert_level
            )
            
            # Send emails
            results = await self._send_email_batch(
                recipients=target_recipients,
                subject=subject,
                text_content=text_content,
                notification_type=NotificationType.ERROR_ALERT,
                priority="high" if alert_level in [NotificationLevel.ERROR, NotificationLevel.CRITICAL] else "normal"
            )
            
            return {
                "success": True,
                "recipients_count": len(target_recipients),
                "delivery_results": results
            }
            
        except Exception as e:
            logger.error(f"Failed to send error alert: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    async def send_success_notification(
        self,
        process_name: str,
        files_processed: int,
        records_uploaded: int,
        processing_time: str,
        success_rate: float,
        highlights: Optional[List[str]] = None,
        recipients: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Send success notification for completed processes
        """
        
        try:
            # Prepare template data
            template_data = {
                'process_name': process_name,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'files_processed': files_processed,
                'records_uploaded': f"{records_uploaded:,}",
                'processing_time': processing_time,
                'success_rate': f"{success_rate:.1f}",
                'highlights': highlights or []
            }
            
            # Generate email content
            text_content = self.success_template.render(**template_data)
            
            # Prepare subject
            subject = f"✅ Success: {process_name} - {files_processed} files, {records_uploaded:,} records"
            
            # Get recipients
            target_recipients = self._get_recipients_for_notification(
                NotificationType.SUCCESS_NOTIFICATION,
                recipients
            )
            
            # Send emails
            results = await self._send_email_batch(
                recipients=target_recipients,
                subject=subject,
                text_content=text_content,
                notification_type=NotificationType.SUCCESS_NOTIFICATION
            )
            
            return {
                "success": True,
                "recipients_count": len(target_recipients),
                "delivery_results": results
            }
            
        except Exception as e:
            logger.error(f"Failed to send success notification: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    async def send_mapping_issues_report(
        self,
        mapping_results: Dict[str, Any],
        recipients: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Send report about column mapping issues
        """
        
        try:
            issues = mapping_results.get('issues', [])
            suggestions = mapping_results.get('suggestions', [])
            
            # Create detailed report
            report_content = f"""
📋 COLUMN MAPPING ANALYSIS REPORT

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SUMMARY:
- Files Analyzed: {mapping_results.get('files_analyzed', 0)}
- Mapping Issues Found: {len(issues)}
- Confidence Score: {mapping_results.get('confidence_score', 0):.2f}

ISSUES IDENTIFIED:
"""
            
            for i, issue in enumerate(issues, 1):
                report_content += f"""
{i}. {issue.get('type', 'Unknown Issue')}
   File: {issue.get('filename', 'N/A')}
   Column: {issue.get('column', 'N/A')}
   Description: {issue.get('description', 'N/A')}
   Severity: {issue.get('severity', 'Medium')}
"""
            
            if suggestions:
                report_content += f"""

SUGGESTED ACTIONS:
"""
                for i, suggestion in enumerate(suggestions, 1):
                    report_content += f"{i}. {suggestion}\n"
            
            report_content += f"""

---
Smart Uploader Platform - Mapping Analysis
Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            # Prepare subject
            severity_count = {
                'high': sum(1 for issue in issues if issue.get('severity', '').lower() == 'high'),
                'medium': sum(1 for issue in issues if issue.get('severity', '').lower() == 'medium'),
                'low': sum(1 for issue in issues if issue.get('severity', '').lower() == 'low')
            }
            
            if severity_count['high'] > 0:
                subject = f"🔴 Critical Mapping Issues: {len(issues)} issues found"
            elif severity_count['medium'] > 0:
                subject = f"🟡 Mapping Issues: {len(issues)} issues found"
            else:
                subject = f"🟢 Mapping Analysis: {len(issues)} minor issues"
            
            # Get recipients
            target_recipients = self._get_recipients_for_notification(
                NotificationType.MAPPING_ISSUES,
                recipients
            )
            
            # Send emails
            results = await self._send_email_batch(
                recipients=target_recipients,
                subject=subject,
                text_content=report_content,
                notification_type=NotificationType.MAPPING_ISSUES
            )
            
            return {
                "success": True,
                "recipients_count": len(target_recipients),
                "delivery_results": results
            }
            
        except Exception as e:
            logger.error(f"Failed to send mapping issues report: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    def _get_recipients_for_notification(
        self,
        notification_type: NotificationType,
        specific_recipients: Optional[List[str]] = None,
        min_level: NotificationLevel = NotificationLevel.INFO
    ) -> List[str]:
        """Get list of recipients for specific notification type"""
        
        if specific_recipients:
            return specific_recipients
        
        recipients = []
        
        for email, recipient in self.recipients.items():
            # Check if recipient wants this notification type
            if notification_type in recipient.notification_types:
                # Check notification level threshold
                level_order = {
                    NotificationLevel.INFO: 0,
                    NotificationLevel.SUCCESS: 1,
                    NotificationLevel.WARNING: 2,
                    NotificationLevel.ERROR: 3,
                    NotificationLevel.CRITICAL: 4
                }
                
                if level_order.get(min_level, 0) >= level_order.get(recipient.level_threshold, 0):
                    recipients.append(email)
        
        return recipients
    
    async def _send_email_batch(
        self,
        recipients: List[str],
        subject: str,
        text_content: str = None,
        html_content: str = None,
        notification_type: NotificationType = None,
        priority: str = "normal",
        attachments: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Send email to multiple recipients"""
        
        results = []
        
        for recipient in recipients:
            try:
                result = await self._send_single_email(
                    recipient=recipient,
                    subject=subject,
                    text_content=text_content,
                    html_content=html_content,
                    priority=priority,
                    attachments=attachments
                )
                
                results.append({
                    "recipient": recipient,
                    "success": result["success"],
                    "message_id": result.get("message_id"),
                    "error": result.get("error")
                })
                
                # Log to notification history
                self.notification_history.append({
                    "timestamp": datetime.now(),
                    "recipient": recipient,
                    "subject": subject,
                    "notification_type": notification_type.value if notification_type else None,
                    "success": result["success"]
                })
                
            except Exception as e:
                logger.error(f"Failed to send email to {recipient}: {str(e)}")
                results.append({
                    "recipient": recipient,
                    "success": False,
                    "error": str(e)
                })
        
        return results
    
    async def _send_single_email(
        self,
        recipient: str,
        subject: str,
        text_content: str = None,
        html_content: str = None,
        priority: str = "normal",
        attachments: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Send single email"""
        
        try:
            # Create message
            message = MIMEMultipart('alternative')
            message['From'] = f"{self.sender_name} <{self.sender_email}>"
            message['To'] = recipient
            message['Subject'] = subject
            
            # Set priority
            if priority == "high":
                message['X-Priority'] = '2'
                message['X-MSMail-Priority'] = 'High'
            
            # Add text content
            if text_content:
                text_part = MIMEText(text_content, 'plain')
                message.attach(text_part)
            
            # Add HTML content
            if html_content:
                html_part = MIMEText(html_content, 'html')
                message.attach(html_part)
            
            # Add attachments
            if attachments:
                for file_path in attachments:
                    if Path(file_path).exists():
                        with open(file_path, 'rb') as f:
                            attachment = MIMEBase('application', 'octet-stream')
                            attachment.set_payload(f.read())
                            encoders.encode_base64(attachment)
                            attachment.add_header(
                                'Content-Disposition',
                                f'attachment; filename= {Path(file_path).name}'
                            )
                            message.attach(attachment)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                
                server.login(self.username, self.password)
                
                message_id = server.sendmail(
                    self.sender_email,
                    [recipient],
                    message.as_string()
                )
                
                logger.info(f"Email sent successfully to {recipient}")
                
                return {
                    "success": True,
                    "message_id": message_id
                }
        
        except Exception as e:
            logger.error(f"Failed to send email to {recipient}: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_notification_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent notification history"""
        
        return sorted(
            self.notification_history,
            key=lambda x: x['timestamp'],
            reverse=True
        )[:limit]
    
    def get_recipient_list(self) -> List[Dict[str, Any]]:
        """Get list of configured recipients"""
        
        return [
            {
                "name": recipient.name,
                "email": recipient.email,
                "notification_types": [nt.value for nt in recipient.notification_types],
                "level_threshold": recipient.level_threshold.value
            }
            for recipient in self.recipients.values()
        ]