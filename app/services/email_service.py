"""
Email Service for sending professional data processing summaries.
Supports both SMTP and cloud email services.
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from typing import List, Dict, Optional
import logging
from pathlib import Path
import mysql.connector
from app.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

logger = logging.getLogger(__name__)


class EmailService:
    """Service for sending email notifications with processing summaries."""
    
    def __init__(self):
        """Initialize email service with configuration from environment."""
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = os.getenv("SMTP_USER", "")
        self.smtp_password = os.getenv("SMTP_PASSWORD", "")
        self.from_email = os.getenv("FROM_EMAIL", self.smtp_user)
        self.from_name = os.getenv("FROM_NAME", "Smart Uploader System")
        self.enabled = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
        
        if not self.enabled:
            logger.warning("⚠️ Email notifications disabled. Set EMAIL_ENABLED=true in .env to enable.")
    
    def get_recipients_by_process(
        self, 
        process_name: str, 
        vendor_name: str = 'ALL',
        notification_type: str = 'both'
    ) -> List[Dict[str, str]]:
        """
        Fetch recipient email addresses from database for a specific process and vendor.
        
        Args:
            process_name: Process type (Upload, Validation, Error_Alert, etc.)
            vendor_name: Name of the vendor (Bank_Receipt, TRM_Data, etc.) or 'ALL'
            notification_type: Filter by notification type (success, failure, both)
        
        Returns:
            List of dictionaries with 'email', 'recipient_name', 'role' keys
        """
        try:
            from app.core.database import db_manager
            with db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor(dictionary=True)
            
            # Get recipients for specific vendor AND 'ALL' vendor (global recipients)
            query = """
                SELECT DISTINCT email, recipient_name, role, vendor_name, notification_type
                FROM email_process_notifications
                WHERE process_name = %s 
                  AND (vendor_name = %s OR vendor_name = 'ALL')
                  AND (notification_type = %s OR notification_type = 'both')
                  AND is_active = 1
                ORDER BY 
                    CASE WHEN vendor_name = %s THEN 0 ELSE 1 END,
                    role, 
                    recipient_name
            """
            
                cursor.execute(query, (process_name, vendor_name, notification_type, vendor_name))
                recipients = cursor.fetchall()
                
                logger.info(f"📧 Found {len(recipients)} recipients for process '{process_name}' (vendor: {vendor_name})")
                return recipients
            
        except mysql.connector.Error as e:
            logger.error(f"❌ Error fetching process recipients: {e}")
            return []
    
    def get_recipients_by_vendor(self, vendor_name: str) -> List[Dict[str, str]]:
        """
        Fetch recipient email addresses from database for a specific vendor.
        DEPRECATED: Use get_recipients_by_process instead.
        
        Args:
            vendor_name: Name of the vendor (Bank_Receipt, TRM_Data, etc.)
        
        Returns:
            List of dictionaries with 'email', 'name', and 'role' keys
        """
        try:
            from app.core.database import db_manager
            with db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor(dictionary=True)
            
                query = """
                    SELECT email, recipient_name, role, vendor_name
                    FROM email_recipients
                    WHERE vendor_name = %s AND is_active = 1
                    ORDER BY role, recipient_name
                """
                
                cursor.execute(query, (vendor_name,))
                recipients = cursor.fetchall()
                
                logger.info(f"📧 Found {len(recipients)} recipients for vendor '{vendor_name}'")
                return recipients
            
        except mysql.connector.Error as e:
            logger.error(f"❌ Error fetching email recipients: {e}")
            return []
    
    def get_all_recipients(self) -> List[Dict[str, str]]:
        """
        Fetch all active email recipients from database.
        
        Returns:
            List of dictionaries with 'email', 'name', 'role', and 'vendor_name' keys
        """
        try:
            from app.core.database import db_manager
            with db_manager.get_mysql_connector() as conn:
                cursor = conn.cursor(dictionary=True)
            
                query = """
                    SELECT email, recipient_name, role, vendor_name
                    FROM email_recipients
                    WHERE is_active = 1
                    ORDER BY vendor_name, role, recipient_name
                """
                
                cursor.execute(query)
                recipients = cursor.fetchall()
                
                logger.info(f"📧 Found {len(recipients)} total active recipients")
                return recipients
            
        except mysql.connector.Error as e:
            logger.error(f"❌ Error fetching email recipients: {e}")
            return []
    
    def generate_processing_summary_html(
        self,
        vendor_name: str,
        table_name: str,
        summary_data: Dict,
        processing_time: float
    ) -> str:
        """
        Generate professional HTML email content with processing summary.
        
        Args:
            vendor_name: Name of the vendor
            table_name: Target database table name
            summary_data: Dictionary with processing statistics
            processing_time: Total processing time in seconds
        
        Returns:
            HTML string for email body
        """
        timestamp = datetime.now().strftime("%B %d, %Y at %I:%M %p")
        
        # Extract summary data with defaults
        total_files = summary_data.get('files_processed', 0)
        total_rows = summary_data.get('total_rows', 0)
        success_count = summary_data.get('success_count', 0)
        failed_count = summary_data.get('failed_count', 0)
        date_range = summary_data.get('date_range', {})
        row_stats = summary_data.get('row_stats', {})
        amount_stats = summary_data.get('amount_stats', {})
        
        # Status indicator
        status_color = "#28a745" if failed_count == 0 else "#ffc107"
        status_text = "✅ SUCCESS" if failed_count == 0 else "⚠️ COMPLETED WITH ERRORS"
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: #f4f4f4;
            margin: 0;
            padding: 20px;
        }}
        .container {{
            max-width: 800px;
            margin: 0 auto;
            background-color: #ffffff;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 28px;
            font-weight: 600;
        }}
        .header p {{
            margin: 10px 0 0 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .status-badge {{
            display: inline-block;
            background-color: {status_color};
            color: white;
            padding: 8px 20px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 14px;
            margin-top: 15px;
        }}
        .content {{
            padding: 30px;
        }}
        .summary-section {{
            margin-bottom: 30px;
        }}
        .summary-section h2 {{
            color: #333;
            font-size: 20px;
            margin-bottom: 15px;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }}
        .stat-card {{
            background-color: #f8f9fa;
            border-left: 4px solid #667eea;
            padding: 15px;
            border-radius: 4px;
        }}
        .stat-label {{
            font-size: 12px;
            color: #6c757d;
            text-transform: uppercase;
            margin-bottom: 5px;
        }}
        .stat-value {{
            font-size: 24px;
            font-weight: 700;
            color: #333;
        }}
        .detail-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        .detail-table th {{
            background-color: #f8f9fa;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            color: #333;
            border-bottom: 2px solid #dee2e6;
        }}
        .detail-table td {{
            padding: 10px 12px;
            border-bottom: 1px solid #dee2e6;
        }}
        .detail-table tr:hover {{
            background-color: #f8f9fa;
        }}
        .footer {{
            background-color: #f8f9fa;
            padding: 20px;
            text-align: center;
            font-size: 12px;
            color: #6c757d;
            border-top: 1px solid #dee2e6;
        }}
        .metric-positive {{
            color: #28a745;
            font-weight: 600;
        }}
        .metric-negative {{
            color: #dc3545;
            font-weight: 600;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Data Processing Report</h1>
            <p>{timestamp}</p>
            <div class="status-badge">{status_text}</div>
        </div>
        
        <div class="content">
            <!-- Overview Section -->
            <div class="summary-section">
                <h2>📋 Processing Overview</h2>
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-label">Vendor</div>
                        <div class="stat-value">{vendor_name}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Target Table</div>
                        <div class="stat-value">{table_name}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Files Processed</div>
                        <div class="stat-value">{total_files:,}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Processing Time</div>
                        <div class="stat-value">{processing_time:.1f}s</div>
                    </div>
                </div>
            </div>
            
            <!-- Data Statistics -->
            <div class="summary-section">
                <h2>📈 Data Statistics</h2>
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-label">Total Rows Loaded</div>
                        <div class="stat-value metric-positive">{total_rows:,}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Success Rate</div>
                        <div class="stat-value metric-positive">{success_count}/{total_files}</div>
                    </div>
"""
        
        # Add failed files if any
        if failed_count > 0:
            html += f"""
                    <div class="stat-card">
                        <div class="stat-label">Failed Files</div>
                        <div class="stat-value metric-negative">{failed_count}</div>
                    </div>
"""
        
        html += """
                </div>
            </div>
"""
        
        # Add date range if available
        if date_range:
            min_date = date_range.get('min_date', 'N/A')
            max_date = date_range.get('max_date', 'N/A')
            days = date_range.get('days', 0)
            
            html += f"""
            <!-- Date Range -->
            <div class="summary-section">
                <h2>📅 Date Range</h2>
                <table class="detail-table">
                    <tr>
                        <th>Metric</th>
                        <th>Value</th>
                    </tr>
                    <tr>
                        <td>Start Date</td>
                        <td>{min_date}</td>
                    </tr>
                    <tr>
                        <td>End Date</td>
                        <td>{max_date}</td>
                    </tr>
                    <tr>
                        <td>Total Days</td>
                        <td>{days}</td>
                    </tr>
                </table>
            </div>
"""
        
        # Add amount statistics if available
        if amount_stats:
            html += """
            <!-- Amount Statistics -->
            <div class="summary-section">
                <h2>💰 Amount Statistics</h2>
                <table class="detail-table">
                    <tr>
                        <th>Metric</th>
                        <th>Value</th>
                    </tr>
"""
            for metric_name, metric_value in amount_stats.items():
                formatted_value = f"${metric_value:,.2f}" if isinstance(metric_value, (int, float)) else metric_value
                html += f"""
                    <tr>
                        <td>{metric_name}</td>
                        <td>{formatted_value}</td>
                    </tr>
"""
            html += """
                </table>
            </div>
"""
        
        # Add row statistics if available
        if row_stats:
            html += """
            <!-- Row Statistics -->
            <div class="summary-section">
                <h2>📊 Row Statistics</h2>
                <table class="detail-table">
                    <tr>
                        <th>Column</th>
                        <th>Non-Null Count</th>
                        <th>Null Count</th>
                        <th>Fill Rate</th>
                    </tr>
"""
            for col_name, stats in row_stats.items():
                non_null = stats.get('non_null', 0)
                null_count = stats.get('null', 0)
                fill_rate = stats.get('fill_rate', 0)
                html += f"""
                    <tr>
                        <td>{col_name}</td>
                        <td>{non_null:,}</td>
                        <td>{null_count:,}</td>
                        <td>{fill_rate:.1f}%</td>
                    </tr>
"""
            html += """
                </table>
            </div>
"""
        
        html += f"""
            <!-- System Information -->
            <div class="summary-section">
                <h2>ℹ️ System Information</h2>
                <table class="detail-table">
                    <tr>
                        <th>Property</th>
                        <th>Value</th>
                    </tr>
                    <tr>
                        <td>Database</td>
                        <td>{MYSQL_DB}</td>
                    </tr>
                    <tr>
                        <td>Host</td>
                        <td>{MYSQL_HOST}</td>
                    </tr>
                    <tr>
                        <td>Processing Engine</td>
                        <td>PySpark + MySQL JDBC</td>
                    </tr>
                    <tr>
                        <td>Timestamp</td>
                        <td>{timestamp}</td>
                    </tr>
                </table>
            </div>
        </div>
        
        <div class="footer">
            <p>This is an automated message from Smart Uploader System.</p>
            <p>For support, please contact your system administrator.</p>
            <p>&copy; {datetime.now().year} Smart Uploader. All rights reserved.</p>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    def send_email(
        self,
        recipients: List[str],
        subject: str,
        html_content: str,
        attachments: Optional[List[str]] = None
    ) -> bool:
        """
        Send email using SMTP.
        
        Args:
            recipients: List of email addresses
            subject: Email subject
            html_content: HTML content for email body
            attachments: Optional list of file paths to attach
        
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("📧 Email not sent - EMAIL_ENABLED=false")
            return False
        
        if not recipients:
            logger.warning("📧 No recipients specified")
            return False
        
        if not self.smtp_user or not self.smtp_password:
            logger.error("❌ SMTP credentials not configured in .env file")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{self.from_name} <{self.from_email}>"
            msg['To'] = ", ".join(recipients)
            
            # Attach HTML content
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Attach files if provided
            if attachments:
                for file_path in attachments:
                    if os.path.exists(file_path):
                        with open(file_path, 'rb') as f:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(f.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename={os.path.basename(file_path)}'
                            )
                            msg.attach(part)
            
            # Send email
            logger.info(f"📧 Connecting to SMTP server {self.smtp_host}:{self.smtp_port}")
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"✅ Email sent successfully to {len(recipients)} recipient(s)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send email: {e}")
            return False
    
    def send_processing_summary(
        self,
        vendor_name: str,
        table_name: str,
        summary_data: Dict,
        processing_time: float,
        process_name: str = "Upload"
    ) -> bool:
        """
        Generate and send processing summary email to process-specific recipients.
        
        Args:
            vendor_name: Name of the vendor
            table_name: Target database table
            summary_data: Processing statistics dictionary
            processing_time: Total processing time in seconds
            process_name: Process type (Upload, Validation, Error_Alert, etc.)
        
        Returns:
            True if email sent successfully, False otherwise
        """
        # Determine notification type based on success/failure
        failed_count = summary_data.get('failed_count', 0)
        notification_type = 'failure' if failed_count > 0 else 'success'
        
        # Get recipients for this process and vendor (includes both specific and 'ALL')
        recipients_data = self.get_recipients_by_process(
            process_name=process_name,
            vendor_name=vendor_name,
            notification_type=notification_type
        )
        
        if not recipients_data:
            logger.warning(f"⚠️ No recipients configured for process '{process_name}' (vendor: {vendor_name})")
            return False
        
        # Remove duplicates (in case same email appears in both vendor-specific and ALL)
        unique_emails = list(set([r['email'] for r in recipients_data]))
        
        logger.info(f"📧 Sending to {len(unique_emails)} unique recipients:")
        for r in recipients_data:
            logger.info(f"   • {r['recipient_name']} ({r['role']}) - {r['email']}")
        
        # Generate HTML content
        html_content = self.generate_processing_summary_html(
            vendor_name,
            table_name,
            summary_data,
            processing_time
        )
        
        # Create subject line
        status = "SUCCESS" if failed_count == 0 else "COMPLETED WITH ERRORS"
        total_rows = summary_data.get('total_rows', 0)
        subject = f"[{status}] {vendor_name} - {total_rows:,} rows processed into {table_name}"
        
        # Send email
        return self.send_email(unique_emails, subject, html_content)

    def generate_upload_summary_html(self, summary: Dict) -> str:
        """
        Generate HTML email for upload summary
        
        Args:
            summary: Upload summary dictionary
        
        Returns:
            HTML string
        """
        # Calculate duration
        if summary.get('start_time') and summary.get('end_time'):
            if isinstance(summary['start_time'], str):
                start_time = datetime.fromisoformat(summary['start_time'])
                end_time = datetime.fromisoformat(summary['end_time'])
            else:
                start_time = summary['start_time']
                end_time = summary['end_time']
            duration = (end_time - start_time).total_seconds()
            duration_str = f"{duration:.1f}s"
        else:
            duration_str = "N/A"
            start_time = datetime.now()
        
        # Determine overall status
        if summary['failed_uploads'] > 0:
            status_color = '#dc3545'
            status_icon = '❌'
            status_text = 'COMPLETED WITH ERRORS'
        elif summary['successful_uploads'] > 0:
            status_color = '#28a745'
            status_icon = '✅'
            status_text = 'COMPLETED SUCCESSFULLY'
        else:
            status_color = '#ffc107'
            status_icon = '⚠️'
            status_text = 'NO FILES PROCESSED'
        
        # Build uploads table
        uploads_rows = ""
        for upload in summary.get('uploads', []):
            if upload['success']:
                if "already uploaded" in upload['message'].lower() or "skipping" in upload['message'].lower():
                    row_color = '#fff3cd'
                    status_badge = '<span style="background-color: #ffc107; color: #000; padding: 2px 8px; border-radius: 3px; font-size: 12px;">⏭️ SKIPPED</span>'
                else:
                    row_color = '#d4edda'
                    status_badge = '<span style="background-color: #28a745; color: #fff; padding: 2px 8px; border-radius: 3px; font-size: 12px;">✅ SUCCESS</span>'
            else:
                row_color = '#f8d7da'
                status_badge = '<span style="background-color: #dc3545; color: #fff; padding: 2px 8px; border-radius: 3px; font-size: 12px;">❌ FAILED</span>'
            
            uploads_rows += f"""
            <tr style="background-color: {row_color};">
                <td style="padding: 12px; border: 1px solid #ddd;">{upload['source_name']}</td>
                <td style="padding: 12px; border: 1px solid #ddd; font-family: monospace; font-size: 11px;">{upload['file_name']}</td>
                <td style="padding: 12px; border: 1px solid #ddd; text-align: center;">{status_badge}</td>
                <td style="padding: 12px; border: 1px solid #ddd; text-align: right;">{upload['records_uploaded']:,}</td>
                <td style="padding: 12px; border: 1px solid #ddd; text-align: right;">{upload['duration_seconds']}s</td>
            </tr>
            """
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 8px 8px 0 0;
            text-align: center;
        }}
        .status-banner {{
            background-color: {status_color};
            color: white;
            padding: 15px;
            text-align: center;
            font-size: 18px;
            font-weight: bold;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            padding: 20px;
            background-color: #f8f9fa;
        }}
        .summary-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            color: #666;
            font-size: 14px;
            text-transform: uppercase;
        }}
        .summary-card .value {{
            font-size: 32px;
            font-weight: bold;
            color: #333;
        }}
        .uploads-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: white;
        }}
        .uploads-table th {{
            background-color: #667eea;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}
        .footer {{
            margin-top: 30px;
            padding: 20px;
            background-color: #f8f9fa;
            border-radius: 8px;
            text-align: center;
            font-size: 12px;
            color: #666;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📤 Data Upload Summary</h1>
        <p style="margin: 5px 0; opacity: 0.9;">Automated Upload Process Report</p>
        <p style="margin: 5px 0; font-size: 14px; opacity: 0.8;">{start_time.strftime('%B %d, %Y at %I:%M %p')}</p>
    </div>
    
    <div class="status-banner">
        {status_icon} {status_text}
    </div>
    
    <div class="summary-grid">
        <div class="summary-card">
            <h3>Total Files</h3>
            <div class="value">{summary['total_files']}</div>
        </div>
        <div class="summary-card">
            <h3>Successful</h3>
            <div class="value" style="color: #28a745;">{summary['successful_uploads']}</div>
        </div>
        <div class="summary-card">
            <h3>Failed</h3>
            <div class="value" style="color: #dc3545;">{summary['failed_uploads']}</div>
        </div>
        <div class="summary-card">
            <h3>Skipped</h3>
            <div class="value" style="color: #ffc107;">{summary['skipped_uploads']}</div>
        </div>
        <div class="summary-card">
            <h3>Records Uploaded</h3>
            <div class="value" style="color: #667eea;">{summary['total_records_uploaded']:,}</div>
        </div>
        <div class="summary-card">
            <h3>Duration</h3>
            <div class="value" style="font-size: 24px;">{duration_str}</div>
        </div>
    </div>
    
    <div style="padding: 20px;">
        <h2 style="color: #667eea; border-bottom: 2px solid #667eea; padding-bottom: 10px;">📋 Upload Details</h2>
        
        <table class="uploads-table">
            <thead>
                <tr>
                    <th>Data Source</th>
                    <th>File Name</th>
                    <th>Status</th>
                    <th style="text-align: right;">Records</th>
                    <th style="text-align: right;">Duration</th>
                </tr>
            </thead>
            <tbody>
                {uploads_rows}
            </tbody>
        </table>
    </div>
    
    <div class="footer">
        <p><strong>Smart Uploader System</strong> - Devyani Data Team</p>
        <p>Generated on {datetime.now().strftime('%B %d, %Y at %I:%M:%S %p')}</p>
        <p style="margin-top: 10px; font-size: 11px;">This is an automated email. Please do not reply.</p>
    </div>
</body>
</html>
        """
        
        return html
    
    def send_upload_summary(self, summary: Dict, to_emails: List[str] = None) -> bool:
        """
        Send upload summary email
        
        Args:
            summary: Upload summary dictionary from upload_orchestrator
            to_emails: List of recipient emails (optional, will fetch from DB if not provided)
        
        Returns:
            True if email sent successfully
        """
        if not self.enabled:
            logger.warning("⚠️ Email is disabled, skipping upload summary email")
            return False
        
        # Get recipients if not provided
        if not to_emails:
            recipients = self.get_recipients_by_process('Upload', 'ALL', 'success' if summary['failed_uploads'] == 0 else 'failure')
            to_emails = [r['email'] for r in recipients]
        
        if not to_emails:
            logger.warning("⚠️ No recipients configured for upload summaries")
            return False
        
        # Determine subject
        if summary['failed_uploads'] > 0:
            subject = f"⚠️ Data Upload Completed with {summary['failed_uploads']} Error(s)"
        elif summary['successful_uploads'] > 0:
            subject = f"✅ Data Upload Successful - {summary['total_records_uploaded']:,} Records"
        else:
            subject = "📤 Data Upload Summary - No Files Processed"
        
        # Generate HTML content
        html_body = self.generate_upload_summary_html(summary)
        
        # Send email
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{self.from_name} <{self.from_email}>"
            msg['To'] = ', '.join(to_emails)
            
            msg.attach(MIMEText(html_body, 'html'))
            
            logger.info(f"📧 Sending upload summary to: {', '.join(to_emails)}")
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"✅ Upload summary email sent successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send upload summary email: {str(e)}")
            return False


def create_email_recipients_table():
    """
    Create the email_recipients table in the database if it doesn't exist.
    This table stores email addresses for each vendor/data source.
    """
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=int(MYSQL_PORT),
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS email_recipients (
            id INT AUTO_INCREMENT PRIMARY KEY,
            vendor_name VARCHAR(100) NOT NULL COMMENT 'Vendor folder name (Bank_Receipt, TRM_Data, etc.)',
            email VARCHAR(255) NOT NULL COMMENT 'Recipient email address',
            recipient_name VARCHAR(255) NOT NULL COMMENT 'Full name of recipient',
            role VARCHAR(100) DEFAULT 'Stakeholder' COMMENT 'Role (Manager, Analyst, Admin, etc.)',
            is_active BOOLEAN DEFAULT TRUE COMMENT 'Whether to send emails to this recipient',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_vendor_email (vendor_name, email),
            INDEX idx_vendor (vendor_name),
            INDEX idx_active (is_active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Email recipients for data processing notifications';
        """
        
                cursor.execute(create_table_query)
                conn.commit()
                
                logger.info("✅ Email recipients table created/verified successfully")
                
                # Insert sample data if table is empty
                cursor.execute("SELECT COUNT(*) FROM email_recipients")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    sample_data = """
                    INSERT INTO email_recipients (vendor_name, email, recipient_name, role) VALUES
                    ('Bank_Receipt', 'finance.manager@company.com', 'Finance Manager', 'Manager'),
                    ('Bank_Receipt', 'accounts@company.com', 'Accounts Team', 'Analyst'),
                    ('TRM_Data', 'operations@company.com', 'Operations Team', 'Manager'),
                    ('TRM_Data', 'data.analyst@company.com', 'Data Analyst', 'Analyst'),
                    ('MPR_UPI', 'payments@company.com', 'Payments Team', 'Manager'),
                    ('MPR_Card', 'payments@company.com', 'Payments Team', 'Manager'),
                    ('POS_Data', 'pos.manager@company.com', 'POS Manager', 'Manager'),
                    ('POS_Data', 'store.ops@company.com', 'Store Operations', 'Analyst'),
                    ('ALL', 'admin@company.com', 'System Admin', 'Admin')
                    """
                    cursor.execute(sample_data)
                    conn.commit()
                    logger.info("✅ Inserted sample email recipients")
                
                return True
        
    except mysql.connector.Error as e:
        logger.error(f"❌ Error creating email_recipients table: {e}")
        return False

    def generate_upload_summary_html(self, summary: Dict) -> str:
        """
        Generate HTML email for upload summary
        
        Args:
            summary: Upload summary dictionary
        
        Returns:
            HTML string
        """
        # Calculate duration
        if summary.get('start_time') and summary.get('end_time'):
            if isinstance(summary['start_time'], str):
                start_time = datetime.fromisoformat(summary['start_time'])
                end_time = datetime.fromisoformat(summary['end_time'])
            else:
                start_time = summary['start_time']
                end_time = summary['end_time']
            duration = (end_time - start_time).total_seconds()
            duration_str = f"{duration:.1f}s"
        else:
            duration_str = "N/A"
            start_time = datetime.now()
        
        # Determine overall status
        if summary['failed_uploads'] > 0:
            status_color = '#dc3545'
            status_icon = '❌'
            status_text = 'COMPLETED WITH ERRORS'
        elif summary['successful_uploads'] > 0:
            status_color = '#28a745'
            status_icon = '✅'
            status_text = 'COMPLETED SUCCESSFULLY'
        else:
            status_color = '#ffc107'
            status_icon = '⚠️'
            status_text = 'NO FILES PROCESSED'
        
        # Build uploads table
        uploads_rows = ""
        for upload in summary.get('uploads', []):
            if upload['success']:
                if "already uploaded" in upload['message'].lower() or "skipping" in upload['message'].lower():
                    row_color = '#fff3cd'
                    status_badge = '<span style="background-color: #ffc107; color: #000; padding: 2px 8px; border-radius: 3px; font-size: 12px;">⏭️ SKIPPED</span>'
                else:
                    row_color = '#d4edda'
                    status_badge = '<span style="background-color: #28a745; color: #fff; padding: 2px 8px; border-radius: 3px; font-size: 12px;">✅ SUCCESS</span>'
            else:
                row_color = '#f8d7da'
                status_badge = '<span style="background-color: #dc3545; color: #fff; padding: 2px 8px; border-radius: 3px; font-size: 12px;">❌ FAILED</span>'
            
            uploads_rows += f"""
            <tr style="background-color: {row_color};">
                <td style="padding: 12px; border: 1px solid #ddd;">{upload['source_name']}</td>
                <td style="padding: 12px; border: 1px solid #ddd; font-family: monospace; font-size: 11px;">{upload['file_name']}</td>
                <td style="padding: 12px; border: 1px solid #ddd; text-align: center;">{status_badge}</td>
                <td style="padding: 12px; border: 1px solid #ddd; text-align: right;">{upload['records_uploaded']:,}</td>
                <td style="padding: 12px; border: 1px solid #ddd; text-align: right;">{upload['duration_seconds']}s</td>
            </tr>
            """
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 8px 8px 0 0;
            text-align: center;
        }}
        .status-banner {{
            background-color: {status_color};
            color: white;
            padding: 15px;
            text-align: center;
            font-size: 18px;
            font-weight: bold;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            padding: 20px;
            background-color: #f8f9fa;
        }}
        .summary-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            color: #666;
            font-size: 14px;
            text-transform: uppercase;
        }}
        .summary-card .value {{
            font-size: 32px;
            font-weight: bold;
            color: #333;
        }}
        .uploads-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: white;
        }}
        .uploads-table th {{
            background-color: #667eea;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}
        .footer {{
            margin-top: 30px;
            padding: 20px;
            background-color: #f8f9fa;
            border-radius: 8px;
            text-align: center;
            font-size: 12px;
            color: #666;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📤 Data Upload Summary</h1>
        <p style="margin: 5px 0; opacity: 0.9;">Automated Upload Process Report</p>
        <p style="margin: 5px 0; font-size: 14px; opacity: 0.8;">{start_time.strftime('%B %d, %Y at %I:%M %p')}</p>
    </div>
    
    <div class="status-banner">
        {status_icon} {status_text}
    </div>
    
    <div class="summary-grid">
        <div class="summary-card">
            <h3>Total Files</h3>
            <div class="value">{summary['total_files']}</div>
        </div>
        <div class="summary-card">
            <h3>Successful</h3>
            <div class="value" style="color: #28a745;">{summary['successful_uploads']}</div>
        </div>
        <div class="summary-card">
            <h3>Failed</h3>
            <div class="value" style="color: #dc3545;">{summary['failed_uploads']}</div>
        </div>
        <div class="summary-card">
            <h3>Skipped</h3>
            <div class="value" style="color: #ffc107;">{summary['skipped_uploads']}</div>
        </div>
        <div class="summary-card">
            <h3>Records Uploaded</h3>
            <div class="value" style="color: #667eea;">{summary['total_records_uploaded']:,}</div>
        </div>
        <div class="summary-card">
            <h3>Duration</h3>
            <div class="value" style="font-size: 24px;">{duration_str}</div>
        </div>
    </div>
    
    <div style="padding: 20px;">
        <h2 style="color: #667eea; border-bottom: 2px solid #667eea; padding-bottom: 10px;">📋 Upload Details</h2>
        
        <table class="uploads-table">
            <thead>
                <tr>
                    <th>Data Source</th>
                    <th>File Name</th>
                    <th>Status</th>
                    <th style="text-align: right;">Records</th>
                    <th style="text-align: right;">Duration</th>
                </tr>
            </thead>
            <tbody>
                {uploads_rows}
            </tbody>
        </table>
    </div>
    
    <div class="footer">
        <p><strong>Smart Uploader System</strong> - Devyani Data Team</p>
        <p>Generated on {datetime.now().strftime('%B %d, %Y at %I:%M:%S %p')}</p>
        <p style="margin-top: 10px; font-size: 11px;">This is an automated email. Please do not reply.</p>
    </div>
</body>
</html>
        """
        
        return html
    
    def send_upload_summary(self, summary: Dict, to_emails: List[str] = None) -> bool:
        """
        Send upload summary email
        
        Args:
            summary: Upload summary dictionary from upload_orchestrator
            to_emails: List of recipient emails (optional, will fetch from DB if not provided)
        
        Returns:
            True if email sent successfully
        """
        if not self.enabled:
            logger.warning("⚠️ Email is disabled, skipping upload summary email")
            return False
        
        # Get recipients if not provided
        if not to_emails:
            recipients = self.get_recipients_by_process('Upload', 'ALL', 'success' if summary['failed_uploads'] == 0 else 'failure')
            to_emails = [r['email'] for r in recipients]
        
        if not to_emails:
            logger.warning("⚠️ No recipients configured for upload summaries")
            return False
        
        # Determine subject
        if summary['failed_uploads'] > 0:
            subject = f"⚠️ Data Upload Completed with {summary['failed_uploads']} Error(s)"
        elif summary['successful_uploads'] > 0:
            subject = f"✅ Data Upload Successful - {summary['total_records_uploaded']:,} Records"
        else:
            subject = "📤 Data Upload Summary - No Files Processed"
        
        # Generate HTML content
        html_body = self.generate_upload_summary_html(summary)
        
        # Send email
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{self.from_name} <{self.from_email}>"
            msg['To'] = ', '.join(to_emails)
            
            msg.attach(MIMEText(html_body, 'html'))
            
            logger.info(f"📧 Sending upload summary to: {', '.join(to_emails)}")
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"✅ Upload summary email sent successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send upload summary email: {str(e)}")
            return False


if __name__ == "__main__":
    # Test/setup script
    logging.basicConfig(level=logging.INFO)
    
    print("🔧 Setting up Email Service...")
    print("=" * 60)
    
    # Create tables
    create_email_recipients_table()
    
    # Test email service
    email_service = EmailService()
    
    if email_service.enabled:
        print("\n✅ Email service is ENABLED")
        print(f"   SMTP Host: {email_service.smtp_host}")
        print(f"   SMTP Port: {email_service.smtp_port}")
        print(f"   From: {email_service.from_name} <{email_service.from_email}>")
    else:
        print("\n⚠️ Email service is DISABLED")
        print("   To enable, add to .env file:")
        print("   EMAIL_ENABLED=true")
        print("   SMTP_HOST=smtp.gmail.com")
        print("   SMTP_PORT=587")
        print("   SMTP_USER=your-email@gmail.com")
        print("   SMTP_PASSWORD=your-app-password")
        print("   FROM_EMAIL=your-email@gmail.com")
        print("   FROM_NAME=Smart Uploader System")
    
    # Show process-based recipients
    print("\n📧 Process-Based Email Recipients:")
    print("=" * 60)
    
    processes = ['Upload', 'Error_Alert', 'Validation', 'Daily_Summary', 'Reconciliation', 'Data_Quality']
    vendors = ['ALL', 'POS_Data', 'Bank_Receipt', 'TRM_Data', 'MPR_Data']
    
    for process in processes:
        print(f"\n📋 {process} Process:")
        for vendor in vendors:
            recipients = email_service.get_recipients_by_process(process, vendor, 'both')
            if recipients:
                print(f"  {vendor}:")
                for r in recipients:
                    notif_type = r.get('notification_type', 'both')
                    print(f"    • {r['recipient_name']} ({r['role']}) - {r['email']} [{notif_type}]")
    
    # Show legacy vendor-based recipients (if any exist)
    print("\n📧 Legacy Vendor-Based Recipients (from email_recipients table):")
    print("=" * 60)
    recipients = email_service.get_all_recipients()
    
    if recipients:
        current_vendor = None
        for r in recipients:
            if r['vendor_name'] != current_vendor:
                current_vendor = r['vendor_name']
                print(f"\n  {current_vendor}:")
            print(f"    • {r['recipient_name']} ({r['role']}) - {r['email']}")
    else:
        print("  No legacy recipients configured")
    
    print("\n" + "=" * 60)
    print("✅ Setup complete!")
    print("\n💡 Tip: Use process-based notifications (email_process_notifications table)")
    print("   for more flexible control over who receives which notifications.")
    print("=" * 60)
