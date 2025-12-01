"""
Configuration Factory for Smart Uploader Automation
Creates automation configurations for different deployment scenarios
"""

from typing import List, Dict, Any
from datetime import time
from app.automation.sftp_service import SFTPConfig, SFTPConfigFactory
from app.automation.email_service import EmailConfig, EmailConfigFactory
from app.automation.notification_service import NotificationRecipient, NotificationType, NotificationLevel
from app.automation.orchestrator import AutomationConfig


class AutomationConfigFactory:
    """Factory for creating automation configurations"""
    
    @staticmethod
    def create_development_config() -> AutomationConfig:
        """Create configuration for development environment"""
        
        # SFTP Configuration - Test server
        sftp_configs = [
            SFTPConfigFactory.create_sftp_config(
                host="test-ftp.example.com",
                port=22,
                username="dev_user",
                password="dev_password",
                remote_directories=["/incoming/data"],
                file_patterns=["*.csv", "*.xlsx"],
                download_path="/tmp/smart_uploader_dev/sftp",
                connection_type="sftp"
            )
        ]
        
        # Email Configuration - Development Gmail
        email_config = EmailConfigFactory.create_gmail_config(
            username="dev.smartuploader@gmail.com",
            password="app_password_here",  # Use app password
            allowed_senders=["test@example.com", "dev@company.com"],
            download_path="/tmp/smart_uploader_dev/email"
        )
        
        # Notification Configuration - Local SMTP
        notification_config = {
            "smtp_host": "localhost",
            "smtp_port": 1025,  # MailHog for development
            "username": "dev@smartuploader.local",
            "password": "dev_password",
            "sender_name": "Smart Uploader Dev",
            "sender_email": "dev@smartuploader.local"
        }
        
        # Recipients for development
        recipients = [
            NotificationRecipient(
                name="Developer",
                email="developer@company.com",
                notification_types=[
                    NotificationType.DAILY_REPORT,
                    NotificationType.ERROR_ALERT,
                    NotificationType.SUCCESS_NOTIFICATION
                ],
                level_threshold=NotificationLevel.INFO
            )
        ]
        
        return AutomationConfig(
            sftp_configs=sftp_configs,
            sftp_schedule="daily",
            sftp_time="09:00",  # 9 AM for development
            email_config=email_config,
            email_check_interval=30,  # Check every 30 minutes in dev
            notification_config=notification_config,
            recipients=recipients,
            auto_mapping=True,
            auto_upload=True,
            validation_enabled=True,
            processed_files_retention_days=7,   # Shorter retention in dev
            failed_files_retention_days=14,
            max_concurrent_files=2,  # Lower concurrency in dev
            batch_size=500
        )
    
    @staticmethod
    def create_production_config() -> AutomationConfig:
        """Create configuration for production environment"""
        
        # SFTP Configuration - Multiple production sources
        sftp_configs = [
            # Primary data source
            SFTPConfigFactory.create_sftp_config(
                host="data-source-1.company.com",
                port=22,
                username="smartuploader_prod",
                password="${SFTP_PASSWORD_1}",  # Environment variable
                remote_directories=["/exports/daily", "/exports/incremental"],
                file_patterns=["sales_*.csv", "inventory_*.xlsx", "customers_*.json"],
                download_path="/data/smart_uploader/sftp/source1",
                connection_type="sftp",
                max_connections=5,
                connection_timeout=30
            ),
            
            # Secondary data source
            SFTPConfigFactory.create_sftp_config(
                host="backup-data-source.company.com",
                port=2222,
                username="backup_user",
                password="${SFTP_PASSWORD_2}",
                remote_directories=["/backup/daily"],
                file_patterns=["backup_*.csv", "archive_*.zip"],
                download_path="/data/smart_uploader/sftp/backup",
                connection_type="sftp",
                max_connections=3,
                connection_timeout=60
            ),
            
            # Partner FTPS source
            SFTPConfigFactory.create_ftps_config(
                host="partners.external.com",
                port=21,
                username="partner_integration",
                password="${FTPS_PASSWORD}",
                remote_directories=["/shared/data"],
                file_patterns=["partner_data_*.csv"],
                download_path="/data/smart_uploader/ftps/partners",
                use_passive=True
            )
        ]
        
        # Email Configuration - Production Exchange
        email_config = EmailConfigFactory.create_exchange_config(
            exchange_server="mail.company.com",
            username="smartuploader@company.com",
            password="${EMAIL_PASSWORD}",
            allowed_senders=[
                "reports@vendor1.com",
                "data@vendor2.com",
                "exports@partner.com",
                "noreply@datasource.com"
            ],
            download_path="/data/smart_uploader/email"
        )
        
        # Notification Configuration - Production SMTP
        notification_config = {
            "smtp_host": "smtp.company.com",
            "smtp_port": 587,
            "username": "smartuploader@company.com",
            "password": "${SMTP_PASSWORD}",
            "sender_name": "Smart Uploader Platform",
            "sender_email": "smartuploader@company.com"
        }
        
        # Production Recipients with different notification levels
        recipients = [
            # Operations Team - All notifications
            NotificationRecipient(
                name="Operations Team",
                email="ops@company.com",
                notification_types=[
                    NotificationType.DAILY_REPORT,
                    NotificationType.ERROR_ALERT,
                    NotificationType.SUCCESS_NOTIFICATION,
                    NotificationType.SFTP_STATUS,
                    NotificationType.MAPPING_ISSUES
                ],
                level_threshold=NotificationLevel.INFO
            ),
            
            # Data Team - Data-related notifications
            NotificationRecipient(
                name="Data Engineering Team",
                email="data-eng@company.com",
                notification_types=[
                    NotificationType.DAILY_REPORT,
                    NotificationType.MAPPING_ISSUES,
                    NotificationType.UPLOAD_COMPLETE,
                    NotificationType.ERROR_ALERT
                ],
                level_threshold=NotificationLevel.WARNING
            ),
            
            # Management - Critical issues and daily summaries
            NotificationRecipient(
                name="IT Management",
                email="it-management@company.com",
                notification_types=[
                    NotificationType.DAILY_REPORT,
                    NotificationType.ERROR_ALERT,
                    NotificationType.SYSTEM_HEALTH
                ],
                level_threshold=NotificationLevel.ERROR
            ),
            
            # On-call Engineer - Critical alerts only
            NotificationRecipient(
                name="On-Call Engineer",
                email="oncall@company.com",
                notification_types=[
                    NotificationType.ERROR_ALERT,
                    NotificationType.SYSTEM_HEALTH
                ],
                level_threshold=NotificationLevel.CRITICAL
            )
        ]
        
        return AutomationConfig(
            sftp_configs=sftp_configs,
            sftp_schedule="daily",
            sftp_time="02:00",  # 2 AM production run
            email_config=email_config,
            email_check_interval=15,  # Check every 15 minutes
            notification_config=notification_config,
            recipients=recipients,
            auto_mapping=True,
            auto_upload=True,
            validation_enabled=True,
            processed_files_retention_days=30,   # 30 days retention
            failed_files_retention_days=90,      # 90 days for failed files
            max_concurrent_files=10,  # Higher concurrency in production
            batch_size=2000           # Larger batches for production
        )
    
    @staticmethod
    def create_enterprise_config() -> AutomationConfig:
        """Create configuration for large enterprise environment"""
        
        # Enterprise SFTP Configuration - Multiple regions and sources
        sftp_configs = [
            # North America data center
            SFTPConfigFactory.create_sftp_config(
                host="na-data-hub.enterprise.com",
                port=22,
                username="smartuploader_na",
                password="${SFTP_PASSWORD_NA}",
                remote_directories=[
                    "/na/sales/daily",
                    "/na/inventory/hourly", 
                    "/na/customers/updates",
                    "/na/transactions/realtime"
                ],
                file_patterns=[
                    "sales_na_*.csv", 
                    "inventory_na_*.xlsx",
                    "customer_updates_*.json",
                    "txn_*.parquet"
                ],
                download_path="/enterprise/data/na/sftp",
                connection_type="sftp",
                max_connections=10,
                connection_timeout=45,
                retry_attempts=3
            ),
            
            # Europe data center
            SFTPConfigFactory.create_sftp_config(
                host="eu-data-hub.enterprise.com",
                port=22,
                username="smartuploader_eu",
                password="${SFTP_PASSWORD_EU}",
                remote_directories=[
                    "/eu/sales/daily",
                    "/eu/inventory/hourly",
                    "/eu/compliance/daily"
                ],
                file_patterns=[
                    "sales_eu_*.csv",
                    "inventory_eu_*.xlsx", 
                    "compliance_*.xml"
                ],
                download_path="/enterprise/data/eu/sftp",
                connection_type="sftp",
                max_connections=8,
                connection_timeout=45
            ),
            
            # Asia-Pacific data center
            SFTPConfigFactory.create_sftp_config(
                host="ap-data-hub.enterprise.com",
                port=22,
                username="smartuploader_ap",
                password="${SFTP_PASSWORD_AP}",
                remote_directories=["/ap/sales/daily", "/ap/inventory/hourly"],
                file_patterns=["sales_ap_*.csv", "inventory_ap_*.xlsx"],
                download_path="/enterprise/data/ap/sftp",
                connection_type="sftp",
                max_connections=6
            ),
            
            # Partner integrations via secure FTP
            SFTPConfigFactory.create_ftps_config(
                host="partners.secure.enterprise.com",
                port=990,
                username="partner_gateway",
                password="${PARTNERS_FTPS_PASSWORD}",
                remote_directories=["/partners/data", "/partners/reports"],
                file_patterns=["partner_*.csv", "external_report_*.xlsx"],
                download_path="/enterprise/data/partners/ftps",
                use_passive=True,
                connection_timeout=120
            )
        ]
        
        # Enterprise Email Configuration - Multiple mailboxes
        email_config = EmailConfigFactory.create_exchange_config(
            exchange_server="exchange.enterprise.com",
            username="data-ingestion@enterprise.com",
            password="${EXCHANGE_PASSWORD}",
            allowed_senders=[
                # Vendors
                "reports@vendor1.enterprise.com",
                "data@vendor2.enterprise.com", 
                "exports@vendor3.enterprise.com",
                
                # Partners
                "datafeeds@partner1.com",
                "reports@partner2.com",
                
                # Internal systems
                "automation@enterprise.com",
                "dataops@enterprise.com",
                "reporting@enterprise.com"
            ],
            download_path="/enterprise/data/email",
            max_attachment_size_mb=500  # Larger attachments for enterprise
        )
        
        # Enterprise Notification Configuration
        notification_config = {
            "smtp_host": "smtp.enterprise.com",
            "smtp_port": 587,
            "username": "smartuploader-platform@enterprise.com",
            "password": "${ENTERPRISE_SMTP_PASSWORD}",
            "sender_name": "Smart Uploader Enterprise Platform",
            "sender_email": "smartuploader-platform@enterprise.com"
        }
        
        # Enterprise Recipients - Multiple teams and escalation levels
        recipients = [
            # Level 1: Operations Team
            NotificationRecipient(
                name="L1 Operations Team",
                email="l1-ops@enterprise.com",
                notification_types=[
                    NotificationType.DAILY_REPORT,
                    NotificationType.SUCCESS_NOTIFICATION,
                    NotificationType.SFTP_STATUS,
                    NotificationType.UPLOAD_COMPLETE
                ],
                level_threshold=NotificationLevel.INFO
            ),
            
            # Level 2: Data Engineering Team
            NotificationRecipient(
                name="Data Engineering Team",
                email="data-engineering@enterprise.com",
                notification_types=[
                    NotificationType.DAILY_REPORT,
                    NotificationType.ERROR_ALERT,
                    NotificationType.MAPPING_ISSUES,
                    NotificationType.UPLOAD_COMPLETE,
                    NotificationType.SYSTEM_HEALTH
                ],
                level_threshold=NotificationLevel.WARNING
            ),
            
            # Level 3: Senior Engineering
            NotificationRecipient(
                name="Senior Data Engineering",
                email="senior-data-eng@enterprise.com",
                notification_types=[
                    NotificationType.ERROR_ALERT,
                    NotificationType.SYSTEM_HEALTH,
                    NotificationType.DAILY_REPORT
                ],
                level_threshold=NotificationLevel.ERROR
            ),
            
            # Platform Operations Manager
            NotificationRecipient(
                name="Platform Operations Manager",
                email="platform-ops-manager@enterprise.com",
                notification_types=[
                    NotificationType.DAILY_REPORT,
                    NotificationType.ERROR_ALERT,
                    NotificationType.SYSTEM_HEALTH
                ],
                level_threshold=NotificationLevel.ERROR
            ),
            
            # Executive Dashboard - Critical issues only
            NotificationRecipient(
                name="Executive Dashboard",
                email="exec-dashboard@enterprise.com",
                notification_types=[
                    NotificationType.SYSTEM_HEALTH,
                    NotificationType.ERROR_ALERT
                ],
                level_threshold=NotificationLevel.CRITICAL
            ),
            
            # 24/7 On-call Team - Critical alerts
            NotificationRecipient(
                name="24/7 On-Call Team",
                email="oncall-l3@enterprise.com",
                notification_types=[
                    NotificationType.ERROR_ALERT,
                    NotificationType.SYSTEM_HEALTH
                ],
                level_threshold=NotificationLevel.CRITICAL
            ),
            
            # Regional Teams
            NotificationRecipient(
                name="North America Data Team",
                email="na-data-team@enterprise.com",
                notification_types=[
                    NotificationType.DAILY_REPORT,
                    NotificationType.ERROR_ALERT,
                    NotificationType.MAPPING_ISSUES
                ],
                level_threshold=NotificationLevel.WARNING
            ),
            
            NotificationRecipient(
                name="Europe Data Team", 
                email="eu-data-team@enterprise.com",
                notification_types=[
                    NotificationType.DAILY_REPORT,
                    NotificationType.ERROR_ALERT,
                    NotificationType.MAPPING_ISSUES
                ],
                level_threshold=NotificationLevel.WARNING
            ),
            
            NotificationRecipient(
                name="Asia-Pacific Data Team",
                email="ap-data-team@enterprise.com", 
                notification_types=[
                    NotificationType.DAILY_REPORT,
                    NotificationType.ERROR_ALERT,
                    NotificationType.MAPPING_ISSUES
                ],
                level_threshold=NotificationLevel.WARNING
            )
        ]
        
        return AutomationConfig(
            sftp_configs=sftp_configs,
            sftp_schedule="daily",
            sftp_time="01:30",  # 1:30 AM for enterprise processing
            email_config=email_config,
            email_check_interval=10,  # Check every 10 minutes for enterprise
            notification_config=notification_config,
            recipients=recipients,
            auto_mapping=True,
            auto_upload=True,
            validation_enabled=True,
            processed_files_retention_days=60,   # 60 days for enterprise
            failed_files_retention_days=180,     # 6 months for failed files
            max_concurrent_files=20,  # High concurrency for enterprise
            batch_size=5000           # Large batches for enterprise scale
        )
    
    @staticmethod
    def create_custom_config(
        environment: str = "production",
        sftp_sources: List[Dict[str, Any]] = None,
        email_settings: Dict[str, Any] = None,
        notification_recipients: List[Dict[str, Any]] = None,
        processing_settings: Dict[str, Any] = None
    ) -> AutomationConfig:
        """Create custom configuration with specific parameters"""
        
        # Default to production template
        if environment == "enterprise":
            base_config = AutomationConfigFactory.create_enterprise_config()
        elif environment == "development":
            base_config = AutomationConfigFactory.create_development_config()
        else:
            base_config = AutomationConfigFactory.create_production_config()
        
        # Customize SFTP sources if provided
        if sftp_sources:
            sftp_configs = []
            for source in sftp_sources:
                if source.get("type") == "sftp":
                    config = SFTPConfigFactory.create_sftp_config(**source)
                elif source.get("type") == "ftps":
                    config = SFTPConfigFactory.create_ftps_config(**source)
                else:
                    config = SFTPConfigFactory.create_ftp_config(**source)
                
                sftp_configs.append(config)
            
            base_config.sftp_configs = sftp_configs
        
        # Customize email settings if provided
        if email_settings:
            if email_settings.get("provider") == "gmail":
                base_config.email_config = EmailConfigFactory.create_gmail_config(**email_settings)
            elif email_settings.get("provider") == "outlook":
                base_config.email_config = EmailConfigFactory.create_outlook_config(**email_settings)
            elif email_settings.get("provider") == "exchange":
                base_config.email_config = EmailConfigFactory.create_exchange_config(**email_settings)
        
        # Customize notification recipients if provided
        if notification_recipients:
            recipients = []
            for recipient_data in notification_recipients:
                recipient = NotificationRecipient(
                    name=recipient_data["name"],
                    email=recipient_data["email"],
                    notification_types=[
                        NotificationType(nt) for nt in recipient_data.get("notification_types", [])
                    ],
                    level_threshold=NotificationLevel(recipient_data.get("level_threshold", "info"))
                )
                recipients.append(recipient)
            
            base_config.recipients = recipients
        
        # Customize processing settings if provided
        if processing_settings:
            for key, value in processing_settings.items():
                if hasattr(base_config, key):
                    setattr(base_config, key, value)
        
        return base_config