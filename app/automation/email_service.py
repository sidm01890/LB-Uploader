"""
Email Processing Service for Smart Uploader Platform
Handles automated email monitoring, attachment download, and processing
"""

import asyncio
import imaplib
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import schedule
import time
from dataclasses import dataclass
from enum import Enum
import json
import os
import re
import tempfile
import zipfile

logger = logging.getLogger(__name__)

class EmailProvider(Enum):
    GMAIL = "gmail"
    OUTLOOK = "outlook"
    EXCHANGE = "exchange"
    IMAP_GENERIC = "imap"

@dataclass
class EmailConfig:
    provider: EmailProvider
    imap_host: str
    imap_port: int
    smtp_host: str
    smtp_port: int
    username: str
    password: str
    use_ssl: bool = True
    allowed_senders: List[str] = None
    download_path: str = "/tmp/email_attachments"
    processed_folder: str = "Processed"
    failed_folder: str = "Failed"
    max_attachment_size_mb: int = 100

@dataclass
class EmailMessage:
    message_id: str
    sender: str
    subject: str
    received_date: datetime
    body: str
    attachments: List[Dict[str, Any]]
    processed: bool = False

@dataclass
class AttachmentInfo:
    filename: str
    content_type: str
    size: int
    local_path: str
    is_data_file: bool

class EmailProcessingService:
    """
    Enterprise email processing service for automated attachment handling
    """
    
    def __init__(self, config: EmailConfig):
        self.config = config
        self.download_path = Path(config.download_path)
        self.download_path.mkdir(parents=True, exist_ok=True)
        
        # Create processing directories
        self.processed_path = self.download_path / "processed"
        self.failed_path = self.download_path / "failed"
        self.temp_path = self.download_path / "temp"
        
        for path in [self.processed_path, self.failed_path, self.temp_path]:
            path.mkdir(parents=True, exist_ok=True)
        
        self.processed_messages = []
        self.is_monitoring = False
        
        # File type patterns for data files
        self.data_file_patterns = [
            r'.*\.(csv|xlsx|xls|json|xml|txt|tsv)$',
            r'.*data.*\.(zip|rar|7z)$',
            r'.*report.*\.(csv|xlsx)$',
            r'.*export.*\.(csv|xlsx|json)$'
        ]
        
        logger.info(f"Email processing service initialized for {config.username}")
    
    async def start_email_monitoring(self, check_interval_minutes: int = 15):
        """
        Start automated email monitoring for new messages with attachments
        
        Args:
            check_interval_minutes: How often to check for new emails
        """
        
        logger.info(f"Starting email monitoring (checking every {check_interval_minutes} minutes)")
        
        # Schedule email checks
        schedule.every(check_interval_minutes).minutes.do(self._execute_email_check)
        
        self.is_monitoring = True
        
        # Run monitoring loop
        while self.is_monitoring:
            schedule.run_pending()
            await asyncio.sleep(60)  # Check scheduler every minute
    
    def _execute_email_check(self):
        """Execute email check (called by scheduler)"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.process_new_emails())
            loop.close()
            
            logger.info(f"Email check completed: {result['summary']}")
            return result
            
        except Exception as e:
            logger.error(f"Email check failed: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    async def process_new_emails(self, since_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Process new emails and download attachments
        
        Args:
            since_date: Only process emails since this date (default: last 24 hours)
        """
        
        if since_date is None:
            since_date = datetime.now() - timedelta(hours=24)
        
        start_time = datetime.now()
        processed_messages = []
        
        try:
            logger.info(f"Processing emails since {since_date}")
            
            # Connect to email server
            imap_connection = await self._connect_imap()
            
            # Search for new emails
            new_messages = await self._search_emails(imap_connection, since_date)
            
            logger.info(f"Found {len(new_messages)} new messages to process")
            
            # Process each message
            for message in new_messages:
                try:
                    processed_msg = await self._process_single_message(imap_connection, message)
                    processed_messages.append(processed_msg)
                    
                except Exception as e:
                    logger.error(f"Failed to process message {message.get('id', 'unknown')}: {str(e)}")
                    continue
            
            # Close IMAP connection
            imap_connection.close()
            imap_connection.logout()
            
            # Calculate results
            successful_messages = [m for m in processed_messages if m.processed]
            failed_messages = [m for m in processed_messages if not m.processed]
            
            total_attachments = sum(len(m.attachments) for m in processed_messages)
            data_file_attachments = sum(1 for m in processed_messages for att in m.attachments if att.get('is_data_file', False))
            
            self.processed_messages.extend(processed_messages)
            
            return {
                "success": True,
                "summary": {
                    "total_messages": len(new_messages),
                    "processed_successfully": len(successful_messages),
                    "processing_failed": len(failed_messages),
                    "total_attachments": total_attachments,
                    "data_file_attachments": data_file_attachments,
                    "processing_time_seconds": (datetime.now() - start_time).total_seconds()
                },
                "processed_messages": [
                    {
                        "sender": m.sender,
                        "subject": m.subject,
                        "attachments": len(m.attachments),
                        "data_files": sum(1 for att in m.attachments if att.get('is_data_file', False))
                    }
                    for m in successful_messages
                ],
                "downloaded_files": [
                    att['local_path'] for m in successful_messages 
                    for att in m.attachments if att.get('is_data_file', False)
                ]
            }
            
        except Exception as e:
            logger.error(f"Email processing failed: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "summary": {
                    "total_messages": 0,
                    "processed_successfully": 0,
                    "processing_failed": 1,
                    "processing_time_seconds": (datetime.now() - start_time).total_seconds()
                }
            }
    
    async def _connect_imap(self):
        """Establish IMAP connection to email server"""
        
        try:
            if self.config.use_ssl:
                imap = imaplib.IMAP4_SSL(self.config.imap_host, self.config.imap_port)
            else:
                imap = imaplib.IMAP4(self.config.imap_host, self.config.imap_port)
            
            imap.login(self.config.username, self.config.password)
            imap.select('INBOX')
            
            logger.info(f"IMAP connection established to {self.config.imap_host}")
            return imap
            
        except Exception as e:
            logger.error(f"IMAP connection failed: {str(e)}")
            raise
    
    async def _search_emails(self, imap_connection, since_date: datetime) -> List[Dict[str, Any]]:
        """Search for emails matching criteria"""
        
        try:
            # Build search criteria
            search_criteria = []
            
            # Date filter
            date_str = since_date.strftime("%d-%b-%Y")
            search_criteria.append(f'SINCE "{date_str}"')
            
            # Sender filter
            if self.config.allowed_senders:
                sender_criteria = []
                for sender in self.config.allowed_senders:
                    sender_criteria.append(f'FROM "{sender}"')
                search_criteria.append(f'({" OR ".join(sender_criteria)})')
            
            # Must have attachments
            search_criteria.append('BODY "attachment"')  # Basic attachment detection
            
            # Combine criteria
            search_query = " ".join(search_criteria) if search_criteria else "ALL"
            
            logger.info(f"Searching emails with criteria: {search_query}")
            
            # Execute search
            status, message_ids = imap_connection.search(None, search_query)
            
            if status != 'OK':
                raise Exception(f"Email search failed: {status}")
            
            # Parse message IDs
            if message_ids[0]:
                ids = message_ids[0].split()
                return [{"id": msg_id.decode()} for msg_id in ids]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Email search failed: {str(e)}")
            raise
    
    async def _process_single_message(self, imap_connection, message_info: Dict[str, Any]) -> EmailMessage:
        """Process a single email message"""
        
        try:
            msg_id = message_info["id"]
            
            # Fetch message
            status, msg_data = imap_connection.fetch(msg_id, '(RFC822)')
            
            if status != 'OK':
                raise Exception(f"Failed to fetch message {msg_id}")
            
            # Parse email
            email_message = email.message_from_bytes(msg_data[0][1])
            
            # Extract message information
            sender = email_message.get('From', '')
            subject = email_message.get('Subject', '')
            date_str = email_message.get('Date', '')
            
            # Parse date
            try:
                received_date = email.utils.parsedate_to_datetime(date_str)
            except:
                received_date = datetime.now()
            
            # Check if sender is allowed
            if self.config.allowed_senders and not any(allowed in sender for allowed in self.config.allowed_senders):
                logger.info(f"Skipping message from unauthorized sender: {sender}")
                return EmailMessage(
                    message_id=msg_id,
                    sender=sender,
                    subject=subject,
                    received_date=received_date,
                    body="",
                    attachments=[],
                    processed=False
                )
            
            # Extract body
            body = self._extract_email_body(email_message)
            
            # Process attachments
            attachments = await self._process_attachments(email_message, msg_id)
            
            # Move processed message to appropriate folder
            if attachments and any(att.get('is_data_file', False) for att in attachments):
                await self._move_message_to_folder(imap_connection, msg_id, self.config.processed_folder)
                processed_success = True
            else:
                processed_success = False
            
            logger.info(f"Processed message from {sender}: {len(attachments)} attachments")
            
            return EmailMessage(
                message_id=msg_id,
                sender=sender,
                subject=subject,
                received_date=received_date,
                body=body,
                attachments=attachments,
                processed=processed_success
            )
            
        except Exception as e:
            logger.error(f"Failed to process message {message_info.get('id', 'unknown')}: {str(e)}")
            
            # Try to move failed message to failed folder
            try:
                await self._move_message_to_folder(imap_connection, msg_id, self.config.failed_folder)
            except:
                pass
            
            raise
    
    def _extract_email_body(self, email_message) -> str:
        """Extract text body from email message"""
        
        try:
            if email_message.is_multipart():
                for part in email_message.walk():
                    if part.get_content_type() == "text/plain":
                        return part.get_payload(decode=True).decode('utf-8', errors='ignore')
            else:
                if email_message.get_content_type() == "text/plain":
                    return email_message.get_payload(decode=True).decode('utf-8', errors='ignore')
            
            return ""
            
        except Exception as e:
            logger.warning(f"Failed to extract email body: {str(e)}")
            return ""
    
    async def _process_attachments(self, email_message, msg_id: str) -> List[Dict[str, Any]]:
        """Process and download email attachments"""
        
        attachments = []
        
        try:
            for part in email_message.walk():
                # Check if this part is an attachment
                if part.get_content_disposition() == 'attachment':
                    filename = part.get_filename()
                    
                    if filename:
                        # Decode filename if needed
                        if filename.startswith('=?'):
                            decoded_header = email.header.decode_header(filename)
                            filename = decoded_header[0][0]
                            if isinstance(filename, bytes):
                                filename = filename.decode('utf-8', errors='ignore')
                        
                        # Check file size
                        content = part.get_payload(decode=True)
                        size_mb = len(content) / (1024 * 1024)
                        
                        if size_mb > self.config.max_attachment_size_mb:
                            logger.warning(f"Attachment {filename} too large ({size_mb:.2f}MB), skipping")
                            continue
                        
                        # Determine if this is a data file
                        is_data_file = self._is_data_file(filename)
                        
                        # Save attachment
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        safe_filename = f"{timestamp}_{self._sanitize_filename(filename)}"
                        
                        if is_data_file:
                            local_path = self.download_path / safe_filename
                        else:
                            local_path = self.temp_path / safe_filename
                        
                        # Write file
                        with open(local_path, 'wb') as f:
                            f.write(content)
                        
                        # Handle compressed files
                        extracted_files = []
                        if filename.lower().endswith(('.zip', '.rar', '.7z')):
                            extracted_files = await self._extract_compressed_file(local_path, filename)
                        
                        attachment_info = {
                            "filename": filename,
                            "safe_filename": safe_filename,
                            "content_type": part.get_content_type(),
                            "size": len(content),
                            "local_path": str(local_path),
                            "is_data_file": is_data_file,
                            "extracted_files": extracted_files
                        }
                        
                        attachments.append(attachment_info)
                        
                        logger.info(f"Downloaded attachment: {filename} ({size_mb:.2f}MB)")
            
            return attachments
            
        except Exception as e:
            logger.error(f"Failed to process attachments for message {msg_id}: {str(e)}")
            return []
    
    def _is_data_file(self, filename: str) -> bool:
        """Check if filename matches data file patterns"""
        
        filename_lower = filename.lower()
        
        for pattern in self.data_file_patterns:
            if re.match(pattern, filename_lower):
                return True
        
        return False
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe storage"""
        
        # Remove or replace unsafe characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'[^\w\s.-]', '', filename)
        filename = filename.strip()
        
        # Limit length
        if len(filename) > 100:
            name, ext = os.path.splitext(filename)
            filename = name[:95] + ext
        
        return filename
    
    async def _extract_compressed_file(self, file_path: Path, original_filename: str) -> List[str]:
        """Extract compressed files and return list of data files"""
        
        extracted_files = []
        
        try:
            if original_filename.lower().endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    for member in zip_ref.namelist():
                        if self._is_data_file(member):
                            # Extract to temp directory
                            extract_path = self.temp_path / f"extracted_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            extract_path.mkdir(exist_ok=True)
                            
                            zip_ref.extract(member, extract_path)
                            extracted_file_path = extract_path / member
                            
                            # Move to main download directory
                            final_path = self.download_path / self._sanitize_filename(member)
                            extracted_file_path.rename(final_path)
                            
                            extracted_files.append(str(final_path))
                            logger.info(f"Extracted data file: {member}")
            
            return extracted_files
            
        except Exception as e:
            logger.error(f"Failed to extract {original_filename}: {str(e)}")
            return []
    
    async def _move_message_to_folder(self, imap_connection, msg_id: str, folder_name: str):
        """Move processed message to specified folder"""
        
        try:
            # Create folder if it doesn't exist
            try:
                imap_connection.create(folder_name)
            except:
                pass  # Folder might already exist
            
            # Move message
            imap_connection.move(msg_id, folder_name)
            imap_connection.expunge()
            
            logger.debug(f"Moved message {msg_id} to {folder_name}")
            
        except Exception as e:
            logger.warning(f"Failed to move message {msg_id} to {folder_name}: {str(e)}")
    
    def stop_monitoring(self):
        """Stop email monitoring"""
        
        self.is_monitoring = False
        schedule.clear()
        logger.info("Email monitoring stopped")
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get email processing statistics"""
        
        if not self.processed_messages:
            return {"total_messages": 0, "total_attachments": 0, "data_files": 0}
        
        total_messages = len(self.processed_messages)
        total_attachments = sum(len(m.attachments) for m in self.processed_messages)
        data_files = sum(1 for m in self.processed_messages for att in m.attachments if att.get('is_data_file', False))
        
        # Recent activity (last 10 messages)
        recent_messages = self.processed_messages[-10:]
        
        return {
            "total_messages_processed": total_messages,
            "total_attachments_downloaded": total_attachments,
            "data_files_extracted": data_files,
            "success_rate": sum(1 for m in self.processed_messages if m.processed) / total_messages,
            "recent_activity": [
                {
                    "sender": m.sender,
                    "subject": m.subject,
                    "received_date": m.received_date.isoformat(),
                    "attachments": len(m.attachments),
                    "processed": m.processed
                }
                for m in recent_messages
            ]
        }
    
    async def manual_email_check(self, sender_filter: Optional[str] = None) -> Dict[str, Any]:
        """Manually trigger email check with optional sender filter"""
        
        logger.info("Manual email check triggered")
        
        # Temporarily modify allowed senders if filter provided
        original_senders = self.config.allowed_senders
        if sender_filter:
            self.config.allowed_senders = [sender_filter]
        
        try:
            result = await self.process_new_emails()
            return result
        finally:
            # Restore original sender list
            self.config.allowed_senders = original_senders


# Configuration factory for different email providers
class EmailConfigFactory:
    """Factory for creating email configurations for common providers"""
    
    @staticmethod
    def create_gmail_config(
        username: str,
        password: str,  # App password for Gmail
        allowed_senders: List[str] = None,
        download_path: str = "/tmp/email_attachments"
    ) -> EmailConfig:
        """Create configuration for Gmail"""
        
        return EmailConfig(
            provider=EmailProvider.GMAIL,
            imap_host="imap.gmail.com",
            imap_port=993,
            smtp_host="smtp.gmail.com",
            smtp_port=587,
            username=username,
            password=password,
            use_ssl=True,
            allowed_senders=allowed_senders or [],
            download_path=download_path,
            processed_folder="Processed",
            failed_folder="Failed",
            max_attachment_size_mb=100
        )
    
    @staticmethod
    def create_outlook_config(
        username: str,
        password: str,
        allowed_senders: List[str] = None,
        download_path: str = "/tmp/email_attachments"
    ) -> EmailConfig:
        """Create configuration for Outlook.com"""
        
        return EmailConfig(
            provider=EmailProvider.OUTLOOK,
            imap_host="outlook.office365.com",
            imap_port=993,
            smtp_host="smtp-mail.outlook.com",
            smtp_port=587,
            username=username,
            password=password,
            use_ssl=True,
            allowed_senders=allowed_senders or [],
            download_path=download_path,
            processed_folder="Processed",
            failed_folder="Failed",
            max_attachment_size_mb=100
        )
    
    @staticmethod
    def create_exchange_config(
        exchange_server: str,
        username: str,
        password: str,
        allowed_senders: List[str] = None,
        download_path: str = "/tmp/email_attachments"
    ) -> EmailConfig:
        """Create configuration for Exchange Server"""
        
        return EmailConfig(
            provider=EmailProvider.EXCHANGE,
            imap_host=exchange_server,
            imap_port=993,
            smtp_host=exchange_server,
            smtp_port=587,
            username=username,
            password=password,
            use_ssl=True,
            allowed_senders=allowed_senders or [],
            download_path=download_path,
            processed_folder="Processed",
            failed_folder="Failed",
            max_attachment_size_mb=100
        )