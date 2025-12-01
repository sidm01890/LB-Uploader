#!/usr/bin/env python3
"""
Main Upload Pipeline
Created: November 12, 2025
Purpose: End-to-end data upload pipeline with email notifications

Process Flow:
1. Discover all data files from configured directories
2. Upload each file to respective database tables
3. Generate upload summary
4. Send email notification to configured recipients

Supports multiple environments: dev, stage, prod

Usage:
    python -m app.cli.upload_pipeline
    python -m app.cli.upload_pipeline --profile prod
    python -m app.cli.upload_pipeline --force --no-email
"""

import os
import sys
import logging
import argparse
from pathlib import Path

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from app.core.environment import load_environment_config, detect_environment
from app.core.properties_loader import get_active_profile
from app.core.config import config
from app.logging_config import setup_logging
from app.services.uploader.upload_orchestrator import UploadOrchestrator
from app.services.email_service import EmailService

# Parse profile argument if provided
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--profile', '--env', choices=['dev', 'stage', 'prod'], default=None, dest='profile')
args, _ = parser.parse_known_args()

# Set profile if provided (supports multiple env var names)
if args.profile:
    os.environ['SPRING_PROFILES_ACTIVE'] = args.profile
    os.environ['APP_PROFILE'] = args.profile
    os.environ['APP_ENV'] = args.profile

# Load properties files (application.properties + application-{profile}.properties)
load_environment_config()
env = detect_environment()
active_profile = get_active_profile()

# Setup logging with environment-aware level
setup_logging(config.log_level)
logger = logging.getLogger('upload_pipeline')
logger.info(f"Running with profile: {active_profile.upper()} (environment: {env.value.upper()})")


def run_upload_pipeline(
    base_path: str = None,
    force: bool = False,
    send_email: bool = True,
    email_recipients: list = None,
    save_summary: bool = True
) -> dict:
    """
    Run the complete upload pipeline
    
    Args:
        base_path: Base directory for data files
        force: Force upload even if files already uploaded
        send_email: Send email notification
        email_recipients: List of email addresses (None = use DB config)
        save_summary: Save summary to JSON file
    
    Returns:
        Upload summary dictionary
    """
    logger.info("="*80)
    logger.info("🚀 STARTING UPLOAD PIPELINE")
    logger.info("="*80)
    
    # Step 1: Upload all data files
    logger.info("\n📤 Step 1: Uploading data files...")
    orchestrator = UploadOrchestrator(base_path=base_path)
    summary = orchestrator.upload_all(force=force)
    
    # Step 2: Save summary if requested
    if save_summary:
        logger.info("\n💾 Step 2: Saving upload summary...")
        summary_path = orchestrator.save_summary()
        logger.info(f"✅ Summary saved: {summary_path}")
    
    # Step 3: Send email notification
    if send_email:
        logger.info("\n📧 Step 3: Sending email notification...")
        email_service = EmailService()
        
        if email_recipients:
            success = email_service.send_upload_summary(summary, email_recipients)
        else:
            success = email_service.send_upload_summary(summary)
        
        if success:
            logger.info("✅ Email notification sent successfully!")
        else:
            logger.warning("⚠️ Email notification failed or disabled")
    
    # Final summary
    logger.info("\n"+"="*80)
    logger.info("✅ UPLOAD PIPELINE COMPLETED")
    logger.info("="*80)
    logger.info(f"Files Processed: {summary['total_files']}")
    logger.info(f"Successful: {summary['successful_uploads']}")
    logger.info(f"Failed: {summary['failed_uploads']}")
    logger.info(f"Skipped: {summary['skipped_uploads']}")
    logger.info(f"Total Records: {summary['total_records_uploaded']:,}")
    logger.info("="*80)
    
    return summary


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='Automated Data Upload Pipeline with Email Notifications',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload all new data files and send email
  python -m app.cli.upload_pipeline
  
  # Force re-upload all files
  python -m app.cli.upload_pipeline --force
  
  # Upload without sending email
  python -m app.cli.upload_pipeline --no-email
  
  # Upload and send to specific recipients
  python -m app.cli.upload_pipeline --to user1@example.com,user2@example.com
  
  # Specify custom data directory
  python -m app.cli.upload_pipeline --base-path /path/to/data
  
  # Run with specific profile
  python -m app.cli.upload_pipeline --profile prod
        """
    )
    
    parser.add_argument(
        '--profile', '--env',
        choices=['dev', 'stage', 'prod'],
        default=None,
        dest='profile',
        help='Active profile (dev, stage, prod). Can also use SPRING_PROFILES_ACTIVE, APP_PROFILE, or APP_ENV'
    )
    
    parser.add_argument(
        '--base-path',
        type=str,
        help='Base directory containing data files (default: current directory)'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force upload even if files already uploaded'
    )
    
    parser.add_argument(
        '--no-email',
        action='store_true',
        help='Skip sending email notification'
    )
    
    parser.add_argument(
        '--to',
        type=str,
        help='Email recipients (comma-separated). If not provided, uses DB configuration'
    )
    
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Skip saving summary to file'
    )
    
    args = parser.parse_args()
    
    # Set profile if provided via CLI
    if args.profile:
        os.environ['SPRING_PROFILES_ACTIVE'] = args.profile
        os.environ['APP_PROFILE'] = args.profile
        os.environ['APP_ENV'] = args.profile
        load_environment_config()
        env = detect_environment()
        active_profile = get_active_profile()
        setup_logging(config.log_level)
        logger.info(f"Profile set to {active_profile.upper()} via CLI")
    
    # Parse email recipients
    email_recipients = None
    if args.to:
        email_recipients = [email.strip() for email in args.to.split(',')]
    
    # Run pipeline
    try:
        summary = run_upload_pipeline(
            base_path=args.base_path,
            force=args.force,
            send_email=not args.no_email,
            email_recipients=email_recipients,
            save_summary=not args.no_save
        )
        
        # Exit with appropriate code
        if summary['failed_uploads'] > 0:
            logger.error(f"\n❌ Pipeline completed with {summary['failed_uploads']} failed upload(s)")
            sys.exit(1)
        else:
            logger.info("\n✅ Pipeline completed successfully!")
            sys.exit(0)
    
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Pipeline interrupted by user")
        sys.exit(130)
    
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed with error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

