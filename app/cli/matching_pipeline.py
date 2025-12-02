#!/usr/bin/env python3
"""
Process 2: Matching Pipeline
============================
Complete matching and exception reporting workflow:
1. Run matching process (TRM→MPR→Bank)
2. Identify and categorize exceptions
3. Generate Excel and HTML reports
4. Send email notifications

Usage:
    python -m app.cli.matching_pipeline --start-date 2024-12-01 --end-date 2024-12-07
    python -m app.cli.matching_pipeline --date-range last7days
    python -m app.cli.matching_pipeline --start-date 2024-12-01 --end-date 2024-12-07 --no-email
    python -m app.cli.matching_pipeline --start-date 2024-12-01 --end-date 2024-12-07 --to user@example.com
    python -m app.cli.matching_pipeline --profile prod --start-date 2024-12-01 --end-date 2024-12-07

Supports multiple environments: dev, stage, prod

Created: November 12, 2025
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from app.core.environment import load_environment_config, detect_environment
from app.core.properties_loader import get_active_profile
from app.core.config import config
from app.services.matching.matching_orchestrator import MatchingOrchestrator
from app.services.matching.exception_handler import ExceptionHandler
from app.services.matching.report_generator import ReportGenerator
from app.services.email_service import EmailService
from app.logging_config import setup_logging

# Parse profile argument early
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
logger = logging.getLogger('matching_pipeline')
logger.info(f"Running with profile: {active_profile.upper()} (environment: {env.value.upper()})")


def run_matching_pipeline(
    start_date: str,
    end_date: str,
    send_email: bool = True,
    email_recipients: List[str] = None,
    generate_reports: bool = True
) -> Dict:
    """
    Run complete matching pipeline
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        send_email: Whether to send email notification
        email_recipients: List of email addresses (optional)
        generate_reports: Whether to generate Excel/HTML reports
    
    Returns:
        Complete pipeline summary dictionary
    """
    pipeline_summary = {
        'success': False,
        'start_time': datetime.now().isoformat(),
        'end_time': None,
        'match_summary': None,
        'exception_summary': None,
        'reports': {
            'excel_path': None,
            'html_path': None
        },
        'email_sent': False,
        'errors': []
    }
    
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING PROCESS 2: MATCHING PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Period: {start_date} to {end_date}")
        logger.info("")
        
        # Step 1: Run matching process
        logger.info("🔗 Step 1: Running matching process...")
        logger.info("-" * 80)
        
        orchestrator = MatchingOrchestrator()
        match_summary = orchestrator.run_matching(start_date, end_date, save_to_db=True)
        pipeline_summary['match_summary'] = match_summary
        
        if not match_summary:
            raise Exception("Matching process failed to return summary")
        
        logger.info("")
        logger.info("✅ Step 1 completed")
        logger.info("")
        
        # Step 2: Process exceptions
        logger.info("⚠️ Step 2: Processing exceptions...")
        logger.info("-" * 80)
        
        # Need to reconnect and fetch source data for exception handler
        exception_handler = ExceptionHandler()
        if exception_handler.connect_db():
            try:
                # Fetch source data again for exception processing
                source_data = orchestrator.fetch_source_data(start_date, end_date)
                
                # Process exceptions
                exception_summary = exception_handler.process_exceptions(
                    batch_id=match_summary['batch_id'],
                    source_data=source_data,
                    matched_records=None  # Could fetch from DB if needed
                )
                pipeline_summary['exception_summary'] = exception_summary
                
            finally:
                exception_handler.disconnect_db()
        else:
            logger.warning("⚠️ Could not connect for exception processing")
            exception_summary = {
                'total_exceptions': 0,
                'by_type': {},
                'by_severity': {},
                'total_amount_at_risk': 0.0
            }
            pipeline_summary['exception_summary'] = exception_summary
        
        logger.info("")
        logger.info("✅ Step 2 completed")
        logger.info("")
        
        # Step 3: Generate reports
        if generate_reports:
            logger.info("📊 Step 3: Generating reports...")
            logger.info("-" * 80)
            
            report_generator = ReportGenerator()
            
            # Generate Excel report
            excel_path = report_generator.generate_excel_report(
                match_summary,
                exception_summary
            )
            pipeline_summary['reports']['excel_path'] = excel_path
            
            # Generate HTML report
            html_content = report_generator.generate_html_report(
                match_summary,
                exception_summary
            )
            html_path = report_generator.save_html_report(
                html_content,
                match_summary['batch_id']
            )
            pipeline_summary['reports']['html_path'] = html_path
            
            logger.info("")
            logger.info("✅ Step 3 completed")
            logger.info("")
        
        # Step 4: Send email notification
        if send_email:
            logger.info("📧 Step 4: Sending email notification...")
            logger.info("-" * 80)
            
            email_service = EmailService()
            
            if email_service.enabled:
                # Prepare email content
                total_exceptions = exception_summary.get('total_exceptions', 0)
                match_rate = match_summary.get('financial', {}).get('match_rate_percentage', 0)
                
                # Determine subject based on results
                if total_exceptions == 0 and match_rate >= 95:
                    subject = f"✅ Matching Report - Perfect Match ({match_rate:.1f}%) - {match_summary['batch_id']}"
                elif total_exceptions > 0 and total_exceptions <= 10:
                    subject = f"⚠️ Matching Report - {total_exceptions} Exceptions Found - {match_summary['batch_id']}"
                else:
                    subject = f"❌ Matching Report - {total_exceptions} Exceptions Require Attention - {match_summary['batch_id']}"
                
                # Get recipients
                if not email_recipients:
                    # Fetch from database for 'Matching' process
                    recipients_data = email_service.get_recipients_by_process(
                        'Matching',
                        'ALL',
                        'success' if total_exceptions == 0 else 'failure'
                    )
                    email_recipients = [r['email'] for r in recipients_data]
                
                # If still no recipients, use a default (or skip)
                if not email_recipients:
                    logger.warning("⚠️ No email recipients configured for Matching process")
                    email_recipients = []
                
                if email_recipients:
                    # Get HTML content from report generator
                    html_body = report_generator.generate_html_report(
                        match_summary,
                        exception_summary
                    )
                    
                    # Send email with Excel attachment
                    from email.mime.multipart import MIMEMultipart
                    from email.mime.text import MIMEText
                    from email.mime.application import MIMEApplication
                    import smtplib
                    
                    msg = MIMEMultipart('mixed')
                    msg['Subject'] = subject
                    msg['From'] = f"{email_service.from_name} <{email_service.from_email}>"
                    msg['To'] = ', '.join(email_recipients)
                    
                    # Attach HTML body
                    msg.attach(MIMEText(html_body, 'html'))
                    
                    # Attach Excel report
                    if pipeline_summary['reports']['excel_path']:
                        with open(pipeline_summary['reports']['excel_path'], 'rb') as f:
                            excel_attachment = MIMEApplication(f.read(), _subtype='xlsx')
                            excel_filename = os.path.basename(pipeline_summary['reports']['excel_path'])
                            excel_attachment.add_header('Content-Disposition', 'attachment', 
                                                      filename=excel_filename)
                            msg.attach(excel_attachment)
                    
                    # Send email
                    try:
                        with smtplib.SMTP(email_service.smtp_host, email_service.smtp_port) as server:
                            server.starttls()
                            server.login(email_service.smtp_user, email_service.smtp_password)
                            server.send_message(msg)
                        
                        logger.info(f"  ✅ Email sent to {len(email_recipients)} recipient(s)")
                        pipeline_summary['email_sent'] = True
                    
                    except Exception as e:
                        logger.error(f"  ❌ Failed to send email: {e}")
                        pipeline_summary['errors'].append(f"Email sending failed: {str(e)}")
                else:
                    logger.warning("  ⚠️ No recipients - skipping email")
            else:
                logger.warning("  ⚠️ Email is disabled in configuration")
            
            logger.info("")
            logger.info("✅ Step 4 completed")
            logger.info("")
        
        # Mark as successful
        pipeline_summary['success'] = True
        pipeline_summary['end_time'] = datetime.now().isoformat()
        
        # Print final summary
        print_pipeline_summary(pipeline_summary)
        
        return pipeline_summary
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        pipeline_summary['errors'].append(str(e))
        pipeline_summary['end_time'] = datetime.now().isoformat()
        return pipeline_summary


def print_pipeline_summary(summary: Dict):
    """Print final pipeline summary"""
    logger.info("=" * 80)
    logger.info("✅ MATCHING PIPELINE COMPLETED")
    logger.info("=" * 80)
    
    match_summary = summary.get('match_summary', {})
    exception_summary = summary.get('exception_summary', {})
    
    logger.info(f"Batch ID: {match_summary.get('batch_id', 'N/A')}")
    logger.info(f"Period: {match_summary.get('date_range', {}).get('start')} to {match_summary.get('date_range', {}).get('end')}")
    
    logger.info("\n🎯 Results:")
    logger.info(f"  - Match Rate: {match_summary.get('financial', {}).get('match_rate_percentage', 0):.2f}%")
    logger.info(f"  - Full Chain Matches: {match_summary.get('matches', {}).get('full_chain_matches', 0):,}")
    logger.info(f"  - Total Exceptions: {exception_summary.get('total_exceptions', 0):,}")
    logger.info(f"  - Amount at Risk: ₹{exception_summary.get('total_amount_at_risk', 0):,.2f}")
    
    logger.info("\n📊 Reports Generated:")
    if summary['reports']['excel_path']:
        logger.info(f"  - Excel: {summary['reports']['excel_path']}")
    if summary['reports']['html_path']:
        logger.info(f"  - HTML: {summary['reports']['html_path']}")
    
    logger.info(f"\n📧 Email: {'✅ Sent' if summary['email_sent'] else '❌ Not sent'}")
    
    if summary['errors']:
        logger.info("\n⚠️ Errors:")
        for error in summary['errors']:
            logger.info(f"  - {error}")
    
    logger.info("=" * 80)


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(
        description='Process 2: Matching and Exception Reporting Pipeline',
        epilog="""
Examples:
  # Run matching for specific date range
  python -m app.cli.matching_pipeline --start-date 2024-12-01 --end-date 2024-12-07
  
  # Use date range shortcut
  python -m app.cli.matching_pipeline --date-range last7days
  python -m app.cli.matching_pipeline --date-range today
  python -m app.cli.matching_pipeline --date-range yesterday
  
  # Send to specific email addresses
  python -m app.cli.matching_pipeline --start-date 2024-12-01 --end-date 2024-12-07 --to user1@example.com,user2@example.com
  
  # Skip email notification
  python -m app.cli.matching_pipeline --start-date 2024-12-01 --end-date 2024-12-07 --no-email
  
  # Run with specific profile
  python -m app.cli.matching_pipeline --profile prod --start-date 2024-12-01 --end-date 2024-12-07
  SPRING_PROFILES_ACTIVE=prod python -m app.cli.matching_pipeline --start-date 2024-12-01 --end-date 2024-12-07
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Profile argument (Spring Boot style)
    parser.add_argument('--profile', '--env',
                       choices=['dev', 'stage', 'prod'],
                       default=None,
                       dest='profile',
                       help='Active profile (dev, stage, prod). Can also use SPRING_PROFILES_ACTIVE, APP_PROFILE, or APP_ENV')
    
    # Date arguments
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument('--date-range',
                           choices=['today', 'yesterday', 'last7days', 'last30days'],
                           help='Predefined date range')
    date_group.add_argument('--start-date',
                           help='Start date (YYYY-MM-DD)')
    
    parser.add_argument('--end-date',
                       help='End date (YYYY-MM-DD). Required if --start-date is used')
    
    # Email arguments
    parser.add_argument('--no-email',
                       action='store_true',
                       help='Skip email notification')
    parser.add_argument('--to',
                       help='Comma-separated list of email recipients')
    
    # Report arguments
    parser.add_argument('--no-reports',
                       action='store_true',
                       help='Skip report generation')
    
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
    
    # Determine date range
    if args.date_range:
        today = datetime.now().date()
        if args.date_range == 'today':
            start_date = end_date = today
        elif args.date_range == 'yesterday':
            start_date = end_date = today - timedelta(days=1)
        elif args.date_range == 'last7days':
            start_date = today - timedelta(days=7)
            end_date = today
        elif args.date_range == 'last30days':
            start_date = today - timedelta(days=30)
            end_date = today
    else:
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date are required together")
        
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        except ValueError as e:
            parser.error(f"Invalid date format: {e}")
    
    # Parse email recipients
    email_recipients = None
    if args.to:
        email_recipients = [email.strip() for email in args.to.split(',')]
    
    # Run pipeline
    try:
        summary = run_matching_pipeline(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            send_email=not args.no_email,
            email_recipients=email_recipients,
            generate_reports=not args.no_reports
        )
        
        if summary['success']:
            print("\n✅ Pipeline completed successfully!")
            print(f"📊 Check reports in: {os.path.dirname(summary['reports'].get('excel_path', ''))}")
            sys.exit(0)
        else:
            print("\n❌ Pipeline failed!")
            print("Check logs for details")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n\n⚠️ Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

