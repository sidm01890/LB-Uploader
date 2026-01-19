#!/usr/bin/env python3
"""
Startup script for Financial Reconciliation Platform API
Supports multiple profiles: dev, stage, prod (Spring Boot style)
"""
import uvicorn
import os
import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from app.core.environment import Environment, load_environment_config, detect_environment
from app.core.properties_loader import get_active_profile
from app.core.config import config


def main():
    """Main startup function."""
    parser = argparse.ArgumentParser(
        description='Start Financial Reconciliation Platform API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Development profile (default)
  python -m app.cli.run
  python -m app.cli.run --profile dev
  SPRING_PROFILES_ACTIVE=dev python -m app.cli.run
  
  # Staging profile
  python -m app.cli.run --profile stage
  SPRING_PROFILES_ACTIVE=stage python -m app.cli.run
  
  # Production profile
  python -m app.cli.run --profile prod
  SPRING_PROFILES_ACTIVE=prod python -m app.cli.run
  
  # Custom port
  python -m app.cli.run --profile prod --port 8080
        """
    )
    
    parser.add_argument(
        '--profile',
        '--env',  # Support both --profile and --env for backward compatibility
        choices=['dev', 'stage', 'prod'],
        default=None,
        dest='profile',
        help='Active profile (dev, stage, prod). Can also use SPRING_PROFILES_ACTIVE, APP_PROFILE, or APP_ENV env var'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=None,
        help='Port to run on (overrides config)'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default=None,
        help='Host to bind to (overrides config)'
    )
    
    parser.add_argument(
        '--reload',
        action='store_true',
        default=None,
        help='Enable auto-reload (overrides config)'
    )
    
    parser.add_argument(
        '--no-reload',
        action='store_true',
        help='Disable auto-reload (overrides config)'
    )
    
    args = parser.parse_args()
    
    # Set profile if provided (supports multiple env var names)
    if args.profile:
        os.environ['SPRING_PROFILES_ACTIVE'] = args.profile
        os.environ['APP_PROFILE'] = args.profile
        os.environ['APP_ENV'] = args.profile
    
    # Load properties files (application.properties + application-{profile}.properties)
    load_environment_config()
    env = detect_environment()
    active_profile = get_active_profile()
    
    # Ensure project root is in path
    sys.path.insert(0, project_root)
    
    # Check for required environment variables (only in production)
    # In development, most variables are optional with defaults
    if env.is_production:
        required_vars = ["OPENAI_API_KEY"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
            print("Please check your application.properties file or environment configuration.")
            sys.exit(1)
    else:
        # In development, warn about missing optional variables but don't fail
        optional_vars = {
            "OPENAI_API_KEY": "OpenAI API key (optional for dev, required for AI features)",
        }
        missing_optional = {var: desc for var, desc in optional_vars.items() if not os.getenv(var)}
        
        if missing_optional:
            print("⚠️  Optional environment variables not set (using defaults):")
            for var, desc in missing_optional.items():
                print(f"   - {var}: {desc}")
            print("   To set these, create .env.dev or application-dev.properties file")
            print()
    
    # Determine reload setting
    reload = config.uvicorn_reload
    if args.reload:
        reload = True
    elif args.no_reload:
        reload = False
    
    # Get host and port
    host = args.host or config.uvicorn_host
    port = args.port or config.uvicorn_port
    
    # Print startup information
    print("=" * 70)
    print("🚀 Financial Reconciliation Platform API")
    print("=" * 70)
    print(f"📦 Active Profile: {active_profile.upper()}")
    print(f"🌍 Environment: {env.value.upper()}")
    print(f"🌐 Host: {host}")
    print(f"🔌 Port: {port}")
    print(f"🔄 Reload: {'✅ Enabled' if reload else '❌ Disabled'}")
    print(f"📚 Docs: {'✅ Enabled' if config.enable_docs else '❌ Disabled'}")
    print(f"🐛 Debug: {'✅ Enabled' if config.debug else '❌ Disabled'}")
    print("=" * 70)
    
    if config.enable_docs:
        print(f"📚 API Documentation: http://{host}:{port}/docs")
        print(f"📖 ReDoc: http://{host}:{port}/redoc")
    print(f"🔍 Health Check: http://{host}:{port}/api/health")
    print("=" * 70)
    print()
    
    # Environment-specific warnings
    if env.is_production:
        if reload:
            print("⚠️  WARNING: Auto-reload is enabled in PRODUCTION!")
            print("   This should be disabled for production deployments.")
            print()
    
    # Start the server
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload,
        log_level=config.log_level.lower(),
        access_log=True
    )


if __name__ == "__main__":
    main()

