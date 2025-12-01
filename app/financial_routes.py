"""
Financial Reconciliation API Routes
Comprehensive API endpoints for restaurant financial reconciliation platform
"""

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, date, timedelta
import logging
from app.services.config_manager import ConfigurationManager, DataSourceConfig
from app.services.reconciliation_engine import RestaurantReconciliationEngine, ReconciliationType
import json

logger = logging.getLogger(__name__)
financial_router = APIRouter(prefix="/financial", tags=["Financial Reconciliation"])

# Pydantic models for request/response validation
class ReconciliationRequest(BaseModel):
    rule_id: Optional[str] = None
    recon_type: Optional[str] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None

class DataSourceConfigRequest(BaseModel):
    source_id: str
    source_name: str
    source_type: str = Field(..., pattern="^(SFTP|FOLDER|EMAIL|API|DATABASE|MANUAL)$")
    entity_type: str
    connection_config: Dict[str, Any]
    file_config: Dict[str, Any]
    ai_mapping_config: Optional[Dict[str, Any]] = None
    data_quality_rules: Optional[Dict[str, Any]] = None
    load_config: Optional[Dict[str, Any]] = None
    recon_config: Optional[Dict[str, Any]] = None
    notification_config: Optional[Dict[str, Any]] = None
    schedule_cron: Optional[str] = None
    is_active: bool = True

class BusinessRule(BaseModel):
    rule_id: str
    rule_category: str = Field(..., pattern="^(COMMISSION|TAX|SETTLEMENT|VALIDATION|TRANSFORMATION)$")
    platform: str
    rule_name: str
    rule_logic: Dict[str, Any]
    effective_date: date
    expiry_date: Optional[date] = None
    region: str = "IN"
    store_types: Optional[List[str]] = None

# Configuration Management Endpoints
@financial_router.get("/config/data-sources", response_model=List[Dict[str, Any]])
async def get_data_sources(
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    source_type: Optional[str] = Query(None, description="Filter by source type"),
    active_only: bool = Query(True, description="Return only active data sources")
):
    """Get all configured data sources with optional filtering"""
    try:
        config_mgr = ConfigurationManager()
        data_sources = config_mgr.list_data_sources(entity_type, source_type)
        
        result = []
        for ds in data_sources:
            if not active_only or ds.is_active:
                result.append({
                    "source_id": ds.source_id,
                    "source_name": ds.source_name,
                    "source_type": ds.source_type,
                    "entity_type": ds.entity_type,
                    "schedule_cron": ds.schedule_cron,
                    "is_active": ds.is_active,
                    "connection_config": ds.connection_config,
                    "file_config": ds.file_config,
                    "ai_mapping_enabled": ds.ai_mapping_config.get('enabled', False) if ds.ai_mapping_config else False
                })
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting data sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@financial_router.get("/config/data-sources/{source_id}", response_model=Dict[str, Any])
async def get_data_source(source_id: str):
    """Get specific data source configuration"""
    try:
        config_mgr = ConfigurationManager()
        config = config_mgr.get_data_source_config(source_id)
        
        if not config:
            raise HTTPException(status_code=404, detail=f"Data source {source_id} not found")
        
        return {
            "source_id": config.source_id,
            "source_name": config.source_name,
            "source_type": config.source_type,
            "entity_type": config.entity_type,
            "connection_config": config.connection_config,
            "file_config": config.file_config,
            "ai_mapping_config": config.ai_mapping_config,
            "data_quality_rules": config.data_quality_rules,
            "load_config": config.load_config,
            "recon_config": config.recon_config,
            "notification_config": config.notification_config,
            "schedule_cron": config.schedule_cron,
            "is_active": config.is_active
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@financial_router.post("/config/data-sources", response_model=Dict[str, str])
async def create_data_source(config_request: DataSourceConfigRequest):
    """Create or update data source configuration"""
    try:
        config_mgr = ConfigurationManager()
        
        # Convert Pydantic model to DataSourceConfig
        config = DataSourceConfig(
            source_id=config_request.source_id,
            source_name=config_request.source_name,
            source_type=config_request.source_type,
            entity_type=config_request.entity_type,
            connection_config=config_request.connection_config,
            file_config=config_request.file_config,
            ai_mapping_config=config_request.ai_mapping_config,
            data_quality_rules=config_request.data_quality_rules,
            load_config=config_request.load_config,
            recon_config=config_request.recon_config,
            notification_config=config_request.notification_config,
            schedule_cron=config_request.schedule_cron,
            is_active=config_request.is_active
        )
        
        # Validate configuration
        validation_result = config_mgr.validate_config(config)
        if validation_result.get('errors'):
            raise HTTPException(
                status_code=400, 
                detail=f"Configuration validation failed: {validation_result['errors']}"
            )
        
        # Save configuration
        success = config_mgr.save_data_source_config(config)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to save configuration")
        
        return {
            "message": f"Data source {config.source_id} created/updated successfully",
            "source_id": config.source_id,
            "warnings": str(validation_result.get('warnings', []))
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@financial_router.get("/config/mapping-examples", response_model=List[Dict[str, Any]])
async def get_all_mapping_examples(
    source_system: Optional[str] = Query(None, description="Filter by source system")
):
    """Get all AI mapping examples"""
    try:
        from app.core.database import db_manager
        
        with db_manager.get_mysql_connector() as connection:
            cursor = connection.cursor(dictionary=True)
            
            query = "SELECT * FROM cfg_mapping_examples WHERE is_active = TRUE"
            params = []
            
            if source_system:
                query += " AND source_system = %s"
                params.append(source_system)
            
            query += " ORDER BY entity_type, source_system"
            cursor.execute(query, params if params else None)
            examples = cursor.fetchall()
            
            return examples
        
    except Exception as e:
        logger.error(f"Error getting mapping examples: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@financial_router.get("/config/mapping-examples/{entity_type}", response_model=List[Dict[str, Any]])
async def get_mapping_examples(
    entity_type: str,
    source_system: Optional[str] = Query(None, description="Filter by source system")
):
    """Get AI mapping examples for an entity type"""
    try:
        config_mgr = ConfigurationManager()
        examples = config_mgr.get_mapping_examples(entity_type, source_system)
        
        result = []
        for ex in examples:
            result.append({
                "example_id": ex.example_id,
                "entity_type": ex.entity_type,
                "source_system": ex.source_system,
                "example_name": ex.example_name,
                "input_columns": ex.input_columns,
                "mapped_columns": ex.mapped_columns,
                "sample_data": ex.sample_data,
                "confidence_score": ex.confidence_score
            })
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting mapping examples: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@financial_router.get("/config/business-rules/{platform}", response_model=List[Dict[str, Any]])
async def get_business_rules(
    platform: str,
    rule_category: Optional[str] = Query(None, description="Filter by rule category")
):
    """Get business rules for a platform"""
    try:
        config_mgr = ConfigurationManager()
        rules = config_mgr.get_business_rules(platform, rule_category)
        
        return rules
        
    except Exception as e:
        logger.error(f"Error getting business rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@financial_router.get("/config/summary", response_model=Dict[str, Any])
async def get_configuration_summary():
    """Get comprehensive configuration summary"""
    try:
        config_mgr = ConfigurationManager()
        summary = config_mgr.get_configuration_summary()
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting configuration summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Reconciliation Engine Endpoints
@financial_router.post("/reconciliation/run", response_model=Dict[str, Any])
async def run_reconciliation(request: ReconciliationRequest):
    """Run financial reconciliation with specified parameters"""
    try:
        engine = RestaurantReconciliationEngine()
        
        # Set date defaults
        date_to = request.date_to or datetime.now().date()
        date_from = request.date_from or (date_to - timedelta(days=7))
        
        # Convert dates to datetime
        date_from_dt = datetime.combine(date_from, datetime.min.time())
        date_to_dt = datetime.combine(date_to, datetime.max.time())
        
        # Parse reconciliation type
        recon_type = None
        if request.recon_type:
            try:
                recon_type = ReconciliationType(request.recon_type)
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Invalid reconciliation type: {request.recon_type}"
                )
        
        # Run reconciliation
        results = engine.run_reconciliation(
            rule_id=request.rule_id,
            recon_type=recon_type,
            date_from=date_from_dt,
            date_to=date_to_dt
        )
        
        # Convert datetime objects to ISO format for JSON serialization
        if 'start_time' in results:
            results['start_time'] = results['start_time'].isoformat()
        if 'end_time' in results:
            results['end_time'] = results['end_time'].isoformat()
        if 'date_range' in results:
            results['date_range']['from'] = results['date_range']['from'].isoformat()
            results['date_range']['to'] = results['date_range']['to'].isoformat()
        
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error running reconciliation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@financial_router.get("/reconciliation/dashboard", response_model=Dict[str, Any])
async def get_reconciliation_dashboard(
    date_from: Optional[date] = Query(None, description="Start date for dashboard data"),
    date_to: Optional[date] = Query(None, description="End date for dashboard data")
):
    """Get reconciliation dashboard data"""
    try:
        engine = RestaurantReconciliationEngine()
        
        # Set date defaults
        date_to_val = date_to or datetime.now().date()
        date_from_val = date_from or (date_to_val - timedelta(days=30))
        
        # Convert to datetime
        date_from_dt = datetime.combine(date_from_val, datetime.min.time())
        date_to_dt = datetime.combine(date_to_val, datetime.max.time())
        
        dashboard_data = engine.get_reconciliation_dashboard_data(date_from_dt, date_to_dt)
        
        # Convert dates in dashboard data
        if 'date_range' in dashboard_data:
            dashboard_data['date_range']['from'] = dashboard_data['date_range']['from'].isoformat()
            dashboard_data['date_range']['to'] = dashboard_data['date_range']['to'].isoformat()
        
        return dashboard_data
        
    except Exception as e:
        logger.error(f"Error getting dashboard data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Data Analytics Endpoints
@financial_router.get("/analytics/transactions/summary", response_model=Dict[str, Any])
async def get_transactions_summary(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    source_system: Optional[str] = Query(None)
):
    """Get transaction analytics summary"""
    try:
        from app.core.database import db_manager
        
        # Use unified database manager
        with db_manager.get_mysql_connector() as connection:
            cursor = connection.cursor(dictionary=True)
            
            # Build query with filters
            base_query = """
                SELECT 
                    COUNT(*) as total_count,
                    SUM(gross_amount) as total_amount,
                    AVG(gross_amount) as avg_amount,
                    MIN(gross_amount) as min_amount,
                    MAX(gross_amount) as max_amount,
                    COUNT(DISTINCT DATE(txn_datetime)) as transaction_days,
                    source_system
                FROM fin_transactions 
                WHERE 1=1
            """
            
            params = []
            
            if date_from:
                base_query += " AND DATE(txn_datetime) >= %s"
                params.append(date_from)
                
            if date_to:
                base_query += " AND DATE(txn_datetime) <= %s"
                params.append(date_to)
                
            if source_system:
                base_query += " AND source_system = %s"
                params.append(source_system)
                
            base_query += " GROUP BY source_system ORDER BY total_amount DESC"
            
            cursor.execute(base_query, params)
            results = cursor.fetchall()
            
            # Get daily trend data
            trend_query = """
                SELECT 
                    DATE(txn_datetime) as txn_date,
                    COUNT(*) as daily_count,
                    SUM(gross_amount) as daily_amount
                FROM fin_transactions 
                WHERE DATE(txn_datetime) >= %s AND DATE(txn_datetime) <= %s
                GROUP BY DATE(txn_datetime)
                ORDER BY txn_date DESC
                LIMIT 30
            """
            
            trend_from = date_from or (datetime.now().date() - timedelta(days=30))
            trend_to = date_to or datetime.now().date()
            
            cursor.execute(trend_query, (trend_from, trend_to))
            trend_data = cursor.fetchall()
            
            # Convert Decimal to float and dates to strings
            for result in results:
                for key, value in result.items():
                    if hasattr(value, 'is_finite'):  # Decimal type
                        result[key] = float(value) if value else 0.0
                        
            for trend in trend_data:
                for key, value in trend.items():
                    if hasattr(value, 'is_finite'):  # Decimal type
                        trend[key] = float(value) if value else 0.0
                    elif hasattr(value, 'isoformat'):  # Date type
                        trend[key] = value.isoformat()
            
            return {
                "summary_by_source": results,
                "daily_trends": trend_data,
                "filter_applied": {
                    "date_from": date_from.isoformat() if date_from else None,
                    "date_to": date_to.isoformat() if date_to else None,
                    "source_system": source_system
                }
            }
        
    except Exception as e:
        logger.error(f"Error getting transaction summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@financial_router.get("/analytics/payments/summary", response_model=Dict[str, Any])
async def get_payments_summary(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    provider: Optional[str] = Query(None)
):
    """Get payment analytics summary"""
    try:
        from app.core.database import db_manager
        
        with db_manager.get_mysql_connector() as connection:
            cursor = connection.cursor(dictionary=True)
            
            # Build query with filters
            base_query = """
                SELECT 
                    provider,
                    COUNT(*) as payment_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    COUNT(CASE WHEN status = 'SETTLED' THEN 1 END) as settled_count,
                    COUNT(CASE WHEN status = 'AUTHORIZED' THEN 1 END) as authorized_count,
                    COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) as cancelled_count
                FROM fin_payments 
                WHERE 1=1
            """
            
            params = []
            
            if date_from:
                base_query += " AND DATE(payment_datetime) >= %s"
                params.append(date_from)
                
            if date_to:
                base_query += " AND DATE(payment_datetime) <= %s"
                params.append(date_to)
                
            if provider:
                base_query += " AND provider = %s"
                params.append(provider)
                
            base_query += " GROUP BY provider ORDER BY total_amount DESC"
            
            cursor.execute(base_query, params)
            results = cursor.fetchall()
            
            # Convert Decimal to float
            for result in results:
                for key, value in result.items():
                    if hasattr(value, 'is_finite'):  # Decimal type
                        result[key] = float(value) if value else 0.0
            
            return {
                "payment_summary": results,
                "filter_applied": {
                    "date_from": date_from.isoformat() if date_from else None,
                    "date_to": date_to.isoformat() if date_to else None,
                    "provider": provider
                }
            }
        
    except Exception as e:
        logger.error(f"Error getting payment summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# System Health and Performance Endpoints
@financial_router.get("/health", response_model=Dict[str, Any])
async def financial_health_check():
    """Comprehensive health check for financial platform"""
    try:
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service": "Financial Reconciliation Platform",
            "version": "1.0.0",
            "components": {}
        }
        
        # Check database connectivity
        try:
            from app.core.database import db_manager
            if db_manager.test_connection():
                health_status["components"]["database"] = "healthy"
            else:
                health_status["components"]["database"] = "unhealthy"
                health_status["status"] = "degraded"
        except Exception as e:
            health_status["components"]["database"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
        
        # Check configuration manager
        try:
            config_mgr = ConfigurationManager()
            summary = config_mgr.get_configuration_summary()
            health_status["components"]["configuration"] = "healthy"
            health_status["configuration_summary"] = {
                "data_sources": len(summary.get('data_sources', [])),
                "mapping_examples": len(summary.get('mapping_examples', [])),
                "business_rules": len(summary.get('business_rules', []))
            }
        except Exception as e:
            health_status["components"]["configuration"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
        
        # Check reconciliation engine
        try:
            engine = RestaurantReconciliationEngine()
            health_status["components"]["reconciliation_engine"] = "healthy"
            health_status["reconciliation_rules"] = len(engine.reconciliation_rules)
        except Exception as e:
            health_status["components"]["reconciliation_engine"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
        
        return health_status
        
    except Exception as e:
        logger.error(f"Error in health check: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@financial_router.get("/performance/metrics", response_model=Dict[str, Any])
async def get_performance_metrics():
    """Get system performance metrics"""
    try:
        import mysql.connector
        import time
        import psutil
        import os
        
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "system": {},
            "database": {},
            "processing": {}
        }
        
        # System metrics
        metrics["system"] = {
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_usage_percent": psutil.disk_usage('/').percent,
            "process_count": len(psutil.pids())
        }
        
        # Database performance metrics
        from app.core.database import db_manager
        from app.core.config import config
        
        start_time = time.time()
        with db_manager.get_mysql_connector() as connection:
            # Connection time
            connection_time = time.time() - start_time
            
            cursor = connection.cursor(dictionary=True)
            
            # Query performance test
            start_time = time.time()
            cursor.execute("SELECT COUNT(*) as count FROM fin_transactions")
            txn_count = cursor.fetchone()['count']
            txn_query_time = time.time() - start_time
            
            start_time = time.time()
            cursor.execute("SELECT COUNT(*) as count FROM fin_payments")
            pay_count = cursor.fetchone()['count']
            pay_query_time = time.time() - start_time
            
            # Table sizes
            cursor.execute("""
                SELECT 
                    TABLE_NAME,
                    TABLE_ROWS,
                    ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) as size_mb
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_NAME LIKE 'fin_%'
                ORDER BY size_mb DESC
            """, (config.database.database,))
            table_sizes = cursor.fetchall()
        
        metrics["database"] = {
            "connection_time_ms": round(connection_time * 1000, 2),
            "transaction_query_time_ms": round(txn_query_time * 1000, 2),
            "payment_query_time_ms": round(pay_query_time * 1000, 2),
            "transaction_count": txn_count,
            "payment_count": pay_count,
            "table_sizes": table_sizes
        }
        
        # Processing performance (test reconciliation engine initialization)
        start_time = time.time()
        engine = RestaurantReconciliationEngine()
        engine_init_time = time.time() - start_time
        
        start_time = time.time()
        config_mgr = ConfigurationManager()
        summary = config_mgr.get_configuration_summary()
        config_load_time = time.time() - start_time
        
        metrics["processing"] = {
            "reconciliation_engine_init_ms": round(engine_init_time * 1000, 2),
            "config_manager_load_ms": round(config_load_time * 1000, 2),
            "loaded_rules": len(engine.reconciliation_rules),
            "loaded_configs": len(summary.get('data_sources', []))
        }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting performance metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))