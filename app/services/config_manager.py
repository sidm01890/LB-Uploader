#!/usr/bin/env python3
"""
Configuration Management Service
Manages data source configurations, mapping examples, reconciliation rules, and workflows
"""

import json
import yaml
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

@dataclass
class DataSourceConfig:
    """Data source configuration"""
    source_id: str
    source_name: str
    source_type: str
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

@dataclass
class MappingExample:
    """AI mapping example"""
    example_id: str
    entity_type: str
    source_system: str
    example_name: str
    input_columns: List[str]
    mapped_columns: Dict[str, str]
    sample_data: Optional[Dict[str, Any]] = None
    confidence_score: float = 1.0

@dataclass
class ReconciliationRuleset:
    """Reconciliation ruleset configuration"""
    ruleset_id: str
    ruleset_name: str
    description: str
    left_entity: str
    right_entity: str
    rule_logic: Dict[str, Any]
    auto_match_threshold: float = 0.85
    manual_review_threshold: float = 0.60

class ConfigurationManager:
    """Comprehensive configuration management for the financial reconciliation platform"""
    
    def __init__(self):
        from app.core.database import db_manager
        self.db_manager = db_manager
        self.db_config = db_manager.get_connection_dict()
    
    def get_data_source_config(self, source_id: str) -> Optional[DataSourceConfig]:
        """Get data source configuration by ID"""
        
        try:
            with self.db_manager.get_mysql_connector() as connection:
                cursor = connection.cursor(dictionary=True)
                
                cursor.execute("""
                    SELECT source_id, source_name, source_type, entity_type,
                           connection_config, file_config, ai_mapping_config,
                           data_quality_rules, load_config, recon_config,
                           notification_config, schedule_cron, is_active
                    FROM cfg_data_sources 
                    WHERE source_id = %s AND is_active = TRUE
                """, (source_id,))
                
                row = cursor.fetchone()
                
                if row:
                    return DataSourceConfig(
                        source_id=row['source_id'],
                        source_name=row['source_name'],
                        source_type=row['source_type'],
                        entity_type=row['entity_type'],
                        connection_config=json.loads(row['connection_config'] or '{}'),
                        file_config=json.loads(row['file_config'] or '{}'),
                        ai_mapping_config=json.loads(row['ai_mapping_config'] or '{}') if row['ai_mapping_config'] else None,
                        data_quality_rules=json.loads(row['data_quality_rules'] or '{}') if row['data_quality_rules'] else None,
                        load_config=json.loads(row['load_config'] or '{}') if row['load_config'] else None,
                        recon_config=json.loads(row['recon_config'] or '{}') if row['recon_config'] else None,
                        notification_config=json.loads(row['notification_config'] or '{}') if row['notification_config'] else None,
                        schedule_cron=row['schedule_cron'],
                        is_active=row['is_active']
                    )
            return None
            
        except Error as e:
            logger.error(f"Failed to get data source config: {e}")
            return None
    
    def list_data_sources(self, entity_type: Optional[str] = None, 
                         source_type: Optional[str] = None) -> List[DataSourceConfig]:
        """List data source configurations with optional filtering"""
        
        try:
            with self.db_manager.get_mysql_connector() as connection:
                cursor = connection.cursor(dictionary=True)
                
                query = "SELECT * FROM cfg_data_sources WHERE is_active = TRUE"
                params = []
                
                if entity_type:
                    query += " AND entity_type = %s"
                    params.append(entity_type)
                    
                if source_type:
                    query += " AND source_type = %s"
                    params.append(source_type)
                    
                query += " ORDER BY source_name"
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                configs = []
                for row in rows:
                    configs.append(DataSourceConfig(
                        source_id=row['source_id'],
                        source_name=row['source_name'],
                        source_type=row['source_type'],
                        entity_type=row['entity_type'],
                        connection_config=json.loads(row['connection_config'] or '{}'),
                        file_config=json.loads(row['file_config'] or '{}'),
                        ai_mapping_config=json.loads(row['ai_mapping_config'] or '{}') if row['ai_mapping_config'] else None,
                        data_quality_rules=json.loads(row['data_quality_rules'] or '{}') if row['data_quality_rules'] else None,
                        load_config=json.loads(row['load_config'] or '{}') if row['load_config'] else None,
                        recon_config=json.loads(row['recon_config'] or '{}') if row['recon_config'] else None,
                        notification_config=json.loads(row['notification_config'] or '{}') if row['notification_config'] else None,
                        schedule_cron=row['schedule_cron'],
                        is_active=row['is_active']
                    ))
                
                return configs
            
        except Error as e:
            logger.error(f"Failed to list data sources: {e}")
            return []
    
    def get_mapping_examples(self, entity_type: str, source_system: Optional[str] = None) -> List[MappingExample]:
        """Get AI mapping examples for an entity type"""
        
        try:
            with self.db_manager.get_mysql_connector() as connection:
                cursor = connection.cursor(dictionary=True)
                
                query = """
                    SELECT example_id, entity_type, source_system, example_name,
                           input_columns, mapped_columns, sample_data, confidence_score
                    FROM cfg_mapping_examples 
                    WHERE entity_type = %s AND is_active = TRUE
                """
                params = [entity_type]
                
                if source_system:
                    query += " AND source_system = %s"
                    params.append(source_system)
                    
                query += " ORDER BY confidence_score DESC, usage_count DESC"
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                examples = []
                for row in rows:
                    examples.append(MappingExample(
                        example_id=row['example_id'],
                        entity_type=row['entity_type'],
                        source_system=row['source_system'],
                        example_name=row['example_name'],
                        input_columns=json.loads(row['input_columns']),
                        mapped_columns=json.loads(row['mapped_columns']),
                        sample_data=json.loads(row['sample_data']) if row['sample_data'] else None,
                        confidence_score=float(row['confidence_score'])
                    ))
                
                return examples
            
        except Error as e:
            logger.error(f"Failed to get mapping examples: {e}")
            return []
    
    def get_reconciliation_ruleset(self, ruleset_id: str) -> Optional[ReconciliationRuleset]:
        """Get reconciliation ruleset by ID"""
        
        try:
            with self.db_manager.get_mysql_connector() as connection:
                cursor = connection.cursor(dictionary=True)
                
                cursor.execute("""
                    SELECT ruleset_id, ruleset_name, description, left_entity, right_entity,
                           rule_logic, auto_match_threshold, manual_review_threshold
                    FROM cfg_recon_rulesets 
                    WHERE ruleset_id = %s AND is_active = TRUE
                """, (ruleset_id,))
                
                row = cursor.fetchone()
                
                if row:
                    return ReconciliationRuleset(
                        ruleset_id=row['ruleset_id'],
                        ruleset_name=row['ruleset_name'],
                        description=row['description'],
                        left_entity=row['left_entity'],
                        right_entity=row['right_entity'],
                        rule_logic=json.loads(row['rule_logic']),
                        auto_match_threshold=float(row['auto_match_threshold']),
                        manual_review_threshold=float(row['manual_review_threshold'])
                    )
                
                return None
            
        except Error as e:
            logger.error(f"Failed to get reconciliation ruleset: {e}")
            return None
    
    def get_business_rules(self, platform: str, rule_category: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get business rules for a platform"""
        
        try:
            with self.db_manager.get_mysql_connector() as connection:
                cursor = connection.cursor(dictionary=True)
                
                query = """
                    SELECT rule_id, rule_category, platform, rule_name, rule_logic,
                           effective_date, expiry_date, region, store_types
                    FROM cfg_business_rules 
                    WHERE platform = %s AND is_active = TRUE
                    AND (expiry_date IS NULL OR expiry_date > CURDATE())
                """
                params = [platform]
                
                if rule_category:
                    query += " AND rule_category = %s"
                    params.append(rule_category)
                    
                query += " ORDER BY effective_date DESC"
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                rules = []
            for row in rows:
                rule = dict(row)
                rule['rule_logic'] = json.loads(row['rule_logic'])
                rule['store_types'] = json.loads(row['store_types']) if row['store_types'] else None
                rules.append(rule)
                
                return rules
            
        except Error as e:
            logger.error(f"Failed to get business rules: {e}")
            return []
    
    def save_data_source_config(self, config: DataSourceConfig) -> bool:
        """Save or update data source configuration"""
        
        try:
            with self.db_manager.get_mysql_connector() as connection:
                cursor = connection.cursor()
                
                upsert_sql = """
                    INSERT INTO cfg_data_sources (
                        source_id, source_name, source_type, entity_type,
                        connection_config, file_config, ai_mapping_config,
                        data_quality_rules, load_config, recon_config,
                        notification_config, schedule_cron, is_active,
                        updated_at, updated_by
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        source_name = VALUES(source_name),
                        source_type = VALUES(source_type),
                        entity_type = VALUES(entity_type),
                        connection_config = VALUES(connection_config),
                        file_config = VALUES(file_config),
                        ai_mapping_config = VALUES(ai_mapping_config),
                        data_quality_rules = VALUES(data_quality_rules),
                        load_config = VALUES(load_config),
                        recon_config = VALUES(recon_config),
                        notification_config = VALUES(notification_config),
                        schedule_cron = VALUES(schedule_cron),
                        is_active = VALUES(is_active),
                        updated_at = VALUES(updated_at),
                        updated_by = VALUES(updated_by)
                """
                
                cursor.execute(upsert_sql, (
                    config.source_id, config.source_name, config.source_type, config.entity_type,
                    json.dumps(config.connection_config),
                    json.dumps(config.file_config),
                    json.dumps(config.ai_mapping_config) if config.ai_mapping_config else None,
                    json.dumps(config.data_quality_rules) if config.data_quality_rules else None,
                    json.dumps(config.load_config) if config.load_config else None,
                    json.dumps(config.recon_config) if config.recon_config else None,
                    json.dumps(config.notification_config) if config.notification_config else None,
                    config.schedule_cron, config.is_active,
                    datetime.now(), 'system'
                ))
                
                connection.commit()
                
                logger.info(f"Saved data source config: {config.source_id}")
                return True
            
        except Error as e:
            logger.error(f"Failed to save data source config: {e}")
            return False
    
    def create_config_from_yaml(self, yaml_path: str) -> Optional[DataSourceConfig]:
        """Create data source configuration from YAML file (like your example)"""
        
        try:
            with open(yaml_path, 'r') as file:
                yaml_config = yaml.safe_load(file)
            
            # Extract configuration sections from YAML
            config = DataSourceConfig(
                source_id=yaml_config.get('source_id', ''),
                source_name=yaml_config.get('source_name', yaml_config.get('source_id', '')),
                source_type=yaml_config.get('mode', 'FOLDER').upper(),
                entity_type=yaml_config.get('entity', ''),
                connection_config=yaml_config.get('location', {}),
                file_config={
                    'file_pattern': yaml_config.get('file_pattern', ''),
                    'delimiter': yaml_config.get('delimiter', ','),
                    'header_rows': yaml_config.get('header_rows', 1),
                    'encoding': yaml_config.get('encoding', 'utf-8'),
                    'date_format_hints': yaml_config.get('date_format_hints', []),
                    'amount_locale': yaml_config.get('amount_locale', 'en_US')
                },
                ai_mapping_config=yaml_config.get('ai_mapping', {}),
                data_quality_rules=yaml_config.get('dq_rules', {}),
                load_config=yaml_config.get('load', {}),
                recon_config=yaml_config.get('recon', {}),
                notification_config=yaml_config.get('notifications', {})
            )
            
            return config
            
        except Exception as e:
            logger.error(f"Failed to create config from YAML: {e}")
            return None
    
    def export_config_to_yaml(self, source_id: str, output_path: str) -> bool:
        """Export data source configuration to YAML format"""
        
        config = self.get_data_source_config(source_id)
        if not config:
            return False
            
        try:
            yaml_config = {
                'source_id': config.source_id,
                'mode': config.source_type.lower(),
                'location': config.connection_config,
                'file_pattern': config.file_config.get('file_pattern', ''),
                'entity': config.entity_type,
                'delimiter': config.file_config.get('delimiter', ','),
                'header_rows': config.file_config.get('header_rows', 1),
                'encoding': config.file_config.get('encoding', 'utf-8'),
                'date_format_hints': config.file_config.get('date_format_hints', []),
                'amount_locale': config.file_config.get('amount_locale', 'en_US'),
                'ai_mapping': config.ai_mapping_config or {},
                'dq_rules': config.data_quality_rules or {},
                'load': config.load_config or {},
                'notifications': config.notification_config or {},
                'recon': config.recon_config or {}
            }
            
            with open(output_path, 'w') as file:
                yaml.dump(yaml_config, file, default_flow_style=False, indent=2)
            
            logger.info(f"Exported config to YAML: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export config to YAML: {e}")
            return False
    
    def validate_config(self, config: DataSourceConfig) -> Dict[str, List[str]]:
        """Validate data source configuration"""
        
        errors = {}
        warnings = []
        
        # Required fields validation
        if not config.source_id:
            errors.setdefault('source_id', []).append("Source ID is required")
            
        if not config.entity_type:
            errors.setdefault('entity_type', []).append("Entity type is required")
            
        if not config.connection_config:
            errors.setdefault('connection_config', []).append("Connection configuration is required")
        
        # Source type specific validation
        if config.source_type == 'SFTP':
            sftp_config = config.connection_config.get('sftp', {})
            required_sftp_fields = ['host', 'username', 'remote_dir']
            
            for field in required_sftp_fields:
                if not sftp_config.get(field):
                    errors.setdefault('connection_config', []).append(f"SFTP {field} is required")
        
        elif config.source_type == 'FOLDER':
            if not config.connection_config.get('local_path'):
                errors.setdefault('connection_config', []).append("Local path is required for FOLDER source")
        
        # File configuration validation
        if not config.file_config.get('file_pattern'):
            warnings.append("File pattern not specified - may match unexpected files")
        
        # AI mapping validation
        if config.ai_mapping_config and config.ai_mapping_config.get('enabled', False):
            if not config.ai_mapping_config.get('model'):
                warnings.append("AI mapping enabled but no model specified")
        
        # Data quality rules validation
        if config.data_quality_rules:
            dq_rules = config.data_quality_rules
            if 'require_columns' in dq_rules and not isinstance(dq_rules['require_columns'], list):
                errors.setdefault('data_quality_rules', []).append("require_columns must be a list")
        
        return {'errors': errors, 'warnings': warnings}
    
    def get_configuration_summary(self) -> Dict[str, Any]:
        """Get summary of all configurations"""
        
        try:
            with self.db_manager.get_mysql_connector() as connection:
                cursor = connection.cursor(dictionary=True)
                
                # Data sources summary
                cursor.execute("""
                    SELECT source_type, entity_type, COUNT(*) as count
                    FROM cfg_data_sources 
                    WHERE is_active = TRUE
                    GROUP BY source_type, entity_type
                """)
                data_sources = cursor.fetchall()
                
                # Mapping examples summary
                cursor.execute("""
                    SELECT entity_type, COUNT(*) as count
                    FROM cfg_mapping_examples
                    WHERE is_active = TRUE
                    GROUP BY entity_type
                """)
                mapping_examples = cursor.fetchall()
                
                # Reconciliation rulesets summary
                cursor.execute("""
                    SELECT left_entity, right_entity, COUNT(*) as count
                    FROM cfg_recon_rulesets
                    WHERE is_active = TRUE
                    GROUP BY left_entity, right_entity
                """)
                recon_rulesets = cursor.fetchall()
                
                # Business rules summary
                cursor.execute("""
                    SELECT platform, rule_category, COUNT(*) as count
                    FROM cfg_business_rules
                    WHERE is_active = TRUE
                    GROUP BY platform, rule_category
                """)
                business_rules = cursor.fetchall()
                
                return {
                    'data_sources': data_sources,
                    'mapping_examples': mapping_examples,
                    'reconciliation_rulesets': recon_rulesets,
                    'business_rules': business_rules,
                    'last_updated': datetime.now().isoformat()
                }
            
        except Error as e:
            logger.error(f"Failed to get configuration summary: {e}")
            return {}

# Example usage and testing
def demo_configuration_manager():
    """Demonstrate the configuration manager"""
    
    config_mgr = ConfigurationManager()
    
    print("🔧 Configuration Manager Demo")
    print("=" * 40)
    
    # Get configuration summary
    summary = config_mgr.get_configuration_summary()
    print(f"📊 Configuration Summary:")
    print(f"  Data Sources: {len(summary.get('data_sources', []))}")
    print(f"  Mapping Examples: {len(summary.get('mapping_examples', []))}")
    print(f"  Reconciliation Rulesets: {len(summary.get('reconciliation_rulesets', []))}")
    print(f"  Business Rules: {len(summary.get('business_rules', []))}")
    
    # List data sources
    print(f"\n📁 Data Sources:")
    data_sources = config_mgr.list_data_sources()
    for ds in data_sources:
        print(f"  • {ds.source_id} ({ds.source_type}) → {ds.entity_type}")
    
    # Get HDFC bank config
    hdfc_config = config_mgr.get_data_source_config('bank_hdfc_ca')
    if hdfc_config:
        print(f"\n🏦 HDFC Bank Config:")
        print(f"  Name: {hdfc_config.source_name}")
        print(f"  Type: {hdfc_config.source_type}")
        print(f"  Entity: {hdfc_config.entity_type}")
        print(f"  File Pattern: {hdfc_config.file_config.get('file_pattern', 'N/A')}")
    
    # Get mapping examples for bank statements
    examples = config_mgr.get_mapping_examples('bank_statement')
    print(f"\n🔄 Bank Statement Mapping Examples: {len(examples)}")
    for ex in examples:
        print(f"  • {ex.example_name} (confidence: {ex.confidence_score:.2f})")
    
    return summary

if __name__ == "__main__":
    demo_configuration_manager()