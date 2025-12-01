#!/usr/bin/env python3
"""
Enhanced AI Column Mapper for Restaurant Financial Reconciliation Platform
Includes domain-specific examples for restaurant/QSR financial data processing
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Tuple, Optional, Any
import logging
from datetime import datetime, date
import json
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class FinancialDomain(Enum):
    """Financial data domains for specialized mapping"""
    RESTAURANT_POS = "restaurant_pos"
    PLATFORM_ORDERS = "platform_orders" 
    BANK_STATEMENTS = "bank_statements"
    CARD_SETTLEMENTS = "card_settlements"
    PAYMENT_GATEWAY = "payment_gateway"
    RECONCILIATION = "reconciliation"

@dataclass
class ColumnMapping:
    """Column mapping with confidence score and domain context"""
    source_column: str
    target_field: str
    confidence: float
    data_type: str
    domain: FinancialDomain
    transformation: Optional[str] = None
    validation_rules: Optional[List[str]] = None

class RestaurantDomainMapper:
    """Enhanced AI Column Mapper with restaurant/QSR financial domain expertise"""
    
    def __init__(self):
        self.domain_patterns = self._initialize_domain_patterns()
        self.platform_specific_patterns = self._initialize_platform_patterns()
        self.financial_field_mappings = self._initialize_financial_mappings()
        
    def _initialize_domain_patterns(self) -> Dict[FinancialDomain, Dict]:
        """Initialize domain-specific column patterns and examples"""
        return {
            FinancialDomain.RESTAURANT_POS: {
                'amount_patterns': [
                    'gross_amount', 'net_sale', 'subtotal', 'total_amount', 'bill_amount',
                    'order_value', 'transaction_amount', 'receipt_total', 'final_total'
                ],
                'tax_patterns': [
                    'gst_at_5_percent', 'gst_ecom_at_5_percent', 'tax_amount', 'sgst', 'cgst', 'igst',
                    'service_tax', 'vat', 'total_tax', 'tax_inclusive', 'tax_exclusive'
                ],
                'payment_patterns': [
                    'payment_mode', 'tender_type', 'payment_method', 'settlement_mode',
                    'cash', 'card', 'upi', 'wallet', 'credit_card', 'debit_card'
                ],
                'order_patterns': [
                    'bill_number', 'order_id', 'receipt_no', 'transaction_id', 'invoice_no',
                    'pos_id', 'terminal_id', 'store_code', 'outlet_id'
                ],
                'datetime_patterns': [
                    'bill_time', 'order_date', 'business_date', 'transaction_datetime',
                    'settlement_date', 'created_at', 'timestamp'
                ]
            },
            
            FinancialDomain.PLATFORM_ORDERS: {
                'swiggy_patterns': [
                    'order_no', 'restaurant_payout', 'payout_amount', 'net_payable_amount_after_tcs_and_tds',
                    'swiggy_platform_service_fee', 'delivery_fee', 'commission_value', 'tcs', 'tds',
                    'item_total', 'customer_payable', 'gst_deduction', 'current_utr', 'nodal_utr'
                ],
                'zomato_patterns': [
                    'order_id', 'final_amount', 'net_amount', 'bill_subtotal', 'commission_rate',
                    'commission_value', 'delivery_charge', 'pgcharge', 'tcs_amount', 'tds_amount',
                    'utr_number', 'payout_date', 'restaurant_id', 'res_id'
                ],
                'settlement_patterns': [
                    'utr_number', 'utr_date', 'settlement_ref', 'payout_date', 'settlement_batch',
                    'nodal_utr', 'current_utr', 'reference_number', 'settlement_id'
                ]
            },
            
            FinancialDomain.BANK_STATEMENTS: {
                'amount_patterns': [
                    'deposit_amt', 'withdrawal_amt', 'transaction_amount', 'credit_amount', 'debit_amount',
                    'balance', 'closing_balance', 'available_balance', 'ledger_balance'
                ],
                'reference_patterns': [
                    'utr_no', 'transaction_id', 'reference_no', 'cheque_no', 'bank_ref',
                    'customer_reference', 'payment_ref', 'batch_ref', 'settlement_ref'
                ],
                'account_patterns': [
                    'account_number', 'account_no', 'account_id', 'iban', 'bic', 'bank_code',
                    'branch_code', 'ifsc_code', 'routing_number'
                ],
                'date_patterns': [
                    'value_date', 'transaction_date', 'posting_date', 'business_date',
                    'settlement_date', 'processed_date'
                ]
            },
            
            FinancialDomain.RECONCILIATION: {
                'match_keys': [
                    'order_id', 'transaction_id', 'reference_no', 'utr_number', 'batch_id',
                    'settlement_ref', 'invoice_no', 'receipt_no', 'customer_id'
                ],
                'status_patterns': [
                    'status', 'order_status', 'payment_status', 'settlement_status',
                    'reconciliation_status', 'match_status', 'cleared', 'pending', 'failed'
                ],
                'exception_patterns': [
                    'disputed_order', 'cancellation', 'refund', 'chargeback', 'reversal',
                    'adjustment', 'variance', 'mismatch', 'unmatched', 'exception'
                ]
            }
        }
    
    def _initialize_platform_patterns(self) -> Dict[str, Dict]:
        """Initialize platform-specific patterns for major aggregators"""
        return {
            'swiggy': {
                'order_identifier': ['order_no', 'base_order_id', 'order_id'],
                'payout_amount': ['payout_amount', 'restaurant_payout', 'net_payable_amount_after_tcs_and_tds'],
                'commission': ['swiggy_platform_service_fee', 'total_swiggy_service_fee', 'swiggy_platform_fee_chargeable_on'],
                'delivery_fee': ['delivery_fee', 'logistics_charge'],
                'taxes': ['tcs', 'tds', 'gst_deduction', 'total_gst'],
                'settlement_ref': ['current_utr', 'nodal_utr', 'utr_number'],
                'customer_amount': ['customer_payable', 'item_total'],
                'order_date': ['order_date', 'business_date', 'payout_date']
            },
            
            'zomato': {
                'order_identifier': ['order_id', 'res_id'],
                'payout_amount': ['final_amount', 'net_amount', 'total_amount'],
                'commission': ['commission_value', 'commission_rate', 'percent'],
                'delivery_fee': ['delivery_charge', 'logistics_charge'],
                'payment_gateway': ['pgcharge', 'pg_chgs_percent', 'pg_applied_on'],
                'taxes': ['tcs_amount', 'tds_amount', 'source_tax', 'tax_paid_by_customer'],
                'settlement_ref': ['utr_number', 'utr_date'],
                'customer_amount': ['bill_subtotal', 'amount'],
                'order_date': ['order_date', 'payout_date']
            },
            
            'bank_settlement': {
                'credit_amount': ['deposit_amt', 'credit_amount', 'inward_amount'],
                'debit_amount': ['withdrawal_amt', 'debit_amount', 'outward_amount'],
                'reference': ['utr_no', 'transaction_id', 'reference_no', 'bank_ref'],
                'narration': ['narration', 'description', 'particulars', 'remarks'],
                'balance': ['closing_balance', 'available_balance', 'ledger_balance'],
                'date': ['value_date', 'transaction_date', 'posting_date'],
                'account': ['account_number', 'account_no']
            }
        }
    
    def _initialize_financial_mappings(self) -> Dict[str, Dict]:
        """Initialize standardized financial field mappings"""
        return {
            'transaction_fields': {
                'transaction_id': ['txn_id', 'transaction_id', 'order_id', 'receipt_no', 'bill_number'],
                'amount': ['gross_amount', 'total_amount', 'net_amount', 'final_amount', 'bill_total'],
                'tax_amount': ['tax_amount', 'gst_amount', 'total_tax', 'service_tax'],
                'net_amount': ['net_sale', 'net_amount', 'amount_after_tax'],
                'payment_method': ['payment_mode', 'tender_type', 'payment_method'],
                'transaction_date': ['order_date', 'business_date', 'txn_datetime', 'created_at'],
                'customer_id': ['customer_id', 'bill_user', 'user_id'],
                'store_id': ['store_code', 'outlet_id', 'branch_code', 'terminal_id']
            },
            
            'payment_fields': {
                'payment_id': ['payment_id', 'txn_id', 'reference_no', 'utr_number'],
                'provider': ['provider', 'platform', 'gateway', 'bank_name'],
                'amount': ['payout_amount', 'settlement_amount', 'final_amount', 'net_amount'],
                'commission': ['commission_value', 'platform_fee', 'service_fee', 'mdr'],
                'settlement_date': ['payout_date', 'settlement_date', 'utr_date'],
                'settlement_ref': ['utr_number', 'settlement_ref', 'batch_id', 'nodal_utr'],
                'status': ['status', 'payment_status', 'settlement_status']
            },
            
            'bank_statement_fields': {
                'statement_line_id': ['stmt_line_id', 'transaction_id', 'seq_no'],
                'account_id': ['account_number', 'account_no', 'account_id'],
                'amount': ['amount', 'transaction_amount', 'credit_amount', 'debit_amount'],
                'debit_credit': ['dr_cr', 'transaction_type', 'debit_credit'],
                'balance': ['balance', 'closing_balance', 'available_balance'],
                'narration': ['narration', 'description', 'particulars', 'memo'],
                'bank_reference': ['reference_no', 'utr_no', 'cheque_no', 'bank_ref'],
                'value_date': ['value_date', 'transaction_date', 'posting_date']
            }
        }
    
    def analyze_file_structure(self, df: pd.DataFrame, filename: str = "") -> Dict[str, Any]:
        """Analyze file structure and determine likely domain"""
        analysis = {
            'filename': filename,
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'likely_domain': None,
            'confidence': 0.0,
            'data_types': {},
            'sample_data': {},
            'domain_indicators': {}
        }
        
        # Analyze data types and sample data
        for col in df.columns:
            analysis['data_types'][col] = str(df[col].dtype)
            sample_values = df[col].dropna().head(3).tolist()
            analysis['sample_data'][col] = [str(val) for val in sample_values]
        
        # Determine likely domain based on column patterns
        domain_scores = {}
        for domain in FinancialDomain:
            score = self._calculate_domain_score(df.columns, domain)
            domain_scores[domain.value] = score
            
        # Find best matching domain
        best_domain = max(domain_scores, key=domain_scores.get)
        analysis['likely_domain'] = best_domain
        analysis['confidence'] = domain_scores[best_domain]
        analysis['domain_indicators'] = domain_scores
        
        # Add platform-specific indicators
        platform_scores = self._detect_platform_patterns(df.columns)
        analysis['platform_indicators'] = platform_scores
        
        return analysis
    
    def _calculate_domain_score(self, columns: List[str], domain: FinancialDomain) -> float:
        """Calculate confidence score for a specific domain"""
        if domain not in self.domain_patterns:
            return 0.0
            
        domain_data = self.domain_patterns[domain]
        total_patterns = 0
        matched_patterns = 0
        
        for pattern_type, patterns in domain_data.items():
            total_patterns += len(patterns)
            for pattern in patterns:
                if any(self._fuzzy_match(pattern, col) for col in columns):
                    matched_patterns += 1
        
        return matched_patterns / total_patterns if total_patterns > 0 else 0.0
    
    def _detect_platform_patterns(self, columns: List[str]) -> Dict[str, float]:
        """Detect platform-specific patterns in column names"""
        platform_scores = {}
        
        for platform, patterns in self.platform_specific_patterns.items():
            score = 0.0
            total_patterns = sum(len(p) for p in patterns.values())
            
            for pattern_type, pattern_list in patterns.items():
                for pattern in pattern_list:
                    if any(self._fuzzy_match(pattern, col) for col in columns):
                        score += 1
            
            platform_scores[platform] = score / total_patterns if total_patterns > 0 else 0.0
        
        return platform_scores
    
    def _fuzzy_match(self, pattern: str, column: str, threshold: float = 0.8) -> bool:
        """Fuzzy matching for column names with restaurant domain awareness"""
        pattern_lower = pattern.lower()
        column_lower = column.lower()
        
        # Exact match
        if pattern_lower == column_lower:
            return True
        
        # Contains match
        if pattern_lower in column_lower or column_lower in pattern_lower:
            return True
        
        # Handle common variations
        variations = {
            'amount': ['amt', 'value', 'val', 'total', 'sum'],
            'number': ['no', 'num', 'id', 'ref'],
            'date': ['dt', 'time', 'timestamp', 'ts'],
            'reference': ['ref', 'no', 'num', 'id'],
            'transaction': ['txn', 'trans', 'trx'],
            'payment': ['pay', 'pmt', 'settle'],
            'customer': ['cust', 'client', 'user'],
            'restaurant': ['rest', 'store', 'outlet', 'branch']
        }
        
        for base_word, variants in variations.items():
            if base_word in pattern_lower:
                if any(var in column_lower for var in variants):
                    return True
            if base_word in column_lower:
                if any(var in pattern_lower for var in variants):
                    return True
        
        # Levenshtein distance based matching
        return self._levenshtein_similarity(pattern_lower, column_lower) >= threshold
    
    def _levenshtein_similarity(self, s1: str, s2: str) -> float:
        """Calculate Levenshtein similarity between two strings"""
        if len(s1) < len(s2):
            return self._levenshtein_similarity(s2, s1)
        
        if len(s2) == 0:
            return 0.0
        
        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        max_len = max(len(s1), len(s2))
        return (max_len - previous_row[-1]) / max_len
    
    def generate_field_mappings(self, df: pd.DataFrame, target_schema: str, 
                               domain_override: Optional[FinancialDomain] = None) -> List[ColumnMapping]:
        """Generate field mappings for restaurant financial data"""
        
        # Analyze file structure
        analysis = self.analyze_file_structure(df)
        
        # Determine domain
        if domain_override:
            domain = domain_override
        else:
            domain = FinancialDomain(analysis['likely_domain'])
        
        # Get target schema fields
        if target_schema not in self.financial_field_mappings:
            raise ValueError(f"Unknown target schema: {target_schema}")
        
        target_fields = self.financial_field_mappings[target_schema]
        mappings = []
        
        # Generate mappings
        for target_field, possible_sources in target_fields.items():
            best_match = None
            best_confidence = 0.0
            
            for col in df.columns:
                for source_pattern in possible_sources:
                    if self._fuzzy_match(source_pattern, col, threshold=0.7):
                        confidence = self._calculate_mapping_confidence(
                            col, target_field, df[col], domain
                        )
                        
                        if confidence > best_confidence:
                            best_confidence = confidence
                            best_match = col
            
            if best_match and best_confidence > 0.5:
                # Determine data type and transformation
                data_type = self._determine_data_type(df[best_match], target_field)
                transformation = self._suggest_transformation(
                    df[best_match], target_field, domain
                )
                validation_rules = self._generate_validation_rules(
                    target_field, domain
                )
                
                mapping = ColumnMapping(
                    source_column=best_match,
                    target_field=target_field,
                    confidence=best_confidence,
                    data_type=data_type,
                    domain=domain,
                    transformation=transformation,
                    validation_rules=validation_rules
                )
                mappings.append(mapping)
        
        return mappings
    
    def _calculate_mapping_confidence(self, source_col: str, target_field: str, 
                                    data_series: pd.Series, domain: FinancialDomain) -> float:
        """Calculate confidence score for a specific mapping"""
        base_confidence = 0.5
        
        # Column name similarity
        name_similarity = self._levenshtein_similarity(
            source_col.lower(), target_field.lower()
        )
        base_confidence += name_similarity * 0.3
        
        # Data type compatibility
        if self._is_data_type_compatible(data_series, target_field):
            base_confidence += 0.1
        
        # Domain-specific boosts
        domain_boost = self._get_domain_specific_boost(
            source_col, target_field, domain
        )
        base_confidence += domain_boost
        
        # Data quality checks
        quality_score = self._assess_data_quality(data_series, target_field)
        base_confidence += quality_score * 0.1
        
        return min(base_confidence, 1.0)
    
    def _is_data_type_compatible(self, series: pd.Series, target_field: str) -> bool:
        """Check if data type is compatible with target field"""
        numeric_fields = ['amount', 'commission', 'tax_amount', 'balance']
        date_fields = ['date', 'timestamp', 'datetime']
        
        if any(field in target_field for field in numeric_fields):
            return pd.api.types.is_numeric_dtype(series)
        elif any(field in target_field for field in date_fields):
            # Try to parse a sample as date
            try:
                pd.to_datetime(series.dropna().iloc[0])
                return True
            except:
                return False
        
        return True
    
    def _get_domain_specific_boost(self, source_col: str, target_field: str, 
                                  domain: FinancialDomain) -> float:
        """Get domain-specific confidence boost"""
        boosts = {
            FinancialDomain.PLATFORM_ORDERS: {
                ('utr', 'settlement_ref'): 0.2,
                ('payout', 'amount'): 0.2,
                ('commission', 'commission'): 0.2,
                ('order_id', 'payment_ref'): 0.15
            },
            FinancialDomain.BANK_STATEMENTS: {
                ('utr_no', 'bank_reference'): 0.2,
                ('deposit_amt', 'amount'): 0.2,
                ('account_number', 'account_id'): 0.2
            },
            FinancialDomain.RESTAURANT_POS: {
                ('gross_amount', 'amount'): 0.2,
                ('bill_number', 'order_id'): 0.2,
                ('payment_mode', 'payment_method'): 0.15
            }
        }
        
        if domain in boosts:
            for (source_pattern, target_pattern), boost in boosts[domain].items():
                if source_pattern in source_col.lower() and target_pattern in target_field.lower():
                    return boost
        
        return 0.0
    
    def _assess_data_quality(self, series: pd.Series, target_field: str) -> float:
        """Assess data quality for the series"""
        quality_score = 0.0
        
        # Non-null percentage
        non_null_pct = series.notna().sum() / len(series)
        quality_score += non_null_pct * 0.5
        
        # Unique values (for non-amount fields)
        if 'amount' not in target_field:
            unique_pct = series.nunique() / len(series)
            quality_score += min(unique_pct, 0.5) * 0.3
        
        # Value range reasonableness for amounts
        if 'amount' in target_field and pd.api.types.is_numeric_dtype(series):
            numeric_series = pd.to_numeric(series, errors='coerce')
            if numeric_series.min() >= 0 and numeric_series.max() < 1000000:
                quality_score += 0.2
        
        return quality_score
    
    def _determine_data_type(self, series: pd.Series, target_field: str) -> str:
        """Determine appropriate data type for target field"""
        if 'amount' in target_field or 'commission' in target_field:
            return 'DECIMAL(15,2)'
        elif 'date' in target_field or 'timestamp' in target_field:
            return 'DATETIME'
        elif 'id' in target_field or 'ref' in target_field:
            return 'VARCHAR(255)'
        elif 'status' in target_field or 'method' in target_field:
            return 'VARCHAR(50)'
        else:
            return 'TEXT'
    
    def _suggest_transformation(self, series: pd.Series, target_field: str, 
                              domain: FinancialDomain) -> Optional[str]:
        """Suggest data transformation for the mapping"""
        transformations = []
        
        # Amount transformations
        if 'amount' in target_field:
            if not pd.api.types.is_numeric_dtype(series):
                transformations.append("CAST(REPLACE(REPLACE(value, ',', ''), '₹', '') AS DECIMAL(15,2))")
            else:
                transformations.append("ROUND(value, 2)")
        
        # Date transformations
        elif 'date' in target_field:
            sample_value = str(series.dropna().iloc[0]) if len(series.dropna()) > 0 else ""
            if re.match(r'\d{2}/\d{2}/\d{4}', sample_value):
                transformations.append("STR_TO_DATE(value, '%d/%m/%Y')")
            elif re.match(r'\d{4}-\d{2}-\d{2}', sample_value):
                transformations.append("DATE(value)")
        
        # Status standardization
        elif 'status' in target_field:
            transformations.append("UPPER(TRIM(value))")
        
        return transformations[0] if transformations else None
    
    def _generate_validation_rules(self, target_field: str, 
                                  domain: FinancialDomain) -> List[str]:
        """Generate validation rules for the field"""
        rules = []
        
        if 'amount' in target_field:
            rules.extend([
                "value >= 0",
                "value < 1000000",
                "value IS NOT NULL"
            ])
        
        if 'date' in target_field:
            rules.extend([
                "value IS NOT NULL",
                "value <= NOW()",
                "value >= '2020-01-01'"
            ])
        
        if target_field in ['status', 'payment_method']:
            rules.append("value IS NOT NULL AND value != ''")
        
        # Domain-specific rules
        if domain == FinancialDomain.PLATFORM_ORDERS:
            if 'commission' in target_field:
                rules.append("value BETWEEN 0 AND 50")  # Commission % should be reasonable
        
        return rules
    
    def generate_mapping_report(self, mappings: List[ColumnMapping], 
                               analysis: Dict[str, Any]) -> str:
        """Generate detailed mapping report for restaurant financial data"""
        
        report = f"""
# Restaurant Financial Data Mapping Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## File Analysis
- **Filename**: {analysis['filename']}
- **Total Rows**: {analysis['total_rows']:,}
- **Total Columns**: {analysis['total_columns']}
- **Detected Domain**: {analysis['likely_domain']} (confidence: {analysis['confidence']:.2%})

## Platform Indicators
"""
        
        for platform, confidence in analysis['platform_indicators'].items():
            if confidence > 0.1:
                report += f"- **{platform.title()}**: {confidence:.2%} match\n"
        
        report += f"""
## Column Mappings ({len(mappings)} mapped)
"""
        
        # Sort mappings by confidence
        sorted_mappings = sorted(mappings, key=lambda x: x.confidence, reverse=True)
        
        for mapping in sorted_mappings:
            confidence_icon = "🟢" if mapping.confidence > 0.8 else "🟡" if mapping.confidence > 0.6 else "🟠"
            report += f"""
### {confidence_icon} {mapping.source_column} → {mapping.target_field}
- **Confidence**: {mapping.confidence:.2%}
- **Data Type**: {mapping.data_type}
- **Domain**: {mapping.domain.value}
"""
            if mapping.transformation:
                report += f"- **Transformation**: `{mapping.transformation}`\n"
            
            if mapping.validation_rules:
                report += f"- **Validation Rules**: {', '.join(mapping.validation_rules)}\n"
        
        # Unmapped columns
        mapped_sources = {m.source_column for m in mappings}
        unmapped = [col for col in analysis['columns'] if col not in mapped_sources]
        
        if unmapped:
            report += f"""
## Unmapped Columns ({len(unmapped)})
These columns could not be automatically mapped:
"""
            for col in unmapped:
                sample_data = analysis['sample_data'].get(col, [])
                report += f"- **{col}**: {analysis['data_types'].get(col, 'unknown')} - Sample: {sample_data}\n"
        
        report += f"""
## Recommendations
1. **High Confidence Mappings**: {len([m for m in mappings if m.confidence > 0.8])} fields can be automatically mapped
2. **Review Required**: {len([m for m in mappings if 0.6 < m.confidence <= 0.8])} fields should be manually verified  
3. **Manual Mapping**: {len([m for m in mappings if m.confidence <= 0.6]) + len(unmapped)} fields require manual attention

## Next Steps
1. Review and approve high-confidence mappings
2. Manually map critical business fields from unmapped columns
3. Set up data validation rules for reconciliation accuracy
4. Configure reconciliation rules for platform-specific patterns
"""
        
        return report

# Example usage and testing
def demo_restaurant_mapper():
    """Demonstrate the restaurant domain mapper capabilities"""
    
    mapper = RestaurantDomainMapper()
    
    # Sample Zomato data
    zomato_data = pd.DataFrame({
        'order_id': ['ZOM123', 'ZOM124', 'ZOM125'],
        'final_amount': [419.62, 380.50, 275.30],
        'commission_value': [25.18, 22.83, 16.52],
        'utr_number': ['UTR001', 'UTR002', 'UTR003'],
        'payout_date': ['2024-01-15', '2024-01-16', '2024-01-17'],
        'res_id': ['REST001', 'REST001', 'REST001']
    })
    
    # Analyze and map
    analysis = mapper.analyze_file_structure(zomato_data, 'zomato_settlements.csv')
    mappings = mapper.generate_field_mappings(zomato_data, 'payment_fields')
    report = mapper.generate_mapping_report(mappings, analysis)
    
    print("🍕 Restaurant Financial Data Mapping Demo")
    print("=" * 50)
    print(report)
    
    return mapper, analysis, mappings

if __name__ == "__main__":
    demo_restaurant_mapper()