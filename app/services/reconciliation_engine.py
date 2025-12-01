#!/usr/bin/env python3
"""
Restaurant Financial Reconciliation Engine
Multi-platform reconciliation system for restaurant/QSR operations
Handles POS vs Platform, Bank vs Settlement, Commission vs Payout reconciliation
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
from datetime import datetime, timedelta
import json
import uuid
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class ReconciliationType(Enum):
    """Types of reconciliation for restaurant operations"""
    POS_VS_PLATFORM = "pos_vs_platform"
    BANK_VS_SETTLEMENT = "bank_vs_settlement"
    COMMISSION_CALCULATION = "commission_calculation"
    SETTLEMENT_BATCH = "settlement_batch"
    PAYMENT_GATEWAY = "payment_gateway"
    CARD_SETTLEMENT = "card_settlement"

class MatchStatus(Enum):
    """Match status for reconciliation"""
    MATCHED = "MATCHED"
    UNMATCHED_LEFT = "UNMATCHED_LEFT" 
    UNMATCHED_RIGHT = "UNMATCHED_RIGHT"
    PARTIAL_MATCH = "PARTIAL_MATCH"
    EXCEPTION = "EXCEPTION"

@dataclass
class ReconciliationRule:
    """Reconciliation rule configuration"""
    rule_id: str
    rule_name: str
    left_entity: str  # Source table/dataset
    right_entity: str  # Target table/dataset  
    match_criteria: Dict[str, Any]
    priority: int = 1
    is_active: bool = True
    tolerance_amount: float = 1.0
    date_window_days: int = 2
    auto_match: bool = False

@dataclass 
class ReconciliationMatch:
    """Reconciliation match result"""
    match_id: str
    rule_id: str
    left_id: str
    right_id: str
    match_status: MatchStatus
    match_score: float
    amount_variance: float
    date_variance_days: int
    match_datetime: datetime
    comments: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ReconciliationException:
    """Reconciliation exception for manual review"""
    exception_id: str
    entity_type: str
    entity_id: str
    exception_type: str
    exception_reason: str
    amount_involved: float
    created_datetime: datetime
    status: str = "OPEN"
    assigned_to: Optional[str] = None
    resolution_notes: Optional[str] = None

class RestaurantReconciliationEngine:
    """Comprehensive reconciliation engine for restaurant financial data"""
    
    def __init__(self):
        from app.core.database import db_manager
        self.db_manager = db_manager
        self.db_config = db_manager.get_connection_dict()
        self.reconciliation_rules = self._load_reconciliation_rules()
        
    def _load_reconciliation_rules(self) -> List[ReconciliationRule]:
        """Load reconciliation rules from database"""
        rules = []
        
        try:
            with self.db_manager.get_mysql_connector() as connection:
                cursor = connection.cursor(dictionary=True)
                
                cursor.execute("""
                    SELECT rule_id, rule_name, left_entity, right_entity, 
                           match_criteria, priority, is_active
                    FROM recon_match_rules 
                    WHERE is_active = TRUE 
                    ORDER BY priority ASC
                """)
                
                for row in cursor.fetchall():
                    criteria = json.loads(row['match_criteria'])
                    
                    rule = ReconciliationRule(
                        rule_id=row['rule_id'],
                        rule_name=row['rule_name'],
                        left_entity=row['left_entity'],
                        right_entity=row['right_entity'],
                    match_criteria=criteria,
                    priority=row['priority'],
                    is_active=row['is_active'],
                    tolerance_amount=criteria.get('amount_tolerance', 1.0),
                    date_window_days=criteria.get('date_window_days', 2),
                    auto_match=criteria.get('auto_match', False)
                )
                rules.append(rule)
                
                logger.info(f"Loaded {len(rules)} reconciliation rules")
            
        except Error as e:
            logger.error(f"Failed to load reconciliation rules: {e}")
            # Return default rules if database fails
            rules = self._get_default_rules()
            
        return rules
    
    def _get_default_rules(self) -> List[ReconciliationRule]:
        """Get default reconciliation rules for restaurant operations"""
        return [
            ReconciliationRule(
                rule_id="swiggy_bank_settlement",
                rule_name="Swiggy Settlement vs Bank Credit",
                left_entity="payments",
                right_entity="bank_statements",
                match_criteria={
                    "provider": "SWIGGY",
                    "amount_tolerance": 1.0,
                    "date_window_days": 2,
                    "settlement_matching": True
                },
                priority=1
            ),
            ReconciliationRule(
                rule_id="zomato_bank_settlement", 
                rule_name="Zomato Settlement vs Bank Credit",
                left_entity="payments",
                right_entity="bank_statements",
                match_criteria={
                    "provider": "ZOMATO", 
                    "amount_tolerance": 1.0,
                    "date_window_days": 2,
                    "settlement_matching": True
                },
                priority=1
            ),
            ReconciliationRule(
                rule_id="pos_platform_reconciliation",
                rule_name="POS Orders vs Platform Orders", 
                left_entity="transactions",
                right_entity="payments",
                match_criteria={
                    "order_matching": True,
                    "amount_tolerance": 5.0,
                    "date_window_hours": 2
                },
                priority=2
            ),
            ReconciliationRule(
                rule_id="card_settlement_matching",
                rule_name="Card Transactions vs Bank Settlement",
                left_entity="payments", 
                right_entity="bank_statements",
                match_criteria={
                    "payment_method": "CARD",
                    "amount_tolerance": 0.1,
                    "date_window_days": 1,
                    "mdr_calculation": True
                },
                priority=3
            )
        ]
    
    def run_reconciliation(self, rule_id: Optional[str] = None, 
                          recon_type: Optional[ReconciliationType] = None,
                          date_from: Optional[datetime] = None,
                          date_to: Optional[datetime] = None) -> Dict[str, Any]:
        """Run reconciliation process"""
        
        logger.info("🔄 Starting Restaurant Financial Reconciliation")
        
        # Filter rules
        rules_to_process = self.reconciliation_rules
        if rule_id:
            rules_to_process = [r for r in rules_to_process if r.rule_id == rule_id]
        
        # Set default date range if not provided
        if not date_to:
            date_to = datetime.now()
        if not date_from:
            date_from = date_to - timedelta(days=7)
            
        results = {
            'run_id': str(uuid.uuid4()),
            'start_time': datetime.now(),
            'date_range': {'from': date_from, 'to': date_to},
            'rules_processed': [],
            'matches_found': 0,
            'exceptions_created': 0,
            'summary': {}
        }
        
        try:
            with self.db_manager.get_mysql_connector() as connection:
                for rule in rules_to_process:
                    logger.info(f"Processing rule: {rule.rule_name}")
                    
                    rule_result = self._process_reconciliation_rule(
                        connection, rule, date_from, date_to
                    )
                    
                    results['rules_processed'].append(rule_result)
                    results['matches_found'] += rule_result['matches_created']
                    results['exceptions_created'] += rule_result['exceptions_created']
            
            # Generate summary
            results['summary'] = self._generate_reconciliation_summary(results)
            results['end_time'] = datetime.now()
            results['duration_seconds'] = (results['end_time'] - results['start_time']).total_seconds()
            
            connection.close()
            
        except Error as e:
            logger.error(f"Reconciliation failed: {e}")
            results['error'] = str(e)
        
        return results
    
    def _process_reconciliation_rule(self, connection, rule: ReconciliationRule,
                                   date_from: datetime, date_to: datetime) -> Dict[str, Any]:
        """Process individual reconciliation rule"""
        
        logger.info(f"🔍 Processing rule: {rule.rule_id}")
        
        # Load datasets
        left_data = self._load_reconciliation_dataset(
            connection, rule.left_entity, date_from, date_to
        )
        right_data = self._load_reconciliation_dataset(
            connection, rule.right_entity, date_from, date_to  
        )
        
        logger.info(f"Loaded {len(left_data)} {rule.left_entity} and {len(right_data)} {rule.right_entity}")
        
        # Perform matching
        matches = []
        exceptions = []
        
        if rule.rule_id == "swiggy_bank_settlement":
            matches, exceptions = self._match_swiggy_bank_settlements(
                left_data, right_data, rule
            )
        elif rule.rule_id == "zomato_bank_settlement":
            matches, exceptions = self._match_zomato_bank_settlements(
                left_data, right_data, rule
            )
        elif rule.rule_id == "pos_platform_reconciliation":
            matches, exceptions = self._match_pos_platform_orders(
                left_data, right_data, rule
            )
        elif rule.rule_id == "card_settlement_matching":
            matches, exceptions = self._match_card_settlements(
                left_data, right_data, rule
            )
        else:
            matches, exceptions = self._generic_reconciliation_matching(
                left_data, right_data, rule
            )
        
        # Save results
        matches_created = self._save_reconciliation_matches(connection, matches)
        exceptions_created = self._save_reconciliation_exceptions(connection, exceptions)
        
        return {
            'rule_id': rule.rule_id,
            'rule_name': rule.rule_name,
            'left_records': len(left_data),
            'right_records': len(right_data), 
            'matches_created': matches_created,
            'exceptions_created': exceptions_created,
            'match_rate': matches_created / max(len(left_data), len(right_data), 1)
        }
    
    def _load_reconciliation_dataset(self, connection, entity_type: str,
                                   date_from: datetime, date_to: datetime) -> pd.DataFrame:
        """Load reconciliation dataset from database"""
        
        cursor = connection.cursor(dictionary=True)
        
        if entity_type == "payments":
            query = """
                SELECT payment_id, provider, payment_ref, payment_datetime, amount,
                       settlement_batch_id, status, created_at
                FROM fin_payments 
                WHERE payment_datetime BETWEEN %s AND %s
                ORDER BY payment_datetime DESC
            """
        elif entity_type == "transactions":
            query = """
                SELECT txn_id, order_id, txn_datetime, gross_amount, net_amount,
                       payment_method, status, source_system, created_at
                FROM fin_transactions
                WHERE txn_datetime BETWEEN %s AND %s  
                ORDER BY txn_datetime DESC
            """
        elif entity_type == "bank_statements":
            query = """
                SELECT stmt_line_id, account_id, statement_date, amount, 
                       debit_credit, bank_ref, narration, created_at
                FROM fin_bank_statements
                WHERE statement_date BETWEEN %s AND %s
                ORDER BY statement_date DESC  
            """
        else:
            logger.warning(f"Unknown entity type: {entity_type}")
            return pd.DataFrame()
        
        cursor.execute(query, (date_from, date_to))
        data = cursor.fetchall()
        
        return pd.DataFrame(data)
    
    def _match_swiggy_bank_settlements(self, payments_df: pd.DataFrame, 
                                     bank_df: pd.DataFrame, 
                                     rule: ReconciliationRule) -> Tuple[List[ReconciliationMatch], List[ReconciliationException]]:
        """Match Swiggy settlements with bank credits"""
        
        matches = []
        exceptions = []
        
        # Filter Swiggy payments
        swiggy_payments = payments_df[payments_df['provider'] == 'SWIGGY'].copy()
        bank_credits = bank_df[bank_df['debit_credit'] == 'CREDIT'].copy()
        
        logger.info(f"Matching {len(swiggy_payments)} Swiggy payments with {len(bank_credits)} bank credits")
        
        for _, payment in swiggy_payments.iterrows():
            best_match = None
            best_score = 0.0
            
            # Look for matching bank credits
            for _, bank_txn in bank_credits.iterrows():
                
                # Amount matching (with tolerance)
                amount_diff = abs(payment['amount'] - bank_txn['amount'])
                if amount_diff > rule.tolerance_amount:
                    continue
                
                # Date matching (within window)
                date_diff = abs((payment['payment_datetime'] - bank_txn['statement_date']).days)
                if date_diff > rule.date_window_days:
                    continue
                    
                # UTR/Reference matching
                utr_match = False
                if pd.notna(payment.get('settlement_batch_id')) and pd.notna(bank_txn.get('bank_ref')):
                    if str(payment['settlement_batch_id']) in str(bank_txn['bank_ref']):
                        utr_match = True
                
                # Narration matching for Swiggy
                narration_match = False
                if pd.notna(bank_txn.get('narration')):
                    narration_lower = str(bank_txn['narration']).lower()
                    if any(keyword in narration_lower for keyword in ['swiggy', 'swig', 'food']):
                        narration_match = True
                
                # Calculate match score
                score = 0.0
                score += 40 if amount_diff <= 0.01 else max(0, 40 - (amount_diff * 10))
                score += 30 if date_diff == 0 else max(0, 30 - (date_diff * 5))
                score += 20 if utr_match else 0
                score += 10 if narration_match else 0
                
                if score > best_score and score >= 60:  # Minimum 60% match
                    best_score = score
                    best_match = bank_txn
            
            # Create match or exception
            if best_match is not None:
                match = ReconciliationMatch(
                    match_id=str(uuid.uuid4()),
                    rule_id=rule.rule_id,
                    left_id=payment['payment_id'],
                    right_id=best_match['stmt_line_id'],
                    match_status=MatchStatus.MATCHED,
                    match_score=best_score/100,
                    amount_variance=abs(payment['amount'] - best_match['amount']),
                    date_variance_days=abs((payment['payment_datetime'] - best_match['statement_date']).days),
                    match_datetime=datetime.now(),
                    comments=f"Swiggy settlement matched with bank credit",
                    metadata={
                        'payment_amount': payment['amount'],
                        'bank_amount': best_match['amount'],
                        'utr': payment.get('settlement_batch_id'),
                        'bank_ref': best_match.get('bank_ref')
                    }
                )
                matches.append(match)
                
                # Remove matched bank transaction from further matching
                bank_credits = bank_credits[bank_credits['stmt_line_id'] != best_match['stmt_line_id']]
                
            else:
                # Create exception for unmatched payment
                exception = ReconciliationException(
                    exception_id=str(uuid.uuid4()),
                    entity_type="payment",
                    entity_id=payment['payment_id'],
                    exception_type="UNMATCHED_SETTLEMENT",
                    exception_reason="No matching bank credit found for Swiggy settlement",
                    amount_involved=payment['amount'],
                    created_datetime=datetime.now()
                )
                exceptions.append(exception)
        
        # Create exceptions for unmatched bank credits (potential Swiggy settlements)
        for _, bank_txn in bank_credits.iterrows():
            if pd.notna(bank_txn.get('narration')):
                narration_lower = str(bank_txn['narration']).lower()
                if any(keyword in narration_lower for keyword in ['swiggy', 'swig']):
                    exception = ReconciliationException(
                        exception_id=str(uuid.uuid4()),
                        entity_type="bank_statement",
                        entity_id=bank_txn['stmt_line_id'],
                        exception_type="UNMATCHED_BANK_CREDIT",
                        exception_reason="Potential Swiggy settlement without matching payment record",
                        amount_involved=bank_txn['amount'],
                        created_datetime=datetime.now()
                    )
                    exceptions.append(exception)
        
        return matches, exceptions
    
    def _match_zomato_bank_settlements(self, payments_df: pd.DataFrame,
                                     bank_df: pd.DataFrame,
                                     rule: ReconciliationRule) -> Tuple[List[ReconciliationMatch], List[ReconciliationException]]:
        """Match Zomato settlements with bank credits"""
        
        matches = []
        exceptions = []
        
        # Filter Zomato payments
        zomato_payments = payments_df[payments_df['provider'] == 'ZOMATO'].copy()
        bank_credits = bank_df[bank_df['debit_credit'] == 'CREDIT'].copy()
        
        logger.info(f"Matching {len(zomato_payments)} Zomato payments with {len(bank_credits)} bank credits")
        
        for _, payment in zomato_payments.iterrows():
            best_match = None
            best_score = 0.0
            
            for _, bank_txn in bank_credits.iterrows():
                
                # Amount matching
                amount_diff = abs(payment['amount'] - bank_txn['amount'])
                if amount_diff > rule.tolerance_amount:
                    continue
                
                # Date matching  
                date_diff = abs((payment['payment_datetime'] - bank_txn['statement_date']).days)
                if date_diff > rule.date_window_days:
                    continue
                
                # UTR matching for Zomato
                utr_match = False
                if pd.notna(payment.get('settlement_batch_id')) and pd.notna(bank_txn.get('bank_ref')):
                    if str(payment['settlement_batch_id']) in str(bank_txn['bank_ref']):
                        utr_match = True
                
                # Narration matching for Zomato
                narration_match = False
                if pd.notna(bank_txn.get('narration')):
                    narration_lower = str(bank_txn['narration']).lower()
                    if any(keyword in narration_lower for keyword in ['zomato', 'zom', 'food']):
                        narration_match = True
                
                # Calculate match score
                score = 0.0
                score += 40 if amount_diff <= 0.01 else max(0, 40 - (amount_diff * 10))
                score += 30 if date_diff == 0 else max(0, 30 - (date_diff * 5))
                score += 20 if utr_match else 0
                score += 10 if narration_match else 0
                
                if score > best_score and score >= 60:
                    best_score = score
                    best_match = bank_txn
            
            # Create match or exception (similar to Swiggy logic)
            if best_match is not None:
                match = ReconciliationMatch(
                    match_id=str(uuid.uuid4()),
                    rule_id=rule.rule_id,
                    left_id=payment['payment_id'],
                    right_id=best_match['stmt_line_id'],
                    match_status=MatchStatus.MATCHED,
                    match_score=best_score/100,
                    amount_variance=abs(payment['amount'] - best_match['amount']),
                    date_variance_days=abs((payment['payment_datetime'] - best_match['statement_date']).days),
                    match_datetime=datetime.now(),
                    comments="Zomato settlement matched with bank credit"
                )
                matches.append(match)
                bank_credits = bank_credits[bank_credits['stmt_line_id'] != best_match['stmt_line_id']]
            else:
                exception = ReconciliationException(
                    exception_id=str(uuid.uuid4()),
                    entity_type="payment",
                    entity_id=payment['payment_id'],
                    exception_type="UNMATCHED_SETTLEMENT",
                    exception_reason="No matching bank credit found for Zomato settlement",
                    amount_involved=payment['amount'],
                    created_datetime=datetime.now()
                )
                exceptions.append(exception)
        
        return matches, exceptions
    
    def _match_pos_platform_orders(self, transactions_df: pd.DataFrame,
                                 payments_df: pd.DataFrame,
                                 rule: ReconciliationRule) -> Tuple[List[ReconciliationMatch], List[ReconciliationException]]:
        """Match POS transactions with platform payments"""
        
        matches = []
        exceptions = []
        
        # Filter POS transactions
        pos_transactions = transactions_df[transactions_df['source_system'] == 'LEGACY_ORDERS'].copy()
        
        logger.info(f"Matching {len(pos_transactions)} POS transactions with {len(payments_df)} platform payments")
        
        for _, pos_txn in pos_transactions.iterrows():
            
            # Look for matching platform payment by order ID
            matching_payments = payments_df[
                payments_df['payment_ref'].str.contains(str(pos_txn.get('order_id', '')), na=False)
            ]
            
            if len(matching_payments) > 0:
                payment = matching_payments.iloc[0]  # Take first match
                
                # Validate amount similarity (allowing for commission deduction)
                amount_diff = abs(pos_txn['gross_amount'] - payment['amount'])
                commission_tolerance = pos_txn['gross_amount'] * 0.30  # Allow up to 30% commission
                
                if amount_diff <= commission_tolerance:
                    match = ReconciliationMatch(
                        match_id=str(uuid.uuid4()),
                        rule_id=rule.rule_id,
                        left_id=pos_txn['txn_id'],
                        right_id=payment['payment_id'],
                        match_status=MatchStatus.MATCHED,
                        match_score=0.95,  # High confidence for order ID match
                        amount_variance=amount_diff,
                        date_variance_days=0,
                        match_datetime=datetime.now(),
                        comments="POS transaction matched with platform payment by order ID"
                    )
                    matches.append(match)
                else:
                    # Amount variance too high - create exception
                    exception = ReconciliationException(
                        exception_id=str(uuid.uuid4()),
                        entity_type="transaction",
                        entity_id=pos_txn['txn_id'],
                        exception_type="AMOUNT_VARIANCE",
                        exception_reason=f"Order ID match but amount variance too high: ₹{amount_diff:.2f}",
                        amount_involved=pos_txn['gross_amount'],
                        created_datetime=datetime.now()
                    )
                    exceptions.append(exception)
            else:
                # No platform payment found
                exception = ReconciliationException(
                    exception_id=str(uuid.uuid4()),
                    entity_type="transaction",
                    entity_id=pos_txn['txn_id'],
                    exception_type="UNMATCHED_POS_ORDER",
                    exception_reason="POS transaction not found in platform payments",
                    amount_involved=pos_txn['gross_amount'],
                    created_datetime=datetime.now()
                )
                exceptions.append(exception)
        
        return matches, exceptions
    
    def _match_card_settlements(self, payments_df: pd.DataFrame,
                              bank_df: pd.DataFrame,
                              rule: ReconciliationRule) -> Tuple[List[ReconciliationMatch], List[ReconciliationException]]:
        """Match card payments with bank settlements"""
        
        matches = []
        exceptions = []
        
        # Filter card payments and bank credits
        card_payments = payments_df[payments_df.get('method', '').str.contains('CARD', na=False)].copy()
        bank_credits = bank_df[bank_df['debit_credit'] == 'CREDIT'].copy()
        
        logger.info(f"Matching {len(card_payments)} card payments with {len(bank_credits)} bank credits")
        
        # Group card payments by date for batch settlement matching
        for date, daily_payments in card_payments.groupby(card_payments['payment_datetime'].dt.date):
            daily_amount = daily_payments['amount'].sum()
            
            # Look for bank credits on same date or next day (T+1 settlement)
            settlement_dates = [date, date + timedelta(days=1)]
            
            for settle_date in settlement_dates:
                matching_credits = bank_credits[
                    bank_credits['statement_date'].dt.date == settle_date
                ]
                
                for _, bank_txn in matching_credits.iterrows():
                    # Check if amount matches (considering MDR deduction)
                    mdr_rate = 0.02  # Assume 2% MDR
                    expected_settlement = daily_amount * (1 - mdr_rate)
                    amount_diff = abs(expected_settlement - bank_txn['amount'])
                    
                    if amount_diff <= rule.tolerance_amount:
                        match = ReconciliationMatch(
                            match_id=str(uuid.uuid4()),
                            rule_id=rule.rule_id,
                            left_id=f"CARD_BATCH_{date}",
                            right_id=bank_txn['stmt_line_id'],
                            match_status=MatchStatus.MATCHED,
                            match_score=0.85,
                            amount_variance=amount_diff,
                            date_variance_days=(settle_date - date).days,
                            match_datetime=datetime.now(),
                            comments=f"Card settlement batch matched for {date}"
                        )
                        matches.append(match)
                        break
        
        return matches, exceptions
    
    def _generic_reconciliation_matching(self, left_df: pd.DataFrame,
                                       right_df: pd.DataFrame,
                                       rule: ReconciliationRule) -> Tuple[List[ReconciliationMatch], List[ReconciliationException]]:
        """Generic reconciliation matching algorithm"""
        
        matches = []
        exceptions = []
        
        logger.info(f"Generic matching: {len(left_df)} left records with {len(right_df)} right records")
        
        # Implement basic amount and date matching
        for _, left_record in left_df.iterrows():
            best_match = None
            best_score = 0.0
            
            for _, right_record in right_df.iterrows():
                score = self._calculate_generic_match_score(left_record, right_record, rule)
                
                if score > best_score and score >= 0.6:
                    best_score = score
                    best_match = right_record
            
            if best_match is not None:
                match = ReconciliationMatch(
                    match_id=str(uuid.uuid4()),
                    rule_id=rule.rule_id,
                    left_id=str(left_record.iloc[0]),  # First column as ID
                    right_id=str(best_match.iloc[0]),  # First column as ID
                    match_status=MatchStatus.MATCHED,
                    match_score=best_score,
                    amount_variance=0.0,
                    date_variance_days=0,
                    match_datetime=datetime.now(),
                    comments="Generic reconciliation match"
                )
                matches.append(match)
        
        return matches, exceptions
    
    def _calculate_generic_match_score(self, left_record: pd.Series, 
                                     right_record: pd.Series, 
                                     rule: ReconciliationRule) -> float:
        """Calculate generic match score between two records"""
        score = 0.0
        
        # Add scoring logic based on available fields
        # This is a simplified version - can be enhanced based on specific needs
        
        return score
    
    def _save_reconciliation_matches(self, connection, matches: List[ReconciliationMatch]) -> int:
        """Save reconciliation matches to database"""
        
        if not matches:
            return 0
            
        cursor = connection.cursor()
        
        insert_sql = """
            INSERT INTO recon_matches 
            (match_id, rule_id, left_entity_id, right_entity_id, match_status, 
             match_score, amount_variance, date_variance_days, match_datetime, comments)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        match_data = []
        for match in matches:
            match_data.append((
                match.match_id, match.rule_id, match.left_id, match.right_id,
                match.match_status.value, match.match_score, match.amount_variance,
                match.date_variance_days, match.match_datetime, match.comments
            ))
        
        cursor.executemany(insert_sql, match_data)
        connection.commit()
        
        logger.info(f"Saved {len(matches)} reconciliation matches")
        return len(matches)
    
    def _save_reconciliation_exceptions(self, connection, exceptions: List[ReconciliationException]) -> int:
        """Save reconciliation exceptions to database"""
        
        if not exceptions:
            return 0
            
        cursor = connection.cursor()
        
        insert_sql = """
            INSERT INTO recon_exceptions
            (exception_id, entity_type, entity_id, exception_type, exception_reason,
             amount_involved, status, created_datetime)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        exception_data = []
        for exception in exceptions:
            exception_data.append((
                exception.exception_id, exception.entity_type, exception.entity_id,
                exception.exception_type, exception.exception_reason, exception.amount_involved,
                exception.status, exception.created_datetime
            ))
        
        cursor.executemany(insert_sql, exception_data)
        connection.commit()
        
        logger.info(f"Saved {len(exceptions)} reconciliation exceptions")
        return len(exceptions)
    
    def _generate_reconciliation_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate reconciliation summary report"""
        
        summary = {
            'total_rules_processed': len(results['rules_processed']),
            'total_matches_found': results['matches_found'],
            'total_exceptions_created': results['exceptions_created'],
            'overall_match_rate': 0.0,
            'rule_performance': {},
            'top_exception_types': {},
            'recommendations': []
        }
        
        # Calculate overall match rate
        total_records = sum(r['left_records'] + r['right_records'] for r in results['rules_processed'])
        if total_records > 0:
            summary['overall_match_rate'] = results['matches_found'] / (total_records / 2)
        
        # Rule performance
        for rule_result in results['rules_processed']:
            summary['rule_performance'][rule_result['rule_id']] = {
                'match_rate': rule_result['match_rate'],
                'matches': rule_result['matches_created'],
                'exceptions': rule_result['exceptions_created']
            }
        
        # Recommendations
        if summary['overall_match_rate'] < 0.7:
            summary['recommendations'].append("Low match rate - review reconciliation rules and data quality")
        
        if summary['total_exceptions_created'] > 50:
            summary['recommendations'].append("High exception count - manual review required")
            
        return summary
    
    def get_reconciliation_dashboard_data(self, date_from: datetime, date_to: datetime) -> Dict[str, Any]:
        """Get reconciliation dashboard data"""
        
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor(dictionary=True)
            
            # Match statistics
            cursor.execute("""
                SELECT rule_id, COUNT(*) as match_count, AVG(match_score) as avg_score,
                       SUM(amount_variance) as total_variance
                FROM recon_matches 
                WHERE match_datetime BETWEEN %s AND %s
                GROUP BY rule_id
            """, (date_from, date_to))
            
            match_stats = cursor.fetchall()
            
            # Exception statistics
            cursor.execute("""
                SELECT exception_type, COUNT(*) as exception_count,
                       SUM(amount_involved) as total_amount
                FROM recon_exceptions
                WHERE created_datetime BETWEEN %s AND %s
                GROUP BY exception_type
            """, (date_from, date_to))
            
            exception_stats = cursor.fetchall()
            
            # Daily reconciliation trends
            cursor.execute("""
                SELECT DATE(match_datetime) as match_date, 
                       COUNT(*) as daily_matches,
                       AVG(match_score) as daily_avg_score
                FROM recon_matches
                WHERE match_datetime BETWEEN %s AND %s
                GROUP BY DATE(match_datetime)
                ORDER BY match_date
            """, (date_from, date_to))
            
            daily_trends = cursor.fetchall()
            
            return {
                'match_statistics': match_stats,
                'exception_statistics': exception_stats,
                'daily_trends': daily_trends,
                'date_range': {'from': date_from, 'to': date_to}
            }
            
        except Error as e:
            logger.error(f"Failed to get dashboard data: {e}")
            return {}

# Example usage
def demo_reconciliation_engine():
    """Demonstrate the reconciliation engine"""
    
    engine = RestaurantReconciliationEngine()
    
    # Run reconciliation for last 7 days
    results = engine.run_reconciliation(
        date_from=datetime.now() - timedelta(days=7),
        date_to=datetime.now()
    )
    
    print("🍕 Restaurant Reconciliation Engine Demo")
    print("=" * 50)
    print(f"Run ID: {results['run_id']}")
    print(f"Duration: {results.get('duration_seconds', 0):.1f} seconds")
    print(f"Rules Processed: {len(results['rules_processed'])}")
    print(f"Matches Found: {results['matches_found']}")
    print(f"Exceptions Created: {results['exceptions_created']}")
    
    if results.get('summary'):
        print(f"Overall Match Rate: {results['summary']['overall_match_rate']:.2%}")
    
    return results

if __name__ == "__main__":
    demo_reconciliation_engine()