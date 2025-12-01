"""
Exception Handler - Process 2
==============================
Identifies and categorizes unmatched records and data quality issues:
1. Missing TRM (POS transaction with no payment gateway record)
2. Missing MPR (TRM transaction with no merchant payment record)
3. Missing Bank (MPR transaction with no bank settlement)
4. Amount Mismatch (transactions with variance beyond tolerance)
5. Duplicate Entries (same transaction appearing multiple times)
6. Data Quality Issues (missing key fields, invalid data)

Created: November 12, 2025
"""

import sys
import os
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Tuple, Optional
import mysql.connector
from mysql.connector import Error

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app import config
from app.logging_config import setup_logging

setup_logging('INFO')
logger = logging.getLogger('exception_handler')


class ExceptionHandler:
    """Handles identification, categorization, and storage of exceptions"""
    
    # Exception categories
    EXCEPTION_TYPES = {
        'MISSING_TRM': 'POS transaction with no TRM (payment gateway) record',
        'MISSING_MPR': 'TRM transaction with no MPR (merchant portal) record',
        'MISSING_BANK': 'MPR transaction with no Bank settlement record',
        'MISSING_POS': 'Bank credit with no matching POS transaction',
        'AMOUNT_MISMATCH': 'Transaction chain with amount variance beyond tolerance',
        'DUPLICATE_TRM': 'Duplicate TRM transaction (same RRN)',
        'DUPLICATE_MPR': 'Duplicate MPR transaction (same UTR/RRN)',
        'DUPLICATE_BANK': 'Duplicate bank credit (same reference)',
        'DATA_QUALITY': 'Missing or invalid critical fields',
        'SETTLEMENT_DELAY': 'Settlement beyond expected timeframe (>3 days)',
        'PARTIAL_MATCH': 'Partial chain match (not all sources linked)'
    }
    
    # Severity levels
    SEVERITY_CRITICAL = 'CRITICAL'  # Financial impact, requires immediate action
    SEVERITY_HIGH = 'HIGH'          # Likely financial impact, needs investigation
    SEVERITY_MEDIUM = 'MEDIUM'      # Potential issue, should be reviewed
    SEVERITY_LOW = 'LOW'            # Minor issue, for information only
    
    def __init__(self, conn=None, cursor=None):
        """Initialize exception handler"""
        self.conn = conn
        self.cursor = cursor
        self.own_connection = False
        self.exceptions = []
        self.exception_summary = {
            'total_exceptions': 0,
            'by_type': {},
            'by_severity': {
                'CRITICAL': 0,
                'HIGH': 0,
                'MEDIUM': 0,
                'LOW': 0
            },
            'total_amount_at_risk': 0.0
        }
    
    def connect_db(self):
        """Establish database connection if not provided"""
        if self.conn and self.cursor:
            return True
        
        try:
            from app.core.database import db_manager
            self.conn = db_manager.get_mysql_connector().__enter__()
            self.cursor = self.conn.cursor(dictionary=True)
            self.own_connection = True
            logger.info("✅ Connected to database")
            return True
        except Error as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def disconnect_db(self):
        """Close database connection if owned by this instance"""
        if self.own_connection:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("🔌 Database connection closed")
    
    def identify_missing_trm(self, pos_records: List[Dict], trm_records: List[Dict]) -> List[Dict]:
        """
        Find POS transactions that have no matching TRM record
        Indicates: Payment gateway didn't record the transaction
        
        Returns:
            List of exception dictionaries
        """
        logger.info("🔍 Identifying POS transactions with missing TRM...")
        
        exceptions = []
        
        # Create lookup set of TRM invoice numbers and transaction IDs
        trm_invoices = {trm.get('invoice') for trm in trm_records if trm.get('invoice')}
        trm_txn_ids = {trm.get('transaction_id') for trm in trm_records if trm.get('transaction_id')}
        
        for pos in pos_records:
            # Skip cash transactions
            if pos.get('settlement_mode') == 'CASH':
                continue
            
            # Check if POS bill number or transaction number appears in TRM
            bill_num = pos.get('bill_number')
            txn_num = pos.get('transaction_number')
            
            found_in_trm = False
            if bill_num in trm_invoices or txn_num in trm_txn_ids:
                found_in_trm = True
            
            if not found_in_trm:
                # Determine severity based on amount
                amount = Decimal(str(pos.get('net_sale', 0)))
                if amount > Decimal('10000'):
                    severity = self.SEVERITY_CRITICAL
                elif amount > Decimal('5000'):
                    severity = self.SEVERITY_HIGH
                elif amount > Decimal('1000'):
                    severity = self.SEVERITY_MEDIUM
                else:
                    severity = self.SEVERITY_LOW
                
                exception = {
                    'exception_type': 'MISSING_TRM',
                    'severity': severity,
                    'transaction_date': pos.get('transaction_date'),
                    'store_name': pos.get('store_name'),
                    'source_system': 'POS',
                    'source_record_id': pos.get('id'),
                    'transaction_ref': pos.get('transaction_number'),
                    'amount': float(amount),
                    'description': f"POS transaction {pos.get('bill_number')} has no TRM record",
                    'details': {
                        'pos_id': pos.get('id'),
                        'bill_number': pos.get('bill_number'),
                        'transaction_number': pos.get('transaction_number'),
                        'settlement_mode': pos.get('settlement_mode'),
                        'amount': float(amount)
                    },
                    'recommended_action': 'Verify with payment gateway. Check if transaction was processed.',
                    'detected_at': datetime.now()
                }
                exceptions.append(exception)
                self.exception_summary['total_amount_at_risk'] += float(amount)
        
        logger.info(f"  ⚠️ Found {len(exceptions)} POS transactions with missing TRM")
        return exceptions
    
    def identify_missing_mpr(self, trm_records: List[Dict], mpr_upi: List[Dict], mpr_card: List[Dict]) -> List[Dict]:
        """
        Find TRM transactions that have no matching MPR record
        Indicates: Payment gateway processed but merchant portal didn't record
        
        Returns:
            List of exception dictionaries
        """
        logger.info("🔍 Identifying TRM transactions with missing MPR...")
        
        exceptions = []
        
        # Create lookup sets for MPR
        mpr_rrns = set()
        for mpr in mpr_upi:
            if mpr.get('rrn'):
                mpr_rrns.add(mpr['rrn'])
        for mpr in mpr_card:
            if mpr.get('rrn'):
                mpr_rrns.add(mpr['rrn'])
        
        for trm in trm_records:
            rrn = trm.get('rrn')
            
            if rrn and rrn not in mpr_rrns:
                amount = Decimal(str(trm.get('amount', 0)))
                
                if amount > Decimal('10000'):
                    severity = self.SEVERITY_CRITICAL
                elif amount > Decimal('5000'):
                    severity = self.SEVERITY_HIGH
                else:
                    severity = self.SEVERITY_MEDIUM
                
                exception = {
                    'exception_type': 'MISSING_MPR',
                    'severity': severity,
                    'transaction_date': trm.get('transaction_date'),
                    'store_name': trm.get('store_name'),
                    'source_system': 'TRM',
                    'source_record_id': trm.get('uid'),
                    'transaction_ref': trm.get('transaction_id'),
                    'amount': float(amount),
                    'description': f"TRM transaction RRN {rrn} has no MPR record",
                    'details': {
                        'trm_uid': trm.get('uid'),
                        'transaction_id': trm.get('transaction_id'),
                        'rrn': rrn,
                        'approval_code': trm.get('approval_code'),
                        'payment_mode': trm.get('payment_mode'),
                        'amount': float(amount)
                    },
                    'recommended_action': 'Check MPR data for this period. Verify RRN in merchant portal.',
                    'detected_at': datetime.now()
                }
                exceptions.append(exception)
                self.exception_summary['total_amount_at_risk'] += float(amount)
        
        logger.info(f"  ⚠️ Found {len(exceptions)} TRM transactions with missing MPR")
        return exceptions
    
    def identify_missing_bank(self, mpr_upi: List[Dict], mpr_card: List[Dict], bank_records: List[Dict]) -> List[Dict]:
        """
        Find MPR transactions that have no matching Bank settlement
        Indicates: Payment processed but not settled to bank account
        
        Returns:
            List of exception dictionaries
        """
        logger.info("🔍 Identifying MPR transactions with missing Bank settlement...")
        
        exceptions = []
        
        # Create lookup sets for Bank
        bank_utrs = set()
        bank_refs = set()
        for bank in bank_records:
            if bank.get('narration'):
                # Extract potential UTR/reference numbers from narration
                narration = str(bank['narration'])
                bank_utrs.add(narration)
            if bank.get('bank_ref'):
                bank_refs.add(bank['bank_ref'])
        
        # Check MPR UPI
        for mpr in mpr_upi:
            utr = mpr.get('utr_number')
            
            if utr:
                # Check if UTR appears in any bank narration
                found = any(utr in narr for narr in bank_utrs)
                
                if not found:
                    # Check settlement date - if recent, might just be delayed
                    settlement_date = mpr.get('settlement_date')
                    if settlement_date:
                        days_since_settlement = (datetime.now().date() - settlement_date).days
                        
                        if days_since_settlement > 3:
                            severity = self.SEVERITY_CRITICAL
                        elif days_since_settlement > 1:
                            severity = self.SEVERITY_HIGH
                        else:
                            severity = self.SEVERITY_MEDIUM
                    else:
                        severity = self.SEVERITY_HIGH
                    
                    amount = Decimal(str(mpr.get('transaction_amount', 0)))
                    
                    exception = {
                        'exception_type': 'MISSING_BANK',
                        'severity': severity,
                        'transaction_date': mpr.get('transaction_date'),
                        'store_name': mpr.get('store_name'),
                        'source_system': 'MPR_UPI',
                        'source_record_id': mpr.get('uid'),
                        'transaction_ref': mpr.get('transaction_id'),
                        'amount': float(amount),
                        'description': f"MPR UPI transaction UTR {utr} has no Bank settlement",
                        'details': {
                            'mpr_uid': mpr.get('uid'),
                            'transaction_id': mpr.get('transaction_id'),
                            'utr_number': utr,
                            'rrn': mpr.get('rrn'),
                            'settlement_date': str(settlement_date) if settlement_date else None,
                            'amount': float(amount),
                            'days_since_settlement': days_since_settlement if settlement_date else None
                        },
                        'recommended_action': 'Check bank statement for this UTR. Contact bank if settlement is delayed.',
                        'detected_at': datetime.now()
                    }
                    exceptions.append(exception)
                    self.exception_summary['total_amount_at_risk'] += float(amount)
        
        logger.info(f"  ⚠️ Found {len(exceptions)} MPR transactions with missing Bank settlement")
        return exceptions
    
    def identify_amount_mismatches(self, matched_records: List[Dict], tolerance: Decimal = Decimal('100.00')) -> List[Dict]:
        """
        Find matched transactions with amount variance beyond tolerance
        
        Args:
            matched_records: List of matched transaction chains
            tolerance: Maximum acceptable variance in rupees
        
        Returns:
            List of exception dictionaries
        """
        logger.info(f"🔍 Identifying amount mismatches (tolerance: ₹{tolerance})...")
        
        exceptions = []
        
        for match in matched_records:
            variance = Decimal(str(match.get('total_variance', 0)))
            
            if variance > tolerance:
                # Determine severity based on variance amount
                if variance > Decimal('1000'):
                    severity = self.SEVERITY_CRITICAL
                elif variance > Decimal('500'):
                    severity = self.SEVERITY_HIGH
                else:
                    severity = self.SEVERITY_MEDIUM
                
                exception = {
                    'exception_type': 'AMOUNT_MISMATCH',
                    'severity': severity,
                    'transaction_date': match.get('recon_date'),
                    'store_name': match.get('store_name'),
                    'source_system': 'RECONCILIATION',
                    'source_record_id': match.get('id'),
                    'transaction_ref': match.get('pos_txn_id') or match.get('trm_txn_id'),
                    'amount': float(variance),
                    'description': f"Amount mismatch of ₹{variance} in transaction chain",
                    'details': {
                        'pos_amount': match.get('pos_amount'),
                        'trm_amount': match.get('trm_amount'),
                        'mpr_amount': match.get('mpr_amount'),
                        'bank_amount': match.get('bank_amount'),
                        'variance': float(variance),
                        'variance_percentage': match.get('variance_percentage')
                    },
                    'recommended_action': 'Review transaction details across all systems. Identify source of variance.',
                    'detected_at': datetime.now()
                }
                exceptions.append(exception)
        
        logger.info(f"  ⚠️ Found {len(exceptions)} amount mismatches beyond tolerance")
        return exceptions
    
    def identify_duplicates(self, records: List[Dict], source_type: str, key_field: str) -> List[Dict]:
        """
        Find duplicate records in a dataset
        
        Args:
            records: List of records to check
            source_type: Source system (TRM, MPR_UPI, MPR_CARD, BANK)
            key_field: Field to check for duplicates (e.g., 'rrn', 'utr_number')
        
        Returns:
            List of exception dictionaries
        """
        logger.info(f"🔍 Identifying duplicate {source_type} records by {key_field}...")
        
        exceptions = []
        seen = {}
        
        for record in records:
            key_value = record.get(key_field)
            
            if not key_value:
                continue
            
            if key_value in seen:
                # Found duplicate
                exception = {
                    'exception_type': f'DUPLICATE_{source_type}',
                    'severity': self.SEVERITY_HIGH,
                    'transaction_date': record.get('transaction_date'),
                    'store_name': record.get('store_name'),
                    'source_system': source_type,
                    'source_record_id': record.get('uid') or record.get('id'),
                    'transaction_ref': key_value,
                    'amount': float(record.get('amount', 0) or record.get('transaction_amount', 0)),
                    'description': f"Duplicate {source_type} record with {key_field}={key_value}",
                    'details': {
                        'original_record_id': seen[key_value]['id'],
                        'duplicate_record_id': record.get('uid') or record.get('id'),
                        key_field: key_value
                    },
                    'recommended_action': f'Review both records. Remove duplicate from {source_type} table.',
                    'detected_at': datetime.now()
                }
                exceptions.append(exception)
            else:
                seen[key_value] = record
        
        logger.info(f"  ⚠️ Found {len(exceptions)} duplicate {source_type} records")
        return exceptions
    
    def save_exceptions(self, batch_id: str) -> int:
        """
        Save all identified exceptions to database
        
        Args:
            batch_id: Reconciliation batch ID
        
        Returns:
            Number of exceptions saved
        """
        if not self.exceptions:
            logger.info("  ℹ No exceptions to save")
            return 0
        
        logger.info(f"💾 Saving {len(self.exceptions)} exceptions to database...")
        
        try:
            insert_query = """
                INSERT INTO recon_exceptions (
                    recon_batch_id, exception_type, severity,
                    transaction_date, store_name, source_system, source_record_id,
                    transaction_ref, amount, description, details,
                    recommended_action, status, detected_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            inserted = 0
            for exc in self.exceptions:
                import json
                
                values = (
                    batch_id,
                    exc['exception_type'],
                    exc['severity'],
                    exc.get('transaction_date'),
                    exc.get('store_name'),
                    exc['source_system'],
                    exc.get('source_record_id'),
                    exc.get('transaction_ref'),
                    exc.get('amount'),
                    exc['description'],
                    json.dumps(exc.get('details', {})),
                    exc.get('recommended_action'),
                    'OPEN',
                    exc['detected_at']
                )
                
                self.cursor.execute(insert_query, values)
                inserted += 1
            
            self.conn.commit()
            logger.info(f"  ✅ Saved {inserted} exceptions")
            return inserted
            
        except Error as e:
            logger.error(f"  ❌ Error saving exceptions: {e}")
            self.conn.rollback()
            return 0
    
    def process_exceptions(self, batch_id: str, source_data: Dict, matched_records: List[Dict] = None) -> Dict:
        """
        Complete exception processing workflow
        
        Args:
            batch_id: Reconciliation batch ID
            source_data: Dict with source records {pos, trm, mpr_upi, mpr_card, bank}
            matched_records: List of matched records (optional, for amount mismatch detection)
        
        Returns:
            Exception summary dictionary
        """
        logger.info("=" * 80)
        logger.info("⚠️ PROCESSING EXCEPTIONS")
        logger.info("=" * 80)
        
        # Initialize exception type counters
        for exc_type in self.EXCEPTION_TYPES:
            self.exception_summary['by_type'][exc_type] = 0
        
        # Identify all exception types
        self.exceptions = []
        
        # 1. Missing TRM
        missing_trm = self.identify_missing_trm(
            source_data.get('pos', []),
            source_data.get('trm', [])
        )
        self.exceptions.extend(missing_trm)
        self.exception_summary['by_type']['MISSING_TRM'] = len(missing_trm)
        
        # 2. Missing MPR
        missing_mpr = self.identify_missing_mpr(
            source_data.get('trm', []),
            source_data.get('mpr_upi', []),
            source_data.get('mpr_card', [])
        )
        self.exceptions.extend(missing_mpr)
        self.exception_summary['by_type']['MISSING_MPR'] = len(missing_mpr)
        
        # 3. Missing Bank
        missing_bank = self.identify_missing_bank(
            source_data.get('mpr_upi', []),
            source_data.get('mpr_card', []),
            source_data.get('bank', [])
        )
        self.exceptions.extend(missing_bank)
        self.exception_summary['by_type']['MISSING_BANK'] = len(missing_bank)
        
        # 4. Amount Mismatches (if matched records provided)
        if matched_records:
            amount_mismatches = self.identify_amount_mismatches(matched_records)
            self.exceptions.extend(amount_mismatches)
            self.exception_summary['by_type']['AMOUNT_MISMATCH'] = len(amount_mismatches)
        
        # 5. Duplicates
        dup_trm = self.identify_duplicates(source_data.get('trm', []), 'TRM', 'rrn')
        self.exceptions.extend(dup_trm)
        self.exception_summary['by_type']['DUPLICATE_TRM'] = len(dup_trm)
        
        dup_mpr_upi = self.identify_duplicates(source_data.get('mpr_upi', []), 'MPR_UPI', 'utr_number')
        self.exceptions.extend(dup_mpr_upi)
        self.exception_summary['by_type']['DUPLICATE_MPR'] = len(dup_mpr_upi)
        
        # Count by severity
        for exc in self.exceptions:
            severity = exc['severity']
            self.exception_summary['by_severity'][severity] += 1
        
        self.exception_summary['total_exceptions'] = len(self.exceptions)
        
        # Save to database
        saved_count = self.save_exceptions(batch_id)
        
        # Print summary
        self._print_summary()
        
        return self.exception_summary
    
    def _print_summary(self):
        """Print exception summary"""
        logger.info("\n📊 EXCEPTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total Exceptions: {self.exception_summary['total_exceptions']:,}")
        
        logger.info("\n⚠️ By Type:")
        for exc_type, count in self.exception_summary['by_type'].items():
            if count > 0:
                logger.info(f"  - {exc_type}: {count:,}")
        
        logger.info("\n🔥 By Severity:")
        for severity, count in self.exception_summary['by_severity'].items():
            if count > 0:
                logger.info(f"  - {severity}: {count:,}")
        
        logger.info(f"\n💰 Total Amount at Risk: ₹{self.exception_summary['total_amount_at_risk']:,.2f}")
        logger.info("=" * 80)


if __name__ == '__main__':
    # Quick test
    handler = ExceptionHandler()
    if handler.connect_db():
        # Test with empty data
        summary = handler.process_exceptions(
            'TEST_BATCH_001',
            {'pos': [], 'trm': [], 'mpr_upi': [], 'mpr_card': [], 'bank': []}
        )
        handler.disconnect_db()
        
        print("\n📊 Exception Summary:")
        import json
        print(json.dumps(summary, indent=2, default=str))
