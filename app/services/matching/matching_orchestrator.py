"""
Matching Orchestrator - Process 2
==================================
Coordinates the complete matching workflow:
1. Fetch source data (POS, TRM, MPR, Bank)
2. Execute matching algorithms (TRM-MPR, MPR-Bank, POS-Bank)
3. Track matched/unmatched records
4. Generate match summary

Created: November 12, 2025
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Tuple, Optional
import mysql.connector
from mysql.connector import Error
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app import config
from app.logging_config import setup_logging

setup_logging('INFO')
logger = logging.getLogger('matching_orchestrator')


class MatchingOrchestrator:
    """Orchestrates the complete matching and reconciliation process"""
    
    def __init__(self):
        """Initialize the matching orchestrator"""
        self.conn = None
        self.cursor = None
        self.batch_id = None
        self.match_summary = {
            'batch_id': None,
            'start_time': None,
            'end_time': None,
            'date_range': {'start': None, 'end': None},
            
            # Source counts
            'source_counts': {
                'pos_records': 0,
                'trm_records': 0,
                'mpr_upi_records': 0,
                'mpr_card_records': 0,
                'bank_records': 0
            },
            
            # Match results
            'matches': {
                'pos_trm': 0,
                'trm_mpr_upi': 0,
                'trm_mpr_card': 0,
                'mpr_bank': 0,
                'pos_bank_direct': 0,
                'full_chain_matches': 0,  # POS->TRM->MPR->Bank
                'partial_matches': 0
            },
            
            # Unmatched records
            'unmatched': {
                'pos_only': 0,
                'trm_only': 0,
                'mpr_only': 0,
                'bank_only': 0
            },
            
            # Exception categories
            'exceptions': {
                'missing_trm': 0,
                'missing_mpr': 0,
                'missing_bank': 0,
                'amount_mismatch': 0,
                'duplicate_entries': 0,
                'data_quality_issues': 0
            },
            
            # Financial summary
            'financial': {
                'total_pos_amount': 0.0,
                'total_matched_amount': 0.0,
                'total_unmatched_amount': 0.0,
                'total_variance': 0.0,
                'match_rate_percentage': 0.0
            },
            
            # Processing metadata
            'processing': {
                'total_records_processed': 0,
                'total_matches_created': 0,
                'total_exceptions_created': 0,
                'processing_time_seconds': 0.0
            }
        }
    
    def connect_db(self):
        """Establish database connection"""
        try:
            from app.core.database import db_manager
            self.conn = db_manager.get_mysql_connector().__enter__()
            self.cursor = self.conn.cursor(dictionary=True)
            logger.info(f"✅ Connected to database: devyani")
            return True
        except Error as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def disconnect_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("🔌 Database connection closed")
    
    def generate_batch_id(self, process_date: str = None) -> str:
        """Generate unique batch ID for matching run"""
        if process_date:
            date_str = process_date.replace('-', '')
        else:
            date_str = datetime.now().strftime('%Y%m%d')
        timestamp = datetime.now().strftime('%H%M%S')
        self.batch_id = f"MATCH_{date_str}_{timestamp}"
        self.match_summary['batch_id'] = self.batch_id
        logger.info(f"📦 Batch ID: {self.batch_id}")
        return self.batch_id
    
    def fetch_source_data(self, start_date: str, end_date: str) -> Dict[str, List[Dict]]:
        """
        Fetch all source data for matching
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            Dictionary with source data: {pos, trm, mpr_upi, mpr_card, bank}
        """
        logger.info(f"📥 Fetching source data from {start_date} to {end_date}...")
        
        data = {
            'pos': [],
            'trm': [],
            'mpr_upi': [],
            'mpr_card': [],
            'bank': []
        }
        
        try:
            # Fetch POS records (exclude cash)
            pos_query = """
                SELECT 
                    id,
                    DATE(date) as transaction_date,
                    store_name,
                    transaction_number,
                    bill_number,
                    settlement_mode,
                    channel,
                    source,
                    gross_amount,
                    discount,
                    net_sale,
                    subtotal
                FROM orders
                WHERE DATE(date) BETWEEN %s AND %s
                    AND settlement_mode != 'CASH'
                ORDER BY date, transaction_number
            """
            self.cursor.execute(pos_query, [start_date, end_date])
            data['pos'] = self.cursor.fetchall()
            self.match_summary['source_counts']['pos_records'] = len(data['pos'])
            logger.info(f"  ✓ POS: {len(data['pos']):,} records")
            
            # Fetch TRM records
            trm_query = """
                SELECT 
                    uid,
                    DATE(date) as transaction_date,
                    store_name,
                    transaction_id,
                    invoice,
                    tid,
                    mid,
                    rrn,
                    approval_code,
                    payment_mode,
                    amount,
                    settlement_date
                FROM trm
                WHERE DATE(date) BETWEEN %s AND %s
                ORDER BY date, transaction_id
            """
            self.cursor.execute(trm_query, [start_date, end_date])
            data['trm'] = self.cursor.fetchall()
            self.match_summary['source_counts']['trm_records'] = len(data['trm'])
            logger.info(f"  ✓ TRM: {len(data['trm']):,} records")
            
            # Fetch MPR UPI records
            mpr_upi_query = """
                SELECT 
                    uid,
                    DATE(transaction_req_date) as transaction_date,
                    merchant_name as store_name,
                    merchant_vpa,
                    payer_vpa,
                    upi_trxn_id as transaction_id,
                    customer_ref_no as rrn,
                    order_id,
                    transaction_amount,
                    DATE(settlement_date) as settlement_date,
                    customer_ref_no as utr_number
                FROM mpr_hdfc_upi
                WHERE DATE(transaction_req_date) BETWEEN %s AND %s
                ORDER BY transaction_req_date, upi_trxn_id
            """
            self.cursor.execute(mpr_upi_query, [start_date, end_date])
            data['mpr_upi'] = self.cursor.fetchall()
            self.match_summary['source_counts']['mpr_upi_records'] = len(data['mpr_upi'])
            logger.info(f"  ✓ MPR UPI: {len(data['mpr_upi']):,} records")
            
            # Fetch MPR Card records
            mpr_card_query = """
                SELECT 
                    uid,
                    DATE(chg_date) as transaction_date,
                    me_name as store_name,
                    terminal_no as terminal_id,
                    cardnbr as card_number,
                    gst_transaction_id as transaction_id,
                    app_code as rrn,
                    app_code as approval_code,
                    pymt_chgamnt as transaction_amount,
                    DATE(process_date) as settlement_date
                FROM mpr_hdfc_card
                WHERE DATE(chg_date) BETWEEN %s AND %s
                ORDER BY chg_date, gst_transaction_id
            """
            self.cursor.execute(mpr_card_query, [start_date, end_date])
            data['mpr_card'] = self.cursor.fetchall()
            self.match_summary['source_counts']['mpr_card_records'] = len(data['mpr_card'])
            logger.info(f"  ✓ MPR Card: {len(data['mpr_card']):,} records")
            
            # Fetch Bank records (credits only)
            bank_query = """
                SELECT 
                    stmt_line_id,
                    account_id,
                    statement_date,
                    value_date,
                    debit_credit,
                    amount,
                    bank_ref,
                    narration,
                    counterparty
                FROM fin_bank_statements
                WHERE value_date BETWEEN %s AND %s
                    AND debit_credit = 'CREDIT'
                ORDER BY value_date, stmt_line_id
            """
            self.cursor.execute(bank_query, [start_date, end_date])
            data['bank'] = self.cursor.fetchall()
            self.match_summary['source_counts']['bank_records'] = len(data['bank'])
            logger.info(f"  ✓ Bank: {len(data['bank']):,} records (credits only)")
            
            # Calculate total POS amount
            total_pos_amount = sum(
                Decimal(str(rec['net_sale'])) 
                for rec in data['pos'] 
                if rec.get('net_sale')
            )
            self.match_summary['financial']['total_pos_amount'] = float(total_pos_amount)
            
            logger.info(f"📊 Total source records: {sum(len(v) for v in data.values()):,}")
            return data
            
        except Error as e:
            logger.error(f"❌ Error fetching source data: {e}")
            return data
    
    def match_trm_to_mpr(self, trm_records: List[Dict], mpr_upi: List[Dict], mpr_card: List[Dict]) -> Dict:
        """
        Match TRM records to MPR (both UPI and Card)
        Matching criteria: RRN, Transaction ID, Amount, Date
        
        Returns:
            Dict with matched pairs and unmatched records
        """
        logger.info("🔗 Matching TRM → MPR (UPI + Card)...")
        
        matches = {
            'trm_mpr_upi': [],
            'trm_mpr_card': [],
            'unmatched_trm': []
        }
        
        matched_trm_ids = set()
        matched_mpr_upi_ids = set()
        matched_mpr_card_ids = set()
        
        # Match TRM to MPR UPI
        for trm in trm_records:
            if trm['uid'] in matched_trm_ids:
                continue
            
            # Try to match by RRN (most reliable)
            if trm.get('rrn'):
                for mpr in mpr_upi:
                    if mpr['uid'] in matched_mpr_upi_ids:
                        continue
                    
                    if mpr.get('rrn') == trm['rrn']:
                        # Verify amount match (within tolerance)
                        trm_amt = Decimal(str(trm.get('amount', 0)))
                        mpr_amt = Decimal(str(mpr.get('transaction_amount', 0)))
                        variance = abs(trm_amt - mpr_amt)
                        
                        if variance < Decimal('1.00'):  # 1 rupee tolerance
                            matches['trm_mpr_upi'].append({
                                'trm_record': trm,
                                'mpr_record': mpr,
                                'match_criteria': 'RRN',
                                'variance': float(variance),
                                'confidence': 95.0
                            })
                            matched_trm_ids.add(trm['uid'])
                            matched_mpr_upi_ids.add(mpr['uid'])
                            self.match_summary['matches']['trm_mpr_upi'] += 1
                            break
        
        # Match TRM to MPR Card
        for trm in trm_records:
            if trm['uid'] in matched_trm_ids:
                continue
            
            # Try to match by RRN + Approval Code
            if trm.get('rrn') and trm.get('approval_code'):
                for mpr in mpr_card:
                    if mpr['uid'] in matched_mpr_card_ids:
                        continue
                    
                    if (mpr.get('rrn') == trm['rrn'] and 
                        mpr.get('approval_code') == trm['approval_code']):
                        
                        # Verify amount
                        trm_amt = Decimal(str(trm.get('amount', 0)))
                        mpr_amt = Decimal(str(mpr.get('transaction_amount', 0)))
                        variance = abs(trm_amt - mpr_amt)
                        
                        if variance < Decimal('1.00'):
                            matches['trm_mpr_card'].append({
                                'trm_record': trm,
                                'mpr_record': mpr,
                                'match_criteria': 'RRN+ApprovalCode',
                                'variance': float(variance),
                                'confidence': 95.0
                            })
                            matched_trm_ids.add(trm['uid'])
                            matched_mpr_card_ids.add(mpr['uid'])
                            self.match_summary['matches']['trm_mpr_card'] += 1
                            break
        
        # Collect unmatched TRM
        for trm in trm_records:
            if trm['uid'] not in matched_trm_ids:
                matches['unmatched_trm'].append(trm)
        
        self.match_summary['unmatched']['trm_only'] = len(matches['unmatched_trm'])
        
        logger.info(f"  ✓ TRM-MPR UPI matches: {len(matches['trm_mpr_upi']):,}")
        logger.info(f"  ✓ TRM-MPR Card matches: {len(matches['trm_mpr_card']):,}")
        logger.info(f"  ⚠ Unmatched TRM: {len(matches['unmatched_trm']):,}")
        
        return matches
    
    def match_mpr_to_bank(self, mpr_matches: Dict, bank_records: List[Dict]) -> Dict:
        """
        Match MPR records to Bank statements
        Matching criteria: UTR/Reference Number, Amount, Settlement Date
        
        Args:
            mpr_matches: Dict from match_trm_to_mpr with matched pairs
            bank_records: List of bank statement records
        
        Returns:
            Dict with full chain matches (TRM->MPR->Bank)
        """
        logger.info("🔗 Matching MPR → Bank...")
        
        full_chain_matches = []
        matched_bank_ids = set()
        
        # Match MPR UPI to Bank
        for match in mpr_matches['trm_mpr_upi']:
            mpr = match['mpr_record']
            
            # Try to match by UTR number
            if mpr.get('utr_number'):
                for bank in bank_records:
                    if bank['stmt_line_id'] in matched_bank_ids:
                        continue
                    
                    # Check if UTR appears in bank narration or reference
                    if (mpr['utr_number'] in str(bank.get('narration', '')) or
                        mpr['utr_number'] in str(bank.get('bank_ref', ''))):
                        
                        # Verify amount
                        mpr_amt = Decimal(str(mpr.get('transaction_amount', 0)))
                        bank_amt = Decimal(str(bank.get('amount', 0)))
                        variance = abs(mpr_amt - bank_amt)
                        
                        if variance < Decimal('10.00'):  # 10 rupee tolerance for bank
                            full_chain_matches.append({
                                'trm_record': match['trm_record'],
                                'mpr_record': mpr,
                                'bank_record': bank,
                                'match_type': 'FULL_CHAIN_UPI',
                                'variance': float(variance),
                                'confidence': 90.0
                            })
                            matched_bank_ids.add(bank['stmt_line_id'])
                            self.match_summary['matches']['mpr_bank'] += 1
                            self.match_summary['matches']['full_chain_matches'] += 1
                            break
        
        # Match MPR Card to Bank
        for match in mpr_matches['trm_mpr_card']:
            mpr = match['mpr_record']
            
            # Try to match by settlement date and amount
            if mpr.get('settlement_date'):
                for bank in bank_records:
                    if bank['stmt_line_id'] in matched_bank_ids:
                        continue
                    
                    # Check date proximity (settlement can be T+1 or T+2)
                    date_diff = abs((bank['value_date'] - mpr['settlement_date']).days)
                    if date_diff <= 2:
                        # Verify amount
                        mpr_amt = Decimal(str(mpr.get('transaction_amount', 0)))
                        bank_amt = Decimal(str(bank.get('amount', 0)))
                        variance = abs(mpr_amt - bank_amt)
                        
                        if variance < Decimal('10.00'):
                            full_chain_matches.append({
                                'trm_record': match['trm_record'],
                                'mpr_record': mpr,
                                'bank_record': bank,
                                'match_type': 'FULL_CHAIN_CARD',
                                'variance': float(variance),
                                'confidence': 85.0
                            })
                            matched_bank_ids.add(bank['stmt_line_id'])
                            self.match_summary['matches']['mpr_bank'] += 1
                            self.match_summary['matches']['full_chain_matches'] += 1
                            break
        
        logger.info(f"  ✓ Full chain matches (TRM→MPR→Bank): {len(full_chain_matches):,}")
        
        return {
            'full_chain': full_chain_matches,
            'matched_bank_ids': matched_bank_ids
        }
    
    def save_matches_to_db(self, matches: List[Dict]) -> int:
        """
        Save all match records to reconciliation_master table
        
        Args:
            matches: List of match dictionaries
        
        Returns:
            Number of records inserted
        """
        if not matches:
            logger.info("  ℹ No matches to save")
            return 0
        
        logger.info(f"💾 Saving {len(matches):,} match records to database...")
        
        try:
            insert_query = """
                INSERT INTO reconciliation_master (
                    recon_date, recon_batch_id, store_name,
                    match_type, match_level, match_status,
                    has_pos, has_trm, has_mpr_upi, has_mpr_card, has_bank,
                    pos_txn_id, pos_bill_number, pos_amount, pos_settlement_mode, pos_record_id,
                    trm_txn_id, trm_invoice, trm_amount, trm_rrn, trm_approval_code, trm_record_id,
                    mpr_txn_id, mpr_amount, mpr_rrn, mpr_utr, mpr_record_id,
                    bank_stmt_id, bank_amount, bank_ref, bank_value_date, bank_narration, bank_record_id,
                    total_variance, variance_percentage, auto_matched, match_confidence, notes
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
            """
            
            inserted = 0
            for match in matches:
                values = (
                    match.get('recon_date'),
                    self.batch_id,
                    match.get('store_name'),
                    match.get('match_type'),
                    match.get('match_level', 'TRANSACTION'),
                    match.get('match_status', 'MATCHED'),
                    match.get('has_pos', False),
                    match.get('has_trm', False),
                    match.get('has_mpr_upi', False),
                    match.get('has_mpr_card', False),
                    match.get('has_bank', False),
                    match.get('pos_txn_id'),
                    match.get('pos_bill_number'),
                    match.get('pos_amount'),
                    match.get('pos_settlement_mode'),
                    match.get('pos_record_id'),
                    match.get('trm_txn_id'),
                    match.get('trm_invoice'),
                    match.get('trm_amount'),
                    match.get('trm_rrn'),
                    match.get('trm_approval_code'),
                    match.get('trm_record_id'),
                    match.get('mpr_txn_id'),
                    match.get('mpr_amount'),
                    match.get('mpr_rrn'),
                    match.get('mpr_utr'),
                    match.get('mpr_record_id'),
                    match.get('bank_stmt_id'),
                    match.get('bank_amount'),
                    match.get('bank_ref'),
                    match.get('bank_value_date'),
                    match.get('bank_narration'),
                    match.get('bank_record_id'),
                    match.get('total_variance', 0.0),
                    match.get('variance_percentage', 0.0),
                    match.get('auto_matched', True),
                    match.get('match_confidence', 0.0),
                    match.get('notes')
                )
                
                self.cursor.execute(insert_query, values)
                inserted += 1
            
            self.conn.commit()
            logger.info(f"  ✅ Inserted {inserted:,} match records")
            return inserted
            
        except Error as e:
            logger.error(f"  ❌ Error saving matches: {e}")
            self.conn.rollback()
            return 0
    
    def run_matching(self, start_date: str, end_date: str, save_to_db: bool = True) -> Dict:
        """
        Main matching workflow
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            save_to_db: Whether to save results to database
        
        Returns:
            Complete match summary dictionary
        """
        start_time = time.time()
        self.match_summary['start_time'] = datetime.now().isoformat()
        self.match_summary['date_range'] = {'start': start_date, 'end': end_date}
        
        logger.info("=" * 80)
        logger.info("🚀 STARTING MATCHING PROCESS")
        logger.info("=" * 80)
        logger.info(f"Period: {start_date} to {end_date}")
        logger.info("")
        
        # Connect to database
        if not self.connect_db():
            logger.error("❌ Failed to connect to database. Aborting.")
            return self.match_summary
        
        try:
            # Generate batch ID
            self.generate_batch_id(start_date)
            
            # Step 1: Fetch source data
            logger.info("📥 Step 1: Fetching source data...")
            source_data = self.fetch_source_data(start_date, end_date)
            logger.info("")
            
            # Step 2: Match TRM to MPR
            logger.info("🔗 Step 2: Matching TRM → MPR...")
            trm_mpr_matches = self.match_trm_to_mpr(
                source_data['trm'],
                source_data['mpr_upi'],
                source_data['mpr_card']
            )
            logger.info("")
            
            # Step 3: Match MPR to Bank (creates full chain)
            logger.info("🔗 Step 3: Matching MPR → Bank (Full Chain)...")
            mpr_bank_matches = self.match_mpr_to_bank(
                trm_mpr_matches,
                source_data['bank']
            )
            logger.info("")
            
            # Step 4: Save matches to database
            if save_to_db:
                logger.info("💾 Step 4: Saving matches to database...")
                # Convert match structures to reconciliation_master format
                db_matches = self._convert_matches_for_db(
                    trm_mpr_matches,
                    mpr_bank_matches
                )
                inserted_count = self.save_matches_to_db(db_matches)
                self.match_summary['processing']['total_matches_created'] = inserted_count
                logger.info("")
            
            # Calculate summary statistics
            self.match_summary['end_time'] = datetime.now().isoformat()
            self.match_summary['processing']['processing_time_seconds'] = time.time() - start_time
            self.match_summary['processing']['total_records_processed'] = sum(
                self.match_summary['source_counts'].values()
            )
            
            # Calculate match rate
            total_pos = self.match_summary['source_counts']['pos_records']
            total_matched = self.match_summary['matches']['full_chain_matches']
            if total_pos > 0:
                self.match_summary['financial']['match_rate_percentage'] = (
                    total_matched / total_pos
                ) * 100
            
            # Print summary
            self._print_summary()
            
            return self.match_summary
            
        except Exception as e:
            logger.error(f"❌ Error in matching process: {e}", exc_info=True)
            return self.match_summary
        finally:
            self.disconnect_db()
    
    def _convert_matches_for_db(self, trm_mpr_matches: Dict, mpr_bank_matches: Dict) -> List[Dict]:
        """Convert match structures to database format"""
        db_matches = []
        
        # Convert full chain matches
        for match in mpr_bank_matches['full_chain']:
            trm = match['trm_record']
            mpr = match['mpr_record']
            bank = match['bank_record']
            
            db_match = {
                'recon_date': trm.get('transaction_date'),
                'store_name': trm.get('store_name'),
                'match_type': match['match_type'],
                'match_level': 'TRANSACTION',
                'match_status': 'MATCHED',
                'has_pos': False,  # Will be linked separately
                'has_trm': True,
                'has_mpr_upi': match['match_type'] == 'FULL_CHAIN_UPI',
                'has_mpr_card': match['match_type'] == 'FULL_CHAIN_CARD',
                'has_bank': True,
                'trm_txn_id': trm.get('transaction_id'),
                'trm_invoice': trm.get('invoice'),
                'trm_amount': float(trm.get('amount', 0)),
                'trm_rrn': trm.get('rrn'),
                'trm_approval_code': trm.get('approval_code'),
                'trm_record_id': trm.get('uid'),
                'mpr_txn_id': mpr.get('transaction_id'),
                'mpr_amount': float(mpr.get('transaction_amount', 0)),
                'mpr_rrn': mpr.get('rrn'),
                'mpr_utr': mpr.get('utr_number') if 'utr_number' in mpr else None,
                'mpr_record_id': mpr.get('uid'),
                'bank_stmt_id': bank.get('stmt_line_id'),
                'bank_amount': float(bank.get('amount', 0)),
                'bank_ref': bank.get('bank_ref'),
                'bank_value_date': bank.get('value_date'),
                'bank_narration': bank.get('narration'),
                'bank_record_id': bank.get('stmt_line_id'),
                'total_variance': match['variance'],
                'variance_percentage': 0.0,
                'auto_matched': True,
                'match_confidence': match['confidence'],
                'notes': f"Full chain match: TRM→MPR→Bank via {match['match_type']}"
            }
            db_matches.append(db_match)
        
        return db_matches
    
    def _print_summary(self):
        """Print matching summary to console"""
        logger.info("=" * 80)
        logger.info("✅ MATCHING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Batch ID: {self.match_summary['batch_id']}")
        logger.info(f"Period: {self.match_summary['date_range']['start']} to {self.match_summary['date_range']['end']}")
        
        logger.info("\n📊 Source Data:")
        for source, count in self.match_summary['source_counts'].items():
            logger.info(f"  - {source.replace('_', ' ').title()}: {count:,}")
        
        logger.info("\n🔗 Match Results:")
        for match_type, count in self.match_summary['matches'].items():
            logger.info(f"  - {match_type.replace('_', ' ').title()}: {count:,}")
        
        logger.info("\n⚠️ Unmatched Records:")
        for unmatch_type, count in self.match_summary['unmatched'].items():
            logger.info(f"  - {unmatch_type.replace('_', ' ').title()}: {count:,}")
        
        logger.info("\n💰 Financial Summary:")
        logger.info(f"  - Total POS Amount: ₹{self.match_summary['financial']['total_pos_amount']:,.2f}")
        logger.info(f"  - Match Rate: {self.match_summary['financial']['match_rate_percentage']:.2f}%")
        
        logger.info(f"\n⏱️ Processing Time: {self.match_summary['processing']['processing_time_seconds']:.2f}s")
        logger.info(f"💾 Records Processed: {self.match_summary['processing']['total_records_processed']:,}")
        logger.info("=" * 80)


if __name__ == '__main__':
    # Quick test
    orchestrator = MatchingOrchestrator()
    summary = orchestrator.run_matching('2024-12-01', '2024-12-07', save_to_db=False)
    print("\n📊 Match Summary:")
    import json
    print(json.dumps(summary, indent=2, default=str))
