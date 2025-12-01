"""
Reconciliation Report Generator
Created: November 12, 2025
Purpose: Generate HTML and Excel reports from reconciliation data
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict, List, Tuple
import mysql.connector
from mysql.connector import Error
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config
from app.logging_config import setup_logging

# Setup logging
setup_logging('INFO')
logger = logging.getLogger('report_generator')


class ReconciliationReportGenerator:
    """Generate reconciliation reports in HTML and Excel formats"""
    
    def __init__(self):
        """Initialize report generator"""
        self.conn = None
        self.cursor = None
        self.report_data = {}
    
    def connect_db(self):
        """Establish database connection"""
        try:
            from app.core.database import db_manager
            self.conn = db_manager.get_mysql_connector().__enter__()
                host=config.MYSQL_HOST,
                user=config.MYSQL_USER,
                password=config.MYSQL_PASSWORD,
                database='devyani'
            )
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
    
    def get_statistics(self, batch_id: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get reconciliation statistics"""
        try:
            if batch_id:
                query = "SELECT * FROM match_statistics WHERE batch_id = %s ORDER BY stat_date DESC LIMIT 1"
                self.cursor.execute(query, [batch_id])
            elif start_date and end_date:
                query = "SELECT * FROM match_statistics WHERE stat_date BETWEEN %s AND %s ORDER BY stat_date DESC LIMIT 1"
                self.cursor.execute(query, [start_date, end_date])
            else:
                query = "SELECT * FROM match_statistics ORDER BY created_at DESC LIMIT 1"
                self.cursor.execute(query)
            
            stats = self.cursor.fetchone()
            return stats if stats else {}
        except Error as e:
            logger.error(f"❌ Error fetching statistics: {e}")
            return {}
    
    def get_daily_summary(self, start_date: str, end_date: str) -> List[Dict]:
        """Get daily reconciliation summary"""
        try:
            query = """
                SELECT * FROM v_daily_recon_summary
                WHERE recon_date BETWEEN %s AND %s
                ORDER BY recon_date DESC
            """
            self.cursor.execute(query, [start_date, end_date])
            return self.cursor.fetchall()
        except Error as e:
            logger.error(f"❌ Error fetching daily summary: {e}")
            return []
    
    def get_exceptions_summary(self) -> List[Dict]:
        """Get open exceptions summary"""
        try:
            query = "SELECT * FROM v_open_exceptions ORDER BY severity DESC, exception_count DESC"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Error as e:
            logger.error(f"❌ Error fetching exceptions: {e}")
            return []
    
    def get_store_performance(self) -> List[Dict]:
        """Get store-wise matching performance"""
        try:
            query = "SELECT * FROM v_store_match_performance ORDER BY match_rate_pct ASC LIMIT 50"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Error as e:
            logger.error(f"❌ Error fetching store performance: {e}")
            return []
    
    def get_matched_records(self, limit: int = 100) -> List[Dict]:
        """Get sample matched records"""
        try:
            query = """
                SELECT 
                    recon_date,
                    store_name,
                    pos_txn_id,
                    pos_amount,
                    bank_amount,
                    total_variance,
                    match_status,
                    match_confidence,
                    notes
                FROM reconciliation_master
                WHERE match_status = 'MATCHED'
                ORDER BY recon_date DESC, match_confidence DESC
                LIMIT %s
            """
            self.cursor.execute(query, [limit])
            return self.cursor.fetchall()
        except Error as e:
            logger.error(f"❌ Error fetching matched records: {e}")
            return []
    
    def get_unmatched_records(self, limit: int = 100) -> List[Dict]:
        """Get sample unmatched records"""
        try:
            query = """
                SELECT 
                    recon_date,
                    store_name,
                    pos_txn_id,
                    pos_amount,
                    bank_stmt_id,
                    bank_amount,
                    match_status,
                    notes
                FROM reconciliation_master
                WHERE match_status = 'UNMATCHED'
                ORDER BY COALESCE(pos_amount, bank_amount) DESC
                LIMIT %s
            """
            self.cursor.execute(query, [limit])
            return self.cursor.fetchall()
        except Error as e:
            logger.error(f"❌ Error fetching unmatched records: {e}")
            return []
    
    def generate_html_report(self, start_date: str, end_date: str, output_file: str):
        """Generate HTML reconciliation report"""
        logger.info(f"📊 Generating HTML report for {start_date} to {end_date}...")
        
        # Gather data
        stats = self.get_statistics(start_date=start_date, end_date=end_date)
        daily_summary = self.get_daily_summary(start_date, end_date)
        exceptions = self.get_exceptions_summary()
        store_perf = self.get_store_performance()
        matched_samples = self.get_matched_records(50)
        unmatched_samples = self.get_unmatched_records(50)
        
        # Generate HTML
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reconciliation Report - {start_date} to {end_date}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 32px;
            margin-bottom: 10px;
        }}
        
        .header .period {{
            font-size: 18px;
            opacity: 0.9;
        }}
        
        .header .generated {{
            font-size: 14px;
            opacity: 0.8;
            margin-top: 10px;
        }}
        
        .content {{
            padding: 30px;
        }}
        
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .stat-card {{
            background: white;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            padding: 20px;
            text-align: center;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .stat-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 8px 16px rgba(0,0,0,0.1);
        }}
        
        .stat-card .value {{
            font-size: 36px;
            font-weight: bold;
            color: #667eea;
            margin: 10px 0;
        }}
        
        .stat-card .label {{
            font-size: 14px;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .stat-card.success .value {{
            color: #10b981;
        }}
        
        .stat-card.warning .value {{
            color: #f59e0b;
        }}
        
        .stat-card.danger .value {{
            color: #ef4444;
        }}
        
        .section {{
            margin-bottom: 40px;
        }}
        
        .section h2 {{
            font-size: 24px;
            color: #333;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #667eea;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }}
        
        thead {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }}
        
        th {{
            padding: 15px;
            text-align: left;
            font-weight: 600;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        tbody tr:nth-child(even) {{
            background-color: #f9fafb;
        }}
        
        tbody tr:hover {{
            background-color: #f3f4f6;
        }}
        
        td {{
            padding: 12px 15px;
            font-size: 14px;
            color: #333;
            border-bottom: 1px solid #e5e7eb;
        }}
        
        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }}
        
        .badge.success {{
            background: #d1fae5;
            color: #065f46;
        }}
        
        .badge.warning {{
            background: #fef3c7;
            color: #92400e;
        }}
        
        .badge.danger {{
            background: #fee2e2;
            color: #991b1b;
        }}
        
        .badge.info {{
            background: #dbeafe;
            color: #1e40af;
        }}
        
        .amount {{
            font-weight: 600;
            color: #059669;
        }}
        
        .variance {{
            font-weight: 600;
            color: #dc2626;
        }}
        
        .footer {{
            background: #f9fafb;
            padding: 20px 30px;
            text-align: center;
            color: #666;
            font-size: 14px;
            border-top: 2px solid #e5e7eb;
        }}
        
        .progress-bar {{
            width: 100%;
            height: 24px;
            background: #e5e7eb;
            border-radius: 12px;
            overflow: hidden;
            margin: 10px 0;
        }}
        
        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #10b981, #059669);
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 12px;
            font-weight: 600;
            transition: width 0.3s;
        }}
        
        .no-data {{
            text-align: center;
            padding: 40px;
            color: #666;
            font-style: italic;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Reconciliation Report</h1>
            <div class="period">{start_date} to {end_date}</div>
            <div class="generated">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>
        
        <div class="content">
            <!-- Summary Statistics -->
            <div class="section">
                <h2>📈 Summary Statistics</h2>
                <div class="summary-grid">
                    <div class="stat-card">
                        <div class="label">Total POS Records</div>
                        <div class="value">{stats.get('total_pos_records', 0):,}</div>
                    </div>
                    <div class="stat-card">
                        <div class="label">Total Bank Records</div>
                        <div class="value">{stats.get('total_bank_records', 0):,}</div>
                    </div>
                    <div class="stat-card success">
                        <div class="label">Matched Records</div>
                        <div class="value">{stats.get('matched_pos_bank', 0):,}</div>
                    </div>
                    <div class="stat-card warning">
                        <div class="label">Unmatched POS</div>
                        <div class="value">{stats.get('unmatched_pos', 0):,}</div>
                    </div>
                    <div class="stat-card danger">
                        <div class="label">Total Exceptions</div>
                        <div class="value">{stats.get('total_exceptions', 0):,}</div>
                    </div>
                    <div class="stat-card">
                        <div class="label">Overall Match Rate</div>
                        <div class="value">{stats.get('overall_match_rate', 0):.1f}%</div>
                    </div>
                </div>
                
                <div class="progress-bar">
                    <div class="progress-fill" style="width: {stats.get('overall_match_rate', 0)}%">
                        {stats.get('overall_match_rate', 0):.1f}% Match Rate
                    </div>
                </div>
            </div>
            
            <!-- Daily Summary -->
            <div class="section">
                <h2>📅 Daily Summary</h2>
                {self._generate_daily_summary_table(daily_summary)}
            </div>
            
            <!-- Exceptions Summary -->
            <div class="section">
                <h2>⚠️ Exception Summary</h2>
                {self._generate_exceptions_table(exceptions)}
            </div>
            
            <!-- Store Performance -->
            <div class="section">
                <h2>🏪 Top 20 Stores - Lowest Match Rates</h2>
                {self._generate_store_performance_table(store_perf)}
            </div>
            
            <!-- Matched Records Sample -->
            <div class="section">
                <h2>✅ Sample Matched Records (Top 20)</h2>
                {self._generate_matched_records_table(matched_samples[:20])}
            </div>
            
            <!-- Unmatched Records Sample -->
            <div class="section">
                <h2>❌ Sample Unmatched Records (Top 20 by Amount)</h2>
                {self._generate_unmatched_records_table(unmatched_samples[:20])}
            </div>
        </div>
        
        <div class="footer">
            <p>Smart Uploader - Devyani Data Team | Reconciliation Report</p>
            <p>Batch ID: {stats.get('batch_id', 'N/A')}</p>
        </div>
    </div>
</body>
</html>
        """
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"✅ HTML report saved to: {output_file}")
        return output_file
    
    def _generate_daily_summary_table(self, data: List[Dict]) -> str:
        """Generate HTML table for daily summary"""
        if not data:
            return '<div class="no-data">No data available</div>'
        
        rows = ""
        for row in data:
            match_rate = (row['matched_count'] / row['total_records'] * 100) if row['total_records'] > 0 else 0
            rows += f"""
                <tr>
                    <td>{row['recon_date']}</td>
                    <td>{row['total_records']:,}</td>
                    <td><span class="badge success">{row['matched_count']:,}</span></td>
                    <td><span class="badge warning">{row['unmatched_count']:,}</span></td>
                    <td><span class="badge danger">{row['exception_count']:,}</span></td>
                    <td>{match_rate:.1f}%</td>
                    <td class="amount">₹{row.get('total_amount', 0):,.2f}</td>
                    <td class="variance">₹{row.get('total_variance', 0):,.2f}</td>
                </tr>
            """
        
        return f"""
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Total Records</th>
                        <th>Matched</th>
                        <th>Unmatched</th>
                        <th>Exceptions</th>
                        <th>Match Rate</th>
                        <th>Total Amount</th>
                        <th>Variance</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        """
    
    def _generate_exceptions_table(self, data: List[Dict]) -> str:
        """Generate HTML table for exceptions"""
        if not data:
            return '<div class="no-data">No exceptions found ✅</div>'
        
        rows = ""
        severity_badge = {
            'CRITICAL': 'danger',
            'HIGH': 'danger',
            'MEDIUM': 'warning',
            'LOW': 'info'
        }
        
        for row in data:
            badge_type = severity_badge.get(row['severity'], 'info')
            rows += f"""
                <tr>
                    <td>{row['exception_type']}</td>
                    <td><span class="badge {badge_type}">{row['severity']}</span></td>
                    <td>{row['exception_count']:,}</td>
                    <td>{row.get('affected_stores', 0):,}</td>
                    <td class="variance">₹{row.get('total_variance', 0):,.2f}</td>
                    <td>{row.get('oldest_exception', 'N/A')}</td>
                </tr>
            """
        
        return f"""
            <table>
                <thead>
                    <tr>
                        <th>Exception Type</th>
                        <th>Severity</th>
                        <th>Count</th>
                        <th>Affected Stores</th>
                        <th>Total Variance</th>
                        <th>Oldest</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        """
    
    def _generate_store_performance_table(self, data: List[Dict]) -> str:
        """Generate HTML table for store performance"""
        if not data:
            return '<div class="no-data">No store data available</div>'
        
        rows = ""
        for row in data[:20]:  # Top 20
            match_rate = row.get('match_rate_pct', 0)
            badge_type = 'success' if match_rate >= 90 else 'warning' if match_rate >= 70 else 'danger'
            
            rows += f"""
                <tr>
                    <td>{row['store_name']}</td>
                    <td>{row['total_transactions']:,}</td>
                    <td>{row['matched']:,}</td>
                    <td>{row['unmatched']:,}</td>
                    <td><span class="badge {badge_type}">{match_rate:.1f}%</span></td>
                    <td class="amount">₹{row.get('total_pos_amount', 0):,.2f}</td>
                    <td class="variance">₹{row.get('total_variance', 0):,.2f}</td>
                </tr>
            """
        
        return f"""
            <table>
                <thead>
                    <tr>
                        <th>Store</th>
                        <th>Total Txns</th>
                        <th>Matched</th>
                        <th>Unmatched</th>
                        <th>Match Rate</th>
                        <th>POS Amount</th>
                        <th>Variance</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        """
    
    def _generate_matched_records_table(self, data: List[Dict]) -> str:
        """Generate HTML table for matched records"""
        if not data:
            return '<div class="no-data">No matched records found</div>'
        
        rows = ""
        for row in data:
            confidence = row.get('match_confidence', 0)
            badge_type = 'success' if confidence >= 90 else 'info'
            
            rows += f"""
                <tr>
                    <td>{row['recon_date']}</td>
                    <td>{row.get('store_name', 'N/A')}</td>
                    <td>{row.get('pos_txn_id', 'N/A')}</td>
                    <td class="amount">₹{row.get('pos_amount', 0):,.2f}</td>
                    <td class="amount">₹{row.get('bank_amount', 0):,.2f}</td>
                    <td class="variance">₹{abs(row.get('total_variance', 0)):,.2f}</td>
                    <td><span class="badge {badge_type}">{confidence:.0f}%</span></td>
                </tr>
            """
        
        return f"""
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Store</th>
                        <th>Transaction ID</th>
                        <th>POS Amount</th>
                        <th>Bank Amount</th>
                        <th>Variance</th>
                        <th>Confidence</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        """
    
    def _generate_unmatched_records_table(self, data: List[Dict]) -> str:
        """Generate HTML table for unmatched records"""
        if not data:
            return '<div class="no-data">No unmatched records found ✅</div>'
        
        rows = ""
        for row in data:
            source = "POS" if row.get('pos_txn_id') else "BANK"
            amount = row.get('pos_amount') or row.get('bank_amount') or 0
            
            rows += f"""
                <tr>
                    <td>{row['recon_date']}</td>
                    <td><span class="badge info">{source}</span></td>
                    <td>{row.get('store_name', 'N/A')}</td>
                    <td>{row.get('pos_txn_id') or row.get('bank_stmt_id', 'N/A')}</td>
                    <td class="amount">₹{float(amount):,.2f}</td>
                    <td>{(row.get('notes', '') or '')[:100]}</td>
                </tr>
            """
        
        return f"""
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Source</th>
                        <th>Store</th>
                        <th>Reference</th>
                        <th>Amount</th>
                        <th>Notes</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        """
    
    def generate_report(self, start_date: str, end_date: str, output_dir: str = 'reports'):
        """Generate complete reconciliation report"""
        logger.info(f"🚀 Generating reconciliation report for {start_date} to {end_date}")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate HTML report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        html_file = os.path.join(output_dir, f'reconciliation_report_{start_date}_to_{end_date}_{timestamp}.html')
        
        self.generate_html_report(start_date, end_date, html_file)
        
        logger.info(f"✅ Report generation complete!")
        logger.info(f"📄 HTML Report: {html_file}")
        
        return {
            'html_file': html_file,
            'success': True
        }


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate Reconciliation Report')
    parser.add_argument('--start-date', type=str, required=True,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True,
                        help='End date (YYYY-MM-DD)')
    parser.add_argument('--output-dir', type=str, default='reports',
                        help='Output directory for reports')
    
    args = parser.parse_args()
    
    # Generate report
    generator = ReconciliationReportGenerator()
    
    if not generator.connect_db():
        print("❌ Failed to connect to database")
        sys.exit(1)
    
    try:
        result = generator.generate_report(
            args.start_date,
            args.end_date,
            args.output_dir
        )
        
        if result['success']:
            print(f"\n✅ Report generated successfully!")
            print(f"📄 HTML: {result['html_file']}")
            sys.exit(0)
        else:
            print("\n❌ Report generation failed!")
            sys.exit(1)
    finally:
        generator.disconnect_db()


if __name__ == '__main__':
    main()
