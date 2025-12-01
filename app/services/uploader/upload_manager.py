"""
Upload Manager - Orchestrates all data uploads
Processes TRM, MPR UPI, MPR Card files in organized manner
"""

import logging
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.services.uploader.data_uploader import DataUploader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class UploadManager:
    """Manages all data uploads for reconciliation"""
    
    def __init__(self):
        self.uploader = DataUploader()
        
        # Define upload configurations
        self.upload_configs = [
            {
                'name': 'TRM Data',
                'file': 'Devyani_Data/TRM_Data/All transactions report (01-Dec-2024 to 07-Dec-2024)Pine Labs - TRM_Report.xlsx',
                'table': 'trm',
                'prefix': 'TRM'
            },
            {
                'name': 'MPR UPI Data',
                'file': 'data/sftp/MPR_Data/HDFC_UPI_Dec24.xlsx',
                'table': 'mpr_hdfc_upi',
                'prefix': 'MPR_UPI'
            },
            {
                'name': 'MPR Card Data',
                'file': 'data/sftp/MPR_Data/HDFC_Card_Dec24.xlsx',
                'table': 'mpr_hdfc_card',
                'prefix': 'MPR_CARD'
            }
        ]
    
    def upload_all(self, force: bool = False):
        """Upload all data files"""
        logger.info("=" * 60)
        logger.info("📦 DATA UPLOAD MANAGER")
        logger.info("=" * 60)
        logger.info("")
        
        results = []
        
        for idx, config in enumerate(self.upload_configs, 1):
            logger.info(f"[{idx}/{len(self.upload_configs)}] Processing: {config['name']}")
            logger.info("-" * 60)
            
            if not os.path.exists(config['file']):
                logger.error(f"❌ File not found: {config['file']}")
                results.append({'name': config['name'], 'success': False, 'message': 'File not found'})
                logger.info("")
                continue
            
            success, message = self.uploader.upload_to_table(
                config['file'],
                config['table'],
                config['prefix'],
                skip_duplicates=not force
            )
            
            results.append({
                'name': config['name'],
                'success': success,
                'message': message
            })
            
            logger.info("")
        
        # Summary
        logger.info("=" * 60)
        logger.info("📊 UPLOAD SUMMARY")
        logger.info("=" * 60)
        
        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        
        for result in results:
            status = "✅" if result['success'] else "❌"
            logger.info(f"{status} {result['name']}: {result['message']}")
        
        logger.info("")
        logger.info(f"Total: {len(results)} | Success: {successful} | Failed: {failed}")
        logger.info("=" * 60)
        
        return all(r['success'] for r in results)


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload all reconciliation data')
    parser.add_argument('--force', action='store_true', 
                       help='Force re-upload even if files already uploaded')
    
    args = parser.parse_args()
    
    manager = UploadManager()
    success = manager.upload_all(force=args.force)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
