import pandas as pd
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import re

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Comprehensive data validation service for mapped columns.
    """
    
    def __init__(self):
        self.validation_rules = {
            'varchar': self._validate_varchar,
            'text': self._validate_text,
            'int': self._validate_int,
            'bigint': self._validate_bigint,
            'decimal': self._validate_decimal,
            'float': self._validate_float,
            'double': self._validate_double,
            'date': self._validate_date,
            'datetime': self._validate_datetime,
            'timestamp': self._validate_timestamp,
            'email': self._validate_email,
            'phone': self._validate_phone,
            'url': self._validate_url
        }
    
    async def validate_mapped_data(
        self, 
        df: pd.DataFrame, 
        mapping: Dict[str, str], 
        db_columns: List[Dict],
        sample_size: int = 100
    ) -> Dict[str, Any]:
        """
        Validate mapped data against database schema constraints.
        """
        try:
            validation_results = {
                "valid": True,
                "errors": [],
                "warnings": [],
                "column_validations": {},
                "data_quality_score": 0.0,
                "recommendations": []
            }
            
            # Get column metadata
            column_metadata = {col["COLUMN_NAME"]: col for col in db_columns}
            
            # Validate each mapped column
            for file_header, db_column in mapping.items():
                if db_column and db_column in column_metadata:
                    col_validation = await self._validate_column(
                        df, file_header, db_column, column_metadata[db_column], sample_size
                    )
                    validation_results["column_validations"][file_header] = col_validation
                    
                    if not col_validation["valid"]:
                        validation_results["valid"] = False
                        validation_results["errors"].extend(col_validation["errors"])
                    
                    validation_results["warnings"].extend(col_validation["warnings"])
            
            # Calculate overall data quality score
            validation_results["data_quality_score"] = self._calculate_quality_score(
                validation_results["column_validations"]
            )
            
            # Generate recommendations
            validation_results["recommendations"] = self._generate_recommendations(
                validation_results["column_validations"]
            )
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return {
                "valid": False,
                "errors": [f"Validation failed: {str(e)}"],
                "warnings": [],
                "column_validations": {},
                "data_quality_score": 0.0,
                "recommendations": []
            }
    
    async def _validate_column(
        self, 
        df: pd.DataFrame, 
        file_header: str, 
        db_column: str, 
        column_metadata: Dict,
        sample_size: int
    ) -> Dict[str, Any]:
        """
        Validate a specific column against its database constraints.
        """
        validation = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "data_type_compatibility": True,
            "null_violations": 0,
            "format_violations": 0,
            "sample_issues": []
        }
        
        try:
            # Get sample data for validation
            sample_data = df[file_header].head(sample_size) if file_header in df.columns else pd.Series()
            
            if sample_data.empty:
                validation["warnings"].append(f"No data found for column '{file_header}'")
                return validation
            
            # Check for null values in non-nullable columns
            if column_metadata.get("IS_NULLABLE") == "NO":
                null_count = sample_data.isnull().sum()
                if null_count > 0:
                    validation["null_violations"] = int(null_count)
                    validation["errors"].append(
                        f"Column '{db_column}' is NOT NULL but found {null_count} null values"
                    )
                    validation["valid"] = False
            
            # Validate data types
            data_type = column_metadata.get("DATA_TYPE", "").lower()
            if data_type in self.validation_rules:
                type_validation = self.validation_rules[data_type](
                    sample_data, column_metadata
                )
                validation.update(type_validation)
            
            # Check for data format issues
            format_issues = self._check_data_formats(sample_data, db_column)
            validation["format_violations"] = len(format_issues)
            validation["sample_issues"] = format_issues[:5]  # Show first 5 issues
            
            if format_issues:
                validation["warnings"].extend([
                    f"Found {len(format_issues)} format issues in column '{db_column}'"
                ])
            
        except Exception as e:
            validation["errors"].append(f"Column validation error: {str(e)}")
            validation["valid"] = False
        
        return validation
    
    def _validate_varchar(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate VARCHAR columns."""
        result = {"data_type_compatibility": True}
        
        # Check for length constraints (if available)
        max_length = self._extract_max_length(metadata)
        if max_length:
            long_values = data.astype(str).str.len() > max_length
            if long_values.any():
                result["data_type_compatibility"] = False
                result["errors"] = [f"Values exceed maximum length of {max_length}"]
        
        return result
    
    def _validate_text(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate TEXT columns."""
        return {"data_type_compatibility": True}
    
    def _validate_int(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate INT columns."""
        result = {"data_type_compatibility": True}
        
        # Try to convert to numeric
        numeric_data = pd.to_numeric(data, errors='coerce')
        non_numeric = data.isnull() != numeric_data.isnull()
        
        if non_numeric.any():
            result["data_type_compatibility"] = False
            result["errors"] = [f"Found {non_numeric.sum()} non-numeric values"]
        
        return result
    
    def _validate_bigint(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate BIGINT columns."""
        return self._validate_int(data, metadata)
    
    def _validate_decimal(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate DECIMAL columns."""
        result = {"data_type_compatibility": True}
        
        # Try to convert to numeric
        numeric_data = pd.to_numeric(data, errors='coerce')
        non_numeric = data.isnull() != numeric_data.isnull()
        
        if non_numeric.any():
            result["data_type_compatibility"] = False
            result["errors"] = [f"Found {non_numeric.sum()} non-numeric values"]
        
        return result
    
    def _validate_float(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate FLOAT columns."""
        return self._validate_decimal(data, metadata)
    
    def _validate_double(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate DOUBLE columns."""
        return self._validate_decimal(data, metadata)
    
    def _validate_date(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate DATE columns."""
        result = {"data_type_compatibility": True}
        
        # Try to parse dates
        date_data = pd.to_datetime(data, errors='coerce')
        invalid_dates = data.isnull() != date_data.isnull()
        
        if invalid_dates.any():
            result["data_type_compatibility"] = False
            result["errors"] = [f"Found {invalid_dates.sum()} invalid date values"]
        
        return result
    
    def _validate_datetime(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate DATETIME columns."""
        return self._validate_date(data, metadata)
    
    def _validate_timestamp(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate TIMESTAMP columns."""
        return self._validate_date(data, metadata)
    
    def _validate_email(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate EMAIL columns."""
        result = {"data_type_compatibility": True}
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        invalid_emails = ~data.astype(str).str.match(email_pattern, na=False)
        
        if invalid_emails.any():
            result["data_type_compatibility"] = False
            result["errors"] = [f"Found {invalid_emails.sum()} invalid email addresses"]
        
        return result
    
    def _validate_phone(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate PHONE columns."""
        result = {"data_type_compatibility": True}
        
        # Basic phone number pattern
        phone_pattern = r'^[\+]?[1-9][\d]{0,15}$'
        invalid_phones = ~data.astype(str).str.replace(r'[\s\-\(\)]', '', regex=True).str.match(phone_pattern, na=False)
        
        if invalid_phones.any():
            result["data_type_compatibility"] = False
            result["errors"] = [f"Found {invalid_phones.sum()} invalid phone numbers"]
        
        return result
    
    def _validate_url(self, data: pd.Series, metadata: Dict) -> Dict[str, Any]:
        """Validate URL columns."""
        result = {"data_type_compatibility": True}
        
        url_pattern = r'^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$'
        invalid_urls = ~data.astype(str).str.match(url_pattern, na=False)
        
        if invalid_urls.any():
            result["data_type_compatibility"] = False
            result["errors"] = [f"Found {invalid_urls.sum()} invalid URLs"]
        
        return result
    
    def _extract_max_length(self, metadata: Dict) -> Optional[int]:
        """Extract maximum length from column metadata."""
        # This would need to be implemented based on your database schema
        # For now, return None
        return None
    
    def _check_data_formats(self, data: pd.Series, column_name: str) -> List[str]:
        """Check for common data format issues."""
        issues = []
        
        # Check for leading/trailing whitespace
        if data.astype(str).str.strip().ne(data.astype(str)).any():
            issues.append("Contains leading/trailing whitespace")
        
        # Check for mixed case inconsistencies
        if data.astype(str).str.islower().any() and data.astype(str).str.isupper().any():
            issues.append("Mixed case inconsistencies")
        
        # Check for special characters in numeric fields
        if column_name.lower() in ['id', 'amount', 'price', 'quantity']:
            non_numeric = data.astype(str).str.contains(r'[^\d\.\-\+]', na=False)
            if non_numeric.any():
                issues.append("Contains non-numeric characters")
        
        return issues
    
    def _calculate_quality_score(self, column_validations: Dict) -> float:
        """Calculate overall data quality score."""
        if not column_validations:
            return 0.0
        
        total_columns = len(column_validations)
        valid_columns = sum(1 for v in column_validations.values() if v.get("valid", False))
        
        return round(valid_columns / total_columns, 3) if total_columns > 0 else 0.0
    
    def _generate_recommendations(self, column_validations: Dict) -> List[str]:
        """Generate data quality recommendations."""
        recommendations = []
        
        for header, validation in column_validations.items():
            if not validation.get("valid", True):
                recommendations.append(f"Fix data issues in column '{header}'")
            
            if validation.get("null_violations", 0) > 0:
                recommendations.append(f"Handle null values in column '{header}'")
            
            if validation.get("format_violations", 0) > 0:
                recommendations.append(f"Clean data format in column '{header}'")
        
        return recommendations

# Global validator instance
validator = DataValidator()
