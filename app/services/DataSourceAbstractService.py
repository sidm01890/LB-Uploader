from abc import ABC, abstractmethod
from typing import List

from fastapi import UploadFile
from sqlalchemy.orm import Session


class DataSourceService(ABC):
    """Abstract base class for data source services"""
    
    @abstractmethod
    def upload(self, files: List[str], db: Session, username: str) -> int:
        """
        Upload and process files for this data source
        
        Returns:
            Total number of rows inserted/updated
        """
        pass

    async def upload_file(self, files: List[UploadFile]) -> List[str]:
        """
        Save uploaded files to disk and return file paths.
        Should use streaming for large files to prevent memory issues.
        """
        pass

