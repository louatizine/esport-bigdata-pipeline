"""
Storage module for Phase 5: BI Integration.
Loads analytics data to PostgreSQL and MongoDB.
"""

from .storage_main import StorageOrchestrator

__all__ = [
    "StorageOrchestrator",
]
