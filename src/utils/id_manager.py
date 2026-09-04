"""
ID Manager - Ensures unique IDs across runs
Stores last used ID in a file
"""

import json
import os
from pathlib import Path

class IDManager:
    """Manages auto-incrementing IDs across runs for multiple entity types"""
    
    def __init__(self, storage_file='data/last_ids.json'):
        self.storage_file = storage_file
        self.last_ids = self._load_last_ids()
    
    def _load_last_ids(self):
        """Load last used IDs from file"""
        
        if os.path.exists(self.storage_file):
            with open(self.storage_file, 'r') as f:
                return json.load(f)
        else:
            Path(self.storage_file).parent.mkdir(parents=True, exist_ok=True)
            return {}
    
    def get_next_id(self, id_type):
        """
        Get next available ID for a given type
        
        Args:
            id_type: Type of ID (e.g., 'order', 'customer', 'order_item')
        
        Returns:
            Next available ID
        """
        current = self.last_ids.get(id_type, 0)
        next_id = current + 1
        self.last_ids[id_type] = next_id
        
        return next_id
    
    def get_next_batch(self, id_type, count):
        """
        Get next N IDs for a given type
        
        Args:
            id_type: Type of ID (e.g., 'order', 'customer', 'order_item')
            count: Number of IDs to get
        
        Returns:
            List of IDs
        """
        current = self.last_ids.get(id_type, 0)
        next_ids = list(range(current + 1, current + count + 1))
        self.last_ids[id_type] = current + count
        
        return next_ids
    
    def get_current_max(self, id_type):
        """
        Get current max ID for a given type (without incrementing)
        
        Args:
            id_type: Type of ID (e.g., 'order', 'customer', 'order_item')
        
        Returns:
            Current max ID (0 if none exists)
        """
        return self.last_ids.get(id_type, 0)
    
    def set_max(self, id_type, value):
        """
        Manually set the max ID for a given type
        
        Args:
            id_type: Type of ID
            value: Max ID value
        """
        self.last_ids[id_type] = value
    
    def save(self):
        """Persist last used IDs to file"""
        
        with open(self.storage_file, 'w') as f:
            json.dump(self.last_ids, f, indent=2)
    
    def status(self):
        """Print current ID status"""
        print("\n" + "=" * 70)
        print("📊 ID MANAGER STATUS")
        print("=" * 70)
        
        if not self.last_ids:
            print("   No IDs tracked yet")
        else:
            for id_type, max_id in sorted(self.last_ids.items()):
                print(f"   {id_type}: {max_id:,}")
        
        print(f"   Storage: {self.storage_file}")
        print("=" * 70)


def get_id_manager(storage_file='data/last_ids.json'):
    """
    Get or create the global ID manager instance
    
    Args:
        storage_file: Path to JSON file storing last used IDs
    
    Returns:
        IDManager instance
    """
    global _id_manager
    
    if _id_manager is None:
        _id_manager = IDManager(storage_file)
    
    return _id_manager
