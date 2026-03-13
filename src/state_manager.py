"""State management for tracking processed Iceberg snapshots."""

import json
import os
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path


class StateManager:
    """
    Manages ETL state persistence for incremental processing.

    Tracks the last processed Iceberg snapshot ID to enable idempotent,
    incremental data processing.
    """

    def __init__(self, state_file_path: str):
        """
        Initialize StateManager.

        Args:
            state_file_path: Path to JSON file for state persistence
        """
        self.state_file_path = Path(state_file_path)
        # Ensure parent directory exists
        self.state_file_path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Dict[str, Any]:
        """
        Load state from JSON file.

        Returns:
            Dictionary with 'snapshot_id' and 'timestamp' keys, or empty dict if not found
        """
        if not self.state_file_path.exists():
            return {}

        try:
            with open(self.state_file_path, 'r') as f:
                state = json.load(f)
            return state
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not load state file: {e}")
            return {}

    def save(self, snapshot_id: int, timestamp: Optional[str] = None) -> None:
        """
        Save state to JSON file atomically.

        Args:
            snapshot_id: Iceberg snapshot ID that was processed
            timestamp: ISO format timestamp (default: current UTC time)
        """
        if timestamp is None:
            timestamp = datetime.utcnow().isoformat()

        state = {
            'snapshot_id': snapshot_id,
            'timestamp': timestamp,
        }

        # Atomic write: write to temp file then rename
        temp_path = self.state_file_path.with_suffix('.tmp')
        try:
            with open(temp_path, 'w') as f:
                json.dump(state, f, indent=2)

            # Atomic rename
            temp_path.replace(self.state_file_path)
            print(f"State saved: snapshot_id={snapshot_id}, timestamp={timestamp}")
        except IOError as e:
            if temp_path.exists():
                temp_path.unlink()
            raise IOError(f"Failed to save state: {e}") from e

    def get_last_snapshot_id(self) -> Optional[int]:
        """
        Get the last processed snapshot ID.

        Returns:
            Last processed snapshot ID, or None if no state exists
        """
        state = self.load()
        return state.get('snapshot_id')

    def get_last_timestamp(self) -> Optional[str]:
        """
        Get the timestamp of the last processing run.

        Returns:
            ISO format timestamp string, or None if no state exists
        """
        state = self.load()
        return state.get('timestamp')

    def clear(self) -> None:
        """Delete the state file to reset processing state."""
        if self.state_file_path.exists():
            self.state_file_path.unlink()
            print(f"State cleared: {self.state_file_path}")
