"""
State management for tracking migration progress
"""
import sqlite3
import logging
from datetime import datetime
from typing import Optional, List, Dict
import json

logger = logging.getLogger(__name__)


class StateManager:
    """Manages migration state and progress tracking"""
    
    def __init__(self, db_file: str):
        """
        Initialize state manager
        
        Args:
            db_file: SQLite database file path
        """
        self.db_file = db_file
        self.conn = None
        self._init_database()
    
    def _init_database(self):
        """Initialize database tables"""
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        cursor = self.conn.cursor()
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                source_email TEXT PRIMARY KEY,
                dest_email TEXT,
                status TEXT,
                files_total INTEGER DEFAULT 0,
                files_completed INTEGER DEFAULT 0,
                files_failed INTEGER DEFAULT 0,
                start_time TEXT,
                end_time TEXT,
                last_updated TEXT
            )
        ''')
        
        # Files table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                file_id TEXT PRIMARY KEY,
                source_email TEXT,
                file_name TEXT,
                mime_type TEXT,
                size INTEGER,
                status TEXT,
                dest_file_id TEXT,
                error_message TEXT,
                attempt_count INTEGER DEFAULT 0,
                last_attempt TEXT,
                completed_time TEXT,
                FOREIGN KEY (source_email) REFERENCES users(source_email)
            )
        ''')
        
        # Migration runs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS migration_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TEXT,
                end_time TEXT,
                status TEXT,
                total_users INTEGER,
                total_files INTEGER,
                successful_files INTEGER,
                failed_files INTEGER,
                config TEXT
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_source_email ON files(source_email)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_status ON files(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)')
        
        self.conn.commit()
        logger.info(f"Database initialized: {self.db_file}")
    
    def start_migration_run(self, config: Dict) -> int:
        """
        Start a new migration run
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Run ID
        """
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO migration_runs 
            (start_time, status, config)
            VALUES (?, ?, ?)
        ''', (datetime.now().isoformat(), 'in_progress', json.dumps(config)))
        
        self.conn.commit()
        run_id = cursor.lastrowid
        logger.info(f"Started migration run: {run_id}")
        return run_id
    
    def end_migration_run(self, run_id: int, status: str, summary: Dict):
        """
        End a migration run
        
        Args:
            run_id: Run ID
            status: Final status
            summary: Summary statistics
        """
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE migration_runs
            SET end_time = ?, status = ?,
                total_users = ?, total_files = ?,
                successful_files = ?, failed_files = ?
            WHERE run_id = ?
        ''', (
            datetime.now().isoformat(),
            status,
            summary.get('total_users', 0),
            summary.get('total_files', 0),
            summary.get('successful_files', 0),
            summary.get('failed_files', 0),
            run_id
        ))
        
        self.conn.commit()
        logger.info(f"Ended migration run {run_id}: {status}")
    
    def add_user(self, source_email: str, dest_email: str):
        """Add or update user record"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO users
            (source_email, dest_email, status, start_time, last_updated)
            VALUES (?, ?, ?, ?, ?)
        ''', (source_email, dest_email, 'pending', 
              datetime.now().isoformat(), datetime.now().isoformat()))
        
        self.conn.commit()
    
    def update_user_status(self, source_email: str, status: str):
        """Update user migration status"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE users
            SET status = ?, last_updated = ?
            WHERE source_email = ?
        ''', (status, datetime.now().isoformat(), source_email))
        
        self.conn.commit()
    
    def mark_user_completed(self, source_email: str):
        """Mark user migration as completed"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE users
            SET status = 'completed', end_time = ?, last_updated = ?
            WHERE source_email = ?
        ''', (datetime.now().isoformat(), datetime.now().isoformat(), source_email))
        
        self.conn.commit()
        logger.debug(f"Marked user completed: {source_email}")
    
    def is_user_completed(self, source_email: str) -> bool:
        """Check if user migration is completed"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT status FROM users WHERE source_email = ?
        ''', (source_email,))
        
        row = cursor.fetchone()
        return row and row['status'] == 'completed'
    
    def add_file(self, file_id: str, source_email: str, file_name: str, 
                mime_type: str, size: int):
        """Add file record"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO files
            (file_id, source_email, file_name, mime_type, size, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (file_id, source_email, file_name, mime_type, size, 'pending'))
        
        self.conn.commit()
    
    def mark_file_completed(self, file_id: str, source_email: str, dest_file_id: str = None):
        """Mark file as successfully migrated"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE files
            SET status = 'completed',
                dest_file_id = ?,
                completed_time = ?,
                last_attempt = ?
            WHERE file_id = ?
        ''', (dest_file_id, datetime.now().isoformat(), 
              datetime.now().isoformat(), file_id))
        
        # Update user stats
        cursor.execute('''
            UPDATE users
            SET files_completed = files_completed + 1,
                last_updated = ?
            WHERE source_email = ?
        ''', (datetime.now().isoformat(), source_email))
        
        self.conn.commit()
    
    def mark_file_failed(self, file_id: str, source_email: str, error: str):
        """Mark file as failed"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE files
            SET status = 'failed',
                error_message = ?,
                attempt_count = attempt_count + 1,
                last_attempt = ?
            WHERE file_id = ?
        ''', (error, datetime.now().isoformat(), file_id))
        
        # Update user stats
        cursor.execute('''
            UPDATE users
            SET files_failed = files_failed + 1,
                last_updated = ?
            WHERE source_email = ?
        ''', (datetime.now().isoformat(), source_email))
        
        self.conn.commit()
    
    def is_file_completed(self, file_id: str) -> bool:
        """Check if file is already migrated"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT status FROM files WHERE file_id = ?
        ''', (file_id,))
        
        row = cursor.fetchone()
        return row and row['status'] == 'completed'
    
    def get_failed_files(self, source_email: Optional[str] = None) -> List[Dict]:
        """Get list of failed files"""
        cursor = self.conn.cursor()
        
        if source_email:
            cursor.execute('''
                SELECT * FROM files 
                WHERE status = 'failed' AND source_email = ?
                ORDER BY last_attempt DESC
            ''', (source_email,))
        else:
            cursor.execute('''
                SELECT * FROM files 
                WHERE status = 'failed'
                ORDER BY last_attempt DESC
            ''')
        
        return [dict(row) for row in cursor.fetchall()]
    
    def get_user_progress(self, source_email: str) -> Dict:
        """Get migration progress for a user"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM users WHERE source_email = ?
        ''', (source_email,))
        
        row = cursor.fetchone()
        if row:
            return dict(row)
        return {}
    
    def get_overall_progress(self) -> Dict:
        """Get overall migration progress"""
        cursor = self.conn.cursor()
        
        # User stats
        cursor.execute('''
            SELECT 
                COUNT(*) as total_users,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_users,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_users,
                SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress_users
            FROM users
        ''')
        user_stats = dict(cursor.fetchone())
        
        # File stats
        cursor.execute('''
            SELECT 
                COUNT(*) as total_files,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_files,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_files,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_files,
                SUM(size) as total_size
            FROM files
        ''')
        file_stats = dict(cursor.fetchone())
        
        return {**user_stats, **file_stats}
    
    def reset_failed_files(self, max_attempts: int = 3):
        """Reset failed files for retry (if under max attempts)"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE files
            SET status = 'pending', error_message = NULL
            WHERE status = 'failed' AND attempt_count < ?
        ''', (max_attempts,))
        
        affected = cursor.rowcount
        self.conn.commit()
        logger.info(f"Reset {affected} failed files for retry")
        return affected
    
    def export_state_report(self, output_file: str):
        """Export current state to JSON"""
        progress = self.get_overall_progress()
        
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM users')
        users = [dict(row) for row in cursor.fetchall()]
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'progress': progress,
            'users': users
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"State report exported to {output_file}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()