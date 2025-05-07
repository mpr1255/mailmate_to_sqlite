#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# ///
"""
MailMate to SQLite Email Importer

This script reads email data from MailMate format and imports it into a SQLite database.
MailMate stores emails as individual files with metadata in headers.

NOTE: If you encounter errors about missing tables (especially "folders_old"),
      it's recommended to delete the database file and start fresh with:
      rm mail.db
"""

import os
import sys
import email
import argparse
import json
import sqlite3
from datetime import datetime
import logging
import email.utils
import re
from pathlib import Path
import random
import signal

# Set up logging
LOG_FILE_PATH = "/tmp/mailmate.log" # Define log file path

# Ensure the directory for the log file exists
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE_PATH) # Add file handler
    ]
)
logger = logging.getLogger('mailmate-importer')

class MailMateImporter:
    def __init__(self, db_path, mailmate_dir, batch_size=1000):
        """
        Initialize the importer with database path and MailMate directory
        
        Args:
            db_path (str): Path to SQLite database file
            mailmate_dir (str): Path to MailMate messages directory
            batch_size (int): Number of emails to process in a single batch
        """
        self.db_path = db_path
        self.mailmate_dir = Path(mailmate_dir)
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.skip_existing = False
        self.use_paths_dedupe = False
        self.timeout = 60
        self.shutdown_requested = False
        
    def connect_to_db(self):
        """Establish connection to SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            
            # Performance optimizations
            self.conn.execute("PRAGMA journal_mode=WAL")  # Enable Write-Ahead Logging
            self.conn.execute("PRAGMA synchronous=NORMAL")  # Less durable but faster than FULL
            self.conn.execute("PRAGMA cache_size=-102400")  # 100MB cache (negative means KB)
            self.conn.execute("PRAGMA temp_store=MEMORY")  # Store temp tables in memory
            self.conn.execute("PRAGMA mmap_size=1073741824")  # Memory-map up to 1GB
            
            # Enable foreign keys
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.cursor = self.conn.cursor()
            logger.info(f"Successfully connected to SQLite database at {self.db_path}")
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            sys.exit(1)
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            # Check if folders table exists
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='folders'")
            folders_exists = self.cursor.fetchone() is not None
            
            # Create tables with correct schema
            self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE,
                subject TEXT,
                from_address TEXT,
                to_address TEXT,
                cc TEXT,
                bcc TEXT,
                date TIMESTAMP,
                received_date TIMESTAMP,
                content_type TEXT,
                size INTEGER,
                has_attachments BOOLEAN DEFAULT 0,
                body_text TEXT,
                body_html TEXT,
                headers TEXT,
                path TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_id INTEGER,
                filename TEXT,
                content_type TEXT,
                size INTEGER,
                content_id TEXT,
                path TEXT,
                FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE
            );
            """)
            
            # Handle the folders table separately to avoid schema issues
            if not folders_exists:
                # If folders table doesn't exist, create it with correct schema
                self.cursor.execute("""
                CREATE TABLE folders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    path TEXT UNIQUE
                )
                """)
                logger.info("Created new folders table")
            else:
                # Check if 'name' column has a UNIQUE constraint
                self.cursor.execute("PRAGMA table_info(folders)")
                columns = self.cursor.fetchall()
                
                has_name_unique = False
                for col in columns:
                    name = col[1]
                    type_name = col[2]
                    if name == 'name' and 'UNIQUE' in type_name:
                        has_name_unique = True
                
                if has_name_unique:
                    logger.info("Fixing folders table to remove UNIQUE constraint on name...")
                    # Create a temporary table with the correct schema
                    self.cursor.executescript("""
                    CREATE TABLE folders_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT,
                        path TEXT UNIQUE
                    );
                    
                    -- Copy data without violating constraints
                    INSERT OR IGNORE INTO folders_new (id, name, path)
                    SELECT id, name, path FROM folders;
                    
                    -- Drop the old table
                    DROP TABLE folders;
                    
                    -- Rename the new table
                    ALTER TABLE folders_new RENAME TO folders;
                    """)
                    logger.info("Fixed folders table schema")
            
            # Create remaining tables
            self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS email_folders (
                email_id INTEGER,
                folder_id INTEGER,
                PRIMARY KEY (email_id, folder_id),
                FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
                FOREIGN KEY (folder_id) REFERENCES folders(id) ON DELETE CASCADE
            );
            
            CREATE INDEX IF NOT EXISTS idx_emails_date ON emails(date);
            CREATE INDEX IF NOT EXISTS idx_emails_from ON emails(from_address);
            CREATE INDEX IF NOT EXISTS idx_emails_message_id ON emails(message_id);
            """)
            
            self.conn.commit()
            logger.info("Database tables created or already exist")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            self.conn.rollback()
            sys.exit(1)
    
    def parse_email_file(self, file_path, timeout=10):
        """
        Parse a MailMate email file with a timeout to prevent hanging
        
        Args:
            file_path (Path): Path to the email file
            timeout (int): Maximum time in seconds to spend parsing a single email
            
        Returns:
            dict: Email data dictionary or None if error/timeout
        """
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Parsing email timed out after {timeout} seconds")
        
        try:
            # Set timeout
            if timeout > 0:
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(timeout)
            
            # Attempt to open and parse the file
            try:
                with open(file_path, 'rb') as f:
                    msg = email.message_from_binary_file(f)
                
                # Extract basic headers
                message_id = msg.get('Message-ID', '').strip('<>')
                subject = msg.get('Subject', '')
                from_addr = msg.get('From', '')
                to_addr = msg.get('To', '')
                cc = msg.get('Cc', '')
                bcc = msg.get('Bcc', '')
                
                # Parse dates
                date_str = msg.get('Date', '')
                received_str = msg.get('Received', '')
                
                date = None
                if date_str:
                    try:
                        date_tuple = email.utils.parsedate_tz(date_str)
                        if date_tuple:
                            date = datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
                    except Exception:
                        pass
                
                received_date = None
                if received_str:
                    try:
                        # Extract date part from received header
                        date_match = re.search(r';(.*)', received_str)
                        if date_match:
                            received_date_str = date_match.group(1).strip()
                            date_tuple = email.utils.parsedate_tz(received_date_str)
                            if date_tuple:
                                received_date = datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
                    except Exception:
                        pass
                
                # Get content type
                content_type = msg.get_content_type()
                
                # Get file size
                size = os.path.getsize(file_path)
                
                # Extract body parts
                body_text = ""
                body_html = ""
                has_attachments = False
                attachments = []
                
                if msg.is_multipart():
                    for part in msg.walk():
                        content_disposition = part.get("Content-Disposition", "")
                        
                        if "attachment" in content_disposition:
                            has_attachments = True
                            filename = part.get_filename()
                            if not filename:
                                filename = f"unknown-{len(attachments)}"
                            
                            attachments.append({
                                "filename": filename,
                                "content_type": part.get_content_type(),
                                "size": len(part.get_payload(decode=True) or b''),
                                "content_id": part.get("Content-ID", "").strip("<>"),
                                "path": str(file_path)
                            })
                        elif part.get_content_type() == "text/plain":
                            payload = part.get_payload(decode=True)
                            if payload:
                                charset = part.get_content_charset() or 'utf-8'
                                try:
                                    body_text += payload.decode(charset, errors='replace')
                                except Exception:
                                    body_text += payload.decode('utf-8', errors='replace')
                        
                        elif part.get_content_type() == "text/html":
                            payload = part.get_payload(decode=True)
                            if payload:
                                charset = part.get_content_charset() or 'utf-8'
                                try:
                                    body_html += payload.decode(charset, errors='replace')
                                except Exception:
                                    body_html += payload.decode('utf-8', errors='replace')
                else:
                    payload = msg.get_payload(decode=True)
                    if payload:
                        charset = msg.get_content_charset() or 'utf-8'
                        try:
                            if content_type == "text/plain":
                                body_text = payload.decode(charset, errors='replace')
                            elif content_type == "text/html":
                                body_html = payload.decode(charset, errors='replace')
                        except Exception:
                            if content_type == "text/plain":
                                body_text = payload.decode('utf-8', errors='replace')
                            elif content_type == "text/html":
                                body_html = payload.decode('utf-8', errors='replace')
                
                # Extract all headers into a dictionary, ensuring they're serializable
                headers = {}
                for k, v in msg.items():
                    # Convert Header objects to strings
                    if isinstance(v, email.header.Header):
                        headers[k] = str(v)
                    else:
                        headers[k] = v
                
                # Fix for relative path calculation
                folder_path = file_path.parent
                # Try to compute relative path, but handle case where it fails
                try:
                    relative_folder_path = folder_path.relative_to(self.mailmate_dir)
                except ValueError:
                    # If the exact relative path can't be calculated, make a best effort
                    # This happens when .noindex is in the path or other path differences
                    str_folder = str(folder_path)
                    str_mailmate = str(self.mailmate_dir)
                    
                    # Try to find the common "Messages" part in both paths
                    if "Messages" in str_folder and "Messages" in str_mailmate:
                        idx_folder = str_folder.find("Messages")
                        idx_mailmate = str_mailmate.find("Messages")
                        
                        if idx_folder >= 0 and idx_mailmate >= 0:
                            # Get the part after "Messages" in the folder path
                            suffix = str_folder[idx_folder + len("Messages"):]
                            relative_folder_path = Path("Messages" + suffix)
                        else:
                            # Fallback: Just use the last 3 path components
                            relative_folder_path = Path(*file_path.parent.parts[-3:])
                    else:
                        # Fallback: Just use the last 3 path components
                        relative_folder_path = Path(*file_path.parent.parts[-3:])
                
                # Normalize the file path
                normalized_path = os.path.abspath(os.path.normpath(str(file_path)))
                
                # Clear timeout
                if timeout > 0:
                    signal.alarm(0)
                
                return {
                    "message_id": message_id,
                    "subject": subject,
                    "from_address": from_addr,
                    "to_address": to_addr,
                    "cc": cc,
                    "bcc": bcc,
                    "date": date,
                    "received_date": received_date,
                    "content_type": content_type,
                    "size": size,
                    "has_attachments": has_attachments,
                    "body_text": body_text,
                    "body_html": body_html,
                    "headers": headers,
                    "path": normalized_path,
                    "folder": str(relative_folder_path),
                    "attachments": attachments
                }
            
            finally:
                # Always clear the alarm
                if timeout > 0:
                    signal.alarm(0)
                
        except TimeoutError as e:
            logger.error(f"Timeout parsing email {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing email {file_path}: {e}")
            return None
    
    def insert_folder(self, folder_path, commit=True):
        """
        Insert a folder into the database
        
        Args:
            folder_path (str): Folder path
            commit (bool): Whether to commit the transaction after inserting the folder
            
        Returns:
            int: Folder ID
        """
        try:
            # Try to get existing folder first
            self.cursor.execute("SELECT id FROM folders WHERE path = ?", (folder_path,))
            result = self.cursor.fetchone()
            
            if result:
                return result[0]
            
            # If folder doesn't exist, insert it
            folder_name = folder_path.split('/')[-1]
            try:
                self.cursor.execute(
                    "INSERT INTO folders (name, path) VALUES (?, ?)",
                    (folder_name, folder_path)
                )
                if commit:
                    self.conn.commit()
                
                # Get the last inserted ID
                folder_id = self.cursor.lastrowid
                logger.debug(f"Created new folder: {folder_name} at path: {folder_path}")
                return folder_id
            except sqlite3.IntegrityError as e:
                # This could happen if another thread/process inserted the folder between our check and insert
                logger.debug(f"Integrity error inserting folder {folder_path}: {e}, retrying lookup")
                self.cursor.execute("SELECT id FROM folders WHERE path = ?", (folder_path,))
                result = self.cursor.fetchone()
                if result:
                    return result[0]
                else:
                    raise
        except Exception as e:
            logger.error(f"Error inserting folder {folder_path} with name {folder_path.split('/')[-1]}: {e}")
            if commit:
                self.conn.rollback()
            return None
    
    def insert_email(self, email_data, commit=True):
        """
        Insert an email and its attachments into the database
        
        Args:
            email_data (dict): Email data dictionary
            commit (bool): Whether to commit the transaction after inserting the email
            
        Returns:
            int: Email ID or None if error
        """
        try:
            # Check if email with message_id exists
            email_id = None
            message_id = email_data["message_id"]
            
            if message_id:
                self.cursor.execute("SELECT id FROM emails WHERE message_id = ?", (message_id,))
                result = self.cursor.fetchone()
                if result:
                    email_id = result[0]
                    
                    # Update existing email
                    self.cursor.execute("""
                    UPDATE emails SET
                        subject = ?,
                        from_address = ?,
                        to_address = ?,
                        cc = ?,
                        bcc = ?,
                        date = ?,
                        received_date = ?,
                        content_type = ?,
                        size = ?,
                        has_attachments = ?,
                        body_text = ?,
                        body_html = ?,
                        headers = ?,
                        path = ?,
                        imported_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """, (
                        email_data["subject"],
                        email_data["from_address"],
                        email_data["to_address"],
                        email_data["cc"],
                        email_data["bcc"],
                        email_data["date"].isoformat() if email_data["date"] else None,
                        email_data["received_date"].isoformat() if email_data["received_date"] else None,
                        email_data["content_type"],
                        email_data["size"],
                        1 if email_data["has_attachments"] else 0,
                        email_data["body_text"],
                        email_data["body_html"],
                        json.dumps(email_data["headers"]),
                        email_data["path"],
                        email_id
                    ))
            
            # If email doesn't exist, insert new one
            if not email_id:
                try:
                    self.cursor.execute("""
                    INSERT INTO emails (
                        message_id, subject, from_address, to_address, cc, bcc, date,
                        received_date, content_type, size, has_attachments, body_text,
                        body_html, headers, path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        email_data["message_id"],
                        email_data["subject"],
                        email_data["from_address"],
                        email_data["to_address"],
                        email_data["cc"],
                        email_data["bcc"],
                        email_data["date"].isoformat() if email_data["date"] else None,
                        email_data["received_date"].isoformat() if email_data["received_date"] else None,
                        email_data["content_type"],
                        email_data["size"],
                        1 if email_data["has_attachments"] else 0,
                        email_data["body_text"],
                        email_data["body_html"],
                        json.dumps(email_data["headers"]),
                        email_data["path"]
                    ))
                    
                    email_id = self.cursor.lastrowid
                except sqlite3.IntegrityError:
                    # If we get an integrity error, the email was inserted by another thread/process
                    # Let's try to get its ID
                    self.cursor.execute("SELECT id FROM emails WHERE message_id = ?", (message_id,))
                    result = self.cursor.fetchone()
                    if result:
                        email_id = result[0]
                        logger.debug(f"Email was inserted by another process: {message_id}")
                    else:
                        # If we still can't find it, re-raise the error
                        raise
            
            # Insert attachments
            if email_id and email_data["attachments"]:
                # First delete existing attachments for this email
                self.cursor.execute("DELETE FROM attachments WHERE email_id = ?", (email_id,))
                
                # Insert new attachments
                for attachment in email_data["attachments"]:
                    self.cursor.execute("""
                    INSERT INTO attachments (
                        email_id, filename, content_type, size, content_id, path
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        email_id,
                        attachment["filename"],
                        attachment["content_type"],
                        attachment["size"],
                        attachment["content_id"],
                        attachment["path"]
                    ))
            
            # Insert folder relation
            if email_id:
                folder_id = self.insert_folder(email_data["folder"], commit=commit)
                if folder_id:
                    # Check if relation exists
                    self.cursor.execute(
                        "SELECT 1 FROM email_folders WHERE email_id = ? AND folder_id = ?",
                        (email_id, folder_id)
                    )
                    if not self.cursor.fetchone():
                        self.cursor.execute(
                            "INSERT INTO email_folders (email_id, folder_id) VALUES (?, ?)",
                            (email_id, folder_id)
                        )
            
            if commit:
                self.conn.commit()
            return email_id
        
        except Exception as e:
            logger.error(f"Error inserting email {email_data.get('message_id')}: {e}")
            if commit:
                self.conn.rollback()
            return None
    
    def find_email_files(self):
        """
        Find all email files in the MailMate directory
        Uses fd if available (much faster), otherwise falls back to os.walk
        
        Returns:
            list: List of paths to email files
        """
        email_files = []
        
        # First, try to use fd if it's available (much faster)
        try:
            import subprocess
            import shutil
            
            # Check if fd is available
            if shutil.which('fd'):
                logger.info("Using 'fd' for fast file discovery...")
                
                # Construct the command - exclude .plist and hidden files
                cmd = [
                    'fd', '.', 
                    '--type', 'f',                # Only regular files
                    '--exclude', '*.plist',       # Exclude plist files
                    '--exclude', '*/.*',          # Exclude hidden files 
                    '--absolute-path',            # Return absolute paths
                    str(self.mailmate_dir)        # Search in mailmate dir
                ]
                
                # Run fd command
                start_time = datetime.now()
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    # Process each line as a file path
                    for line in result.stdout.splitlines():
                        email_files.append(Path(line))
                    
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    logger.info(f"Found {len(email_files)} potential email files with fd in {duration:.2f}s")
                    
                    # Print some sample paths
                    if email_files and logger.level <= logging.INFO:
                        sample_paths = []
                        for i in range(min(3, len(email_files))):
                            sample_paths.append(str(email_files[i]))
                        logger.info(f"Sample paths from fd: {sample_paths}")
                    
                    return email_files
                else:
                    logger.warning(f"fd command failed with error: {result.stderr}")
                    # Fall back to os.walk
            else:
                logger.info("fd not found, falling back to standard file discovery...")
        except Exception as e:
            logger.warning(f"Error using fd: {e}, falling back to standard file discovery...")
        
        # Fall back to traditional os.walk method
        logger.info("Using standard os.walk for file discovery (slower)...")
        start_time = datetime.now()
        for root, dirs, files in os.walk(self.mailmate_dir):
            for file in files:
                file_path = Path(root) / file
                if file_path.is_file() and not file.endswith('.plist') and not file.startswith('.'):
                    email_files.append(file_path)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Found {len(email_files)} potential email files with os.walk in {duration:.2f}s")
        return email_files
    
    def import_emails(self):
        """Main method to import emails from MailMate to SQLite"""
        if hasattr(self, 'connect_to_db'):
            self.connect_to_db()
            self.create_tables()
        
        existing_message_ids = set()
        existing_paths = set()
        initial_count = 0 # Initialize initial_count
        
        if self.skip_existing:
            try:
                self.cursor.execute("SELECT message_id FROM emails WHERE message_id IS NOT NULL")
                for row in self.cursor.fetchall():
                    msg_id_from_db = row[0]
                    existing_message_ids.add(msg_id_from_db)
                
                logger.info(f"Found {len(existing_message_ids)} existing emails in the database (by Message-ID)")

                if self.use_paths_dedupe:
                    logger.info("Loading paths from database for deduplication...")
                    start_time = datetime.now()
                    self.cursor.execute("SELECT path FROM emails WHERE path IS NOT NULL")
                    
                    path_count = 0
                    alt_path_count = 0
                    
                    for row in self.cursor.fetchall():
                        path_count += 1
                        orig_path = row[0]
                        norm_path = os.path.normpath(orig_path)
                        existing_paths.add(norm_path)
                        
                        if "/Messages/" in norm_path and ".noindex" not in norm_path:
                            alt_path = norm_path.replace("/Messages/", "/Messages.noindex/")
                            existing_paths.add(alt_path)
                            alt_path_count += 1
                        elif "/Messages.noindex/" in norm_path:
                            alt_path = norm_path.replace("/Messages.noindex/", "/Messages/")
                            existing_paths.add(alt_path)
                            alt_path_count += 1
                    
                    path_load_end = datetime.now()
                    duration = (path_load_end - start_time).total_seconds()
                    logger.info(f"Loaded {path_count} file paths and generated {alt_path_count} alternates in {duration:.2f}s")
                    logger.info(f"Total unique paths in memory for deduplication: {len(existing_paths)}")
                    
                    if logger.level <= logging.DEBUG and existing_paths:
                        sample_paths = list(existing_paths)[:3]
                        logger.debug(f"Example stored paths from DB (for dedupe): {sample_paths}")
            except Exception as e:
                logger.error(f"Error fetching existing data: {e}")
                existing_message_ids = set()
                existing_paths = set()
        
        all_files = self.find_email_files()
        initial_count = len(all_files) # Set initial_count after finding files
        
        files_to_process = []
        skipped_by_path_count = 0
        if self.skip_existing and self.use_paths_dedupe and existing_paths:
            for file_path_obj in all_files:
                should_skip = False
                norm_path = os.path.normpath(str(file_path_obj))
                
                if norm_path in existing_paths:
                    should_skip = True
                else:
                    if "/Messages.noindex/" in norm_path:
                        alt_path = norm_path.replace("/Messages.noindex/", "/Messages/")
                        if alt_path in existing_paths:
                            should_skip = True
                    elif "/Messages/" in norm_path:
                        alt_path = norm_path.replace("/Messages/", "/Messages.noindex/")
                        if alt_path in existing_paths:
                            should_skip = True
                
                if should_skip:
                    logger.debug(f"Skipping (path already in DB): {file_path_obj}")
                    skipped_by_path_count += 1
                else:
                    files_to_process.append(file_path_obj)
            
            logger.info(f"Path-based filtering: {skipped_by_path_count} files already in database (by path), {len(files_to_process)} potentially new to process")
            
            if skipped_by_path_count < 1000 and initial_count > 10000 and initial_count - skipped_by_path_count > 1000:
                logger.warning(f"Few path matches found ({skipped_by_path_count} of {initial_count}) - may indicate path comparison issues if many files are re-imported.")
                if all_files and existing_paths:
                    file_example = str(all_files[0])
                    db_example = next(iter(existing_paths), "No paths in DB example")
                    logger.info(f"Example path comparison for debugging:")
                    logger.info(f"  File path (normalized): {os.path.normpath(file_example)}")
                    logger.info(f"  DB path example:        {db_example}")
        else:
            files_to_process = all_files
        
        email_files = files_to_process
        total_files_to_process = len(email_files)
        
        logger.info(f"Starting processing of {total_files_to_process} email files...")

        processed = 0
        imported_new = 0
        updated = 0
        errors = 0
        skipped_empty = 0
        skipped_due_to_existing_message_id = 0 
        timeouts = 0 # This counter's accuracy depends on parse_email_file logic
        
        check_shutdown = hasattr(self, 'shutdown_requested') and callable(self.shutdown_requested)
        
        for i in range(0, total_files_to_process, self.batch_size):
            if check_shutdown and self.shutdown_requested():
                logger.info("Shutdown requested, breaking before new batch.")
                break
                
            batch = email_files[i:i+self.batch_size]
            batch_start_time = datetime.now()
            batch_new = 0
            batch_updated = 0
            batch_skipped_message_id = 0
            batch_timeouts = 0 # For batch summary, related to parse_email_file behavior
            batch_errors = 0
            
            if self.conn:
                self.conn.execute("BEGIN TRANSACTION")
            
            for file_path in batch:
                if check_shutdown and processed % 100 == 0 and self.shutdown_requested():
                    logger.info("Shutdown requested mid-batch, will complete current file then break batch.")
                    break 
                    
                processed += 1
                
                if processed % 100 == 0 and total_files_to_process > 0:
                    current_time = datetime.now().strftime("%H:%M:%S")
                    total_skipped_so_far = skipped_by_path_count + skipped_due_to_existing_message_id
                    logger.info(f"[{current_time}] Progress: {processed}/{total_files_to_process} ({(processed/total_files_to_process)*100:.1f}%) - "
                                f"New: {imported_new}, Updated: {updated}, Skipped (total): {total_skipped_so_far}, "
                                f"Empty: {skipped_empty}, Timeouts: {timeouts}, Errors: {errors}")

                logger.debug(f"Processing file: {file_path}")

                try:
                    timeout_value = getattr(self, 'timeout', 60)
                    email_data = self.parse_email_file(file_path, timeout=timeout_value)
                    if email_data is None:
                        # parse_email_file logs specifics. If it returns None, it's an error or timeout.
                        # We increment general errors; specific type (timeout) depends on parse_email_file's logging/behavior.
                        # For batch stats, we'll count this as a batch_error. `timeouts` is an overall counter.
                        errors +=1 # General error/timeout from parsing
                        batch_errors +=1
                        # Potentially increment batch_timeouts here if parse_email_file could signal a timeout specifically
                        continue 
                except TimeoutError: # Explicitly catch TimeoutError if parse_email_file re-raises it
                    logger.error(f"Timeout parsing email {file_path} (caught in import_emails)") # Already logged by parse_email_file
                    timeouts += 1
                    batch_timeouts += 1
                    errors += 1 # Count as an error for overall stats
                    batch_errors +=1
                    continue
                except Exception as e: 
                    logger.error(f"Unexpected error calling parse_email_file for {file_path}: {e}")
                    errors += 1
                    batch_errors += 1
                    continue
                
                if not email_data.get("message_id") :
                    logger.debug(f"Skipping (no Message-ID): {file_path}")
                    skipped_empty += 1
                    continue
                if not email_data.get("body_text") and not email_data.get("body_html"):
                    logger.debug(f"Skipping (empty body, Message-ID: {email_data.get('message_id')}): {file_path}")
                    skipped_empty += 1
                    continue
                
                message_id = email_data["message_id"]
                is_new_message_id = message_id not in existing_message_ids
                
                if self.skip_existing and not is_new_message_id:
                    
                    if not (message_id in existing_message_ids): # This should ideally not happen if not is_new_message_id is true
                        logger.error(f"CRITICAL LOGIC FLAW or UNEXPECTED STATE: {message_id} reported as duplicate (is_new_message_id=False) but test '(message_id in existing_message_ids)' is False!")
                    
                    logger.debug(f"Skipping (Message-ID duplicate: {message_id}): {file_path}")
                    skipped_due_to_existing_message_id += 1
                    batch_skipped_message_id += 1
                    continue
                
                try:
                    email_id_returned = self.insert_email(email_data, commit=False) 
                    if email_id_returned:
                        if is_new_message_id:
                            logger.debug(f"Inserted NEW (DB ID: {email_id_returned}, Message-ID: {message_id}): {file_path}")
                            imported_new += 1
                            batch_new += 1
                            existing_message_ids.add(message_id)
                            norm_path_for_set = os.path.normpath(str(file_path))
                            existing_paths.add(norm_path_for_set)
                            if "/Messages.noindex/" in norm_path_for_set:
                                existing_paths.add(norm_path_for_set.replace("/Messages.noindex/", "/Messages/"))
                            elif "/Messages/" in norm_path_for_set:
                                existing_paths.add(norm_path_for_set.replace("/Messages/", "/Messages.noindex/"))
                        else:
                            logger.debug(f"Updated EXISTING (DB ID: {email_id_returned}, Message-ID: {message_id}): {file_path}")
                            updated += 1
                            batch_updated += 1
                    else:
                        logger.error(f"Insert_email returned None/False for Message-ID: {message_id}, File: {file_path}. Check previous logs.")
                        errors += 1
                        batch_errors += 1
                except Exception as e:
                    logger.error(f"Error during insert_email call for Message-ID {message_id}, File {file_path}: {e}")
                    errors += 1
                    batch_errors += 1
            
            if check_shutdown and self.shutdown_requested() and i * self.batch_size + len(batch) < total_files_to_process:
                 logger.info("Shutdown caused batch to terminate early.")

            if self.conn and batch: 
                try:
                    self.conn.commit()
                    batch_end_time = datetime.now()
                    batch_duration = (batch_end_time - batch_start_time).total_seconds()
                    items_attempted_in_batch = len(batch) # Number of files *attempted* in this batch run
                    if batch_duration > 0 and items_attempted_in_batch > 0 :
                        emails_per_second = items_attempted_in_batch / batch_duration
                        current_time = batch_end_time.strftime("%H:%M:%S")
                        logger.info(f"[{current_time}] Batch committed: {items_attempted_in_batch} emails attempted in {batch_duration:.1f}s ({emails_per_second:.1f}/s) - "
                                  f"New: {batch_new}, Updated: {batch_updated}, Skipped (MsgID): {batch_skipped_message_id}, Batch Timeouts: {batch_timeouts}, Batch Errors: {batch_errors}")
                except Exception as e:
                    logger.error(f"Error committing batch: {e}")
                    if self.conn: self.conn.rollback()
                
                if check_shutdown and self.shutdown_requested():
                    logger.info("Shutdown requested, stopping after committing current batch.")
                    break 
        
        total_overall_skipped = skipped_by_path_count + skipped_due_to_existing_message_id

        logger.info("=" * 50)
        logger.info("Import Summary:")
        logger.info(f"Total files found by scanner: {initial_count}")
        if self.skip_existing and self.use_paths_dedupe:
             logger.info(f"Skipped by path deduplication: {skipped_by_path_count}")
        logger.info(f"Total files attempted for processing: {processed} (out of {total_files_to_process} after path filter)")
        logger.info(f"New emails imported:     {imported_new}")
        logger.info(f"Existing emails updated: {updated}")
        logger.info(f"Skipped (existing Message-ID): {skipped_due_to_existing_message_id}")
        logger.info(f"Total skipped (path + Message-ID): {total_overall_skipped}")
        logger.info(f"Empty/invalid (no MsgID/body): {skipped_empty}")
        logger.info(f"Timeouts (during parsing): {timeouts}")
        logger.info(f"Errors (parsing/insertion): {errors}")
        if processed > 0:
            success_rate_processed = (imported_new + updated) / processed * 100
            logger.info(f"Success rate (new+updated vs processed): {success_rate_processed:.1f}%")
        logger.info("=" * 50)
        
        if self.conn:
            logger.info("Optimizing database...")
            try:
                if self.cursor: self.cursor.execute("PRAGMA optimize")
            except Exception as e:
                logger.error(f"Error during PRAGMA optimize: {e}")
            finally:
                if self.cursor: self.cursor.close()
                self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='Import emails from MailMate to SQLite')
    parser.add_argument('--mailmate-dir', required=True, help='Path to MailMate messages directory')
    parser.add_argument('--db-path', default='mail.db', help='Path to SQLite database file')
    parser.add_argument('--batch-size', default=1000, type=int, help='Batch size for processing')
    parser.add_argument('--dry-run', action='store_true', help='Run without writing to the database (for testing)')
    parser.add_argument('--limit', type=int, help='Limit the number of emails to process')
    parser.add_argument('--sample', action='store_true', help='Process a random sample instead of all emails')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose debug logging')
    parser.add_argument('--skip-existing', action='store_true', help='Skip emails that are already in the database')
    parser.add_argument('--use-paths-dedupe', action='store_true', help='Use file paths for faster deduplication')
    parser.add_argument('--timeout', type=int, default=60, help='Timeout in seconds for processing a single email')
    
    args = parser.parse_args()
    
    # Set up more detailed logging if requested
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        # Also set the level for all handlers if changing root logger level
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)
        logger.info(f"Verbose logging enabled. Logging to console and {LOG_FILE_PATH}")
    else:
        logger.info(f"Logging to console and {LOG_FILE_PATH}")
    
    # Setup graceful shutdown handler
    shutdown_requested = False
    
    def signal_handler(sig, frame):
        nonlocal shutdown_requested
        if not shutdown_requested:
            logger.info("Shutdown requested, completing current batch and exiting...")
            shutdown_requested = True
        else:
            logger.info("Forced exit requested, stopping immediately...")
            sys.exit(1)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    importer = MailMateImporter(args.db_path, args.mailmate_dir, args.batch_size)
    importer.skip_existing = args.skip_existing
    importer.use_paths_dedupe = args.use_paths_dedupe
    importer.timeout = args.timeout
    importer.shutdown_requested = lambda: shutdown_requested
    
    # Check if the database exists and try to handle schema changes
    db_exists = os.path.exists(args.db_path)
    if db_exists:
        logger.info(f"Using existing database: {args.db_path}")
    else:
        logger.info(f"Creating new database: {args.db_path}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE: No changes will be written to the database")
        # Override methods that write to the database
        importer.connect_to_db = lambda: None
        importer.create_tables = lambda: None
        importer.insert_email = lambda email_data, commit=True: logger.info(f"Would insert email: {email_data.get('subject', 'No subject')}")
        importer.insert_folder = lambda folder_path, commit=True: logger.info(f"Would insert folder: {folder_path}")
    
    # If limit is specified, limit the number of files to process
    if args.limit and args.limit > 0:
        email_files = importer.find_email_files()
        if args.sample:
            # Take a random sample
            email_files = random.sample(email_files, min(args.limit, len(email_files)))
        else:
            # Just take the first N
            email_files = email_files[:args.limit]
        
        # Override find_email_files to return our limited set
        importer.find_email_files = lambda: email_files
    
    try:
        importer.import_emails()
    except KeyboardInterrupt:
        logger.info("Import interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
