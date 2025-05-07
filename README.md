# MailMate to SQLite Importer
This project is a fork of [keithrbennett/mailmate-to-pg](https://github.com/keithrbennett/mailmate-to-pg). Many thanks to Keith R. Bennett for the original work.

&lt;human&gt; strictly vibecoded. Works on my computers. No guarantees of any sort, implied or etc. Based on mailmate to postgres, but basically rewritten by claude then 2.5 pro. It has zero dependencies but having fd installed will make it run way faster. I run this with cron at midnight and it works nicely as a backup. &lt;/human&gt;

This script processes email data from MailMate's file storage and imports it into a SQLite database. It is designed to handle individual email files, extract metadata, content, and attachments, and organize them into a structured relational database.

## Overview

The script scans a specified MailMate messages directory, parses each email file (typically in EML format or similar), and extracts key information such as headers (Subject, From, To, Date, Message-ID), body content (plain text and HTML), and attachments. This data is then inserted into a SQLite database, creating tables for emails, attachments, folders, and their relationships. The script includes features for performance, error handling, and flexibility in processing.

## Features

*   **Comprehensive Email Parsing**: Extracts standard email headers, plain text, and HTML body parts.
*   **Attachment Handling**: Identifies attachments, storing metadata like filename, content type, and size. (Note: Attachment content is not stored in the DB, but its path/metadata is).
*   **Folder Structure**: Replicates MailMate's folder structure within the SQLite database.
*   **SQLite Integration**:
    *   Creates necessary tables (`emails`, `attachments`, `folders`, `email_folders`) if they don't exist.
    *   Applies SQLite PRAGMA settings for performance optimization (WAL mode, cache size, etc.).
    *   Includes basic schema migration logic for the `folders` table.
*   **Efficient Processing**:
    *   Utilizes `fd` (if available) for fast discovery of email files, with a fallback to `os.walk`.
    *   Processes emails in configurable batches.
    *   Includes a timeout mechanism for parsing individual email files to prevent hangs.
*   **Deduplication**:
    *   Option to skip emails already present in the database based on `Message-ID`.
    *   Option to use file paths for faster deduplication, including handling of `.noindex` path variations.
*   **Robustness & Logging**:
    *   Detailed logging to both console and a log file (`/tmp/mailmate.log`).
    *   Configurable log verbosity via `--verbose` flag.
    *   Graceful shutdown handling (SIGINT, SIGTERM) to complete current batch.
*   **Flexible Usage**:
    *   Command-line interface with various options for customization (database path, MailMate directory, batch size, processing limits, dry run, etc.).
    *   Supports processing a limited number of emails or a random sample.

## Prerequisites

*   **Python**: Version >=3.11, <3.12 (as specified in the script's shebang).
*   **MailMate**: An installed and configured MailMate application. The script needs access to MailMate's email data directory (usually `~/Library/Application Support/MailMate/Messages/` or similar).
*   **`uv`**: The script uses `uv` for managing Python environments and dependencies. Ensure `uv` is installed. See [astral-sh/uv](https://github.com/astral-sh/uv) for installation instructions.
*   **`fd` (Optional, Recommended)**: For significantly faster email file discovery, install the `fd` command-line tool. If not found, the script will fall back to a slower Python-based method.

## Setup

1.  **Ensure `uv` is installed.** If not, follow the instructions on the [official `uv` GitHub page](https://github.com/astral-sh/uv).

2.  **Make the script executable:**
    ```bash
    chmod +x mailmate_to_sqlite_original.py
    ```

3.  **Dependencies:**
    The script is self-contained and relies only on the Python standard library (version >=3.11, <3.12). It does not require any external packages to be installed via `uv` or `pip`, as `uv` will use the Python interpreter it runs under.

## Usage

Run the script from your terminal:

```bash
./mailmate_to_sqlite_original.py --mailmate-dir /path/to/your/MailMate/Messages/ [OPTIONS]
```

**Required Argument:**

*   `--mailmate-dir TEXT`: Path to the MailMate messages directory (e.g., `~/Library/Application Support/MailMate/Messages/`).

**Common Options:**

*   `--db-path TEXT`: Path to the SQLite database file.
    *   Default: `mail.db`
*   `--batch-size INTEGER`: Number of emails to process in a single database transaction.
    *   Default: `1000`
*   `--skip-existing`: Skip importing emails whose `Message-ID` is already found in the database.
*   `--use-paths-dedupe`: Use file paths for faster deduplication in conjunction with `--skip-existing`. This checks normalized paths and common variations (e.g., `Messages/` vs `Messages.noindex/`).
*   `--verbose`: Enable verbose debug logging to console and file.
*   `--timeout INTEGER`: Timeout in seconds for parsing a single email file.
    *   Default: `60`
*   `--limit INTEGER`: Limit the total number of emails to process.
*   `--sample`: If `--limit` is used, process a random sample of emails up to the limit, rather than the first N.
*   `--dry-run`: Perform a trial run. Scans files and simulates parsing/insertion, but makes no changes to the database.

**Example:**

Import all emails from the default MailMate path to `my_mail.db`, skipping already imported ones using Message-ID and path deduplication, with verbose logging:

```bash
./mailmate_to_sqlite_original.py \
    --mailmate-dir ~/Library/Application\ Support/MailMate/Messages/ \
    --db-path my_mail.db \
    --skip-existing \
    --use-paths-dedupe \
    --verbose
```

Process only the first 500 emails:

```bash
./mailmate_to_sqlite_original.py \
    --mailmate-dir ~/Library/Application\ Support/MailMate/Messages/ \
    --limit 500
```

## Database Schema

The script creates and populates the following tables in the SQLite database:

*   **`emails`**: Stores individual email messages.
    *   `id` (INTEGER PRIMARY KEY AUTOINCREMENT)
    *   `message_id` (TEXT UNIQUE): The email's Message-ID header.
    *   `subject` (TEXT)
    *   `from_address` (TEXT)
    *   `to_address` (TEXT)
    *   `cc` (TEXT)
    *   `bcc` (TEXT)
    *   `date` (TIMESTAMP): Parsed from the Date header.
    *   `received_date` (TIMESTAMP): Parsed from the Received header (best effort).
    *   `content_type` (TEXT)
    *   `size` (INTEGER): Size of the email file in bytes.
    *   `has_attachments` (BOOLEAN): Flag indicating if attachments are present.
    *   `body_text` (TEXT): Plain text content of the email.
    *   `body_html` (TEXT): HTML content of the email.
    *   `headers` (TEXT): JSON string containing all email headers.
    *   `path` (TEXT): Absolute path to the original email file.
    *   `imported_at` (TIMESTAMP): Timestamp of when the email was imported/updated.

*   **`attachments`**: Stores information about email attachments.
    *   `id` (INTEGER PRIMARY KEY AUTOINCREMENT)
    *   `email_id` (INTEGER): Foreign key referencing `emails.id`.
    *   `filename` (TEXT)
    *   `content_type` (TEXT)
    *   `size` (INTEGER): Size of the attachment payload in bytes.
    *   `content_id` (TEXT)
    *   `path` (TEXT): Path to the original email file containing this attachment.

*   **`folders`**: Stores MailMate folder information.
    *   `id` (INTEGER PRIMARY KEY AUTOINCREMENT)
    *   `name` (TEXT): Name of the folder.
    *   `path` (TEXT UNIQUE): Relative path of the folder within the MailMate messages directory.

*   **`email_folders`**: Links emails to their respective folders (many-to-many relationship).
    *   `email_id` (INTEGER): Foreign key referencing `emails.id`.
    *   `folder_id` (INTEGER): Foreign key referencing `folders.id`.
    *   PRIMARY KEY (`email_id`, `folder_id`)

## How it Works

1.  **Initialization**: Parses command-line arguments, sets up logging.
2.  **Database Connection**: Connects to the specified SQLite database file (creates it if it doesn't exist). Applies performance PRAGMAs.
3.  **Schema Setup**: Creates the required tables if they don't already exist. It includes logic to gracefully handle a schema change for the `folders` table (removing a legacy UNIQUE constraint on the `name` column).
4.  **File Discovery**:
    *   Attempts to use the `fd` command-line tool for fast, recursive searching of email files within the `--mailmate-dir`. It excludes `.plist` files and hidden files/directories.
    *   If `fd` is not available or fails, it falls back to Python's `os.walk`.
5.  **Deduplication (Optional)**: If `--skip-existing` is enabled:
    *   Loads existing `Message-ID`s from the `emails` table into memory.
    *   If `--use-paths-dedupe` is also enabled, loads existing file paths from the `emails` table, generating alternate paths (e.g., `Messages/` vs. `Messages.noindex/`) for more robust matching.
    *   Filters the list of discovered files against these sets to avoid re-processing.
6.  **Batch Processing**: Iterates through the email files in batches:
    *   For each file, it calls `parse_email_file`.
    *   **Parsing**: The `parse_email_file` function:
        *   Opens and parses the email file using Python's `email` module.
        *   Extracts headers, date fields (with timezone handling), content types, and body parts (text and HTML, decoding with charset information).
        *   Identifies attachments and gathers their metadata.
        *   Calculates the relative folder path for the email.
        *   Includes a timeout (default 60s) to prevent hanging on problematic files.
    *   **Database Insertion**: The `insert_email` function:
        *   Checks if an email with the same `Message-ID` already exists. If so, it updates the existing record. Otherwise, it inserts a new record.
        *   Inserts associated attachment metadata.
        *   Ensures the email's folder exists in the `folders` table (inserting it if new) and links the email to the folder in `email_folders`.
        *   Database transactions are used for each batch to ensure atomicity.
    *   **Logging & Progress**: Provides progress updates every 100 emails processed, and a summary at the end of each batch.
    *   **Completion**: After processing all files, optimizes the database (`PRAGMA optimize`) and closes the connection.
    *   **Signal Handling**: Catches `SIGINT` (Ctrl+C) and `SIGTERM` for graceful shutdown, attempting to complete the current batch before exiting.

## Troubleshooting

*   **"Missing tables" or "folders_old" errors**:
    The script has a note: "If you encounter errors about missing tables (especially "folders_old"), it's recommended to delete the database file (e.g., `rm mail.db`) and start fresh." This is particularly relevant if you've run older versions of a similar script.
*   **Permission Denied**: Ensure the script has:
    *   Read access to the MailMate messages directory (`--mailmate-dir`) and all its contents.
    *   Read/write access to the directory where the SQLite database (`--db-path`) is being created/updated.
*   **`uv` not found**: Make sure `uv` is installed and available in your system's PATH.
*   **`fd` not found (Performance)**: If `fd` is not installed, file discovery will be slower. The script will log this and fall back automatically. For large mailboxes, installing `fd` is highly recommended.
*   **Slow Performance**:
    *   Ensure `fd` is installed for faster file scanning.
    *   If importing a very large number of new emails, the initial import can take time. Subsequent runs with `--skip-existing` should be much faster.
    *   Check system resources (CPU, I/O).
*   **Encoding Errors**: The script attempts to handle various charsets but malformed emails might still cause issues. Errors during parsing are logged.
*   **Timeouts**: If many emails are timing out during parsing (`--timeout` option), it might indicate unusually complex or corrupted email files. You might need to investigate these files or adjust the timeout.

## License

[Specify your license here, e.g., MIT License, Apache 2.0, etc. If unspecified, it's typically proprietary.]
