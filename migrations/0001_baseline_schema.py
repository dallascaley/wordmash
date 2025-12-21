"""
Baseline migration - documents the initial database schema.
This migration should be marked as applied for existing databases.
"""

from yoyo import step

__depends__ = []

steps = [
    step(
        """
        CREATE TABLE `projects` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `name` varchar(255) NOT NULL,
            `clean_root` varchar(500) NOT NULL,
            `dirty_root` varchar(500) NOT NULL,
            `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
            `description` text DEFAULT NULL,
            `url` varchar(500) DEFAULT NULL,
            `clean_db` varchar(255) DEFAULT NULL,
            `dirty_db` varchar(255) DEFAULT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
        """,
        "DROP TABLE IF EXISTS `projects`"
    ),
    step(
        """
        CREATE TABLE `files` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `file_name` varchar(255) NOT NULL,
            `path` varchar(1000) NOT NULL,
            `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
            `updated_at` timestamp NOT NULL DEFAULT current_timestamp(),
            `processed` tinyint(1) DEFAULT 0,
            `project_id` int(11) NOT NULL,
            `is_binary` tinyint(1) DEFAULT 0,
            `is_dirty` tinyint(1) NOT NULL DEFAULT 1,
            `status` enum('valid','bad','mixed','research') DEFAULT NULL,
            PRIMARY KEY (`id`),
            KEY `project_id` (`project_id`),
            CONSTRAINT `files_ibfk_1` FOREIGN KEY (`project_id`) REFERENCES `projects` (`id`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
        """,
        "DROP TABLE IF EXISTS `files`"
    ),
    step(
        """
        CREATE TABLE `file_rows` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `text` text NOT NULL,
            `file_id` int(11) NOT NULL,
            `processed` tinyint(1) DEFAULT 0,
            `is_dirty` tinyint(1) NOT NULL DEFAULT 1,
            `status` enum('valid','bad','mixed','research') DEFAULT NULL,
            PRIMARY KEY (`id`),
            KEY `file_id` (`file_id`),
            CONSTRAINT `file_rows_ibfk_1` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
        """,
        "DROP TABLE IF EXISTS `file_rows`"
    ),
    step(
        """
        CREATE TABLE `db_tables` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `table_name` varchar(255) NOT NULL,
            `processed` tinyint(1) DEFAULT 0,
            `project_id` int(11) NOT NULL,
            `is_dirty` tinyint(1) NOT NULL DEFAULT 1,
            `status` enum('valid','bad','mixed','research') DEFAULT NULL,
            PRIMARY KEY (`id`),
            KEY `project_id` (`project_id`),
            CONSTRAINT `db_tables_ibfk_1` FOREIGN KEY (`project_id`) REFERENCES `projects` (`id`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
        """,
        "DROP TABLE IF EXISTS `db_tables`"
    ),
    step(
        """
        CREATE TABLE `db_table_rows` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `field_name` varchar(255) NOT NULL,
            `contents` mediumtext NOT NULL,
            `table_id` int(11) NOT NULL,
            `processed` tinyint(1) DEFAULT 0,
            `is_dirty` tinyint(1) NOT NULL DEFAULT 1,
            `status` enum('valid','bad','mixed','research') DEFAULT NULL,
            PRIMARY KEY (`id`),
            KEY `table_id` (`table_id`),
            CONSTRAINT `db_table_rows_ibfk_1` FOREIGN KEY (`table_id`) REFERENCES `db_tables` (`id`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        "DROP TABLE IF EXISTS `db_table_rows`"
    ),
    step(
        """
        CREATE TABLE `inventory` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `project_id` int(11) NOT NULL,
            `is_dirty` tinyint(1) NOT NULL DEFAULT 1,
            `files_count` int(11) DEFAULT 0,
            `files_processed` int(11) DEFAULT 0,
            `file_rows_count` int(11) DEFAULT 0,
            `file_rows_processed` int(11) DEFAULT 0,
            `db_tables_count` int(11) DEFAULT 0,
            `db_tables_processed` int(11) DEFAULT 0,
            `db_table_rows_count` int(11) DEFAULT 0,
            `db_table_rows_processed` int(11) DEFAULT 0,
            `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
            PRIMARY KEY (`id`),
            UNIQUE KEY `unique_project_dirty` (`project_id`,`is_dirty`),
            CONSTRAINT `inventory_ibfk_1` FOREIGN KEY (`project_id`) REFERENCES `projects` (`id`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
        """,
        "DROP TABLE IF EXISTS `inventory`"
    ),
    step(
        """
        CREATE TABLE `jobs` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `job_type` varchar(100) NOT NULL COMMENT 'Type of job (e.g., file_scan, db_update, training)',
            `status` enum('pending','running','completed','failed','cancelled') NOT NULL DEFAULT 'pending',
            `progress` int(11) NOT NULL DEFAULT 0 COMMENT 'Number of items processed',
            `total` int(11) DEFAULT NULL COMMENT 'Total items to process (NULL if unknown)',
            `message` text DEFAULT NULL COMMENT 'Status message or description',
            `error_details` text DEFAULT NULL COMMENT 'Error information if failed',
            `project_id` int(11) DEFAULT NULL COMMENT 'Associated project ID if applicable',
            `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
            `started_at` timestamp NULL DEFAULT NULL,
            `ended_at` timestamp NULL DEFAULT NULL,
            PRIMARY KEY (`id`),
            KEY `idx_status` (`status`),
            KEY `idx_job_type` (`job_type`),
            KEY `idx_project_id` (`project_id`),
            KEY `idx_created_at` (`created_at`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        "DROP TABLE IF EXISTS `jobs`"
    ),
]
