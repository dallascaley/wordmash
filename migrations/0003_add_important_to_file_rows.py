"""
Add 'important' boolean field to file_rows table.
"""

from yoyo import step

__depends__ = ['0002_add_important_to_db_table_rows']

steps = [
    step(
        "ALTER TABLE `file_rows` ADD COLUMN `important` TINYINT(1) NOT NULL DEFAULT 0",
        "ALTER TABLE `file_rows` DROP COLUMN `important`"
    ),
]
