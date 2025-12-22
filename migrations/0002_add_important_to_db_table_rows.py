"""
Add 'important' boolean field to db_table_rows table.
"""

from yoyo import step

__depends__ = ['0001_baseline_schema']

steps = [
    step(
        "ALTER TABLE `db_table_rows` ADD COLUMN `important` TINYINT(1) NOT NULL DEFAULT 0",
        "ALTER TABLE `db_table_rows` DROP COLUMN `important`"
    ),
]
