-- Engines other than InnoDB become InnoDB, with the warnings MySQL 8.4 gives
-- when the engine is in disabled_storage_engines and the session allows
-- substitution. The reference runs with MyISAM, MEMORY, CSV and ARCHIVE
-- disabled; MySQL substitutes only without NO_ENGINE_SUBSTITUTION, the
-- gateway always does. Not here, where MySQL differs on purpose:
-- CREATE TABLE ... ENGINE=MyISAM AS SELECT warns and still makes a MyISAM
-- table, and a disabled default_storage_engine fails with 3161 even
-- without NO_ENGINE_SUBSTITUTION; the gateway makes InnoDB tables.
SET sql_mode = '';
CREATE TABLE c_me_my (a int, KEY k (a DESC)) ENGINE=MyISAM;
SHOW WARNINGS;
SHOW CREATE TABLE c_me_my;
CREATE TABLE c_me_mem (a int) ENGINE = MEMORY COMMENT 'm';
SHOW WARNINGS;
CREATE TABLE c_me_heap (a int) ENGINE=heap;
SHOW WARNINGS;
CREATE TABLE c_me_csv (a int NOT NULL) ENGINE CSV;
SHOW WARNINGS;
CREATE TABLE c_me_arch (a int) ENGINE='ARCHIVE';
SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'c\_me\_%' ORDER BY TABLE_NAME;
-- ALTER TABLE ... ENGINE= keeps the engine, with warning 1286.
INSERT INTO c_me_my VALUES (1), (2);
ALTER TABLE c_me_my ENGINE=MyISAM;
SHOW WARNINGS;
ALTER TABLE c_me_my ADD COLUMN b int, ENGINE=MEMORY;
SHOW WARNINGS;
SHOW CREATE TABLE c_me_my;
SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'c\_me\_%' ORDER BY TABLE_NAME;
DROP TABLE c_me_my, c_me_mem, c_me_heap, c_me_csv, c_me_arch;
SET sql_mode = DEFAULT;
