DO $$
DECLARE
    idx RECORD;
BEGIN
    FOR idx IN
        SELECT n.nspname AS schema_name,
               c.relname AS index_name,
               t.oid AS table_oid,
               t.relname AS table_name
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class t ON t.oid = i.indrelid
        WHERE NOT i.indisvalid
    LOOP
        BEGIN
            -- try to take an ACCESS EXCLUSIVE lock on the table, without waiting
            EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE NOWAIT',
                           idx.schema_name, idx.table_name);

            -- if lock succeeds, drop index
            RAISE NOTICE 'Dropping invalid index: %.%', idx.schema_name, idx.index_name;
            EXECUTE format('DROP INDEX IF EXISTS %I.%I;', idx.schema_name, idx.index_name);

        EXCEPTION WHEN lock_not_available THEN
            RAISE NOTICE 'Skipped index %.% (table %.% is busy)', 
                         idx.schema_name, idx.index_name, idx.schema_name, idx.table_name;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
