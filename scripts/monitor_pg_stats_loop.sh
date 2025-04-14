while true; do
    partitions=`psql -Atqc "select relname from pg_stat_user_tables where relname ~ 'stat_h3_transposed_p' and (last_autovacuum > greatest(last_autoanalyze, last_analyze)) and n_live_tup < 500000"`
    if [ -n "$partitions" ]; then
        command="vacuumdb -v -j 300 --analyze-only $(for p in $partitions; do echo -n ' -t ' $p; done)"
        echo "fixing stats: running" $command
        eval $command
    fi
    sleep 20s
done
