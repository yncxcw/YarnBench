#!/usr/bin/env bash
this="`dirname .`"
this="`cd "$this"/../;pwd`"

cd ../

export YARN_BENCH="$this"

nohup python $YARN_BENCH/Scheduler.py > $YARN_BENCH/sbin/myout.file 2>&1&

echo "starting scheduler as process $!" 
