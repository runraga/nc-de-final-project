#!/bin/bash

file=$1
shift 1

while (( "$#" )); do
    kv=$1
    key=${kv%::*}
    val=${kv#*::}
    printf "%s = \"%s\"\n" "$key" "$val" >> $file
    shift 1
done