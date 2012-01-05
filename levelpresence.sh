#!/bin/bash

t=0
while sleep 1 ; do
    let "t=t+1"
    printf "%5d [" "$t"

    for ((i=0; i<35; i++)) ; do
	if ! [ -f "A-$i.data" ] ; then
	    echo -n " "
	elif ! [ -f "B-$i.data" ] ; then
	    echo -n "-"
	elif ! [ -f "X-$i.data" ] ; then
	    echo -n "="
	else
	    echo -n "*"
	fi
    done
    echo
done

