#!/bin/bash

function periodic() {
    t=0
    while sleep 1 ; do
	let "t=t+1"
	printf "%5d [" "$t"

	for ((i=0; i<35; i++)) ; do
	    if ! [ -f "A-$i.data" ] ; then
		echo -n " "
	    elif ! [ -f "B-$i.data" ] ; then
		echo -n "-"
	    elif ! [ -f "C-$i.data" ] ; then
		echo -n "#"
	    elif ! [ -f "X-$i.data" ] ; then
		echo -n "="
	    else
		echo -n "*"
	    fi
	done
	echo
    done
}

function dynamic() {
    local old s t start now
    t=0
    start=`date +%s`
    while true ; do
	s=""
	for ((i=0; i<35; i++)) ; do
	    if ! [ -f "A-$i.data" ] ; then
		s="$s "
	    elif ! [ -f "B-$i.data" ] ; then
		s="$s-"
	    elif ! [ -f "C-$i.data" ] ; then
		s="$s="
	    elif ! [ -f "X-$i.data" ] ; then
		s="$s%"
	    else
		s="$s*"
	    fi
	done

	if [[ "$s" != "$old" ]] ; then
	    let "t=t+1"
	    now=`date +%s`
	    let "now=now-start"
	    printf "%5d %6d [%s\n" "$t" "$now" "$s"
	    old="$s"
	else
	    # Sleep a little bit:
	    perl -e 'use Time::HiRes; Time::HiRes::usleep(100000)'
	fi
    done
}

dynamic
