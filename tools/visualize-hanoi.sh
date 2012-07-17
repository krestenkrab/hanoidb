#!/bin/bash

## ----------------------------------------------------------------------------
##
## hanoi: LSM-trees (Log-Structured Merge Trees) Indexed Storage
##
## Copyright 2011-2012 (c) Trifork A/S.  All Rights Reserved.
## http://trifork.com/ info@trifork.com
##
## Copyright 2012 (c) Basho Technologies, Inc.  All Rights Reserved.
## http://basho.com/ info@basho.com
##
## This file is provided to you under the Apache License, Version 2.0 (the
## "License"); you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
## License for the specific language governing permissions and limitations
## under the License.
##
## ----------------------------------------------------------------------------

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

merge_diff() {
    SA=`ls -l A-${ID}.data 2> /dev/null | awk '{print $5}'`
    SB=`ls -l B-${ID}.data 2> /dev/null | awk '{print $5}'`
    SX=`ls -l X-${ID}.data 2> /dev/null | awk '{print $5}'`
    if [ \( -n "$SA" \) -a \( -n "$SB" \)  -a \( -n "$SX" \) ]; then
      export RES=`expr ${SX}0 / \( $SA + $SB \)`
    else
      export RES="?"
    fi
}

function dynamic() {
    local old s t start now
    t=0
    start=`date +%s`
    while true ; do
        s=""
        for ((i=8; i<22; i++)) ; do
            if [ -f "C-$i.data" ] ; then
                s="${s}C"
            else
                s="$s "
            fi
            if [ -f "B-$i.data" ] ; then
                s="${s}B"
            else
                s="$s "
            fi
            if [ -f "A-$i.data" ] ; then
                s="${s}A"
            else
                s="$s "
            fi
            if [ -f "X-$i.data" ] ; then
                export ID="$i"
                merge_diff
                s="${s}$RES"
            elif [ -f "M-$i.data" ] ; then
                s="${s}M"
            else
                s="$s "
            fi
            s="$s|"
        done

        if [[ "$s" != "$old" ]] ; then
            let "t=t+1"
            now=`date +%s`
            let "now=now-start"
            free=`df -m . 2> /dev/null | tail -1 | awk '{print $4}'`
            used=`du -m 2> /dev/null | awk '{print $1}' `
            printf "%5d %6d [%s\n" "$t" "$now" "$s ${used}MB (${free}MB free)"
            old="$s"
        else
            # Sleep a little bit:
            sleep 1
        fi
    done
}

dynamic
