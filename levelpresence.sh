#!/bin/bash

## ----------------------------------------------------------------------------
##
## lsm_btree: LSM-trees (Log-Structured Merge Trees) Indexed Storage
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
