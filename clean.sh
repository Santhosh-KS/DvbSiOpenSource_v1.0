#!/bin/sh
REPO=${PWD} 
echo "REPO Dir = "$REPO

function clean()
{
    cd $REPO
    cd ./sectionparser
    make clean

    cd $REPO
    cd ./storage
    make clean

    cd $REPO
    cd ./sqlite3pp
    make clean
}

clean
