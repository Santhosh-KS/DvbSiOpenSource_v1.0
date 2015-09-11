#!/bin/sh
REPO=${PWD} 
echo "REPO Dir = "$REPO

function build()
{
    cd $REPO
    cd ./sectionparser
    make

    cd $REPO
    cd ./storage
    make

    cd $REPO
    cd ./sqlite3pp
    make
}

build
