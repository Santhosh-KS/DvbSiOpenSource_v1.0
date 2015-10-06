#!/bin/sh

# DVB_SI for Reference Design Kit (RDK)
#
# Copyright (C) 2015 Santhosh K S (Santhosh.KS@arris.com), Manu Shrot (Manu.Shrot@arris.com), Dmitry Barablin & Stan Partridge (stan.partridge@arris.com)
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

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
