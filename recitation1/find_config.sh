#!/bin/bash

filename="$HOME/.ssh/config"

if [ -f "$filename" ]
then 
    echo "$filename found. Proceed."
else
    echo "$filename not found. Creating empty config file."
    touch "$filename"
fi
