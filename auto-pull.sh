#!/bin/sh
# Poll a remote until changes are detected. Pull the changes and exit.

while true
do

git fetch;
LOCAL=$(git rev-parse HEAD);
REMOTE=$(git rev-parse @{u});

# if the local revision id doesn't match the remote, pull the changes
if [ $LOCAL != $REMOTE ]; then
    #pull and merge changes
    git pull origin master;
else
    echo Checking at $(date);
    sleep 5;
fi
done