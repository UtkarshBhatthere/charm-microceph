#!/bin/bash
charm=$( awk '/^name:/{ print $2 }' < metadata.yaml )
echo "renaming ${charm}_*.charm to ${charm}.charm"
echo -n "pwd: "
pwd
ls -al
echo "Renaming charm here."
mv ${charm}_*.charm ${charm}.charm
