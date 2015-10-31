#!/bin/bash
# Run this to remote connect to hdfs cluster
##########################################################################################

set -eu

chmod 400 id_rsa
ssh -i id_rsa root@158.85.79.185
