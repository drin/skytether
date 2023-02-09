"""
A thin wrapper around kctl for functionality not yet built-in.
"""


# ------------------------------
# pylint directives

# pylint: disable=invalid-name


# ------------------------------
# Dependencies

import os
import subprocess

from skytether.util import GetSliceIndex, NextKey


# ------------------------------
# Module variables

# >> reference for how to do a range
# /opt/libkinetic/bin/kctl -h 10.23.89.5 range -n 5 -S "E-CURD-10"

kctl_bin     = os.path.join('/opt', 'libkinetic', 'bin', 'kctl')
default_host = '10.23.89.5'


# ------------------------------
# Functions

def KineticRange(key_start, key_limit=0, key_end=''):
    """ This wrapper around the kctl range command. """

    limit_clause = (
        ['-n', str(key_limit)]
        if   key_limit > 0
        else []
    )

    end_clause = (
        ['-E', key_end]
        if   key_end
        else []
    )

    range_proc = subprocess.run(
           [
                kctl_bin
               ,'-h', default_host
               ,'range'
               ,'-S', key_start
           ]
         + limit_clause
         + end_clause
        ,capture_output=True
        ,encoding='utf-8'
        ,check=False
        ,stdin=subprocess.DEVNULL
    )

    return range_proc.stdout.strip().split('\n')


# ------------------------------
# Main Logic

if __name__ == '__main__':
    # Use E-CURD-10 as the test sample
    test_key = 'E-CURD-10'

    # >> test with a limit
    # key_list = KineticRange(test_key, key_limit=5)

    # >> test without a limit
    key_list = KineticRange(test_key, key_limit=50, key_end=NextKey(test_key))
    key_list = sorted(key_list, key=GetSliceIndex)

    print(f'key list ({len(key_list)}):\n\t{key_list}')
