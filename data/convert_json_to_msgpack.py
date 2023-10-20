#! /usr/bin/env python3

"""
requirement: msgpack package https://pypi.org/project/msgpack/
"""

import os
import sys
import json

import msgpack


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"USAGE: {sys.argv[0]} json_file")
        sys.exit(1)

    json_file = sys.argv[1]
    msgpack_file = os.path.splitext(json_file)[0] + ".msgpack"
    if os.path.exists(msgpack_file):
        print(f"output file {msgpack_file} already exists")
        sys.exit(1)

    objs = [json.loads(l) for l in open(json_file)]
    with open(msgpack_file, "wb") as fo:
        for o in objs:
            msgpack.pack(o, fo)

    print(f"created {msgpack_file}")
