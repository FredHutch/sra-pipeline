#!/usr/bin/env python3

"show missing items"


import sys

def main():
    "do the work"
    if len(sys.argv) != 2:
        print("supply a number")
        sys.exit(1)


    raw = sys.stdin.read()
    lines = raw.strip().split("\n")
    setx = set([int(x.strip()) for x in lines])
    sety = set(list(range(int(sys.argv[1]))))

    print(sety - setx)


if __name__ == "__main__":
    main()
