from __future__ import (print_function, division)

import argparse

import sys


def main(opts):
    if opts.text:
        print(opts.text)
    if opts.exception:
        print(opts.exception)
        raise Exception(opts.exception)
    if opts.exit:
        sys.exit(opts.exit)


if __name__=="__main__":
    if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        parser.add_argument("-text", dest="text", action='store')
        parser.add_argument("-exception", dest="exception", action='store')
        parser.add_argument("-exit", dest="exit", action='store')
        opts = parser.parse_args()
        main(opts)
