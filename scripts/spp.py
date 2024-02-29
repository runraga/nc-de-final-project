#!/usr/bin/env python3
import re
from io import TextIOWrapper as TFile
from typing import Dict


def template(infile: TFile, outfile: TFile, vars: Dict[str, str]):
    """run substitutions line-by-line from infile to outfile

    Args:
        infile (TextIOWrapper): file to read template from
        outfile (TextIOWrapper): file to write substituted text into
        vars (Dict[str, str]): template bindings
    """
    for line in infile:
        for sym, val in vars.items():
            line = line.replace(sym, val)
        outfile.write(line)


if __name__ == '__main__':
    from argparse import ArgumentParser, FileType

    def kvpair(arg: str):
        return arg.split('=', 1)

    parser = ArgumentParser(
        description="Simple PreProcessor, substitution based")

    parser.add_argument('infile', type=FileType(encoding='utf-8'))
    parser.add_argument('outfile', type=FileType('wt'))
    parser.add_argument('pairs', type=kvpair, nargs='*',
                        help='macro variables')

    args = parser.parse_args()
    vars = {key.upper(): value for key, value in args.pairs}
    template(args.infile, args.outfile, vars)
