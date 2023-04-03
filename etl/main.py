import argparse
from clients.hh import HHClient

def parse_args():
    """Getting  data from the user from the command line for searching"""

    parser = argparse.ArgumentParser()
    parser.add_argument('--specialization', required=True, choices=['data-engineer'])
    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':

    args = parse_args()
    HHClient.parse_and_save(args["specialization"])
