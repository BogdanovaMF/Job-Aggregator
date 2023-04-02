import argparse
from datetime import date
from clients.hh import HHClient
from clients.config import OUTPUT_FILEPATH_TEMPLATE


def parse_args():
    """Getting  data from the user from the command line for searching"""

    parser = argparse.ArgumentParser()
    parser.add_argument('--specialization', required=True, choices=['data-engineer'])
    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':

    args = parse_args()

    hh = HHClient()
    data = hh.parse_and_save(args['specialization'])
    hh._save_data(data=data, output_filepath=OUTPUT_FILEPATH_TEMPLATE.format(source_type='hh',
                                                                             specialization=args['specialization'],
                                                                             source_upload_dt=date.today()))
