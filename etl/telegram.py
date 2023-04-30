import os
import csv
import argparse
from pathlib import Path
from datetime import date
from typing import List, Optional

from dotenv import load_dotenv
from telethon.sync import TelegramClient

from utils import get_logger
from config import OUTPUT_FILEPATH_TEMPLATE

load_dotenv()

api_id = os.getenv('api_id')
api_hash = os.getenv('api_hash')
phone = os.getenv('phone')
source_type = 'telegram'


def parse_args():
    """Getting  data from the user from the command line for loader PG"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', nargs='?')
    parser.add_argument('--link_channel', required=True)
    parser.add_argument('--specialization', required=True, choices=['data_engineer'])
    args = parser.parse_args()
    return vars(args)


class Telegram_Client:
    """Class for searching for vacancies on Telegram"""

    def __init__(self, t_id: int, t_hash: str, number_phone: str) -> None:
        """Class initialization
        :param t_id: telegram client's id
        :param t_hash: telegram client's hash
        :param number_phone: telegram client's number
        """

        self.t_id = t_id
        self.t_hash = t_hash
        self.number_phone = number_phone

        try:
            self.client = TelegramClient(self.number_phone, self.t_id, self.t_hash)
            logger.info('Connection established')
        except Exception as ex:
            logger.error(f'Error "{ex}"')
            raise Exception

    def message_collection(self, link_channel: str, date_search: Optional[str] = None) -> List[str]:
        """Collection of messages from the telegram channel
        :param link_channel: link of telegram channel
        :param date_search: date from which the search starts, if absent - search all messages
        :return: list of messages
        """

        self.client.start()
        if date_search:
            date_search = date.fromisoformat(date_search)
        all_messages = []
        try:
            logger.info('The search starts')
            for message in self.client.iter_messages(entity=link_channel,
                                                     reverse=True,
                                                     offset_date=date_search):
                all_messages.append(message.text)
            logger.info('The search messages completed successfully')
            return all_messages
        except Exception as ex:
            logger.error(f'Error "{ex}"')
            raise Exception

    @staticmethod
    def save_data(messages: List[str], output_filepath: str) -> None:
        """We form the received information into the scv file
        :param messages: list with information on vacancies
        :param output_filepath: path to save the file
        """

        if not os.path.exists(output_filepath):
            os.makedirs(output_filepath)
        file = str(Path(output_filepath, 'vacancies.csv'))

        logger.info("Saving data to a file")
        with open(file, "w", encoding="UTF-8") as f:
            writer = csv.writer(f, delimiter=",", lineterminator="\n")
            for message in messages:
                writer.writerow([message])
        logger.info('Group message parsing completed successfully')

    def __del__(self):
        logger.info('Closing the connection')
        self.client.disconnect()


if __name__ == '__main__':
    logger = get_logger()
    args = parse_args()
    tg_parser = Telegram_Client(t_id=int(api_id), t_hash=api_hash, number_phone=phone)
    all_messages = tg_parser.message_collection(link_channel=args['link_channel'],
                                                date_search=args['date'])
    tg_parser.save_data(messages=all_messages,
                        output_filepath=OUTPUT_FILEPATH_TEMPLATE.format(source_type=source_type,
                                                                        specialization=args['specialization'],
                                                                        source_upload_dt=date.today()
                                                                        )
                        )
    del tg_parser
