import re
import time
import locale
import argparse
import random
from typing import List, Tuple
from datetime import date, datetime

import requests
from parsel import Selector

from utils import get_logger, save_data
from config import OUTPUT_FILEPATH_TEMPLATE

headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
           'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
           'accept-Encoding': 'gzip, deflate, br',
           'Accept-Language': 'ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7',
           'cache-Control': 'max-age=0'
           }

cookies = {'': ''}

logger = get_logger()

def parse_args():
    """Getting  data from the user from the command line for searching"""

    parser = argparse.ArgumentParser()
    parser.add_argument('--specialization', required=True, choices=['data-engineer'])
    args = parser.parse_args()
    return vars(args)


class HHClient:
    """Class for searching for vacancies on the site hh.ru"""

    url = 'https://hh.ru/vacancies/{specialization}?customDomain=1&{num_page}'
    source_type = 'hh'

    @classmethod
    def parse_and_save(cls, specialization):
        """Parsing start method"""

        job_hh = requests.get(cls.url.format(specialization=specialization, num_page='page=0'), headers=headers)

        init_content = job_hh.content.decode('utf-8')
        html_sel = Selector(init_content).xpath('//html')[0]

        # find the number of available vacancies
        search_quantity = html_sel.xpath("//div[contains(@class, 'main-content')]")
        quantity = search_quantity.xpath("//span[contains(@data-qa, 'vacancies-total-found')]//text()").get()
        logger.info(f'Всего {quantity[2:]}')

        page_nav_buttons = html_sel.xpath(
            '//div[contains(@class, "pager")]/span[contains(@class, "pager-item-not-in-short-range")]'
            '//span//text()').getall()

        # find the number of pages to process
        total_pages = int(page_nav_buttons[-1])

        # list links for all vacancies from all pages
        links = []
        for i in range(total_pages):
            links += cls._get_links(cls.url.format(specialization=specialization, num_page=f'page={i}'))
            logger.info(f'Page №{i + 1}/{total_pages}: {specialization.replace("-", " ")}')
            # time.sleep(1.5)
            time.sleep(random.randrange(1, 2))
        logger.info(f'{len(links)} links found')

        # job listing
        data = []
        try:
            for i, link in enumerate(links):
                logger.info(f'Processed vacancy №{i + 1}')
                data.append(cls._get_vacancy_data(link))
                time.sleep(random.randrange(1, 2))

        except ConnectionError:
            time.sleep(30)

        save_data(
            data=data,
            output_filepath=OUTPUT_FILEPATH_TEMPLATE.format(
                source_type=cls.source_type,
                specialization=specialization.replace('-', '_'),
                source_upload_dt=date.today()
            )
        )

    @staticmethod
    def _get_links(url: str) -> List[str]:
        """Extract job links from given page
        :param url: link to first page
        :return: a list with links to vacancies from one page
        """

        response = requests.get(url, headers=headers)
        job_content = response.content.decode('utf-8')

        links_sel = Selector(job_content).xpath("//div[contains(@class, 'HH-MainContent HH-Supernova-MainContent')]")
        if links_sel:
            for l_sell in links_sel:
                links_page = l_sell.xpath("//span[contains(@class,'serp-item__title-link-wrapper')]/a[contains(@class, 'bloko-link')]/@href").getall()
                return links_page
        else:
            raise IndexError(f'Failed to parse links to vacancies by url={url}')

    @staticmethod
    def _get_vacancy_data(link: str) -> Tuple[str]:
        """Method parses vacancy data from given url hmtl
        :param link: job posting link
        :return: job data tuple
        """

        response = requests.get(link, headers=headers)
        html_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]

        text_vacancy = html_sel.xpath("//div[contains(@data-qa, 'vacancy-description')]//text()").getall()
        text_vacancy = ' '.join([i.strip() for i in text_vacancy if i.strip()])

        vacancy_name = str(html_sel.xpath("//h1[contains(@data-qa,'vacancy-title')]/text()").get())

        company = html_sel.xpath("//a[contains(@data-qa,'vacancy-company-name')]//text()").getall()
        company = ' '.join(company)
        company = re.sub(r'[^\d,^\w.]', ' ', company)[0:(len(company) // 2 + 1)]

        skill = html_sel.xpath("//div[contains(@class, 'bloko-tag-list')]//text()").getall()
        skill = ','.join([i for i in skill])

        salary = html_sel.xpath("//div[contains(@data-qa, 'vacancy-salary')]//text()").getall()
        salary = ' '.join([i for i in ' '.join(salary).strip().split()])

        experience = html_sel.xpath("//span[contains(@data-qa, 'vacancy-experience')]//text()").get()

        # find the date and convert to the desired format
        try:
            date = html_sel.xpath("//p[contains(@class, 'vacancy-creation-time-redesigned')]"
                                  "//text()").getall()[1]
            date = re.sub(r'[^\d,^\w.]', ' ', date)
            locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
            pub_date = datetime.strptime(date, "%d %B %Y")

        except:
            logger.exception(f'Date processing error, {link}')
            try:
                date = html_sel.xpath("//p[contains(@class, 'vacancy-creation-time')]//text()").getall()[1]
                date = re.sub(r'[^\d,^\w.]', ' ', date)
                locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
                pub_date = datetime.strptime(date, "%d %B %Y")
            except:
                logger.exception(f'Date processing error, {link}')
                pub_date = None

        logger.info(f'Receive information about the vacancy "{vacancy_name}", company "{company}"')
        return pub_date, vacancy_name, experience, company, link, salary, skill, text_vacancy


if __name__ == '__main__':
    args = parse_args()
    HHClient.parse_and_save(args["specialization"])
