import re
import os
import csv
import time
import locale
import requests
from pathlib import Path
from parsel import Selector
from typing import List, Tuple
from datetime import datetime

from .utils import logger


headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}


class HHClient:
    """Class for searching for vacancies on the site hh.ru"""

    url = 'https://hh.ru/vacancies/{specialization}?customDomain=1&{num_page}'
    source_type = 'hh'

    def parse_and_save(self, specialization):
        """Parsing start method"""

        job_hh = requests.get(self.url.format(specialization=specialization, num_page='page=0'), headers=headers)

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
            links += self._get_links(self.url.format(specialization=specialization, num_page=f'page={i}'))
            logger.info(f'Page №{i + 1}/{total_pages}: {specialization.replace("-", " ")}')
            time.sleep(0.7)
        logger.info(f'{len(links)} links found')

        # job listing
        data_vacancies = []
        try:
            for i, link in enumerate(links):
                logger.info(f'Processed vacancy №{i + 1}')
                data_vacancies.append(self._get_vacancy_data(link))
                time.sleep(0.7)

        except ConnectionError:
            time.sleep(2)

        return data_vacancies

    @staticmethod
    def _get_links(url: str) -> List[str]:
        """Extract job links from given page
        :param url: link to first page
        :return: a list with links to vacancies from one page
        """

        response = requests.get(url, headers=headers)

        job_content = response.content.decode('utf-8')
        links_sel = Selector(job_content).xpath('/html').xpath(
            "//div[contains(@class, 'HH-MainContent HH-Supernova-MainContent')]")
        for l_sell in links_sel:
            links_page = l_sell.xpath(
                "//a[contains(@class,'serp-item__title')]/@href").getall()
        return links_page

    @classmethod
    def _get_vacancy_data(cls, link: str) -> Tuple[str]:
        """Method parses vacancy data from given url hmtl
        :param link: job posting link
        :return: job data tuple
        """

        response = requests.get(link, headers=headers)
        html_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]

        info_v = html_sel.xpath("//div[contains(@class, 'vacancy-section')]").xpath(
            "//div[contains(@data-qa, 'vacancy-description')]//text()").getall()
        text_vacancy = ''.join(info_v)  # merged into a string, removed extra characters

        vacancy_name = str(html_sel.xpath("//h1[contains(@data-qa,'vacancy-title')]/text()").get())
        company = html_sel.xpath("//a[contains(@data-qa,'vacancy-company-name')]//text()").getall()
        company = ' '.join(company)
        company = re.sub(r'[^\d,^\w.]', ' ', company)
        company = company[0:(len(company) // 2 + 1)]

        # find the date and convert to the desired format
        try:
            date = html_sel.xpath("//p[contains(@class, 'vacancy-creation-time-redesigned')]"
                                  "//text()").getall()[1]
            date = re.sub(r'[^\d,^\w.]', ' ', date)
            locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
            pub_date = datetime.strptime(date, "%d %B %Y").date()

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

        skill = html_sel.xpath("//div[contains(@class, 'bloko-tag-list')]//text()").getall()
        skill = ','.join(skill)
        skill = re.sub(r'[^\w.]', ' ', skill)

        salary = html_sel.xpath(
            "//div[contains(@data-qa, 'vacancy-salary')]//text()").getall()
        salary = ' '.join(salary).split()
        salary = ' '.join(salary)

        experience = html_sel.xpath("//span[contains(@data-qa, 'vacancy-experience')]//text()").get()
        logger.info(f'Receive information about the vacancy "{vacancy_name}", company "{company.strip()}"')
        return pub_date, vacancy_name, experience, company, link, salary, skill, text_vacancy

    def _save_data(self, data: List[Tuple], output_filepath: str):
        """We form the received information into the scv file."
        :param data: list with information on vacancies
        :param output_filepath: path to save the file
        """

        if not os.path.exists(output_filepath):
            os.makedirs(output_filepath)

        file = str(Path(output_filepath, 'vacancies.csv'))

        with open(file, 'wt') as f:
            csv_out = csv.writer(f)
            csv_out.writerows(data)



