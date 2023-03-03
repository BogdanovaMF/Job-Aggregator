import re
import os
import csv
import time
import locale
import argparse
import requests
from pathlib import Path
from parsel import Selector
from pandas import DataFrame
from datetime import datetime
from typing import List, Tuple

def parser_name_jobs():
    """Getting  data from the user from the command line for searching"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', required=True, choices=['data engineer'])
    args = parser.parse_args()
    args.name = args.name.lower().replace(' ', '-')
    return args.name

headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
num_page = f'page=0'
url = f'https://hh.ru/vacancies/{parser_name_jobs()}?customDomain=1&{num_page}'

class HHClient:
    @staticmethod
    def get_selector_html():
        """Formation of the Selector object of the first html page.
        The selector object is html page in text format.
        The selector object is needed to find the number of pages and vacancies"""
        job_hh = requests.get(url, headers=headers)
        init_content = job_hh.content.decode('utf-8')
        html_sel = Selector(init_content).xpath('//html')[0]
        return html_sel

    def quantity_vac(self):
        """Looking for the number of vacancies to process"""
        search_quantity = self.get_selector_html().xpath("//div[contains(@class, 'main-content')]")
        quantity = search_quantity.xpath("//span[contains(@data-qa, 'vacancies-total-found')]//text()").get()
        print(f'Всего {quantity[2:]}')

    def count_pages(self):
        """Find the number of pages to process"""
        page_nav_buttons = self.get_selector_html().xpath(
            '//div[contains(@class, "pager")]/span[contains(@class, "pager-item-not-in-short-range")]'
            '//span//text()').getall()
        total_pages = int(page_nav_buttons[-1])
        return total_pages

    def links(self, url: str) -> List[str]:
        """Search for all job links:
        param url: str - url to get a request to get job links
        return: List[str, str, ...]"""
        links = []
        for i in range(self.count_pages()):
            job_all = requests.get(f'{url}&page={i}', headers=headers).content.decode('utf-8')
            html_sel = Selector(job_all).xpath('/html')
            links_sel = html_sel.xpath("//div[contains(@class, 'vacancy-serp-content')]")
            for l_sell in links_sel:
                link = l_sell.xpath(
                    "//a[contains(@class,'serp-item__title')]/@href").getall()
                links += link
        return links

    def get_information(self, links: List[str]) -> List[Tuple[str]]:
        """Collecting information from the site and forming it into tuples, then into a single list
        param: links - list of all job links
        return: list[[tuple[str, str]]] with information on vacancies: publication date, vacancy name,
        company name, required experience, vacancy text, link to the vacancy, required skills """
        information_job = []
        try:
            for l in self.links(url):
                response = requests.get(l, headers=headers)
                html_info_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]

                info_v = html_info_sel.xpath("//div[contains(@class, 'vacancy-section')]").xpath(
                "//div[contains(@data-qa, 'vacancy-description')]//text()").getall()
                text_vacancy = ''.join(info_v) #merged into a string, removed extra characters

                vacancy_name = str(html_info_sel.xpath("//h1[contains(@data-qa,'vacancy-title')]/text()").get())

                company = html_info_sel.xpath("//a[contains(@data-qa,'vacancy-company-name')]//text()").getall()
                company = ' '.join(company)
                company = re.sub(r'[^\d,^\w.]', ' ', company)
                company = company[0:(len(company)//2+1)]

                #find the date and convert to the desired format
                try:
                    date = html_info_sel.xpath("//p[contains(@class, 'vacancy-creation-time-redesigned')]"
                                               "//text()").getall()[1]
                    date = re.sub(r'[^\d,^\w.]', ' ', date)
                    locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
                    pub_date = datetime.strptime(date, "%d %B %Y")

                except:
                    try:
                        date = html_info_sel.xpath("//p[contains(@class, 'vacancy-creation-time')]//text()").getall()[1]
                        date = re.sub(r'[^\d,^\w.]', ' ', date)
                        locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
                        pub_date = datetime.strptime(date, "%d %B %Y")
                    except:
                        pub_date = '-'

                skill = html_info_sel.xpath("//div[contains(@class, 'bloko-tag-list')]//text()").getall()
                skill = ','.join(skill)
                skill = re.sub(r'[^\w.]', ' ', skill)

                salary = html_info_sel.xpath(
                    "//div[contains(@data-qa, 'vacancy-salary')]//text()").getall()
                salary = ' '.join(salary).split()
                salary = ' '.join(salary)

                experience = html_info_sel.xpath("//span[contains(@data-qa, 'vacancy-experience')]//text()").get()

                information_job.append((pub_date, vacancy_name, experience, company, l, salary, skill, text_vacancy))
                print(f'Получаем инфомарцию по вакансии {vacancy_name}, компания "{company.strip()}"')

        except ConnectionError:
            time.sleep(5)
        return information_job

    def file_csv(self, data_vacancies: List[Tuple]):
        """We form the received information into the scv file. The file is saved in the folder "result"
        param: data_vacancies: list with information on vacancies"""
        dir_name = 'result'
        if not os.path.exists("result"):
            os.mkdir(dir_name)
        df = DataFrame(data=data_vacancies,
                       columns=['pub_date', 'vacancy', 'experience', 'company','link', 'salary', 'skills', 'description'])
        file = df.to_csv(f'{dir_name}/{parser_name_jobs()} {datetime.now().date()}.csv', index=False)

if __name__ == '__main__':
    hh_client = HHClient()
    hh_client.quantity_vac()
    links_list = hh_client.links(url)
    data_vacancies = hh_client.get_information(links_list)
    final_file = hh_client.file_csv(data_vacancies)