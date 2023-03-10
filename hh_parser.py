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


def parse_args():
    """Getting  data from the user from the command line for searching"""

    parser = argparse.ArgumentParser()
    parser.add_argument('--name', required=True, choices=['data-engineer'])
    args = parser.parse_args()
    return vars(args)

def get_links(number_page: int) -> List[str]:
    """Extract job links from given page
    :param number_page: number of page for request
    :return: a list with links to vacancies from one page
    """

    response = requests.get(f'{url}&page={number_page}', headers=headers)
    job_content = response.content.decode('utf-8')
    links_sel = Selector(job_content).xpath('/html').xpath("//div[contains(@class, 'vacancy-serp-content')]")
    for l_sell in links_sel:
        links_page = l_sell.xpath(
            "//a[contains(@class,'serp-item__title')]/@href").getall()
    return links_page


def get_vacancy_data(link: str) -> Tuple[str]:
    """Function parses vacancy data from given url hmtl
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
        pub_date = datetime.strptime(date, "%d %B %Y")

    except:
        try:
            date = html_sel.xpath("//p[contains(@class, 'vacancy-creation-time')]//text()").getall()[1]
            date = re.sub(r'[^\d,^\w.]', ' ', date)
            locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
            pub_date = datetime.strptime(date, "%d %B %Y")
        except:
            pub_date = None

    skill = html_sel.xpath("//div[contains(@class, 'bloko-tag-list')]//text()").getall()
    skill = ','.join(skill)
    skill = re.sub(r'[^\w.]', ' ', skill)

    salary = html_sel.xpath(
        "//div[contains(@data-qa, 'vacancy-salary')]//text()").getall()
    salary = ' '.join(salary).split()
    salary = ' '.join(salary)

    experience = html_sel.xpath("//span[contains(@data-qa, 'vacancy-experience')]//text()").get()

    print(f'???????????????? ???????????????????? ???? ???????????????? {vacancy_name}, ???????????????? "{company.strip()}"')
    return pub_date, vacancy_name, experience, company, link, salary, skill, text_vacancy


def save_data(data_vacancies: List[Tuple]):
    """We form the received information into the scv file. The file is saved in the folder "result"
    :param data_vacancies: list with information on vacancies
    """

    dir_name = 'result'
    if not os.path.exists('result'):
        os.mkdir(dir_name)
    df = DataFrame(data=data_vacancies,
                   columns=['pub_date', 'vacancy', 'experience', 'company', 'link', 'salary', 'skills', 'description'])
    df.to_csv(f'{dir_name}/{args["name"]}_{datetime.now()}.csv', index=False)


if __name__ == '__main__':
    headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
    num_page = f'page=0'
    args = parse_args()
    url = f'https://hh.ru/vacancies/{args["name"]}?customDomain=1&{num_page}'

    job_hh = requests.get(url, headers=headers)

    init_content = job_hh.content.decode('utf-8')
    html_sel = Selector(init_content).xpath('//html')[0]

    # find the number of available vacancies
    search_quantity = html_sel.xpath("//div[contains(@class, 'main-content')]")
    quantity = search_quantity.xpath("//span[contains(@data-qa, 'vacancies-total-found')]//text()").get()
    print(f'?????????? {quantity[2:]}')

    page_nav_buttons = html_sel.xpath(
        '//div[contains(@class, "pager")]/span[contains(@class, "pager-item-not-in-short-range")]'
        '//span//text()').getall()

    # find the number of pages to process
    total_pages = int(page_nav_buttons[-1])

    # list links for all vacancies from all pages
    links = []
    for i in range(total_pages):
        links += get_links(i)

    # job listing
    data_vacancies = []
    try:
        for link in links:
            data_vacancies.append(get_vacancy_data(link))
    except ConnectionError:
        time.sleep(5)

    # csv file generation
    save_data(data_vacancies)