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


def get_selector_html(main_url: str):
    """A main page's link to the input, and a selector object is html page to the output.

    :param main_url: a link for main page request
    :return: a selector object for a get request for the number of vacancies and the number of pages
    """

    job_hh = requests.get(main_url, headers=headers)
    init_content = job_hh.content.decode('utf-8')
    html_sel = Selector(init_content).xpath('//html')[0]
    return html_sel


def quantity_vac(html):
    """Main page request: find the number of available vacancies.

    :param html: a selector object is main html page
    """

    search_quantity = html.xpath("//div[contains(@class, 'main-content')]")
    quantity = search_quantity.xpath("//span[contains(@data-qa, 'vacancies-total-found')]//text()").get()
    print(f'Всего {quantity[2:]}')


def count_pages(html) -> int:
    """Find the number of pages to process

    :param html: a selector object is main html page
    :return: total pages
    """

    page_nav_buttons = html.xpath(
        '//div[contains(@class, "pager")]/span[contains(@class, "pager-item-not-in-short-range")]'
        '//span//text()').getall()
    total_pages = int(page_nav_buttons[-1])
    return total_pages


def get_links(count_pages: int, url: str) -> List[str]:
    """Number of available pages and main url for input, output - a list with links to all vacancies [link, link, ...]

    :param count_pages: number of pages for request
    :param url: url for a get request for issuing a vacancy for page_num
    :return: a list with links
    """

    links = []
    for i in range(count_pages):
        job_all = requests.get(url.format(name=parser_name_jobs(), page_num=f'page={i}'), headers=headers). \
            content.decode('utf-8')
        links_sel = Selector(job_all).xpath('/html').xpath("//div[contains(@class, 'vacancy-serp-content')]")
        for l_sell in links_sel:
            link = l_sell.xpath(
                "//a[contains(@class,'serp-item__title')]/@href").getall()
            links += link
    return links


def get_vacancy_data(link: str) -> Tuple[str]:
    """Logic for collecting information about the vacancy, the output is data about the vacancy

    :param link: job posting link
    :return: job data tuple
    """

    response = requests.get(link, headers=headers)
    html_info_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]

    info_v = html_info_sel.xpath("//div[contains(@class, 'vacancy-section')]").xpath(
        "//div[contains(@data-qa, 'vacancy-description')]//text()").getall()
    text_vacancy = ''.join(info_v)  # merged into a string, removed extra characters

    vacancy_name = str(html_info_sel.xpath("//h1[contains(@data-qa,'vacancy-title')]/text()").get())
    company = html_info_sel.xpath("//a[contains(@data-qa,'vacancy-company-name')]//text()").getall()
    company = ' '.join(company)
    company = re.sub(r'[^\d,^\w.]', ' ', company)
    company = company[0:(len(company) // 2 + 1)]

    # find the date and convert to the desired format
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

    print(f'Получаем инфомарцию по вакансии {vacancy_name}, компания "{company.strip()}"')
    return pub_date, vacancy_name, experience, company, link, salary, skill, text_vacancy

def parse_vacancies(all_vacancies_links: List[str]) -> List[Tuple]:
    """Сollection of data of all vacancies.

    :param all_vacancies_links: the input is a list with links to vacancies
    :return: data vacancies - the output is a list of files with information
    """

    data_vacancies = []  # job listing
    try:
        for link in all_vacancies_links:
            data_vacancies.append(get_vacancy_data(link))
    except ConnectionError:
        time.sleep(5)
    return data_vacancies

def save_data(data_vacancies: List[Tuple]):
    """We form the received information into the scv file. The file is saved in the folder "result"

    :param data_vacancies: list with information on vacancies
    """

    dir_name = 'result'
    if not os.path.exists('result'):
        os.mkdir(dir_name)
    df = DataFrame(data=data_vacancies,
                   columns=['pub_date', 'vacancy', 'experience', 'company', 'link', 'salary', 'skills', 'description'])
    file = df.to_csv(f'{dir_name}/{parser_name_jobs()} {datetime.now().date()}.csv', index=False)


if __name__ == '__main__':
    headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
    num_page = f'page=0'
    url = f'https://hh.ru/vacancies/{parser_name_jobs()}?customDomain=1&{num_page}'
args = parse_args()
    html_content = get_selector_html(url)  # get the selector object
    quantity_vac(html_content)  # displaying the number of vacancies found
    count_pages_ = count_pages(html_content)  # number of all pages found
    all_vacancies_links = get_links(count_pages_, url)  # list of all job links
    data_vacancies = parse_vacancies(all_vacancies_links) #getting all data about found vacancies
    save_data(data_vacancies)  # csv file generation