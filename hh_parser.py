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

parser = argparse.ArgumentParser()
parser.add_argument('--name', required=True, choices=['data engineer'])
args = parser.parse_args()
args.name = args.name.lower().replace(' ', '-')

def selector_html(name_vacancy):
    """Формируем запрос"""
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    }
    num_page = f'page=0'
    url = f'https://hh.ru/vacancies/{name_vacancy}?customDomain=1&{num_page}'
    job_hh = requests.get(url, headers=headers)
    init_content = job_hh.content
    dec_content = init_content.decode('utf-8')
    html_sel = Selector(dec_content).xpath('//html')[0]
    return html_sel, url, headers

def quantity_vac(html_sel):
    """Получаем количество найденных вакансий"""
    search_quantity = html_sel.xpath("//div[contains(@class, 'main-content')]")
    quantity = search_quantity.xpath("//span[contains(@data-qa, 'vacancies-total-found')]//text()").get()
    print(f'Всего {quantity[2:]}')

def count_pages(html_sel, url):
    """Ищем количество страниц"""
    page_nav_buttons = html_sel.xpath('//div[contains(@class, "pager")]/span[contains(@class, "pager-item-not-in-short-range")]//span//text()').getall()#print(f'Последние кнопки навигации: {_page_nav_buttons}')
    total_pages = int(page_nav_buttons[-1])
    return total_pages

def information(total_pages, url, headers):
    """Получаем всю нужную информацию"""
    information_job = []
    for i in range(total_pages):
        counter = f'page={i}'  # переменная для перехода по страницам (для изменения url)
        job = requests.get(f'{url}&{counter}', headers=headers)  # переменная для запроса
        job_all = job.content.decode('utf-8')  # декодим
        html_sel = Selector(job_all).xpath('/html')  # превращаем в объект селектора
        links_sel = html_sel.xpath("//div[contains(@class, 'vacancy-serp-content')]")  # выделили общий див

        for l_sell in links_sel:
            link = l_sell.xpath("//a[contains(@class,'serp-item__title')]/@href").getall() #получили ссылки с одной страницы, обходим их
            try:
                for l in link:
                    response = requests.get(l, headers=headers)
                    html_info_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]

                    info_v = html_info_sel.xpath("//div[contains(@class, 'vacancy-section')]").xpath(
                    "//div[contains(@data-qa, 'vacancy-description')]//text()").getall()
                    text_vacancy = ''.join(info_v) #объединили в строку, убрали лишние сиволы

                    vacancy_name = str(html_info_sel.xpath("//h1[contains(@data-qa,'vacancy-title')]/text()").get())

                    company = html_info_sel.xpath("//a[contains(@data-qa,'vacancy-company-name')]//text()").getall()
                    company = ' '.join(company)
                    company = re.sub(r'[^\d,^\w.]', ' ', company)
                    company = company[0:(len(company)//2+1)]

                    #вытаскиваем дату и преобразовываем в нужный формат
                    try:
                        date = html_info_sel.xpath("//p[contains(@class, 'vacancy-creation-time-redesigned')]//text()").getall()[1]
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

                    information_job.append((pub_date, vacancy_name, company, l, salary, skill, text_vacancy))
                    print(f'Получаем инфомарцию по вакансии {vacancy_name}, компания "{company.strip()}"')

            except ConnectionError:
                time.sleep(5)

        print(f'Обработана {i + 1} страница')

    """Создаем файл с полученный информацией"""
    dir_info = 'Result'
    if not os.path.exists("Result"):
        os.mkdir(dir_info)
    df = DataFrame(data=information_job, columns=['pub_date', 'vacancy', 'company', 'link', 'salary', 'skills', 'description'])
    file = df.to_csv(f'{dir_info}/{args.name} {datetime.now().date()}.csv', index=False)

html_sel, url, headers = selector_html(args.name)
quantity_vac(html_sel)
total_pages = count_pages(html_sel, url)
information_job = information(total_pages, url, headers)













