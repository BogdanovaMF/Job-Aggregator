import re
import csv
import time
import requests
import pandas as pd
from pandas import DataFrame
from bs4 import BeautifulSoup as bs

if __name__ == '__main__':

    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    }
    num_page = f'page=0'
    url = f'https://hh.ru/vacancies/data-engineer?customDomain=1&{num_page}'
    job_hh = requests.get(url, headers=headers)

    # формируем в суп текст полученного html
    job_soup = bs(job_hh.text, 'html.parser')
    # ищем в супе ссылки на страницы
    job_soup_page = job_soup.find('div', {'class': 'bloko-gap bloko-gap_top'})
    pages = job_soup_page.find_all('a')
    pages_count = []  # список номеров страниц
    for i in job_soup_page.text:
        if i.isdigit():  # находим номера страниц
            pages_count.append(int(i))
    count = pages_count[-1]  # количество страниц (последняя страница)

    # начинам парсить сами страницы с вакансиями поочереди
    job_link_list = []  # список с кортежами (вакансия, ссылка, компания)
    for i in range(count):
        counter = f'page={i}'  # переменная для перехода по страницам (для изменения url)
        result = requests.get(f'{url}&{counter}', headers=headers)  # переменная для запроса страницы
        # парсим каждую ссылку
        time.sleep(1)
        job = bs(result.text, 'html.parser')
        results = job.find_all('div', {'class': 'vacancy-serp-item-body__main-info'})
        # по циклу проходим часть кода html и вытаскиваем заголовки в переменную-имя вакансии
        for result in results:
            name_job = result.find('a').text
            link = result.find('a')['href']
            company = result.find('div', {'class': "vacancy-serp-item__meta-info-company"}).find('a').text
            link_c = result.find('div', {'class': "vacancy-serp-item__meta-info-company"}).find('a')[
                'href']  # нашли ссылку на компанию
            link_company = f'hh.ru{link_c}'  # слепили в урл

            # проходимся по самой ссылке на вакансию и вытаскиваем из нее информацию
            link_vacancy = requests.get(link, headers=headers)
            link_vacancy_soup = bs(link_vacancy.text, 'html.parser')
            link_result = link_vacancy_soup.find_all('div', {'class': 'vacancy-description'})
            date = link_vacancy_soup.find('p', {'class': "vacancy-creation-time-redesigned"}).text
            for info in link_result:
                text_vacancy = info.find('div', {'class':"vacancy-section"}).find('div', {'data-qa': "vacancy-description"}).text

    # все в кортеж, а затем в список включаем
    job_link = (date, name_job, link, company, link_company, text_vacancy)
    job_link_list.append(job_link)

    # создаем файл data.csv + названия колонок
    df = DataFrame(data=job_link_list, columns=['Date','Vacancy', 'Link', 'Company', 'Link Company', 'Text Vacancy'])
    df.to_csv('data.csv', index=False)