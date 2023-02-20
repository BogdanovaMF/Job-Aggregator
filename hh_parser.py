import re
import os
import time
import shutil
import locale
import requests
from parsel import Selector
from pandas import DataFrame
from datetime import datetime

class HHParser:
    """Определяем атрибут"""
    def __init__(self, find_hh):
        self.find_hh = find_hh
        print('Обработка веб-страницы...')

    def initial(self):
        """Формируем запрос"""
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        }
        num_page = f'page=0'
        url = f'https://hh.ru/vacancies/{self.find_hh}?customDomain=1&{num_page}'
        job_hh = requests.get(url, headers=headers)
        init_content = job_hh.content
        dec_content = init_content.decode('utf-8')
        html_sel = Selector(dec_content).xpath('//html')[0]

        """Получаем количество найденных вакансий"""
        search_quantity = html_sel.xpath("//div[contains(@class, 'main-content')]")
        quantity = search_quantity.xpath("//span[contains(@data-qa, 'vacancies-total-found')]//text()").get()
        print(f'Всего {quantity[2:]}')

        """Ищем количество страниц"""
        page_nav_buttons = html_sel.xpath('//div[contains(@class, "pager")]/span[contains(@class, "pager-item-not-in-short-range")]//span//text()').getall()#print(f'Последние кнопки навигации: {_page_nav_buttons}')
        total_pages = int(page_nav_buttons[-1])
        print(f'Обработано {total_pages} страниц')

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
                for l in link:
                    response = requests.get(l, headers=headers)
                    html_info_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]

                    # текст вакансии
                    info_v = html_info_sel.xpath("//div[contains(@class, 'vacancy-section')]").xpath(
                    "//div[contains(@data-qa, 'vacancy-description')]//text()").getall()
                    info_vac = ''.join(info_v) #объединили в строку, убрали лишние сиволы

                    #название вакансии
                    name = str(html_info_sel.xpath("//h1[contains(@data-qa,'vacancy-title')]/text()").get())

                    #имя компании
                    company = html_info_sel.xpath("//a[contains(@data-qa,'vacancy-company-name')]//text()").get()

                    #вытаскиваем дату и преобразовываем в нужный формат
                    try:
                        date = html_info_sel.xpath("//p[contains(@class, 'vacancy-creation-time-redesigned')]//text()").getall()[1]
                        date = re.sub(r'[^\d,^\w.]', ' ', date)
                        locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
                        dateline = datetime.strptime(date, "%d %B %Y")
                    except IndexError:
                        dateline = '-'

                    #получаем навыки
                    skill = html_info_sel.xpath("//div[contains(@class, 'bloko-tag-list')]//text()").getall()
                    skill = ','.join(skill)
                    skill = re.sub(r'[^\w.]', ' ', skill)

                    # получаем ЗП
                    salary = html_info_sel.xpath(
                        "//div[contains(@data-qa, 'vacancy-salary')]//text()").getall()
                    salary = ' '.join(salary).split()
                    salary = ' '.join(salary)

                    information_job.append((dateline, name, company, l, salary, skill, info_vac))


        df = DataFrame(data=information_job, columns=['Pub_date', 'Vacancy', 'Company', 'Link', 'Salary', 'Skills', 'Text vacancy'])
        file = df.to_csv(f'{new} {datetime.now().date()}.csv', index=False)

        # создаем папку и переносим туда файл scv
        os.mkdir('Result')
        shutil.move(file, 'Result')

new = input('Введите имя вакансии для поиска').lower().replace(' ','-')
print(f'Поехали, ищем вакансию {new.upper()}')
lets_find = HHParser(new)
lets_find.initial()




