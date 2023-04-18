import re
import locale
import datetime
import requests
from parsel import Selector
from datetime import datetime
from etl.clients.hh import headers, HHClient


def test_get_response1():
    var = HHClient.url
    response = requests.get(url=var.format(specialization='data-engineer', num_page='page=0'), headers=headers)
    assert response.status_code == 200


def test_get_response2():
    var = HHClient.url
    response = requests.get(url=var.format(specialization='python-developer', num_page='page=0'), headers=headers)
    assert response.status_code == 200


def test_get_pages():
    var = HHClient.url
    job_hh = requests.get(url=var.format(specialization='python-developer', num_page='page=0'), headers=headers)
    init_content = job_hh.content.decode('utf-8')
    html_sel = Selector(init_content).xpath('//html')[0]
    page_nav_buttons = html_sel.xpath(
        '//div[contains(@class, "pager")]/span[contains(@class, "pager-item-not-in-short-range")]'
        '//span//text()').getall()
    assert int(page_nav_buttons[-1]) == 40


def test_get_links():
    var = HHClient.url
    response = requests.get(url=var.format(specialization='python-developer', num_page='page=0'), headers=headers)
    job_content = response.content.decode('utf-8')
    links_sel = Selector(job_content).xpath("//div[contains(@class, 'HH-MainContent HH-Supernova-MainContent')]")
    for l_sell in links_sel:
        links_page = l_sell.xpath("//a[contains(@class,'serp-item__title')]/@href").getall()
    assert links_page[0] == 'https://hh.ru/vacancy/78542200?query=Python+developer'


def test_get_text_html1():
    var = HHClient.url
    response = requests.get(url=var.format(specialization='python-developer', num_page='page=0'), headers=headers)
    init_content = response.content.decode('utf-8')
    html_sel = Selector(init_content).xpath('//html')[0]
    search_text = html_sel.xpath("//div[contains(@class, 'main-content')]")
    text = search_text.xpath("//h1[contains(@data-qa, 'vacancies-catalog-header')]//text()").get()
    assert text == 'Работа Python developer в Москве'


def test_get_text_html2():
    link = 'https://hh.ru/vacancy/79428379?query=data%20scientist&from=vacancy_search_list'
    response = requests.get(link, headers=headers)
    html_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]
    vacancy_name = str(html_sel.xpath("//h1[contains(@data-qa,'vacancy-title')]/text()").get())
    assert vacancy_name == 'Junior Data Scientist в команду b2c'


def test_get_text_html3():
    link = 'https://hh.ru/vacancy/79110745?query=data%20scientist&from=vacancy_search_list'
    response = requests.get(link, headers=headers)
    html_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]
    skill = html_sel.xpath("//div[contains(@class, 'bloko-tag-list')]//text()").getall()
    skill = ','.join([i for i in skill])
    assert skill == """Python,SQL,AWS,Английский\xa0— B2 — Средне-продвинутый"""


def test_get_text_html4():
    link = 'https://hh.ru/vacancy/79428379?from=vacancy_search_list&query=Python%20junior'
    response = requests.get(link, headers=headers)
    html_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]
    company = html_sel.xpath("//a[contains(@data-qa,'vacancy-company-name')]//text()").getall()
    company = ' '.join(company)
    company = re.sub(r'[^\d,^\w.]', ' ', company)[0:(len(company) // 2 + 1)]
    assert company == 'Сбер для экспертов '


def test_get_text_html5():
    link = 'https://hh.ru/vacancy/79428379?from=vacancy_search_list&query=Python%20junior'
    response = requests.get(link, headers=headers)
    html_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]
    skill = html_sel.xpath("//div[contains(@class, 'bloko-tag-list')]//text()").getall()
    skill = ','.join([i for i in skill])
    assert skill == ''


def test_get_text_html6():
    link = 'https://hh.ru/vacancy/79428379?from=vacancy_search_list&query=Python%20junior'
    response = requests.get(link, headers=headers)
    html_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]
    salary = html_sel.xpath("//div[contains(@data-qa, 'vacancy-salary')]//text()").getall()
    salary = ' '.join([i for i in ' '.join(salary).strip().split()])
    assert salary == 'з/п не указана'


def test_get_text_html7():
    link = 'https://hh.ru/vacancy/79428379?from=vacancy_search_list&query=Python%20junior'
    response = requests.get(link, headers=headers)
    html_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]
    experience = html_sel.xpath("//span[contains(@data-qa, 'vacancy-experience')]//text()").get()
    assert experience == '1–3 года'


def test_get_text_html8():
    link = 'https://hh.ru/vacancy/79428379?from=vacancy_search_list&query=Python%20junior'
    response = requests.get(link, headers=headers)
    html_sel = Selector(response.content.decode('utf-8')).xpath('/html')[0]
    date = html_sel.xpath("//p[contains(@class, 'vacancy-creation-time-redesigned')]"
                                  "//text()").getall()[1]
    date = re.sub(r'[^\d,^\w.]', ' ', date)
    locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
    pub_date = datetime.strptime(date, "%d %B %Y")
    assert pub_date == datetime(2023, 4, 18, 0, 0)