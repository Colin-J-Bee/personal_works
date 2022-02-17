"""
数据分析岗位爬取-多线程爬取数据 + 线程池写数据 + 队列的使用 + mysql
"""
import os
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Thread

import requests
from re import findall
from json import loads
import time
import pymysql
from multiprocessing import Queue


def get_one_page(page, city_code='000000'):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    }

    # url = f'https://search.51job.com/list/{city_code},000000,0000,00,9,99,+,2,{page}.html%slang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare='
    url = f'https://search.51job.com/list/000000,000000,0000,00,9,99,数据分析,2,{page}.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare='
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        json_data = findall(r'window.__SEARCH_RESULT__\s*=\s*(\{.+?\})</script>', response.text)[0]
        return loads(json_data)['engine_search_result']
    else:
        print('请求失败!')


def get_all_data(start_page, step):
    for i in range(step):
        result = get_one_page(start_page)
        if not result:
            print('没有更多数据')
            break

        queue.put(result)
        # print(f'获取第{start_page}页数据成功!')

        start_page += 1


def save_page_data(data: list):

    conn = pymysql.connect(host='localhost', user='root', password='123456')
    try:
        conn.select_db('pythonDB2021_6')

        with conn.cursor() as cur:

            jobs = []
            for job in data:
                # 写入对应的数据
                # titles = ['岗位名称', '薪资', '公司名称', '公司性质', '公司地址', '要求', '福利']
                job_info = [
                    job.get('job_name', ''),
                    job.get('providesalary_text', ''),
                    job.get('company_name', ''),
                    job.get('companytype_text', ''),
                    job.get('workarea_text', ''),
                    '-'.join(job.get('attribute_text', ['-', '-', '-', '-', '-'])),
                    job.get('jobwelf', '')
                ]
                jobs.append(job_info)

            # print(jobs)
            sql_str = '''insert into job 
                                (job_id,job_name,providesalary_text,company_name,companytype_text,workarea_text,attribute_text,jobwelf) 
                                values 
                                (default,%s,%s,%s,%s,%s,%s,%s)'''
            row = cur.executemany(sql_str, jobs)
            if row == 0:
                print('写入错误！')
            else:
                print('写入数据成功!')
            conn.commit()
    except Exception as e:
        conn.rollback()
        print('写入报错！',e)
    finally:
        conn.close()


def database_found():
    # 打开数据库连接，不需要指定数据库，因为需要创建数据库
    conn = pymysql.connect(
        host='localhost',
        port=3306,
        user=os.environ.get('DB_USER') or "root",
        passwd=os.environ.get('DB_PASS') or "123456",
        charset='utf8mb4'
    )
    try:
        # 获取游标
        with conn.cursor() as cursor:
            # 创建pythonBD数据库
            cursor.execute('drop database if exists pythonDB2021_6;')
            cursor.execute('create database pythonDB2021_6 default charset utf8mb4;')

            conn.select_db('pythonDB2021_6')
            cursor = conn.cursor()
            #
            sql = '''create table if not exists `job` (
                      `job_id` int auto_increment comment '编号',
                      `job_name` varchar(255) comment '工作名',
                      `providesalary_text` varchar(255) comment '薪资',
                      `company_name` varchar(255) comment '公司名',
                      `companytype_text` varchar(255) comment '公司简介',
                      `workarea_text` varchar(255) comment '工作地点',
                      `attribute_text` varchar(255) comment '要求',
                      `jobwelf` varchar(255) comment '福利',
                      primary key (job_id)
                    ) engine =InnoDB  default charset =utf8mb4 auto_increment=0'''

            cursor.execute(sql)
            conn.commit()
    except pymysql.MySQLError:
        conn.rollback()
        print('建表有误')
    finally:
        conn.close()  # 再关闭数据库连接


def all_t_down():
    for t in ts:
        t.join()
    queue.put('end')


if __name__ == '__main__':
    start_time = time.time()

    queue = Queue()
    ts = []
    step = 20
    page_num = 2000
    for i in range(1, page_num, step):
        t = Thread(target=get_all_data, args=(i, step))
        ts.append(t)
        t.start()

    Thread(target=all_t_down()).start()

    database_found()

    with ThreadPoolExecutor(max_workers=32) as pool:
        while True:
            data = queue.get()
            if data == 'end':
                break
            pool.submit(save_page_data, data)
        pool.shutdown(wait=True)

    print(f'共爬取{page_num}页数据，共耗时(包括写入数据){time.time() - start_time}s')