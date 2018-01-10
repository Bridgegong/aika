# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 9:52
# @Author  : Bridge
# @Email   : Bridge320@163.com
# @File    : aika_jiexi.py
# @Software: PyCharm

import re, requests, time, redis, xlrd, datetime, pymysql, pyodbc
from bs4 import BeautifulSoup
from selenium import webdriver

class AiKa(object):
    def __init__(self):
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.109 Safari/537.36',
            'Cookie': '_Xdwnewuv=1; _PVXuv=5a127c66a41c2; _fwck_www=8b8cbb9be874930c814d23f57c617b22; _appuv_www=0823d689dce4ca4877a84b377d0dbb0b; _locationInfo_=%7Burl%3A%22http%3A%2F%2Fbj.xcar.com.cn%2F%22%2Ccity_id%3A%22475%22%2Cprovince_id%3A%221%22%2C%20city_name%3A%22%25E5%258C%2597%25E4%25BA%25AC%22%7D; _Xdwuv=5a127c650cf9c; _fwck_newcar=14fe1795c65e6ee0a717804601de37aa; _appuv_newcar=fe35d1c1696223c4b408d8def7f6bd7e; bbs_sid=L3hJtL; bbs_visitedfid=1046D1753D114D589D840D545D756D1859D1350D1449; place_prid=1; place_crid=475; place_ip=118.187.12.40_1; uv_firstv_refers=http%3A//www.xcar.com.cn/bbs/f1046askp1.html; _Xdwstime=1511270074; Hm_lvt_53eb54d089f7b5dd4ae2927686b183e0=1511233157,1511242406,1511245350,1511270075; Hm_lpvt_53eb54d089f7b5dd4ae2927686b183e0=1511270075'
        }

    def read_file(self, str):
        info = {}
        data = xlrd.open_workbook('brandcar.xls', 'r')
        table = data.sheets()[0]
        nrows = table.nrows
        for i in range(nrows):
            info[table.row_values(i)[1]] = table.row_values(i)[0]
        for li in list(info.keys()):
            if str in li:
                brank1 = info[li]
                return brank1

    # def if_img(self, content):
    #     str = content
    #     print(str)
    #     str = str.lower()
    #     if '<img' in str:
    #         return True
    #     else:
    #         return False

    def get_redis(self):
        red = redis.StrictRedis('localhost', '6379', '4')
        urls = str(red.spop('cars_urls'), encoding="utf-8")
        conn1 = pymysql.connect(host='localhost', db='auto', user='root', passwd='123456', charset='utf8')
        cur1 = conn1.cursor()

        conn = pyodbc.connect(
            r'DRIVER={ODBC Driver 11 for SQL Server};'
            r'SERVER=192.168.3.152\DEV,4000;'
            r'DATABASE=MediaLog2017;'
            r'UID=sa;'
            r'PWD=123.abc'
        )
        cur = conn.cursor()  # connection.cursor()：返回一个游标对象，该对象可以用于查询并从数据库中获取结果。
        # connection.commit()：提交当前事务。你必须调用这个方法来确保你的数据执行。
        if cur:
            print("连接成功")
        else:
            print("连接失败")
        try:
            # urls = 'http://www.xcar.com.cn/bbs/viewthread.php?tid=30749915'
            r = requests.get(urls, headers=self.header)
            r.encoding = r.apparent_encoding
            bea = BeautifulSoup(r.text, 'lxml')
            start_times = bea.find('div', style="padding-top: 4px;float:left").text.replace('\n','').replace('                                                ','').replace('发表于','').replace(' | 来自 爱卡触屏版 ','')
            car_name = bea.find('b', 'w610').find_all('a')[2].text.replace('论坛', '')
            car_type = self.read_file(car_name)
            issues = bea.find('div', 'question_title').text
            # answe = bea.find('div','question_info').find_all('p')[0].text.replace('\n','').replace('\t','').replace('\r','')
            answe = bea.find('div', 'question_info')
            img = answe.find('img')
            if not img:
                answe1 = bea.find('div', 'question_info')
                answe1 = answe1.find_all('p')[0].text.replace('\n','').replace('\t','').replace('\r','')
                answer = re.sub(r'[ 本帖.编辑]', '',answe1)
                if answer == "":
                    answe = bea.find('div', 'question_info').find('p', 'p_phiz').text.replace('\n','').replace('\t','').replace('\r','')
                    answer = re.sub(r'[ 本帖.编辑]', '',answe)
                if issues == answer:
                    answer = ''
                    issues = issues
                info = self.read_files()
                one, two = self.trans(info, issues + answer)  # 匹配  一级  二级
                combine = start_times + urls + issues
                print(one, two)
                print(start_times)
                print(urls)
                print(car_type)
                print(car_name)
                print(one, two)
                print(issues)
                print(answer)
                print('=' * 68)

                sql = "insert into CarAnswer(Url,Brand,Car_Type,Type1,Type2,Issue,Answer,Resource,PublisTime,Uniqueness) VALUES ('%s','%s','%s','%s','%s','%s','%s','%d','%s','%s')" % (
                urls, car_type, car_name, one, two, issues, answer, 3, start_times,combine)
                cur.execute(sql)
                conn.commit()

                sql = "insert into cars_auto(times,urls,pinpai,car_type,yilei,erlei,issue,answer,types,Uniqueness) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                start_times, urls, car_type,car_name, one, two, issues, answer, 3, combine)
                cur1.execute(sql)
                conn1.commit()
        except Exception as e:
            print(e)
            redis_key = 'cars_urls'
            red = redis.StrictRedis('localhost', '6379', 5)
            red.sadd(redis_key, urls)

    def trans(self, info, content):
        count = {}
        index = 0
        for ll in info:
            l1 = ll[2:]
            count[index] = 0
            for l in l1:
                if l in content:
                    count[index] += 1
                    print(ll[0:2])
            index += 1
        count = sorted(count.items(), key=lambda count: count[1])[-1]
        cla = info[count[0]][0:2]
        print(cla)
        return cla

    def read_files(self):
        info = []
        data = xlrd.open_workbook('纬度信息点关键词(0324).xlsx')
        table = data.sheets()[2]
        nrows = table.nrows
        for i in range(nrows):
            ls = table.row_values(i)
            while '' in ls:
                ls.remove('')
            info.append(ls)
        # data.close()
        return info

    def main(self):
        self.get_redis()



if __name__ == '__main__':
    while True:
        time.sleep(1)
        ak = AiKa().main()
