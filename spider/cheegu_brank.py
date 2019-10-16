#！coding: utf-8

import requests
import pymysql
import os
from threading import Thread,RLock,enumerate
import queue
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# 拦截SSL警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

current_path = os.path.abspath(__file__) # 获取当前文件的绝对路径

father_path = os.path.abspath(os.path.dirname(current_path) + os.path.sep + ".") # 获取当前文件的父目录

error_path = os.path.join(os.path.abspath(os.path.dirname(current_path) + os.path.sep + "."),'error.txt') # 获取报错文档的路径
# print(current_path)
# print(father_path)
# print(error_path)

def writefile(field): # 写入文件
    # file = "E:\spider\error\error.txt"
    f = open(error_path,'a+',encoding='utf-8')
    f.write(field)
    f.close()

def removefile(): # 删除文件
    # file = "E:\spider\error\error.txt"
    if os.path.exists(error_path):
        os.remove(error_path)
    else:
        f = open(error_path, 'w+', encoding='utf-8')
        f.close()

s = requests.session()

audi_urls = [] # 车系url列表
moto_urls = [] # 车型url列表
moto_ids= [] # 车型id列表
moto_error = {} # 车型插入出错

brank_url = "http://api.cheegu.com/CARDB.getMasterBrand" # 品牌接口url
moto_url = "http://api.cheegu.com/CARDB.getModel?serialId={}" # 车型接口url
audi_url = "http://api.cheegu.com/CARDB.getSerial?masterid={}&full=false&hasmodel=true" # 车系接口url

h = {
    "Accept":"application/json, text/javascript, */*; q=0.01",
    "Accept-Language":"zh-CN,zh;q=0.9",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
    "Accept-Encoding":"gzip, deflate",
    "Connection":"keep-alive",
    "Referer":"http://www.cheegu.com/"
} # 请求header
q = queue.Queue() # 创建队列

lock = RLock() # 创建线程锁

db = pymysql.connect("localhost", "root", "123456", "branddb", charset='utf8')  # 连接数据库

cursor = db.cursor()  # 使用cursor()方法获取操作游标

removefile() # 删除已有的错误日志文件

def spider_brand(h): # 插入品牌信息
     # 线程上锁
    lock.acquire()
    r_brank = s.get(brank_url,headers=h,verify=False) # 请求品牌接口
    brank_f = r_brank.json() # 返回页面json信息
    # current_time = datetime.datetime.now().strftime('%Y-%m-%d') # 获取本地时间
    for brank_i in brank_f["message"]:
        # print(find_b)
        # 提取品牌字段
        brand_id = brank_i['id']
        brand_name = brank_i['name']
        alphabet_code = brank_i['char']
        logo = brank_i['logo'] if brank_i['logo'] else '-'
        # print(logo)
        # 插入品牌数据
        # sql插入语句
        insert_sql = """INSERT INTO car_brand_info(id, brand_name, alphabet_code, logo)
                        VALUES ('{}','{}','{}','{}')""".format(brand_id,brand_name,alphabet_code,logo)
        # print(insert_sql)
        try:
            # 执行sql语句
            cursor.execute(insert_sql)
            db.commit()
            print("---------------品牌：%s，插入成功---------------" % brand_name)
        except Exception as e:
            es = str(e)
            if 'PRIMARY' in es:
                print("---------------跳过品牌：%s，该品牌已存在---------------" % brand_name)
                # continue
            # 发生错误时回滚
            else:
                print("---------------品牌：%s，插入失败---------------" % brand_name)
                print(e)
                brandid =str(brand_id)
                error1 = str(brand_name)
                writefile(f"{brandid}, {error1}\n {es}")
                # db.rollback()

        # for brank_i in brank_f["message"]:
            # print(brank_i)
            # 拼接车系url
        audi_urls.append(brank_i["id"]) # 添加车系id
        q.put(brand_id)
    lock.release()

def spider_audi(h): # 插入车系信息
    lock.acquire()
    # for audi_u in audi_urls:
    i = 0
    while i < len(audi_urls):
        audi_u = q.get()
        audi_id = audi_url.format(audi_u)
        r_audi = s.get(audi_id,headers=h,verify=False)
        audi_f = r_audi.json()
        # print(audi_f["message"])
        for audi_i in audi_f["message"]:
        # 提取车系字段
            au_id = audi_i['id']
            brand_name = audi_i['brand']
            masterid = audi_u
            picture = audi_i['picture'] if audi_i['picture'] else '-'
            series_name = audi_i['name']
            # print(au_id,brand_name,masterid,picture,series_name)
            # 插入车系
            # 车系插入sql语句
            audi_sql = """INSERT INTO car_series_info(id, brand_name, masterid, picture, series_name)
                        VALUES ( '{}','{}','{}','{}','{}')""".format(au_id,brand_name,masterid,picture,series_name)
            # print(insert_sql)
            moto_urls.append(audi_i["id"])
            try:
                # 执行sql语句
                cursor.execute(audi_sql)
                db.commit()
                print("---------------车系：%s，插入成功---------------" % series_name)
            except Exception as e:
                es = str(e)
                if 'PRIMARY' in es:
                    print("---------------跳过车系：%s，该车系已存在---------------" % series_name)
                else:
                    #     # 发生错误时回滚
                    print("---------------车系：%s，插入失败---------------" % series_name)
                    print(e)
                    auid = str(au_id)
                    error2 = str(series_name)
                    writefile(f"{auid} {error2}\n {es}")
                    # db.rollback()
            q.put(au_id)
        i += 1
    # # print(moto_urls)
    lock.release()

def list_Average(lists,avg): # 平均等分列表中的数据
    lock.acquire()
    len_avg = round(len(lists) / avg)
    list_avg = [moto_urls[i:i + len_avg] for i in range(0, len(moto_urls), len_avg)]
    lock.release()
    return list_avg

def spider_moto_01(h,lists,avg): # 插入车型信息
    # lock.acquire()
    # i = 0
    list_avg = list_Average(lists,avg)
    print(list_avg[0])
    for moto_u in list_avg[0]:
    # while i < len(moto_urls):
    #     moto_u = q.get()
        moto_id = moto_url.format(moto_u)  # 车系url
        r_moto = s.get(moto_id,headers=h,verify=False)
        moto_f = r_moto.json()
        # print(moto_f["message"])
        for moto_i in moto_f["message"]:
            # print(i["fullname"])
            moto_id = moto_i["id"]
            engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
            engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
            engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
            drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
            gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
            regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
            mile = moto_i["miles"] if moto_i["miles"] else 0
            miles = round(mile,1)
            serial_id = moto_u
            fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
            year = moto_i["y"] if moto_i["y"] else 0
            price = moto_i["v"] if moto_i["v"] else '-'
            name = moto_i["name"] if moto_i["name"] else '-'
            moto_ids.append(moto_id )
            # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
            #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
            moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                      drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                        VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                serial_id,fullname,year,price,name)
            try:
                # 执行sql语句
                cursor.execute(moto_sql)
                db.commit()
                print("---------------车型：%s，插入成功---------------" % fullname)
            except Exception as e:
                if 'PRIMARY' in str(e):
                    print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                else:
                    #
                    # 发生错误时回滚
                    print("---------------车型：%s，插入失败---------------" % fullname)
                    print(e)
                    moto_error[moto_u] = moto_error[moto_i["id"]] # 将入错误的车型数据和错误信息写入error.txt文件
    #     i += 1
    # lock.release()
    print(moto_error)
    # print(f['message'])
    # print(find)s%d
    # 循环

def spider_moto_02(h,lists,avg): # 插入车型信息
    # lock.acquire()
    # i = 0
    list_avg = list_Average(lists,avg)
    print(list_avg[1])
    for moto_u in list_avg[1]:
    # while i < len(moto_urls):
    #     moto_u = q.get()
        moto_id = moto_url.format(moto_u)  # 车系url
        r_moto = s.get(moto_id,headers=h,verify=False)
        moto_f = r_moto.json()
        # print(moto_f["message"])
        for moto_i in moto_f["message"]:
            # print(i["fullname"])
            moto_id = moto_i["id"]
            engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
            engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
            engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
            drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
            gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
            regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
            mile = moto_i["miles"] if moto_i["miles"] else 0
            miles = round(mile,1)
            serial_id = moto_u
            fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
            year = moto_i["y"] if moto_i["y"] else 0
            price = moto_i["v"] if moto_i["v"] else '-'
            name = moto_i["name"] if moto_i["name"] else '-'
            moto_ids.append(moto_id)
            # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
            #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
            moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                      drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                        VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                serial_id,fullname,year,price,name)
            try:
                # 执行sql语句
                cursor.execute(moto_sql)
                db.commit()
                print("---------------车型：%s，插入成功---------------" % fullname)
            except Exception as e:
                if 'PRIMARY' in str(e):
                    print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                else:
                    #
                    # 发生错误时回滚
                    print("---------------车型：%s，插入失败---------------" % fullname)
                    print(e)
                    moto_error[moto_u] = moto_error[moto_i["id"]]
    #     i += 1
    # lock.release()
    print(moto_error)
    # print(f['message'])
    # print(find)s%d
    # 循环

def spider_moto_03(h,lists,avg): # 插入车型信息
    # lock.acquire()
    # i = 0
    list_avg = list_Average(lists,avg)
    print(list_avg[2])
    for moto_u in list_avg[2]:
    # while i < len(moto_urls):
    #     moto_u = q.get()
        moto_id = moto_url.format(moto_u)  # 车系url
        r_moto = s.get(moto_id,headers=h,verify=False)
        moto_f = r_moto.json()
        # print(moto_f["message"])
        for moto_i in moto_f["message"]:
            # print(i["fullname"])
            moto_id = moto_i["id"]
            engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
            engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
            engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
            drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
            gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
            regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
            mile = moto_i["miles"] if moto_i["miles"] else 0
            miles = round(mile,1)
            serial_id = moto_u
            fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
            year = moto_i["y"] if moto_i["y"] else 0
            price = moto_i["v"] if moto_i["v"] else '-'
            name = moto_i["name"] if moto_i["name"] else '-'
            moto_ids.append(moto_id)
            # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
            #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
            moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                      drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                        VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                serial_id,fullname,year,price,name)
            try:
                # 执行sql语句
                cursor.execute(moto_sql)
                db.commit()
                print("---------------车型：%s，插入成功---------------" % fullname)
            except Exception as e:
                if 'PRIMARY' in str(e):
                    print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                else:
                    #
                    # 发生错误时回滚
                    print("---------------车型：%s，插入失败---------------" % fullname)
                    print(e)
                    moto_error[moto_u] = moto_error[moto_i["id"]]
    #     i += 1
    # lock.release()
    print(moto_error)
    # print(f['message'])
    # print(find)s%d
    # 循环

def spider_moto_04(h,lists,avg): # 插入车型信息
    # lock.acquire()
    # i = 0
    list_avg = list_Average(lists,avg)
    print(list_avg[3])
    for moto_u in list_avg[3]:
    # while i < len(moto_urls):
    #     moto_u = q.get()
        moto_id = moto_url.format(moto_u)  # 车系url
        r_moto = s.get(moto_id,headers=h,verify=False)
        moto_f = r_moto.json()
        # print(moto_f["message"])
        for moto_i in moto_f["message"]:
            # print(i["fullname"])
            moto_id = moto_i["id"]
            engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
            engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
            engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
            drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
            gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
            regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
            mile = moto_i["miles"] if moto_i["miles"] else 0
            miles = round(mile,1)
            serial_id = moto_u
            fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
            year = moto_i["y"] if moto_i["y"] else 0
            price = moto_i["v"] if moto_i["v"] else '-'
            name = moto_i["name"] if moto_i["name"] else '-'
            moto_ids.append(moto_id)
            # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
            #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
            moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                      drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                        VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                serial_id,fullname,year,price,name)
            try:
                # 执行sql语句
                cursor.execute(moto_sql)
                db.commit()
                print("---------------车型：%s，插入成功---------------" % fullname)
            except Exception as e:
                if 'PRIMARY' in str(e):
                    print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                else:
                    #
                    # 发生错误时回滚
                    print("---------------车型：%s，插入失败---------------" % fullname)
                    print(e)
                    moto_error[moto_u] = moto_error[moto_i["id"]]
    #     i += 1
    # lock.release()
    print(moto_error)
    # print(f['message'])
    # print(find)s%d
    # 循环

def spider_moto_05(h,lists,avg): # 插入车型信息
    # lock.acquire()
    # i = 0
    list_avg = list_Average(lists,avg)
    print(list_avg[4])
    for moto_u in list_avg[4]:
    # while i < len(moto_urls):
    #     moto_u = q.get()
        moto_id = moto_url.format(moto_u)  # 车系url
        r_moto = s.get(moto_id,headers=h,verify=False)
        moto_f = r_moto.json()
        # print(moto_f["message"])
        for moto_i in moto_f["message"]:
            # print(i["fullname"])
            moto_id = moto_i["id"]
            engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
            engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
            engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
            drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
            gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
            regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
            mile = moto_i["miles"] if moto_i["miles"] else 0
            miles = round(mile,1)
            serial_id = moto_u
            fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
            year = moto_i["y"] if moto_i["y"] else 0
            price = moto_i["v"] if moto_i["v"] else '-'
            name = moto_i["name"] if moto_i["name"] else '-'
            moto_ids.append(moto_id)
            # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
            #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
            moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                      drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                        VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                serial_id,fullname,year,price,name)
            try:
                # 执行sql语句
                cursor.execute(moto_sql)
                db.commit()
                print("---------------车型：%s，插入成功---------------" % fullname)
            except Exception as e:
                if 'PRIMARY' in str(e):
                    print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                else:
                    #
                    # 发生错误时回滚
                    print("---------------车型：%s，插入失败---------------" % fullname)
                    print(e)
                    moto_error[moto_u] = moto_error[moto_i["id"]]
    #     i += 1
    # lock.release()
    print(moto_error)
    # print(f['message'])
    # print(find)s%d
    # 循环

def spider_moto_06(h,lists,avg): # 插入车型信息
    # lock.acquire()
    # i = 0
    list_avg = list_Average(lists,avg)
    print(list_avg[5])
    for moto_u in list_avg[5]:
    # while i < len(moto_urls):
    #     moto_u = q.get()
        moto_id = moto_url.format(moto_u)  # 车系url
        r_moto = s.get(moto_id,headers=h,verify=False)
        moto_f = r_moto.json()
        # print(moto_f["message"])
        for moto_i in moto_f["message"]:
            # print(i["fullname"])
            moto_id = moto_i["id"]
            engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
            engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
            engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
            drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
            gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
            regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
            mile = moto_i["miles"] if moto_i["miles"] else 0
            miles = round(mile,1)
            serial_id = moto_u
            fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
            year = moto_i["y"] if moto_i["y"] else 0
            price = moto_i["v"] if moto_i["v"] else '-'
            name = moto_i["name"] if moto_i["name"] else '-'
            moto_ids.append(moto_id)
            # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
            #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
            moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                      drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                        VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                serial_id,fullname,year,price,name)
            try:
                # 执行sql语句
                cursor.execute(moto_sql)
                db.commit()
                print("---------------车型：%s，插入成功---------------" % fullname)
            except Exception as e:
                if 'PRIMARY' in str(e):
                    print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                else:
                    #
                    # 发生错误时回滚
                    print("---------------车型：%s，插入失败---------------" % fullname)
                    print(e)
                    moto_error[moto_u] = moto_error[moto_i["id"]]
    #     i += 1
    # lock.release()
    print(moto_error)
    # print(f['message'])
    # print(find)s%d
    # 循环

def spider_moto_07(h,lists,avg): # 插入车型信息
    # lock.acquire()
    # i = 0
    list_avg = list_Average(lists,avg)
    print(list_avg[6])
    for moto_u in list_avg[6]:
    # while i < len(moto_urls):
    #     moto_u = q.get()
        moto_id = moto_url.format(moto_u)  # 车系url
        r_moto = s.get(moto_id,headers=h,verify=False)
        moto_f = r_moto.json()
        # print(moto_f["message"])
        for moto_i in moto_f["message"]:
            # print(i["fullname"])
            moto_id = moto_i["id"]
            engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
            engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
            engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
            drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
            gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
            regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
            mile = moto_i["miles"] if moto_i["miles"] else 0
            miles = round(mile,1)
            serial_id = moto_u
            fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
            year = moto_i["y"] if moto_i["y"] else 0
            price = moto_i["v"] if moto_i["v"] else '-'
            name = moto_i["name"] if moto_i["name"] else '-'
            moto_ids.append(moto_id)
            # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
            #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
            moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                      drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                        VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                serial_id,fullname,year,price,name)
            try:
                # 执行sql语句
                cursor.execute(moto_sql)
                db.commit()
                print("---------------车型：%s，插入成功---------------" % fullname)
            except Exception as e:
                if 'PRIMARY' in str(e):
                    print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                else:
                    #
                    # 发生错误时回滚
                    print("---------------车型：%s，插入失败---------------" % fullname)
                    print(e)
                    moto_error[moto_u] = moto_error[moto_i["id"]]
    #     i += 1
    # lock.release()
    print(moto_error)
    # print(f['message'])
    # print(find)s%d
    # 循环

def spider_moto_08(h,lists,avg): # 插入车型信息
    # lock.acquire()
    # i = 0
    list_avg = list_Average(lists,avg)
    print(list_avg[7])
    for moto_u in list_avg[7]:
    # while i < len(moto_urls):
    #     moto_u = q.get()
        moto_id = moto_url.format(moto_u)  # 车系url
        r_moto = s.get(moto_id,headers=h,verify=False)
        moto_f = r_moto.json()
        # print(moto_f["message"])
        for moto_i in moto_f["message"]:
            # print(i["fullname"])
            moto_id = moto_i["id"]
            engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
            engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
            engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
            drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
            gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
            regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
            mile = moto_i["miles"] if moto_i["miles"] else 0
            miles = round(mile,1)
            serial_id = moto_u
            fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
            year = moto_i["y"] if moto_i["y"] else 0
            price = moto_i["v"] if moto_i["v"] else '-'
            name = moto_i["name"] if moto_i["name"] else '-'
            moto_ids.append(moto_id)
            # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
            #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
            moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                      drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                        VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                serial_id,fullname,year,price,name)
            try:
                # 执行sql语句
                cursor.execute(moto_sql)
                db.commit()
                print("---------------车型：%s，插入成功---------------" % fullname)
            except Exception as e:
                if 'PRIMARY' in str(e):
                    print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                else:
                    #
                    # 发生错误时回滚
                    print("---------------车型：%s，插入失败---------------" % fullname)
                    print(e)
                    moto_error[moto_u] = moto_error[moto_i["id"]]
    #     i += 1
    # lock.release()
    print(moto_error)
    # print(f['message'])
    # print(find)s%d
    # 循环

def spider_moto_09(h,lists,avg): # 插入车型信息
    # lock.acquire()
    # i = 0
    list_avg = list_Average(lists,avg)
    print(list_avg[8])
    for moto_u in list_avg[8]:
    # while i < len(moto_urls):
    #     moto_u = q.get()
        moto_id = moto_url.format(moto_u)  # 车系url
        r_moto = s.get(moto_id,headers=h,verify=False)
        moto_f = r_moto.json()
        # print(moto_f["message"])
        for moto_i in moto_f["message"]:
            # print(i["fullname"])
            moto_id = moto_i["id"]
            engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
            engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
            engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
            drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
            gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
            regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
            mile = moto_i["miles"] if moto_i["miles"] else 0
            miles = round(mile,1)
            serial_id = moto_u
            fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
            year = moto_i["y"] if moto_i["y"] else 0
            price = moto_i["v"] if moto_i["v"] else '-'
            name = moto_i["name"] if moto_i["name"] else '-'
            moto_ids.append(moto_id)
            # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
            #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
            moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                      drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                        VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                serial_id,fullname,year,price,name)
            try:
                # 执行sql语句
                cursor.execute(moto_sql)
                db.commit()
                print("---------------车型：%s，插入成功---------------" % fullname)
            except Exception as e:
                if 'PRIMARY' in str(e):
                    print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                else:
                    #
                    # 发生错误时回滚
                    print("---------------车型：%s，插入失败---------------" % fullname)
                    print(e)
                    moto_error[moto_u] = moto_error[moto_i["id"]]
    #     i += 1
    # lock.release()
    print(moto_error)
    # print(f['message'])
    # print(find)s%d
    # 循环

def spider_moto_10(h,lists,avg): # 插入车型信息
    # lock.acquire()
    # i = 0
    list_avg = list_Average(lists,avg)
    print(list_avg[9])
    for moto_u in list_avg[9]:
    # while i < len(moto_urls):
    #     moto_u = q.get()
        moto_id = moto_url.format(moto_u)  # 车系url
        r_moto = s.get(moto_id,headers=h,verify=False)
        moto_f = r_moto.json()
        # print(moto_f["message"])
        for moto_i in moto_f["message"]:
            # print(i["fullname"])
            moto_id = moto_i["id"]
            engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
            engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
            engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
            drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
            gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
            regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
            mile = moto_i["miles"] if moto_i["miles"] else 0
            miles = round(mile,1)
            serial_id = moto_u
            fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
            year = moto_i["y"] if moto_i["y"] else 0
            price = moto_i["v"] if moto_i["v"] else '-'
            name = moto_i["name"] if moto_i["name"] else '-'
            moto_ids.append(moto_id)
            # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
            #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
            moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                      drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                        VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                serial_id,fullname,year,price,name)
            try:
                # 执行sql语句
                cursor.execute(moto_sql)
                db.commit()
                print("---------------车型：%s，插入成功---------------" % fullname)
            except Exception as e:
                if 'PRIMARY' in str(e):
                    print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                else:
                    #
                    # 发生错误时回滚
                    print("---------------车型：%s，插入失败---------------" % fullname)
                    print(e)
                    moto_error[moto_u] = moto_error[moto_i["id"]]
    #     i += 1
    # lock.release()
    print(moto_error) # 打印插入车型错误的列表
    # print(f['message'])
    # print(find)s%d
    # 循环

def spider_error(h):
        lock.acquire()
        if moto_error:
            for moto_ur in moto_error.keys():
                moto_idr = moto_url.format(moto_ur)  # 车系url
                r_motor = s.get(moto_idr, headers=h, verify=False)
                moto_fr = r_motor.json()
                # print(moto_f["message"])
                for moto_i in moto_fr["message"]:
                    # print(i["fullname"])
                    if moto_i["id"] in moto_error.values():
                        moto_id = moto_i["id"]
                        engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else '-'
                        engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else '-'
                        engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else '-'
                        drive_type = moto_i["drive_type"] if moto_i["drive_type"] else '-'
                        gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else '-'
                        regdate = moto_i["regdate"] if moto_i["regdate"] else '-'
                        # mile = moto_i["miles"] if moto_i["miles"] else 0
                        # miles = round(mile, 1)
                        serial_id = moto_ur
                        fullname = moto_i["fullname"] if moto_i["fullname"] else '-'
                        year = moto_i["y"] if moto_i["y"] else 0
                        price = moto_i["v"] if moto_i["v"] else '-'
                        name = moto_i["name"] if moto_i["name"] else '-'
                        # print(type(moto_id), type(engine_envirstandard),type(engine_exhaustforfloat), type(engine_maxpower),
                        #     type(drive_type), type(gearbox_type), type(regdate),type(miles), type(serial_id), type(fullname), type(year), type(price), type(name))
                        moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
                                              drive_type, gearbox_type, regdate, serial_id, fullname, `year`, price, `name`)
                                VALUES ( '{}', '{}','{}', '{}', '{}', '{}', '{}','{}', '{}', '{}', '{}', '{}')""".format(
                    moto_id,engine_envirstandard,engine_exhaustforfloat,engine_maxpower,drive_type,gearbox_type,regdate,
                    serial_id,fullname,year,price,name)
                        try:
                            # 执行sql语句
                            cursor.execute(moto_sql)
                            db.commit()
                            print("---------------车型：%s，插入成功---------------" % fullname)
                        except Exception as e:
                            es = str(e)
                            if 'PRIMARY' in es:
                                print("---------------跳过车型：%s，该车型已存在---------------" % fullname)
                            else:
                                # 发生错误时回滚
                                print("---------------车型：%s，插入失败---------------" % fullname)
                                print(e)
                                motoid = str(moto_id)
                                error3 = str(fullname)
                                writefile(f"{motoid} {error3}\n {es}")
                                db.rollback()
                    else:
                        pass
        else:
            print(f"---------------插入品牌{len((audi_urls))}条、车系{len(set(moto_urls))}条、车型{len(set(moto_ids))}条，已完成---------------")
            pass

            # except:
            #
            #     db.rollback()
        lock.release()

def main():
    brand = Thread(target=spider_brand(h),name="品牌")
    audi = Thread(target=spider_audi(h),name="车系")
    moto1 = Thread(target=spider_moto_01(h,moto_urls,10),name="车型1")
    moto2 = Thread(target=spider_moto_02(h,moto_urls,10), name="车型2")
    moto3 = Thread(target=spider_moto_03(h,moto_urls,10), name="车型3")
    moto4 = Thread(target=spider_moto_04(h,moto_urls,10), name="车型4")
    moto5 = Thread(target=spider_moto_05(h,moto_urls,10), name="车型5")
    moto6 = Thread(target=spider_moto_06(h,moto_urls,10), name="车型6")
    moto7 = Thread(target=spider_moto_07(h,moto_urls,10), name="车型7")
    moto8 = Thread(target=spider_moto_08(h,moto_urls,10), name="车型8")
    moto9 = Thread(target=spider_moto_09(h,moto_urls,10), name="车型9")
    moto10 = Thread(target=spider_moto_10(h,moto_urls,10), name="车型10")
    error = Thread(target=spider_error(h),name="重新插入错误数据")
    brand.start()
    audi.start()
    moto1.start()
    moto2.start()
    moto3.start()
    moto4.start()
    moto5.start()
    moto6.start()
    moto7.start()
    moto8.start()
    moto9.start()
    moto10.start()
    # moto.join()
    error.start()
    db.close()
    enumerate()

if __name__ == '__main__':
    main()