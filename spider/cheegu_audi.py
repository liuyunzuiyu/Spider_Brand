#！coding: utf-8

import requests
import pymysql
import json

# import time,datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 拦截SSL警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

s = requests.session()

# 获取品牌id
# 品牌url
brank_url = "http://api.cheegu.com/CARDB.getMasterBrand"
# 车型url
moto_url = "http://api.cheegu.com/CARDB.getModel?serialId={}"
# 车系url
audi_url = "http://api.cheegu.com/CARDB.getSerial?masterid={}&full=false&hasmodel=true"
# 车系url列表
audi_urls = []
# 车型url列表
moto_urls = []
# print(brank_url)
moto_ids = []

# 请求header
h = {
    "Accept":"application/json, text/javascript, */*; q=0.01",
    "Accept-Language":"zh-CN,zh;q=0.9",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
    "Accept-Encoding":"gzip, deflate",
    "Connection":"keep-alive",
    "Referer":"http://www.cheegu.com/"
}
# 连接数据库
db = pymysql.connect("localhost", "root", "123456", "branddb", charset='utf8')

# 使用cursor()方法获取操作游标
cursor = db.cursor()


# 请求品牌接口
r_brank = s.get(brank_url,headers=h,verify=False)
brank_f = json.loads(r_brank.text)

# current_time = datetime.datetime.now().strftime('%Y-%m-%d')

for brank_i in brank_f["message"]:
    # # print(find_b)
    # # 提取品牌字段
    # brand_id = brank_i['id']
    # brand_name = brank_i['name']
    # alphabet_code = brank_i['char']
    # logo = brank_i['logo']
    # print(brand_id,brand_name,alphabet_code,logo)
    audi_urls.append(brank_i['id'])
print(audi_urls)
print(len(set(audi_urls)))
for audi_u in audi_urls:
    audi_id = audi_url.format(audi_u)
    r_audi = s.get(audi_id, headers=h, verify=False)
    audi_f = r_audi.json()
    # print(audi_f["message"])
    # print(audi_u)
    for audi_i in audi_f["message"]:
        # 提取车系字段
        # au_id = audi_i['id']
        # brand_na = audi_i['brand']
        # masterid = audi_u
        # picture = audi_i['picture']
        # series_name = audi_i['name']
        # # print(au_id, brand_na, masterid, picture, series_name)
        moto_urls.append(audi_i['id'])
print(moto_urls)
print(len(set(moto_urls)))
for moto_u in moto_urls:

    moto_id = moto_url.format(moto_u)  # 车系url
    # moto_id = moto_url.format(3891)
    r_moto = s.get(moto_id, headers=h, verify=False)
    # print(r_moto.content.decode('utf-8'))
    moto_f = r_moto.json()
    # moto_f = r_moto.json()
    #             # print(moto_f["message"])
    #     print(moto_u)
    for moto_i in moto_f["message"]:
        # print(i["fullname"])
        # moto_id = moto_i["id"]
        # engine_envirstandard = moto_i["Engine_EnvirStandard"] if moto_i["Engine_EnvirStandard"] else 'null'
        # engine_exhaustforfloat = moto_i["Engine_ExhaustForFloat"] if moto_i["Engine_ExhaustForFloat"] else 'null'
        # engine_maxpower = moto_i["Engine_MaxPower"] if moto_i["Engine_MaxPower"] else 'null'
        # drive_type = moto_i["drive_type"] if moto_i["drive_type"] else 'null'
        # gearbox_type = moto_i["gearbox_type"] if moto_i["gearbox_type"] else 'null'
        # regdate = moto_i["regdate"] if moto_i["regdate"] else 'null'
        # miles = moto_i["miles"] if moto_i["miles"] else 'null'
        # # serial_id = moto_u
        # serial_id = 3891
        fullname = moto_i["fullname"] if moto_i["fullname"] else 'null'
        # year = moto_i["y"]  if moto_i["y"] else 'null'
        # price = moto_i["v"] if moto_i["v"] else 'null'
        # name = moto_i["name"] if moto_i["name"] else 'null'
        # print(moto_id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower, drive_type, gearbox_type,
        #       regdate, miles, serial_id, fullname, year, price, name)
        moto_ids.append(moto_i["id"])
        # moto_sql = """INSERT INTO car_type_info(id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower,
        #                                   drive_type, gearbox_type, regdate, miles, serial_id, fullname, `year`, price, `name`)
        #                     VALUES ( %d,'%s','%s',%s,'%s','%s','%s',%f,%d,'%s',%d,'%s','%s')""" % ( moto_id, engine_envirstandard,
        #                                 engine_exhaustforfloat, engine_maxpower, drive_type, gearbox_type, regdate,
        #                                 miles, serial_id, fullname, year, price, name)
        # try:
        #     # 执行sql语句
        #     cursor.execute(moto_sql)
        #     db.commit()
        #     print("---------------车型：%s，插入成功---------------" % fullname)
        # except:
        #     # 发生错误时回滚
        #     print(moto_id, engine_envirstandard, engine_exhaustforfloat, engine_maxpower, drive_type, gearbox_type,
        #           regdate, miles, serial_id, fullname, year, price, name)
        #     print("---------------车型：%s，插入失败---------------" % fullname)
        #     db.rollback()
print(moto_ids)
print(len(set(moto_ids)))
# db.close()