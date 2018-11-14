import csv
import json
import hashlib
import os
import xml.etree.ElementTree as ET
import requests
import datetime, time
import re
import collections
import urllib.parse
import math
import copy
import calendar
from collections import defaultdict

from bs4 import BeautifulSoup
from sqlalchemy import func, or_, and_, extract, not_, case, desc
import requests

import dal.models as models
import dal.models_statistic as models_statistic
import dal.models_account_periods_statistic as models_account_periods_statistic
from dal.customer.model_group import CustomerGroupORM
from dal.db_configs import redis, redis_config, auth_redis, redis_export
from dal.dot_array import chinese_array
from dal.holiday_dict import holiday_tag
from handlers.base.pub_log import log_msg, log_msg_dict
from libs.lcsw import LcswPay
from settings import LCSW_HANDLE_HOST, ROOT_HOST_NAME, AUTH_HOST_NAME, AUTH_API_SECRETKEY, EXPORT_PATH
from service.goods.batch import GoodsBatchService
from handlers.celery_tplmsg_task import send, CustomerFlow
from dal.courier.accesser import CourierRecordAccesser


# 时间
class TimeFunc():
    @classmethod
    def get_date_split(cls, choose_datetime):
        t_year = choose_datetime.year
        t_month = choose_datetime.month
        t_day = choose_datetime.day
        t_week = int(choose_datetime.strftime('%W'))
        return t_year, t_month, t_day, t_week

    @classmethod
    def get_date(cls, choose_datetime):
        return choose_datetime.strftime('%Y%m%d')

    @classmethod
    def get_time(cls, choose_datetime):
        return choose_datetime.strftime('%H:%M:%S')

    # 将时间类型换为时间字符串
    @classmethod
    def time_to_str(self, time, _type="all"):
        if _type == "all":
            fromat = "%Y-%m-%d %H:%M:%S"
        elif _type == "date":
            fromat = "%Y-%m-%d"
        elif _type == "hour":
            fromat = "%H:%M"
        elif _type == "month":
            fromat = "%m-%d"
        elif _type == "year":
            fromat = "%Y-%m"
        elif _type == "full":
            fromat = "%Y%m%d%H%M"
        elif _type == "no_year":
            fromat = "%m-%d %H:%M:%S"
        elif _type == "time":
            fromat = "%H:%M:%S"
        else:
            fromat = "%Y-%m-%d %H:%M"
        try:
            time_res = time.strftime(fromat)
        except:
            time_res = ""
        return time_res

    # 根据参数时间所在周的开始时间
    def getweekfirstday(current_date):
        yearnum = current_date.year
        weeknum = int(current_date.strftime("%W"))
        daynum = int(current_date.weekday()) + 1

        yearstart = datetime.datetime(yearnum, 1, 1)
        yearstartweekday = int(yearstart.weekday()) + 1
        if yearstartweekday < int(daynum):
            daydelat = (7 - int(yearstartweekday)) + (int(weeknum)) * 7
        else:
            daydelat = (7 - int(yearstartweekday)) + (int(weeknum) - 1) * 7
        a = yearstart + datetime.timedelta(days=daydelat + 1)
        return a

    # 获取fake清算状态
    # 已清算状态不用重新更新，待清算和清算中的状态，把对应日期传进来，然后检查对应日期和今天截头不去尾,如果没有工作日,那就仍然是待清算状态，如果有一个工作日，就是清算中状态，如果有两个，就是已清算了。
    @classmethod
    def get_fake_clearing_state(cls, the_date):
        delta_day = (
            datetime.datetime.combine(datetime.datetime.now(),
                                      datetime.time.min) -
            datetime.datetime.combine(the_date, datetime.time.min)).days
        fake_clearing_state = 0
        for i in range(delta_day):
            temp_day = (
                the_date + datetime.timedelta(i + 1)).strftime('%Y-%m-%d')
            if holiday_tag[temp_day] == 0:
                fake_clearing_state += 1
                if fake_clearing_state == 2:
                    break
        return fake_clearing_state

    # 获取当天的开始时间
    @classmethod
    def get_day_start_time(cls):
        now_time = datetime.datetime.now()
        start_time = datetime.datetime(now_time.year, now_time.month,
                                       now_time.day)
        return start_time

    # 获取XX天的日期
    @classmethod
    def get_date_list(cls, date_range=7, date_type="day"):
        date_now = datetime.datetime.now()
        date_list = []
        current_month_first_day = 0
        for i in range(date_range):
            if date_type == "day":
                date = date_now - datetime.timedelta(days=i)
                date = date.date()
            else:
                date = cls.get_date_month(date_now, i)
            date_list.append(date)
        date_list.reverse()
        return date_list

    # 获取指定日期的后xx个月
    @classmethod
    def get_later_month(cls, date, month_num):
        year = date.year
        month = date.month + month_num
        if month > 12:
            month -= 12
            year += 1
        date = datetime.datetime(year, month, date.day)
        return date

    # 获取指定日期的后xx个月
    @classmethod
    def get_later_month_date(cls, date, month_num):
        year = date.year
        month = date.month + month_num
        if month > 12:
            month -= 12
            year += 1
        date = datetime.date(year, month, date.day)
        return date

    # 获取XX月的月份
    @classmethod
    def get_date_month(cls, date, range_num):
        year = date.year
        month = date.month
        if month - range_num <= 0:
            res_month = 12 + (month - range_num)
            res_year = year - 1
        else:
            res_month = month - range_num
            res_year = year
        date = "%d-%d" % (res_year, res_month)
        return date

    # 获取xx天的日期
    @classmethod
    def get_assign_date(cls, days=0):
        now_date = datetime.datetime.now()
        assign_date = datetime.datetime(
            now_date.year, now_date.month,
            now_date.day) - datetime.timedelta(days=days)
        return assign_date

    # 拼接时间字符串
    @classmethod
    def splice_time(cls, year, month, day, time):
        time = "{year}-{month}-{day} {time}".format(
            year=year,
            month=month,
            day=day,
            time=cls.time_to_str(time, "time"))
        return time

    # 获取今天的date类型
    @classmethod
    def get_today_date(cls):
        return datetime.date.today()

    # 获取今天的datetime类型
    @classmethod
    def get_today_datetime(cls):
        return datetime.datetime.combine(datetime.datetime.now(),
                                         datetime.time.min)

    @classmethod
    def transfer_input_range_to_date(cls, from_date, to_date):
        """ 对于传了`from_date`和`to_date`的情况，对参数进行校验，
        并将传入的string型参数转换成date型

        :rtype: tuple
        """
        try:
            if from_date:
                from_date = datetime.datetime.strptime(from_date,
                                                       '%Y-%m-%d').date()
            if to_date:
                to_date = datetime.datetime.strptime(to_date, '%Y-%m-%d')
                # to_date 加上一天的偏移量，即支持当天到当天的形式
                to_date = (to_date + datetime.timedelta(days=1)).date()
        except BaseException:
            raise ValueError("日期传入参数格式错误，应为`yyyy-mm-dd`")
        if from_date and to_date and from_date > to_date:
            raise ValueError("参数错误，`from_date`应该小于`to_date`")
        return from_date, to_date

    @classmethod
    def transfer_input_range_to_date_or_default_range(cls, from_date, to_date):
        """ 对于传了`from_date`和`to_date`的情况，对参数进行校验，
        并将传入的string型参数转换成date型
        如果没有传时间参数，默认区间为今天0点到今天24点

        :rtype: tuple
        """
        from_date, to_date = cls.transfer_input_range_to_date(
            from_date, to_date)
        if not from_date:
            from_date = datetime.date.today() if not from_date else from_date
        # 查询时，如果不加to_date, 查询速度应该是最快的，默认就是查询包括今天的数据，所以to_date默认为null最佳
        return from_date, to_date

    @classmethod
    def transfer_input_range_to_datetime(cls, from_date, to_date):
        """ 对于传了`from_date`和`to_date`的情况，对参数进行校验，
        并将传入的string型参数转换成datetime型

        :rtype: tuple
        """
        try:
            if to_date:
                to_date = datetime.datetime.strptime(to_date, '%Y-%m-%d')
                # to_date 加上一天的偏移量，即支持当天到当天的形式
                to_date = (to_date + datetime.timedelta(days=1))
            if from_date:
                from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
                if not to_date:
                    to_date = from_date + datetime.timedelta(days=1)
        except BaseException:
            raise ValueError("日期传入参数格式错误，应为`yyyy-mm-dd`")
        if from_date and to_date and from_date > to_date:
            raise ValueError("参数错误，`from_date`应该小于`to_date`")
        return from_date, to_date

    @classmethod
    def transfer_range_normal_to_accounting(cls, session, shop_id, from_date,
                                            to_date):
        """ 将自然日的时间区间转换成扎帐日的时间区间

        :param from_date: `class datetime.date`
        :param to_date: `class datetime.date`
        """
        choose_date_start = AccountingPeriodFunc.get_accounting_time(
            session, shop_id, from_date - datetime.timedelta(1))
        choose_date_end = AccountingPeriodFunc.get_accounting_time(
            session, shop_id, to_date)
        if not choose_date_start:
            choose_date_start = datetime.datetime(
                from_date.year, from_date.month, from_date.day, 0, 0, 0)
        if not choose_date_end:
            choose_date_end = datetime.datetime(to_date.year, to_date.month,
                                                to_date.day, 23, 59, 59)
        return choose_date_start, choose_date_end

    @classmethod
    def check_input_date(cls, date, statistic_type, return_type="datetime"):
        """ 需要传入两个参数的统计参数校验, 日统计传日期，月统计传月份和年份, 年统计仅传年份

        :param date: string, 字符串型的日期
        :param statistic_type: 统计类型 1:日统计, 3:月统计, 4: 年统计

        :rtype: obj `class datetime.datetime`
        """
        if statistic_type not in [1, 3, 4]:
            raise ValueError("统计类型不受支持")
        try:
            if statistic_type == 1:
                result = datetime.datetime.strptime(date, "%Y-%m-%d")
            elif statistic_type == 3:
                result = datetime.datetime.strptime(date, "%Y-%m")
            else:
                result = datetime.datetime.strptime(date, "%Y")
            if return_type == "date":
                result = result.date()
        except ValueError:
            raise ValueError("日期格式传入错误，请检查")
        return result

    @classmethod
    def get_to_date_by_from_date(cls, from_date, to_date, statistic_type):
        """根据选择的日期，月份，年份获取起止区间
        """
        if statistic_type not in [1, 3, 4]:
            raise ValueError("统计类型不受支持")
        try:
            if statistic_type == 1:
                from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
                if to_date:
                    to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d") \
                              + datetime.timedelta(days=1)
                else:
                    to_date = from_date + datetime.timedelta(days=1)
            elif statistic_type == 3:
                from_date = datetime.datetime.strptime(from_date, "%Y-%m")
                to_date = from_date
                if to_date.month + 1 > 12:
                    month = 1
                    year = to_date.year + 1
                else:
                    year = to_date.year
                    month = to_date.month + 1
                to_date = datetime.datetime(year=year, month=month, day=1)
            elif statistic_type == 4:
                from_date = datetime.datetime.strptime(from_date, "%Y")
                to_date = from_date
                to_date = datetime.datetime(
                    year=to_date.year + 1, month=1, day=1)
        except ValueError:
            raise ValueError("日期格式传入错误，请检查")
        return from_date, to_date

    @classmethod
    def list_date_range(cls, statistic_type, **kwargs):
        """ 获取某天/某月/某周的时间区间, 其中月/周的数据都需要年+月/周的值,
        返回值的结束日期都是理论区间减1,
        例如, 2018-07-01的返回值为(2018-07-01, 2018-07-01)
        如此设计为为了展示和传参的时候支持当天-当天的形式, 实际使用结束日期需要加一天

        :param statistic_type: 统计类型，1:日，2:周，3:月
        :param date: `datetime.date` 某天
        :param year: int 某年
        :param month: int 某月
        :param week: int 某周

        :rtype: tuple
        """
        if statistic_type == 1:
            day = kwargs.get("date")
            return day.strftime("%Y-%m-%d"), day.strftime("%Y-%m-%d")
        elif statistic_type == 2:
            year = kwargs.get("year")
            week = kwargs.get("week")
            # 组装1月4日的日期
            day_jan_4th = datetime.date(year, 1, 4)
            # 今年第一个日历星期的开始日期
            first_week_start = day_jan_4th - datetime.timedelta(
                days=day_jan_4th.isoweekday() - 1)
            # 所求星期的开始时间
            week_start = datetime.datetime.combine(
                first_week_start + datetime.timedelta(weeks=week - 1),
                datetime.time(),
            )
            # 减一天支持当天到当天
            week_end = (week_start + datetime.timedelta(weeks=1) -
                        datetime.timedelta(days=1))
            return week_start.strftime("%Y-%m-%d"), week_end.strftime(
                "%Y-%m-%d")
        elif statistic_type == 3:
            year = kwargs.get("year")
            month = kwargs.get("month")
            month_start = datetime.datetime(year, month, 1)
            if month == 12:
                month_end = datetime.datetime(year + 1, 1, 1)
            else:
                month_end = datetime.datetime(year, month + 1, 1)
            # 支持一波当天到当天
            month_end = month_end - datetime.timedelta(days=1)
            return month_start.strftime("%Y-%m-%d"), month_end.strftime(
                "%Y-%m-%d")
        elif statistic_type == 4:
            year = kwargs.get("year")
            year_start = datetime.datetime(year, 1, 1)
            year_end = datetime.datetime(year, 12, 31)
            return year_start.strftime("%Y-%m-%d"), year_end.strftime(
                "%Y-%m-%d")
        else:
            raise ValueError("unsupported datetime")


# 字符
class CharFunc():
    # 检查是否包含汉字
    @classmethod
    def check_contain_chinese(cls, check_str):
        for ch in check_str:
            if '\u4e00' <= ch <= '\u9fff':
                return True
        return False

    # 检查汉字字符个数
    @classmethod
    def get_contain_chinese_number(cls, check_str):
        number = 0
        for ch in check_str:
            if '\u4e00' <= ch <= '\u9fff' or ch == "：":
                number += 1
        return number


# 数字
class NumFunc():
    # 处理金额小数位数（1.00处理为1; 1.10处理为1.1; 1.111处理为1.11; 非数字返回0）
    @classmethod
    def check_float(cls, number, place=2):
        try:
            num = round(float(number), place)
        except:
            num = 0
        if num == int(num):
            num = int(num)
        return num

    # 将数字处理为整数（非数字返回0）
    @classmethod
    def check_int(cls, number):
        try:
            num = int(number)
        except:
            num = 0
        return num

    # 将阿拉伯数字转为中文大写
    @classmethod
    def upcase_number(cls, n):
        units = ['', '万', '亿']
        nums = ['零', '壹', '贰', '叁', '肆', '伍', '陆', '柒', '捌', '玖']
        decimal_label = ['角', '分']
        small_int_label = ['', '拾', '佰', '仟']
        int_part, decimal_part = str(int(n)), str(
            cls.check_float(n - int(n)))[2:]  # 分离整数和小数部分
        res = []
        if decimal_part:
            decimal_tmp = ''.join([
                nums[int(x)] + y
                for x, y in list(zip(decimal_part, decimal_label))
            ])
            decimal_tmp = decimal_tmp.replace('零角', '零').replace('零分', '')
            res.append(decimal_tmp)

        if not decimal_part:
            res.append('整')

        if int_part != '0':
            res.append('圆')
            while int_part:
                small_int_part, int_part = int_part[-4:], int_part[:-4]
                tmp = ''.join([
                    nums[int(x)] + (y if x != '0' else '') for x, y in list(
                        zip(small_int_part[::-1], small_int_label))[::-1]
                ])
                tmp = tmp.rstrip('零').replace('零零零', '零').replace('零零', '零')
                unit = units.pop(0)
                if tmp:
                    tmp += unit
                    res.append(tmp)
        if int_part == '0' and not decimal_part:
            res.append('零圆')
        return ''.join(res[::-1])

    # 按精度设置规则对金额进行处理
    # precision:精度设置 1:到分 2:到角 3:到元
    # precision_type:精度方式 1:四舍五入 2:抹掉尾数 3:进一法
    @classmethod
    def handle_precision(cls, in_money, precision, precision_type):
        in_money_cent = int(round(in_money * 100))
        if precision == 1:
            return in_money_cent
        else:
            # 整数部分
            int_part = cls.check_int(str(in_money_cent)[:-2])
            # 十分位
            tenths_digit_part = cls.check_int(str(in_money_cent)[-2:-1])
            # 百分位
            percentile_part = cls.check_int(str(in_money_cent)[-1:])
            if precision == 2:
                # (四舍五入并且百分位大于等于5)或者(进一法并且百分位大于等于1)
                if (precision_type == 1 and percentile_part >= 5) or (
                        precision_type == 3 and percentile_part >= 1):
                    return int_part * 100 + (tenths_digit_part + 1) * 10
                else:
                    return int_part * 100 + (tenths_digit_part) * 10
            else:
                # (四舍五入并且十分位大于等于5)或者(进一法并且十分位大于等于1)
                if (precision_type == 1 and tenths_digit_part >= 5) or (
                        precision_type == 3 and tenths_digit_part >= 1):
                    return (int_part + 1) * 100
                else:
                    return int_part * 100

    @classmethod
    def dynamic_add(cls, a, b, action="add"):
        if action == "add":
            return a + b
        else:
            return a - b


check_float = NumFunc.check_float
check_int = NumFunc.check_int


# 转换数据格式
class DataFormatFunc():
    @classmethod
    def format_str_to_int_inlist(cls, data_list):
        res_list = []
        for data in data_list:
            try:
                data = int(data)
            except:
                continue
            res_list.append(data)
        return res_list

    @classmethod
    def format_int_to_str_inlist(cls, data_list):
        res_list = []
        for data in data_list:
            try:
                data = int(data)
            except:
                continue
            data = str(data)
            res_list.append(data)
        return res_list

    @classmethod
    def split_str(cls, string, symbol=","):
        return string.split(symbol)

    @classmethod
    def join_str(cls, string, symbol=","):
        return symbol.join(string)


# 特殊字符
class Emoji():
    @classmethod
    def filter_emoji(cls, keyword):
        keyword = re.compile(u'[\U00010000-\U0010ffff]').sub(u'', keyword)
        return keyword

    @classmethod
    def check_emoji(cls, keyword):
        reg_emoji = re.compile(u'[\U00010000-\U0010ffff]')
        has_emoji = re.search(reg_emoji, keyword)
        if has_emoji:
            return True
        else:
            return False


class GraphFunc():
    @classmethod
    def convert(cls, data_list, key):
        """ 根据统计数据获取绘制折线图的数据, 为美观暂控制最多12条

        :param data_list: 代转换的数据列表
        :param key: 键名

        :rtype: list
        """
        result = []
        for data in data_list:
            result.insert(0, data[key])
            if len(result) >= 12:
                break
        return result


# 省份城市转换
class ProvinceCityFunc():
    @classmethod
    def city_to_province(cls, code):
        from dal.dis_dict import dis_dict
        province_code = int(code / 10000) * 10000
        if dis_dict.get(province_code, None):
            return province_code
        else:
            return None

    @classmethod
    def county_to_province(cls, code):
        from dal.dis_dict import dis_dict
        province_code = int(code / 10000) * 10000
        if dis_dict.get(province_code, None):
            return province_code
        else:
            return None

    @classmethod
    def get_city(cls, code):
        from dal.dis_dict import dis_dict
        try:
            if "city" in dis_dict[int(code / 10000) * 10000].keys():
                text = dis_dict[int(code / 10000) *
                                10000]["city"][code]["name"]
            else:
                text = ""
        except:
            text = ""
        return text

    @classmethod
    def get_province(cls, code):
        from dal.dis_dict import dis_dict
        try:
            text = dis_dict.get(int(code), {}).get("name", '')
        except:
            text = ""
        return text


# 店铺
class ShopFunc():
    @classmethod
    def get_lcpay_account(cls, session, shop_id):
        lcpay_account_key = "lcpay:%s" % str(shop_id)
        if redis.get(lcpay_account_key):
            lcpay_account_key = redis.get(lcpay_account_key).decode('utf-8')
            lc_merchant_no, lc_terminal_id, lc_access_token = lcpay_account_key.split(
                "|")
        else:
            lc_merchant_no, lc_terminal_id, lc_access_token = "", "", ""
            shop = session.query(models.Shop).filter_by(id=shop_id).first()
            if shop:
                lc_merchant_no = shop.lc_merchant_no
                lc_terminal_id = shop.lc_terminal_id
                lc_access_token = shop.lc_access_token
                redis.set(
                    lcpay_account_key, lc_merchant_no + "|" + lc_terminal_id +
                    "|" + lc_access_token, 24 * 3600)
        return lc_merchant_no, lc_terminal_id, lc_access_token

    # 店铺设置
    @classmethod
    def get_config(cls, session, shop_id):
        config = session.query(models.Config).filter_by(id=shop_id).first()
        return config

    # 付费信息
    @classmethod
    def get_paid_info(cls, session, shop_id):
        info = {}
        info["multi_weigh_paid"] = FunctionPaidFunc.get_service_paid(
            session, shop_id, service_type=1)
        info["multi_sale_goods_paid"] = FunctionPaidFunc.get_service_paid(
            session, shop_id, service_type=2)
        info["accounting_periods_paid"] = FunctionPaidFunc.get_service_paid(
            session, shop_id, service_type=5)
        info["courier"] = FunctionPaidFunc.get_service_paid(
            session, shop_id, service_type=6)
        return info

    # 小票设置
    @classmethod
    def get_receipt_config(cls, session, shop_id):
        receipt_config = session.query(
            models.ReceiptConfig).filter_by(id=shop_id).first()
        return receipt_config

    # 生成店铺号（9位随机数字+字母组合）
    @classmethod
    def make_shop_code(cls, session):
        from random import Random
        chars = '0123456789abcdefghijkl0123456789mnopqrstuvwxyz'
        str = ''
        random = Random()
        while 1:
            for i in range(9):
                str += chars[random.randint(0, len(chars) - 1)]
            shop = session.query(
                models.Shop.id).filter_by(shop_code=str).scalar()
            if not shop:
                break
        return str

    # 收银台是否需要打印小票
    @classmethod
    def check_cashier_need_print(self, session, shop_id, receipt_config=None):
        need_print = False
        if not receipt_config:
            receipt_config = ShopFunc.get_receipt_config(session, shop_id)
        if receipt_config.cashier_accountant_receipt or receipt_config.cashier_goods_receipt:
            need_print = True
        return need_print

    # 开票助手是否需要打印小票
    @classmethod
    def check_salesman_need_print(self, session, shop_id, receipt_config=None):
        need_print = False
        if not receipt_config:
            receipt_config = ShopFunc.get_receipt_config(session, shop_id)
        if receipt_config.salesman_async_customer_receipt or receipt_config.salesman_async_accountant_receipt or receipt_config.salesman_async_goods_receipt:
            need_print = True
        return need_print

    # 获取店铺信息
    @classmethod
    def get_shop(cls, session, shop_id):
        shop = session.query(models.Shop).filter_by(id=shop_id).first()
        return shop

    # 获取店铺信息
    @classmethod
    def get_shop_name_through_id(cls, session, shop_id):
        shop_name = session.query(
            models.Shop.shop_name).filter_by(id=shop_id).scalar() or ""
        return shop_name

    @classmethod
    def get_shop_name_dict(cls, session, shop_id_list):
        """ 批量获取店铺名
        """
        Shop = models.Shop

        shop_name_list = session.query(Shop.id, Shop.shop_name) \
            .filter(Shop.id.in_(shop_id_list)) \
            .all()
        name_dict = {x.id: x.shop_name for x in shop_name_list}
        return name_dict

    @classmethod
    def get_shop_name_dict_through_bossid(cls, session, boss_id):
        """ 批量获取店铺名
        """
        Shop = models.Shop

        shop_name_list = session.query(Shop.id, Shop.shop_name) \
            .filter_by(boss_id=boss_id, status=1) \
            .all()
        name_dict = {x.id: x.shop_name for x in shop_name_list}
        return name_dict

    @classmethod
    def list_goods_under_shop(cls, session, shop_id):
        """ 店铺内所有上架商品id """
        goods = session.query(models.Goods.id).filter_by(
            shop_id=shop_id, active=1).all()
        result = [g.id for g in goods]
        return result

    @classmethod
    def get_supplier_ids_in_shop(cls, session, shop_id):
        """ 店鋪內所有供应商id """
        suppliers = session.query(models.ShopSupplier).filter_by(
            shop_id=shop_id, status=1).all()
        supplier_id_list = [supplier.id for supplier in suppliers]
        return supplier_id_list

    @classmethod
    def is_admin_have_many_shop(cls, session, shop_id):
        """该店的超管是否拥有多个店铺, 历史店铺也有效"""
        shops_count = 0
        result = session.query(
            models.Shop.boss_id).filter_by(id=shop_id).first()
        if result:
            shops_count = session.query(
                models.Shop).filter_by(boss_id=result.boss_id).count()
        return shops_count >= 2

    @classmethod
    def is_max_staff(cls, session, shop_id, config=None):
        """判断店铺员工是否达到上限(salesman, accountant, admin)"""
        is_max = []
        HireLink = models.HireLink
        if not config:
            Config = models.Config
            result = session.query(Config.max_accountant_count, Config.max_admin_count, Config.max_salesman_count) \
                .filter_by(id=shop_id).first()
            if result:
                max_accountant_count, max_admin_count, max_salesman_count = result[
                    0], result[1], result[2]
            else:
                max_accountant_count, max_admin_count, max_salesman_count = -1, -1, -1
        else:
            max_accountant_count, max_admin_count, max_salesman_count = \
                config.max_accountant_count, config.max_admin_count, config.max_salesman_count
        admin_count = session.query(HireLink.id) \
            .filter(HireLink.active_admin == 1, HireLink.shop_id == shop_id).count()
        salesman_count = session.query(HireLink.id) \
            .filter(HireLink.active_salesman == 1, HireLink.shop_id == shop_id).count()
        accountant_count = session.query(HireLink.id) \
            .filter(HireLink.active_accountant == 1, HireLink.shop_id == shop_id).count()
        if max_salesman_count >= 0 and salesman_count >= max_salesman_count:
            is_max.append(1)
        else:
            is_max.append(0)

        if max_accountant_count >= 0 and accountant_count >= max_accountant_count:
            is_max.append(1)
        else:
            is_max.append(0)

        if max_admin_count >= 0 and admin_count >= max_admin_count:
            is_max.append(1)
        else:
            is_max.append(0)
        return is_max


# 统计数据获取
class StatisticFunc():
    @classmethod
    def get_static_query_base(cls,
                              session,
                              sTable,
                              statistic_type,
                              extra_groups=None):
        """ 统计表中各项数据在取日/周/月数据的时候的基本取法

        :param sTable: obj, 统计表的类名
        :param statistic_type: 统计类型, 1:日统计 2:周统计 3:月统计 4:年统计
        :param extra_group: list, 附加的分组条件

        :rtype: obj, `class sa_Query`
        """
        q_param = cls.get_query_param(sTable)
        g_param = cls.get_group_by_param(sTable, statistic_type)
        if extra_groups:
            for extra_group in extra_groups:
                g_param.append(extra_group)
        params = q_param + g_param
        result = session.query(*params).group_by(*g_param)
        return result

    @classmethod
    def get_static_sum_base(cls, session, sTable):
        """ 统计表中各项数据在取日/周/月数据的时候的汇总基本查询

        :param sTable: obj, 统计表的类名

        :rtype: obj, `class sa_Query`
        """
        q_param = cls.get_query_param(sTable)
        result = session.query(*q_param)
        return result

    @classmethod
    def get_group_by_param(cls, sTable, statistic_type):
        """ 4种类型的统计的分组标准 """
        if statistic_type == 1:
            return [sTable.statistic_date]
        elif statistic_type == 2:
            return [sTable.year, sTable.week]
        elif statistic_type == 3:
            return [sTable.year, sTable.month]
        elif statistic_type == 4:
            return [sTable.year]
        else:
            return []
        raise ValueError("未知统计类型")

    @classmethod
    def get_query_param(cls, sTable):
        """ 返回需要查询的数据

        :rtype: list
        """
        sum_param = [
            func.sum(getattr(sTable, column)).label(column)
            for column in sTable.get_sum_column()
        ]
        fix_param = [
            getattr(sTable, column).label(column)
            for column in sTable.get_fix_column()
        ]
        return sum_param + fix_param

    @classmethod
    def get_filter_type(cls, statistic_type):
        """ 统计过程中的统计类型过滤条件 """
        # 日统计和周统计用的是日统计的数据
        if statistic_type in [1, 2]:
            return 1
        # 月统计和年统计用的年统计的数据
        elif statistic_type in [3, 4]:
            return 3
        raise ValueError("未知统计类型")

    @classmethod
    def get_filter_date(cls, statistic_type, date):
        """ 统计过程中的日期过滤条件

        :rtype: dict
        """
        if statistic_type == 1:
            return {"statistic_date": date}
        # 周统计这里瞎写的
        elif statistic_type == 2:
            return {"year": date.year, "week": date.week}
        elif statistic_type == 3:
            return {"year": date.year, "month": date.month}
        elif statistic_type == 4:
            return {"year": date.year}
        raise ValueError("未知统计类型")

    @classmethod
    def get_basic_order_by_param(cls, sTable, statistic_type):
        """ 统计类型不同基本的倒序排序条件也不同, 基本排序条件就是分组条件倒序 """
        g_param = cls.get_group_by_param(sTable, statistic_type)
        result = []
        for g in g_param:
            result.append(g.desc())
        return result

    @classmethod
    def format_date(cls, r_obj, statistic_type):
        """ 根据统计类型的不同，第一列的日期时间的格式化方式不同

        :param r_obj: obj, `class sa_Result`
        """
        if statistic_type == 1:
            return r_obj.statistic_date.strftime("%Y-%m-%d")
        elif statistic_type == 2:
            return str(r_obj.year) + "年-" + str(r_obj.week) + "周"
        elif statistic_type == 3:
            return str(r_obj.year) + "-" + str(r_obj.month)
        elif statistic_type == 4:
            return str(r_obj.year) + "年"
        raise ValueError("未知统计类型")

    @classmethod
    def is_after_accounting(cls, r_obj, statistic_type, today):
        """ 判断一条统计数据是否是扎帐以后的数据 """
        if statistic_type not in (1, 2, 3, 4):
            raise ValueError("未知统计类型")
        if statistic_type == 2:
            _, this_week, _ = today.isocalendar()

        is_after_accounting = 0
        if statistic_type == 1 and r_obj.statistic_date > today.date():
            is_after_accounting = 1
        elif statistic_type == 2 and (r_obj.year > today.year
                                      or r_obj.year == today.year
                                      and r_obj.week > this_week):
            is_after_accounting = 1
        elif statistic_type == 3 and (r_obj.year > today.year
                                      or r_obj.year == today.year
                                      and r_obj.month > today.month):
            is_after_accounting = 1
        elif statistic_type == 4 and r_obj.year > today.year:
            is_after_accounting = 1
        return is_after_accounting

    @classmethod
    def sum_data(cls, data_list, key_list):
        """ 将列表数据中的特定内容进行求和操作

        :param data_list: 待求和的数据列表
        :param key_list: 数据列表中字典对应的键
        """
        result = {}
        for key in key_list:
            result[key] = 0
        for data in data_list:
            for key in key_list:
                try:
                    result[key] = check_float(result.get(key, 0) + data[key])
                # 例如重量，如果是"-"就不能直接加
                except TypeError:
                    pass
        return result

    # 统计的时候，根据类型(按天：1，按周：2，按月：3)，得到统计应该展示的记录数量
    @classmethod
    def get_static_range(cls, type, current_year, current_month):
        now = datetime.datetime.now()
        # 数组的大小
        if type == 1 and int(current_year) == now.year and int(
                current_month) == now.month:
            rangeOfArray = now.day
        elif type == 1:
            if current_month in ('01', '03', '05', '07', '08', '10', '12'):
                rangeOfArray = 31
            elif current_month in ('04', '06', '09', '11'):
                rangeOfArray = 30
            elif (int(current_year) % 4 == 0
                  and not int(current_year) % 100 == 0
                  ) or int(current_year) % 400 == 0:
                rangeOfArray = 29
            else:
                rangeOfArray = 28
        elif (type == 2 or type == 3) and int(current_year) == now.year:
            if type == 2:
                rangeOfArray = int(now.strftime('%W'))
            else:
                rangeOfArray = now.month
        elif type == 2:
            rangeOfArray = 53
        else:
            rangeOfArray = 12
        return rangeOfArray

    @classmethod
    def update_statistic_data(self,
                              session,
                              statistic_session,
                              shop_id,
                              if_force_update=False):
        key_update_statistic = "statistic:%d" % (int(shop_id))
        # 如果已经在进行更新中，就不进行更新，方法防抖
        if not redis.setnx(key_update_statistic, '1'):
            return
        # 设置过期时间30s,方法节流
        redis.expire(key_update_statistic, 30)
        # 强制更新时更新当天所有数据
        if if_force_update:
            last_update_timestamp = 0
            new_timestamp = 0
        else:
            chooseday = datetime.datetime.now().strftime('%Y-%m-%d')
            last_update_timestamp = statistic_session.query(models_statistic.StatisticDataDaily.last_update_timestamp) \
                .filter_by(shop_id=shop_id, statistic_date=chooseday, statistic_type=1).scalar()
            if not last_update_timestamp:
                last_update_timestamp = 0
            new_timestamp = int(time.time())
        # 根据各基本表来进行统计指标的更新
        import handlers.base.pub_statistic as reslove_statistic

        reslove_statistic.order(session, statistic_session, 0, shop_id,
                                last_update_timestamp, new_timestamp)
        reslove_statistic.refund_order(session, statistic_session, 0, shop_id,
                                       last_update_timestamp, new_timestamp)
        reslove_statistic.goods_sale_record(session, statistic_session, 0,
                                            shop_id, last_update_timestamp,
                                            new_timestamp)
        reslove_statistic.pb_order(session, statistic_session, 0, shop_id,
                                   last_update_timestamp, new_timestamp)
        reslove_statistic.deposit_refund_order(session, statistic_session, 0,
                                               shop_id, last_update_timestamp,
                                               new_timestamp)
        reslove_statistic.fund_check(session, statistic_session, 0, shop_id,
                                     last_update_timestamp, new_timestamp)
        reslove_statistic.payout_order(session, statistic_session, 0, shop_id,
                                       last_update_timestamp, new_timestamp)
        reslove_statistic.salary_payout_order(session, statistic_session, 0,
                                              shop_id, last_update_timestamp,
                                              new_timestamp)
        reslove_statistic.other_income_order(session, statistic_session, 0,
                                             shop_id, last_update_timestamp,
                                             new_timestamp)
        reslove_statistic.waste_selling_income_order(
            session, statistic_session, 0, shop_id, last_update_timestamp,
            new_timestamp)
        reslove_statistic.shop_supplier_borrowing(
            session, statistic_session, 0, shop_id, last_update_timestamp,
            new_timestamp)
        reslove_statistic.payout_supplier_clearing(
            session, statistic_session, 0, shop_id, last_update_timestamp,
            new_timestamp)
        reslove_statistic.payout_supplier_borrowing(
            session, statistic_session, 0, shop_id, last_update_timestamp,
            new_timestamp)
        reslove_statistic.order_fee(session, statistic_session, 0, shop_id,
                                    last_update_timestamp, new_timestamp)
        reslove_statistic.update_goods_stockin(session, statistic_session, 0,
                                               shop_id, last_update_timestamp,
                                               new_timestamp)
        reslove_statistic.goods_stockin_sales(session, statistic_session, 0,
                                              shop_id, last_update_timestamp,
                                              new_timestamp)
        reslove_statistic.goods_storage(session, statistic_session, 0, shop_id,
                                        last_update_timestamp, new_timestamp)
        reslove_statistic.supplier_money_agent(session, statistic_session, 0,
                                               shop_id, last_update_timestamp,
                                               new_timestamp)
        reslove_statistic.supplier_money_self(session, statistic_session, 0,
                                              shop_id, last_update_timestamp,
                                              new_timestamp)
        reslove_statistic.supplier_money_borrowing(
            session, statistic_session, 0, shop_id, last_update_timestamp,
            new_timestamp)

        redis.delete(key_update_statistic)

    @classmethod
    def update_accounting_periods_statistic_data(self,
                                                 session,
                                                 statistic_session,
                                                 shop_id,
                                                 if_force_update=False):
        key_update_statistic = "accounting_periods_statistic:%d" % (
            int(shop_id))
        # 如果已经在进行更新中，就不进行更新，方法防抖
        if not redis.setnx(key_update_statistic, '1'):
            return
        # 设置过期时间30s,方法节流
        redis.expire(key_update_statistic, 30)
        # 强制更新时更新当天所有数据
        if if_force_update:
            last_update_timestamp = 0
            new_timestamp = 0
        else:
            chooseday = datetime.datetime.now().strftime('%Y-%m-%d')
            last_update_timestamp = statistic_session.query(
                models_account_periods_statistic.AccountPeriodsStatisticDataDaily.last_update_timestamp) \
                .filter_by(shop_id=shop_id, statistic_date=chooseday, statistic_type=1).scalar()
            if not last_update_timestamp:
                last_update_timestamp = 0
            new_timestamp = int(time.time())
        update_time_types = [0]
        # 如果今天有扎账，才更新明天的数据
        if AccountingPeriodFunc.get_accounting_time(session, shop_id):
            update_time_types.append(-1)
        # 根据各基本表来进行统计指标的更新
        import handlers.base.pub_statistic_account_periods as reslove_statistic
        for time_type in update_time_types:
            reslove_statistic.order(session, statistic_session, time_type,
                                    shop_id, last_update_timestamp,
                                    new_timestamp)
            reslove_statistic.refund_order(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.goods_sale_record(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.pb_order(session, statistic_session, time_type,
                                       shop_id, last_update_timestamp,
                                       new_timestamp)
            reslove_statistic.deposit_refund_order(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.fund_check(session, statistic_session, time_type,
                                         shop_id, last_update_timestamp,
                                         new_timestamp)
            reslove_statistic.payout_order(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.salary_payout_order(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.other_income_order(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.waste_selling_income_order(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.shop_supplier_borrowing(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.payout_supplier_clearing(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.payout_supplier_borrowing(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.update_goods_stockin(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.goods_stockin_sales(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.goods_storage(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.order_fee(session, statistic_session, time_type,
                                        shop_id, last_update_timestamp,
                                        new_timestamp)
            reslove_statistic.supplier_money_agent(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.supplier_money_self(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)
            reslove_statistic.supplier_money_borrowing(
                session, statistic_session, time_type, shop_id,
                last_update_timestamp, new_timestamp)

        redis.delete(key_update_statistic)

    # 店铺层面的资金对账都需要从统计数据中整合出一份显示为：
    #
    # 现金、条码收款、客户扫码付(订单统计和实际流入)、刷卡、收款累计、支票、汇款、在线支付累计、微信、支付宝
    @classmethod
    def get_funds_info_per_shop_statitic_instance(cls,
                                                  single_fund,
                                                  data_info,
                                                  source="accountant"):
        fund_cash = single_fund.fund_cash or 0 if single_fund else 0
        lc_fund_wxbar = single_fund.lc_fund_wxbar or 0 if single_fund else 0
        lc_fund_alibar = single_fund.lc_fund_alibar or 0 if single_fund else 0
        lc_fund_wxwap = single_fund.lc_fund_wxwap or 0 if single_fund else 0
        lc_fund_aliwap = single_fund.lc_fund_aliwap or 0 if single_fund else 0
        lc_fund_wxtag = single_fund.lc_fund_wxtag or 0 if single_fund else 0
        lc_fund_alitag = single_fund.lc_fund_alitag or 0 if single_fund else 0
        fund_pos = single_fund.fund_pos or 0 if single_fund else 0
        fund_cheque = single_fund.fund_cheque or 0 if single_fund else 0
        fund_remittance = single_fund.fund_remittance or 0 if single_fund else 0
        deposit_net_cent = single_fund.deposit_net_cent or 0 if single_fund else 0

        data_info["cash_money"] = check_float(fund_cash / 100)
        data_info["bar_money"] = check_float(
            (lc_fund_wxbar + lc_fund_alibar) / 100)
        data_info["wap_money"] = check_float(
            (lc_fund_wxwap + lc_fund_aliwap) / 100)
        data_info["tag_money"] = check_float(
            (lc_fund_wxtag + lc_fund_alitag) / 100)
        data_info["ali_money"] = check_float(
            (lc_fund_alibar + lc_fund_alitag) / 100)
        data_info["wx_money"] = check_float(
            (lc_fund_wxbar + lc_fund_wxtag) / 100)
        data_info["pos_money"] = check_float(fund_pos / 100)
        data_info["cheque_money"] = check_float(fund_cheque / 100)
        data_info["remittance_money"] = check_float(fund_remittance / 100)
        data_info["bank_money"] = check_float(
            (fund_remittance + fund_pos) / 100)
        online_cent = lc_fund_alibar + lc_fund_aliwap + lc_fund_wxbar + lc_fund_wxwap
        data_info["online_money"] = check_float(online_cent / 100)
        data_info["deposit"] = check_float(deposit_net_cent / 100)
        total_cent = fund_cash + fund_pos + lc_fund_wxbar + lc_fund_alibar + lc_fund_wxtag + lc_fund_alitag + fund_cheque + fund_remittance
        # total_money这个key给手机展示详细统计时是给全店收入用的，但是这里又给资金对账的总额用了，所以只在资金对账才重算这个值
        if source == "accountant":
            data_info["total_money"] = check_float(total_cent / 100)

    # 收银员层面的资金对账都需要从统计数据中整合出一份显示为现金、条码收款、客户扫码付(订单统计)、刷卡、支票、汇款、收款累计、微信、支付宝
    @classmethod
    def get_funds_info_per_accountant_statitic_instance(
            cls, single_accountant_fund, data_info):
        fund_cash = single_accountant_fund.fund_cash or 0 if single_accountant_fund else 0
        lc_fund_wxbar = single_accountant_fund.lc_fund_wxbar or 0 if single_accountant_fund else 0
        lc_fund_alibar = single_accountant_fund.lc_fund_alibar or 0 if single_accountant_fund else 0
        lc_fund_wxtag = single_accountant_fund.lc_fund_wxtag or 0 if single_accountant_fund else 0
        lc_fund_alitag = single_accountant_fund.lc_fund_alitag or 0 if single_accountant_fund else 0
        fund_pos = single_accountant_fund.fund_pos or 0 if single_accountant_fund else 0
        fund_cheque = single_accountant_fund.fund_cheque or 0 if single_accountant_fund else 0
        fund_remittance = single_accountant_fund.fund_remittance or 0 if single_accountant_fund else 0

        data_info["cash_money"] = check_float(fund_cash / 100)
        data_info["bar_money"] = check_float(
            (lc_fund_wxbar + lc_fund_alibar) / 100)
        data_info["tag_money"] = check_float(
            (lc_fund_wxtag + lc_fund_alitag) / 100)
        data_info["ali_money"] = check_float(
            (lc_fund_alibar + lc_fund_alitag) / 100)
        data_info["wx_money"] = check_float(
            (lc_fund_wxbar + lc_fund_wxtag) / 100)
        data_info["pos_money"] = check_float(fund_pos / 100)
        data_info["cheque_money"] = check_float(fund_cheque / 100)
        data_info["remittance_money"] = check_float(fund_remittance / 100)
        data_info["bank_money"] = check_float(
            (fund_remittance + fund_pos) / 100)
        total_cent = fund_cash + fund_pos + lc_fund_wxbar + lc_fund_alibar + lc_fund_wxtag + lc_fund_alitag + fund_cheque + fund_remittance
        data_info["total_money"] = check_float(total_cent / 100)

    @classmethod
    def get_goods_stockin_data(cls,
                               session,
                               statistic_session,
                               shop_id,
                               goods_dict,
                               weight_unit_text,
                               args={},
                               if_total=False,
                               only_goods=False):
        """
        货品入库记录日/月/年统计汇总数据
        :param page: 页码
        :param year: 年份
        :param month: 月份
        :param supplier_id: 供货商id
        :param statistic_type: 统计类型 1:日统计 2:周统计 3:月统计 4:年统计
        :return:
        """
        page_size = 20
        page = args.get("page", -1)
        year = args.get("year")
        month = args.get("month")
        date = args['date']
        end_date = args.get("end_date", "")
        data_type = args.get("data_type", 0)
        statistic_type = args.get("statistic_type", 1)
        sort_type = args.get("sort_type", "desc")
        goods_ids = args.get("goods_ids", [])
        supplier_id = args.get("supplier_id", 0)
        supply_type = args.get("supply_type", -1)
        goods_type = args.get("goods_type", -1)
        group_id = args.get("group_id", 0)

        # 日期区间筛选
        start_date = TimeFunc.check_input_date(
            date, statistic_type, return_type="date")

        if end_date:
            end_date = TimeFunc.check_input_date(
                end_date, statistic_type, return_type="date")
        else:
            end_date = start_date

        if data_type:
            StatisticGoodsStockin = models_account_periods_statistic.AccountPeriodsStatisticGoodsStockin
        else:
            StatisticGoodsStockin = models_statistic.StatisticGoodsStockin

        # 过滤条件
        filter_type = StatisticFunc.get_filter_type(statistic_type)
        filter_date = StatisticFunc.get_filter_date(statistic_type, start_date)

        # 基础查询
        if only_goods:
            columns_stockin = StatisticFunc.get_query_param(
                StatisticGoodsStockin)
            query_base_stockin = statistic_session.query(
                StatisticGoodsStockin.goods_id,
                *columns_stockin).group_by(StatisticGoodsStockin.goods_id)
        else:
            query_base_stockin = StatisticFunc.get_static_query_base(
                statistic_session,
                StatisticGoodsStockin,
                statistic_type,
                extra_groups=[
                    StatisticGoodsStockin.goods_id,
                    StatisticGoodsStockin.statistic_date,
                    StatisticGoodsStockin.year, StatisticGoodsStockin.month
                ])

        filter_params = [
            models_statistic.StatisticGoodsStockin.shop_id == shop_id,
            models_statistic.StatisticGoodsStockin.statistic_type ==
            filter_type
        ]

        if statistic_type == 1:
            filter_params.append(
                StatisticGoodsStockin.statistic_date >= start_date)
            filter_params.append(
                StatisticGoodsStockin.statistic_date <= end_date)
        else:
            if not if_total:
                query_base_stockin = query_base_stockin.filter_by(
                    **filter_date)

        # 过滤货品ID
        if goods_ids:
            filter_params.append(StatisticGoodsStockin.goods_id.in_(goods_ids))

        # 过滤分组ID
        if group_id:
            goods_in_group = session.query(models.Goods).filter_by(
                group_id=group_id, shop_id=shop_id).all()
            filter_params.append(
                StatisticGoodsStockin.goods_id.in_(
                    [x.id for x in goods_in_group]))
        # 过滤销售类型
        if supply_type in [0, 1, 2, 3]:
            supply_goods_id_list = GoodsFunc.get_goods_through_supply_type(
                session, shop_id, [supply_type])
            filter_params.append(
                StatisticGoodsStockin.goods_id.in_(supply_goods_id_list))
        # 过滤货品类型
        if goods_type in [0, 1, 2]:
            type_goods_id_list = GoodsFunc.get_goods_through_goods_type(
                session, shop_id, [goods_type])
            filter_params.append(
                StatisticGoodsStockin.goods_id.in_(type_goods_id_list))

        if supplier_id:
            filter_params.append(
                StatisticGoodsStockin.supplier_id == supplier_id)
        if year:
            filter_params.append(StatisticGoodsStockin.year == year)
        if month:
            filter_params.append(
                models_statistic.StatisticGoodsStockin.month == month)
        query_base_stockin = query_base_stockin.filter(
            *filter_params).order_by(StatisticGoodsStockin.id)
        if page == -1:
            statistic_all = query_base_stockin.all()
            page_sum = 1
        else:
            statistic_all = query_base_stockin.offset(page_size * page) \
                .limit(page_size) \
                .all()
            # 总条目数
            db_count = len(query_base_stockin.all())
            page_sum = math.ceil(db_count / page_size)
        data_list = []
        sum_base = StatisticFunc.get_static_sum_base(
            statistic_session, models_statistic.StatisticGoodsStockin)
        sum_data = sum_base.filter(*filter_params).first()
        total_data_sum = dict(
            quantity_sum=check_float(sum_data.quantity / 100)
            if sum_data.quantity else 0,
            weight_sum=check_float(
                UnitFunc.convert_weight_unit(
                    sum_data.weight,
                    source="weight",
                    weight_unit_text=weight_unit_text) / 100)
            if sum_data.weight else 0,
            total_price_sum=check_float(sum_data.total_price / 100)
            if sum_data.total_price else 0,
            credit_cent_sum=check_float(sum_data.credit_cent / 100)
            if sum_data.credit_cent else 0,
            paid_cent_sum=check_float(sum_data.paid_cent / 100)
            if sum_data.paid_cent else 0,
            payback_cent_sum=check_float(sum_data.payback_cent / 100)
            if sum_data.payback_cent else 0,
            should_pay_cent_sum=check_float(sum_data.should_pay_cent / 100)
            if sum_data.should_pay_cent else 0)
        for statistic in statistic_all:
            data = {}
            goods = goods_dict.get(statistic.goods_id)
            # 回传日期区间给前端，有利于日/周/月/年展示流水数据的选取日期区间
            date_range = TimeFunc.list_date_range(
                statistic_type,
                date=getattr(statistic, "statistic_date", None),
                year=getattr(statistic, "year", None),
                week=getattr(statistic, "week", None),
                month=getattr(statistic, "month", None))
            data["date_range"] = date_range
            data["shop_id"] = shop_id
            data["supplier_id"] = goods.shop_supplier_id
            data["goods_id"] = goods.id
            data["goods_name"] = GoodsFunc.get_goods_name_with_attr(goods)
            data["goods_purchase_unit"] = goods.purchase_unit
            data["supply_type"] = goods.supply_type
            data["supply_type_text"] = goods.supply_type_text
            data["quantity"] = check_float(statistic.quantity / 100)
            data["weight"] = check_float(
                UnitFunc.convert_weight_unit(
                    statistic.weight,
                    source="weight",
                    weight_unit_text=weight_unit_text) / 100)
            data["quantity_unit_price"] = check_float(
                statistic.total_price /
                statistic.quantity if statistic.quantity else 0)
            data["weight_unit_price"] = check_float(
                UnitFunc.convert_weight_unit(
                    statistic.total_price / statistic.weight
                    if statistic.weight else 0,
                    source="price",
                    weight_unit_text=weight_unit_text))
            data["weight_unit_text"] = weight_unit_text
            data["total_price"] = check_float(statistic.total_price / 100)
            data["credit_cent"] = check_float(statistic.credit_cent / 100)
            data["paid_cent"] = check_float(statistic.paid_cent / 100)
            data["payback_cent"] = check_float(statistic.payback_cent / 100)
            data["should_pay_cent"] = check_float(
                statistic.should_pay_cent / 100)
            data_list.append(data)

        return data_list, page_sum, total_data_sum


# 销售货品相关的统计值获取
class StatisticGoodsSalesFunc():
    _goods_info_dict = {"goods_unit": 0, "supperlier_id": 0, "goods_name": ""}
    _sales_kpi_base_dict = {
        "commission_mul": 0,
        "sales_weigh": 0,
        "total_commission": 0,
        "deposit_cent": 0,
        "total_goods_cent": 0
    }
    _salesman_kpi_dict = {
        "sales_count": 0,
        "sales_pay_count": 0,
        "refund_count": 0,
        "avg_reciept_cent": 0
    }
    _salesman_unpay_kpi_dict = {"unpay_count": 0}
    _goods_kpi_dict = {"avg_price": 0, "gross_cent": 0, "gross_percent": 0}
    _goods_unpay_kpi_dict = {"unpay_commission_mul": 0}
    _goods_financial_base_dict = {
        "total_goods_fact_price": 0,
        "payback_cent": 0,
    }

    @classmethod
    def _update_goods_info(cls, target_dict, goods):
        """ 在统计字典中更新货品信息

        :param target_dict: 待更新字典
        :param goods: 货品对象

        :rtype: dict
        """
        t_dict = dict(
            goods_unit=goods.unit,
            supperlier_id=goods.shop_supplier_id,
            goods_name=goods.name)
        return target_dict.update(t_dict)

    @classmethod
    def init_goods_dicts(cls, goods_id_list, map_goods):
        """ 初始化货品统计字典, 并更新货品信息(所有)

        :param goods_id_list: 货品id列表
        :param map_goods: dict, 货品id到货品信息的映射

        :rtype: dict
        """
        result = {}
        base_goods_dict = dict(
            collections.ChainMap(cls._goods_info_dict,
                                 cls._sales_kpi_base_dict, cls._goods_kpi_dict,
                                 cls._goods_unpay_kpi_dict,
                                 cls._goods_financial_base_dict))
        for goods_id in goods_id_list:
            goods = map_goods[goods_id]
            goods_dicts_value = copy.deepcopy(base_goods_dict)
            cls._update_goods_info(goods_dicts_value, goods)
            result[goods_id] = goods_dicts_value
        return result

    @classmethod
    def init_salesman_dicts(cls, salesman_id_list):
        """ 初始化开票员统计字典(所有)

        :param salesman_id_list: 开票员id列表

        :rtype: dict
        """
        result = {}
        base_salesman_dict = dict(
            collections.ChainMap(cls._sales_kpi_base_dict,
                                 cls._salesman_kpi_dict,
                                 cls._salesman_unpay_kpi_dict))
        for salesman_id in salesman_id_list:
            result[salesman_id] = copy.deepcopy(base_salesman_dict)
        return result

    @classmethod
    def init_goods_salesman_dicts(cls, salesman_id_goods_id_list, map_goods):
        """ 初始化开票员货品统计字典, 并更新货品信息(所有)

        :param salesman_id_goods_id_list: 开票员id-货品id列表
        :param map_goods: dict, 货品id到货品信息的映射

        :rtype: dict
        """
        result = {}
        base_goods_salesman_dict = dict(
            collections.ChainMap(cls._goods_info_dict, cls._salesman_kpi_dict,
                                 cls._sales_kpi_base_dict,
                                 cls._goods_kpi_dict))
        for salesman_id_goods_id in salesman_id_goods_id_list:
            goods_id = int(salesman_id_goods_id.split('-')[1])
            goods = map_goods[goods_id]
            goods_salesman_dicts_value = copy.deepcopy(
                base_goods_salesman_dict)
            cls._update_goods_info(goods_salesman_dicts_value, goods)
            result[salesman_id_goods_id] = goods_salesman_dicts_value
        return result

    @classmethod
    def init_goods_accountant_dicts(cls, accountant_id_goods_id_list,
                                    map_goods):
        """ 初始化收银员货品统计字典, 并更新货品信息(所有)

        :param acccountant_id_goods_id_list: 开票员id-货品id列表
        :param map_goods: dict, 货品id到货品信息的映射

        :rtype: dict
        """
        result = {}
        base_goods_accountant_dict = dict(
            collections.ChainMap(cls._goods_info_dict,
                                 cls._sales_kpi_base_dict))
        for accountant_id_goods_id in accountant_id_goods_id_list:
            goods_id = int(accountant_id_goods_id.split('-')[1])
            goods = map_goods[goods_id]
            goods_accountant_dicts_value = copy.deepcopy(
                base_goods_accountant_dict)
            cls._update_goods_info(goods_accountant_dicts_value, goods)
            result[accountant_id_goods_id] = goods_accountant_dicts_value
        return result

    @classmethod
    def _update_sales_kpi(cls, target_dict, record):
        """ 更新单个字典的销售kpi数据, **方法内部字典被重写**

        :param target_dict: 待更新的字典

        :rtype: None
        """
        if record.status not in (3, 5):
            raise ValueError("状态错误, 必须是已结算的小票")
        # 统计字段名到GoodsSalesRecord字段名的映射的映射
        map_keys = {
            "sales_weigh": "sales_num",
            "commission_mul": "commission_mul",
            "total_commission": "commission_money",
            "deposit_cent": "deposit_money",
            "total_goods_cent": "sales_money",
        }
        for key in cls._sales_kpi_base_dict:
            record_key = map_keys.get(key)
            attr = getattr(record, record_key)
            val = attr if record.record_type == 0 else -attr
            if key == "sales_weigh":
                # 重量在数据库中存的是斤*100,如果以后会有斤数出现按小数进行售卖的情况，尽量减小精度损失
                target_dict[key] += round(val * 100)
            else:
                target_dict[key] += val
        return None

    @classmethod
    def update_goods_dict(cls, goods_dict_single, record, gross_cent,
                          total_goods_fact_price):
        """ 通过销售数据更新单个货品统计字典, **方法内部字典被重写**

        :param goods_dict_single: 货品统计字典(单个)
        :param record: obj, `class sa.GoodsSalesRecord`, 销售记录
        :param gross_cent: 毛利, 因为店铺可能设置了代卖不展示, 所以从外部传递
        :param total_goods_fact_price: 货品总额, 主要考虑到赊账的情况

        :rtype: None
        """
        if record.status == 1 and record.record_type == 0:
            goods_dict_single["unpay_commission_mul"] += record.commission_mul
        elif record.status in (3, 5):
            goods_dict_single["gross_cent"] \
                += gross_cent if record.record_type == 0 else -gross_cent
            goods_dict_single["total_goods_fact_price"] \
                += total_goods_fact_price if record.record_type == 0 else -total_goods_fact_price
            cls._update_sales_kpi(goods_dict_single, record)
        return None

    @classmethod
    def update_salesman_dict(cls, salesman_dict_single, record):
        """ 通过销售数据更新单个开票员统计字典, **方法内部字典被重写**

        :param goods_dict_single: 货品统计字典(单个)
        :param record: obj, `class sa.GoodsSalesRecord`, 销售记录

        :rtype: None
        """
        if record.status == 1:
            salesman_dict_single["unpay_count"] += 1
        elif record.status in (3, 5):
            salesman_dict_single["sales_pay_count"] \
                += 1 if record.record_type == 0 else -1
            cls._update_sales_kpi(salesman_dict_single, record)
        if record.status in (
                3, 5) and record.record_type == 1 or record.status == 4:
            salesman_dict_single["refund_count"] += 1
        salesman_dict_single["sales_count"] += 1
        return None

    @classmethod
    def update_goods_salesman_dict(cls, goods_salesman_dict_single, record,
                                   gross_cent):
        """ 通过销售数据更新单个货品-开票员统计字典, **方法内部字典被重写**

        :param goods_salesman_dict_single: 开票员货品统计字典(单个)
        :param record: obj, `class sa.GoodsSalesRecord`, 销售记录
        :param gross_cent: 毛利, 因为店铺可能设置了代卖不展示, 所以从外部传递

        :rtype: None
        """
        if record.status in (3, 5):
            goods_salesman_dict_single["sales_pay_count"] \
                += 1 if record.record_type == 0 else -1
            goods_salesman_dict_single["gross_cent"] \
                += gross_cent if record.record_type == 0 else -gross_cent
            cls._update_sales_kpi(goods_salesman_dict_single, record)
        if record.status in (
                3, 5) and record.record_type == 1 or record.status == 4:
            goods_salesman_dict_single["refund_count"] += 1
        goods_salesman_dict_single["sales_count"] += 1
        return None

    @classmethod
    def update_goods_accountant_dict(cls, goods_accountant_dict_single,
                                     record):
        """ 通过销售数据更新单个货品-收银员统计字典, **方法内部字典被重写**

        :param goods_accountant_dict_single: 收银员货品统计字典(单个)
        :param record: obj, `class sa.GoodsSalesRecord`, 销售记录

        :rtype: None
        """
        if record.accountant_id == 0:
            raise ValueError("未结算无法更新收银员统计")
        if record.status in (3, 5):
            cls._update_sales_kpi(goods_accountant_dict_single, record)
        return None

    @classmethod
    def update_daily_dict(cls, daily_dict, record, gross_cent):
        """ 通过销售数据更新每日数据字典, **方法内部字典被重写**

        :param daily_dict: 每日数据字典
        :param record: obj, `class sa.GoodsSalesRecord`, 销售记录
        :param gross_cent: 毛利, 因为店铺可能设置了代卖不展示, 所以从外部传递

        :rtype: None
        """
        if record.status in (3, 5):
            total_cent = record.receipt_money - record.deposit_money
            daily_dict["total_cent"] += total_cent \
                if record.record_type == 0 else -total_cent
            daily_dict["sales_cent"] += record.sales_money \
                if record.record_type == 0 else -record.sales_money
            daily_dict["commission_cent"] += record.commission_money \
                if record.record_type == 0 else -record.commission_money
            daily_dict["deposit_cent"] += record.deposit_money \
                if record.record_type == 0 else -record.deposit_money
            daily_dict["gross_cent"] += gross_cent \
                if record.record_type == 0 else -gross_cent
        return None

    @classmethod
    def cal_money(cls, record, disable_statistic_gross, supply_type):
        """ 通过设置计算毛利和货品金额

        :param disable_statistic_gross: 代卖货品是否计算毛利
        :param supply_type: 货品供应类型

        :rtype: tuple
        """
        # 计算货品实收资金额
        if record.status == 3 and record.pay_type != 9:
            # 已结算非赊账流水
            total_goods_fact_price = record.receipt_money
        else:
            total_goods_fact_price = 0

        # 货品为代卖时并且店铺并且开启了代卖货品计算毛利时不计算相应的毛利
        gross_cent = record.gross_money
        if supply_type == 1 and disable_statistic_gross:
            gross_cent = 0

        return total_goods_fact_price, gross_cent


# 商品
class GoodsFunc():
    @classmethod
    def getUnit(cls, unit_id, weight_unit_text="斤"):
        unit_list = {0: weight_unit_text, 1: '件'}
        name = unit_list.get(unit_id, "")
        if name:
            return name
        else:
            return ""

    # 销售类型
    @classmethod
    def get_supply_type(cls, supply_type):
        type_text = {0: "自营", 1: "代卖", 2: "货主", 3: "联营"}
        text = type_text.get(supply_type, "")
        return text

    # 货品类型
    @classmethod
    def get_goods_type(cls, goods_type):
        type_text = {0: "", 1: "国产", 2: "进口"}
        text = type_text.get(goods_type, "")
        return text

    @classmethod
    def get_goods_info(cls,
                       goods,
                       source="",
                       multi=False,
                       weight_unit_text="斤"):
        check_float = NumFunc.check_float

        goods_info = {}
        goods_info["id"] = goods.id
        goods_info["name"] = goods.name
        goods_info["name_with_attr"] = cls.get_goods_name_with_attr(goods)
        goods_info["img_url"] = goods.firstimg
        goods_info["unit"] = goods.unit
        goods_info["unit_text"] = cls.getUnit(
            goods.unit, weight_unit_text=weight_unit_text)
        goods_info["commission"] = check_float(goods.commission / 100)
        goods_info["price"] = check_float(
            goods.get_price(weight_unit_text=weight_unit_text) / 100)
        # 成本价先兼容两个版本
        goods_info["cost_price"] = check_float(goods.cost_price / 100)
        goods_info["cost_price_quantity"] = check_float(
            goods.cost_price_quantity / 100)
        goods_info["cost_price_weight"] = check_float(
            goods.get_cost_price_weight(weight_unit_text=weight_unit_text) /
            100)
        goods_info["storage"] = check_float(goods.storage / 100)
        goods_info["storage_num"] = check_float(
            goods.get_storage_num(weight_unit_text=weight_unit_text) / 100)
        goods_info["storage_unit"] = goods.storage_unit
        goods_info["storage_text"] = cls.get_storage_text(
            goods, weight_unit_text=weight_unit_text)
        goods_info["storage_unit_text"] = cls.getUnit(
            goods.storage_unit, weight_unit_text=weight_unit_text)
        goods_info["add_time"] = TimeFunc.time_to_str(goods.add_time)
        goods_info["supply_type_code"] = goods.supply_type
        goods_info["supply_type"] = cls.get_supply_type(goods.supply_type)
        goods_info["goods_type_code"] = goods.goods_type
        goods_info["goods_type"] = cls.get_goods_type(goods.goods_type)
        goods_info["active"] = goods.active
        goods_info["edit_commission"] = goods.edit_commission
        goods_info["deposit"] = goods.deposit
        goods_info["edit_deposit"] = goods.edit_deposit
        goods_info["group_id"] = goods.group_id
        goods_info["group_name"] = goods.group.name if not multi else ""
        goods_info["production_place"] = goods.production_place
        goods_info["specification"] = goods.specification
        goods_info["brand"] = goods.brand
        goods_info["commodity_code"] = goods.commodity_code
        goods_info["tare_weight"] = 0
        goods_info["is_master"] = goods.is_master
        goods_info["master_goods_id"] = goods.master_goods_id
        return goods_info

    @classmethod
    def get_goods_storage_info(cls, goods, stockin_unit, weight_unit_text="斤"):
        check_float = NumFunc.check_float
        # 原本的计价方式，现在需要调整，但是为了对比 这里保留源代码
        # cost_total = check_float(goods.cost_price_quantity*goods.storage) if goods.cost_price_quantity else check_float(goods.cost_price_weight*goods.storage_num)
        cost_total = check_float(
            goods.cost_price_quantity *
            goods.storage) if stockin_unit else check_float(
                goods.cost_price_weight * goods.storage_num)

        goods_info = {}
        goods_info["id"] = goods.id
        goods_info["name"] = goods.name
        goods_info["name_with_attr"] = cls.get_goods_name_with_attr(goods)
        goods_info["unit"] = goods.unit
        goods_info["unit_text"] = cls.getUnit(
            goods.unit, weight_unit_text=weight_unit_text)
        goods_info["cost_total"] = check_float(cost_total / 10000)
        goods_info["storage"] = check_float(goods.storage / 100)
        goods_info["storage_num"] = check_float(
            goods.get_storage_num(weight_unit_text=weight_unit_text) / 100)
        goods_info["storage_unit"] = goods.storage_unit
        goods_info["storage_unit_text"] = cls.get_storage_text(
            goods, weight_unit_text=weight_unit_text)
        goods_info["supply_type_code"] = goods.supply_type
        goods_info["supply_type"] = cls.get_supply_type(goods.supply_type)
        goods_info["group_id"] = goods.group_id
        goods_info["group_name"] = goods.group.name
        goods_info["production_place"] = goods.production_place
        goods_info["specification"] = goods.specification
        goods_info["brand"] = goods.brand
        goods_info["commodity_code"] = goods.commodity_code
        goods_info["supplier_id"] = goods.shop_supplier_id
        return goods_info

    @classmethod
    def get_storage_text(cls, goods, weight_unit_text="斤"):
        '''商品库存，根据库存单位来显示对应的库存'''
        if goods.storage_unit == 0:
            storage_text = "%s%s" % (NumFunc.check_float(
                goods.get_storage_num(weight_unit_text=weight_unit_text) /
                100), weight_unit_text)
        else:
            storage_text = "%s件" % (NumFunc.check_float(goods.storage / 100))
        return storage_text

    # 根据id查询商品
    @classmethod
    def get_goods(cls, session, shop_id, goods_id):
        goods = session.query(models.Goods).filter_by(
            shop_id=shop_id, id=goods_id).first()
        return goods

    @classmethod
    def get_goods_dict(cls, session, **filter_by_params):
        goods = session.query(models.Goods).filter_by(**filter_by_params).all()
        goods_dict = {g.id: g for g in goods}
        return goods_dict

    @classmethod
    def get_active_goods_list_in_shop(cls, session, shop_id):
        goods = session.query(models.Goods).filter_by(
            shop_id=shop_id, active=1).all()
        return goods

    @classmethod
    def get_goods_list(cls, session, **filter_by_params):
        goods = session.query(models.Goods).filter_by(**filter_by_params).all()
        return goods

    @classmethod
    def get_supplier_goods(cls, session, shop_id, goods_id, supplier_id):
        goods = session.query(models.Goods).filter_by(
            shop_id=shop_id, id=goods_id,
            shop_supplier_id=supplier_id).first()
        return goods

    @classmethod
    def get_goods_name_through_id_list(cls, session, goods_id_list):
        """批量获取带属性的货品名"""
        goods_name_dict = {}
        if goods_id_list:
            Goods = models.Goods
            goods = session.query(Goods) \
                .filter(Goods.id.in_(goods_id_list)) \
                .all()
            goods_name_dict = {
                x.id: cls.get_goods_name_with_attr(x)
                for x in goods
            }
        return goods_name_dict

    @classmethod
    def get_active_goods_ids(cls, session, shop_id):
        """ 获取店铺未删除的货品id """
        Goods = models.Goods
        query = session.query(models.Goods.id) \
            .filter(Goods.active.in_([1, 2]), Goods.shop_id == shop_id) \
            .all()
        return [q.id for q in query]

    @classmethod
    def get_goods_name_unit_through_id_list(cls, session, goods_id_list):
        goods_name_dict = {}
        if goods_id_list:
            Goods = models.Goods
            goods = session.query(Goods)\
                .filter(Goods.id.in_(goods_id_list))\
                .all()
            weight_unit_text = ""
            if goods:
                shop_id = goods[0].shop_id
                weight_unit_text = ConfigFunc.get_weight_unit_text(
                    session, shop_id)
            goods_name_dict = {
                g.id: {
                    "name":
                    cls.get_goods_name_with_attr(g),
                    "supply_type":
                    g.supply_type,
                    "supply_type_text":
                    cls.get_supply_type(g.supply_type),
                    "unit_text":
                    cls.getUnit(g.unit, weight_unit_text=weight_unit_text),
                    "unit":
                    g.unit
                }
                for g in goods
            }
        return goods_name_dict

    @classmethod
    def get_group_id_dict_with_goods_ids(cls, session, goods_id_list):
        goods_id_group_id = session.query(
            models.Goods.id, models.Goods.group_id).filter(
                models.Goods.id.in_(goods_id_list))
        group_id_dict = {}
        for goods_id, group_id in goods_id_group_id:
            group_id_dict[goods_id] = group_id
        return group_id_dict

    @classmethod
    def get_group_list_through_id_list(cls, session, goods_id_list):
        group_list = []
        if goods_id_list:
            Goods = models.Goods
            GoodsGroup = models.GoodsGroup
            group_id_name_count_list = session \
                .query(Goods.group_id.label("group_id"), GoodsGroup.name.label("group_name"),
                       func.count(Goods.id).label("group_count")) \
                .join(GoodsGroup, Goods.group_id == GoodsGroup.id) \
                .filter(Goods.id.in_(goods_id_list)) \
                .group_by(Goods.group_id).all()
            for group_id_name_count in group_id_name_count_list:
                group_list.append({
                    "group_id":
                    group_id_name_count.group_id,
                    "group_name":
                    group_id_name_count.group_name,
                    "group_count":
                    group_id_name_count.group_count,
                })

        return group_list

    @classmethod
    def get_groups_dict_through_id_list(cls, session, goods_id_list):
        """通过goods_id_list获得以goods_id为主键的字典，元素为字典，有group_id,和group_name属性"""
        groups_dict = {}
        if goods_id_list:
            Goods = models.Goods
            GoodsGroup = models.GoodsGroup
            group_id_name_count_list = session \
                .query(Goods.id.label("goods_id"), Goods.group_id.label("group_id"), GoodsGroup.name.label("group_name")) \
                .join(GoodsGroup, Goods.group_id == GoodsGroup.id) \
                .filter(Goods.id.in_(goods_id_list)).all()
            for group_id_name_count in group_id_name_count_list:
                groups_dict[group_id_name_count.goods_id] = dict(
                    group_id=group_id_name_count.group_id,
                    group_name=group_id_name_count.group_name,
                )

        return groups_dict

    @classmethod
    def get_group_dict_through_goods_id_list(cls, session, goods_id_list):
        """通过goods_ids获得group以id为主键的名字字典"""
        group_id_name_dict = {}
        if goods_id_list:
            Goods = models.Goods
            GoodsGroup = models.GoodsGroup
            group_id_name_list = session \
                .query(Goods.group_id.label("group_id"), GoodsGroup.name.label("group_name")) \
                .join(GoodsGroup, Goods.group_id == GoodsGroup.id) \
                .filter(Goods.id.in_(goods_id_list)).all()
            for group_id, group_name in group_id_name_list:
                group_id_name_dict[group_id] = group_name

        return group_id_name_dict

    @classmethod
    def get_group_name_dict_through_id_list(cls, session, group_id_list):
        """通过group_ids获得group以id为主键的名字字典"""
        group_name_dict = {}
        if group_id_list:
            GoodsGroup = models.GoodsGroup
            group_id_name_list = session.query(GoodsGroup.id, GoodsGroup.name)\
                .filter(GoodsGroup.id.in_(group_id_list)).all()
            for group_id, group_name in group_id_name_list:
                group_name_dict[group_id] = group_name

        return group_name_dict

    @classmethod
    def get_goods_supplytype_goodstype_through_id_list(cls, session,
                                                       goods_id_list):
        supplytype_goodstype_dict = {}
        if goods_id_list:
            Goods = models.Goods
            goods = session.query(Goods) \
                .filter(Goods.id.in_(goods_id_list)) \
                .all()
            supplytype_goodstype_dict = {
                x.id: {
                    "supply_type": x.supply_type_text,
                    "goods_type": x.goods_type_text
                }
                for x in goods
            }
        return supplytype_goodstype_dict

    @classmethod
    def get_goods_name_unit_through_id(cls, session, goods_id):
        Goods = models.Goods
        goods = session.query(Goods) \
            .filter_by(id=goods_id) \
            .first()
        goods_name = cls.get_goods_name_with_attr(goods) or ""
        goods_unit = goods.unit or 0
        return goods_name, goods_unit

    @classmethod
    def get_goods_info_list_through_id_list(cls, session, shop_id,
                                            goods_id_list):
        goods_info_dict = {}
        if goods_id_list:
            Goods = models.Goods
            goods_list = session.query(Goods) \
                .filter_by(shop_id=shop_id) \
                .filter(Goods.id.in_(goods_id_list)) \
                .all()
            shop_supplier_id_list = [x.shop_supplier_id for x in goods_list]
            supplier_dict = SupplierFunc.get_shop_supplier_base_info(
                session, shop_id, shop_supplier_id_list)
            for goods in goods_list:
                goods_id = goods.id
                goods_name = cls.get_goods_name_with_attr(goods)
                storage = NumFunc.check_float(goods.storage / 100)
                storage_num = goods.storage_num
                shop_supplier_id = goods.shop_supplier_id
                supplier_name = supplier_dict.get(shop_supplier_id, "")
                if supplier_name:
                    supplier_name = supplier_name["name"]
                goods_info_dict[goods_id] = {
                    "goods_id": goods_id,
                    "name": goods_name,
                    "supplier_name": supplier_name,
                    "storage": storage,
                    "storage_num": storage_num
                }
        return goods_info_dict

    @classmethod
    def get_goods_info_with_supplier(cls, session, shop_id, goods, source=""):
        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)
        goods_info = cls.get_goods_info(
            goods, source=source, weight_unit_text=weight_unit_text)
        shop_supplier_id = goods.shop_supplier_id

        goods_info["supplier_id"] = shop_supplier_id
        goods_info["supplier_name"] = ""
        goods_info["supplier_phone"] = ""

        if shop_supplier_id:
            shop_supplier = SupplierFunc.get_shop_supplier_through_id(
                session, shop_id, shop_supplier_id)
            goods_info["supplier_name"] = shop_supplier.realname
            goods_info["supplier_phone"] = shop_supplier.phone or ""
        return goods_info

    @classmethod
    def get_goods_name_with_supplier(cls, session, shop_id, goods_id):
        goods = cls.get_goods(session, shop_id, goods_id)
        goods_name = cls.get_goods_name_with_attr(goods)
        shop_supplier_id = goods.shop_supplier_id
        if shop_supplier_id:
            shop_supplier = SupplierFunc.get_shop_supplier_through_id(
                session, shop_id, shop_supplier_id)
            shop_supplier_name = shop_supplier.realname if shop_supplier else ""
        else:
            shop_supplier_name = ""
        if shop_supplier_name:
            goods_name = "%s(%s)" % (goods_name, shop_supplier_name)
        return goods_name

    @classmethod
    def get_salesman_goods_limit_list(cls, session, shop_id, account_id):
        SalesmanGoodsLimit = models.SalesmanGoodsLimit
        goods_limit = session.query(SalesmanGoodsLimit.limit_goods_list) \
            .filter_by(shop_id=shop_id, account_id=account_id) \
            .scalar()
        if goods_limit:
            goods_limit = DataFormatFunc.split_str(goods_limit)
            goods_limit = DataFormatFunc.format_str_to_int_inlist(goods_limit)
        else:
            goods_limit = []
        return goods_limit

    @classmethod
    def get_goods_through_supply_type(cls, session, shop_id, supply_type_list):
        """ 获取某一销售类型的货品ID列表
        """
        Goods = models.Goods
        goods_id_list = session.query(Goods.id) \
            .filter(Goods.shop_id==shop_id,
                    Goods.supply_type.in_(supply_type_list)) \
            .all()
        goods_id_list = {x.id for x in goods_id_list}
        return goods_id_list

    @classmethod
    def get_goods_through_goods_type(cls, session, shop_id, goods_type_list):
        """ 获取某一货品类型的货品ID列表
        """
        Goods = models.Goods
        goods_id_list = session.query(Goods.id) \
            .filter(Goods.shop_id==shop_id,
                    Goods.goods_type.in_(goods_type_list)) \
            .all()
        goods_id_list = {x.id for x in goods_id_list}
        return goods_id_list

    # 获取商品信息
    @classmethod
    def get_goods_dict_through_query(cls, session, shop_id, data_query):
        goods_id_list = [x.goods_id for x in data_query]
        if goods_id_list:
            Goods = models.Goods
            goods_list = session.query(Goods) \
                .filter(Goods.id.in_(goods_id_list)) \
                .all()
            goods_dict = {x.id: x for x in goods_list}
        else:
            goods_dict = {}
        return goods_dict

    @classmethod
    def get_id_goup_by_supply_type(cls, session, shop_id):
        """ 获取各经营类型的货品ID
        """
        Goods = models.Goods
        goods_id_list = session.query(Goods.id, Goods.supply_type) \
            .filter_by(shop_id=shop_id) \
            .all()
        self_goods_id_list = [
            x.id for x in goods_id_list if x.supply_type == 0
        ]
        agent_goods_id_list = [
            x.id for x in goods_id_list if x.supply_type == 1
        ]
        union_goods_id_list = [
            x.id for x in goods_id_list if x.supply_type == 3
        ]
        return self_goods_id_list, agent_goods_id_list, union_goods_id_list

    @classmethod
    def get_count_goup_by_supply_type(cls, session, shop_id, active=1):
        """ 获取各经营类型的货品数量
        """
        Goods = models.Goods
        goods_id_list = session.query(Goods.supply_type, func.count(1)) \
            .filter_by(shop_id=shop_id, active=active) \
            .group_by(Goods.supply_type) \
            .all()
        supply_type_count_dict = {x[0]: x[1] for x in goods_id_list}
        return supply_type_count_dict

    @classmethod
    def get_goods_detail_names(cls, session, goods_id_list, map_suppliers):
        goods = session.query(models.Goods).filter(
            models.Goods.id.in_(goods_id_list)).all() if goods_id_list else []
        return {
            g.id: cls.get_goods_detail_name(g, map_suppliers)
            for g in goods
        }

    @classmethod
    def get_goods_detail_names_dict_suppliers_type_dict(
            cls, session, goods_id_list, map_suppliers):
        """通过goods_ids获得goods_name_dict, goods_supply_type_dict的元组"""
        goods = session.query(models.Goods).filter(
            models.Goods.id.in_(goods_id_list)).all() if goods_id_list else []
        goods_name_dict = {}
        goods_supply_type_dict = {}
        for g in goods:
            goods_name_dict[g.id] = cls.get_goods_detail_name(g, map_suppliers)
            goods_supply_type_dict[g.id] = g.supply_type
        return goods_name_dict, goods_supply_type_dict

    @classmethod
    def get_goods_and_supplier_names(cls, session, goods_id_list):
        goods_supplier_name_dict = {}
        if goods_id_list:
            Goods = models.Goods
            goods_list = session.query(Goods) \
                .filter(Goods.id.in_(goods_id_list)) \
                .all()

            # 获取供货商信息
            supplier_id_list = {x.shop_supplier_id for x in goods_list}
            supplier_name_dict = SupplierFunc.get_shop_supplier_names(
                session, supplier_id_list)
            # 组装货品名与供货商名
            for goods in goods_list:
                goods_id = goods.id
                goods_name = cls.get_goods_detail_name(goods)
                supplier_name = supplier_name_dict.get(goods.shop_supplier_id,
                                                       "")
                goods_supplier_name_dict[goods_id] = dict(
                    goods_name=goods_name,
                    supplier_name=supplier_name,
                    supplier_id=goods.shop_supplier_id)

        return goods_supplier_name_dict

    @classmethod
    def get_goods_detail_name(cls, goods, map_suppliers=None):
        """ 获取商品的详细名称，通过`|`将商品的供应商/产地等信息连起来

        :param goods: obj, class `Goods`
        :param map_suppliers: dict, 供应商id到其真实姓名的映射

        :rtype: string
        """
        result = goods.name
        # 入库单不再需要供应商
        if map_suppliers:
            supplier_name = map_suppliers.get(goods.shop_supplier_id, "")
            if supplier_name:
                result += " | " + supplier_name
        if goods.specification:
            result += " | " + goods.specification
        if goods.production_place:
            result += " | " + goods.production_place
        if goods.brand:
            result += " | " + goods.brand
        return result

    @classmethod
    def get_goods_storage_version(cls, goods_id):
        """ 获取商品的库存版本号, 来自缓存

        :param goods_id: 商品id

        :rtype: int
        """
        return redis.get("goods_storage_version:%d" % goods_id)

    @classmethod
    def set_goods_storage_version(cls, goods_id):
        """ 更新商品的库存版本号, 自增

        :param goods_id: 商品id

        :rtype: None
        """
        s_key = "goods_storage_version:%d" % goods_id
        redis.incr(s_key)
        redis.expire(s_key, 5 * 60)
        return None

    @classmethod
    def get_goods_name_with_attr(cls, goods):
        """ 获取商品的详细名称，通过`|`将商品的供应商/产地等信息连起来

        :param goods: obj, class `Goods`
        :param map_suppliers: dict, 供应商id到其真实姓名的映射

        :rtype: string
        """
        result = goods.name

        if goods.specification:
            result += "/%s" % goods.specification
        if goods.production_place:
            result += "/%s" % goods.production_place
        if goods.brand:
            result += "/%s" % goods.brand
        return result

    @classmethod
    def get_latest_pay_type(cls, session, goods):
        """ 获取最近入库的支付类型 """
        detail = session.query(
            models.StockInDocsDetail).filter_by(goods_id=goods.id).order_by(
                models.StockInDocsDetail.id.desc()).first()
        if goods.supply_type in [0, 3]:
            pay_type = detail.pay_type or 5 if detail else 5
        else:
            pay_type = 0
        return pay_type

    @classmethod
    def get_supplier_id_list_through_goods(cls, session, goods_id_list):
        """ 根据货品ID获取对应供应商ID
        """
        Goods = models.Goods
        goods_query = session.query(Goods.shop_supplier_id) \
            .filter(Goods.id.in_(goods_id_list),
                    Goods.shop_supplier_id > 0) \
            .all()
        return {x.shop_supplier_id for x in goods_query}

    @classmethod
    def get_id_by_unit(cls, session, shop_id, unit=0):
        """ 获取各经营类型的货品ID
        """
        Goods = models.Goods
        goods_id_list = session.query(Goods.id) \
            .filter_by(shop_id=shop_id, unit=unit) \
            .all()
        return {x.id for x in goods_id_list}

    @classmethod
    def get_id_by_storage_unit(cls, session, shop_id, storage_unit=0):
        """ 获取各种库存单位的货品ID
        """
        Goods = models.Goods
        goods_id_list = session.query(Goods.id) \
            .filter_by(shop_id=shop_id, storage_unit=storage_unit) \
            .all()
        return {x.id for x in goods_id_list}

    @classmethod
    def get_map_id_goods(cls, session, goods_id_list):
        """ 获取商品id到商品对象的映射

        :rtype: dict
        """
        goods = session.query(models.Goods).filter(
            models.Goods.id.in_(goods_id_list)).all() if goods_id_list else []
        result = {g.id: g for g in goods}
        return result

    @classmethod
    def get_goods_supplier_count(cls, session, shop_id, active=1):
        """ 获取不同状态货品供货商数量
        """
        Goods = models.Goods
        supplier_count = session.query(func.distinct(Goods.shop_supplier_id)) \
            .filter_by(shop_id=shop_id, active=active) \
            .filter(Goods.shop_supplier_id > 0) \
            .count()
        return supplier_count

    @classmethod
    def get_goods_active_count(cls, session, shop_id, active=1):
        """ 获取不同状态货品数量

        :rtype: int
        """
        Goods = models.Goods
        master_goods_id_set = GoodsMasterBranchFunc.list_goods_id_master(
            session, shop_id)
        active_count = session.query(func.count(Goods.id)) \
                           .filter_by(shop_id=shop_id, active=active) \
                           .filter(Goods.id.in_(master_goods_id_set)) \
                           .scalar() or 0 if master_goods_id_set else 0
        return active_count

    @classmethod
    def get_date_acitve_goods_ids(cls, session, shop_id, choose_time):
        """ 获取当日上架的商品id

        :param choose_time: datetime.datetime, 选择的日期时间

        :rtype: list
        """
        query_base = session.query(
            func.max(models.GoodsHistory.id).label("history_id")).group_by(
                models.GoodsHistory.goods_id).filter_by(shop_id=shop_id)
        id_querys = query_base.filter(
            models.GoodsHistory.edit_time < choose_time).all()
        history_ids = [q.history_id for q in id_querys]
        goods_historys = session.query(models.GoodsHistory).filter(
            models.GoodsHistory.id.in_(
                history_ids)).all() if history_ids else []
        result = []
        for h in goods_historys:
            if h.edit_content not in ["删除商品", "下架商品", "货款结算清零"]:
                result.append(h.goods_id)
        return result

    @classmethod
    def get_goods_in_shop(cls,
                          session,
                          shop_id,
                          weight_unit_text,
                          goods_id_list=None,
                          merge_self_goods=True,
                          contains_off=True):
        map_suppliers = SupplierFunc.get_shop_supplier_all(session, shop_id)

        query_base = GoodsMasterBranchFunc.get_goods_base_query(
            session, shop_id, merge_branch_goods=merge_self_goods)
        if contains_off:
            query_base = query_base.filter(models.Goods.active.in_([0, 1, 2]))
        else:
            query_base = query_base.filter(models.Goods.active.in_([
                1,
            ]))
        if goods_id_list:
            query_base = query_base.filter(models.Goods.id.in_(goods_id_list))
        goods = query_base.all()

        goods_id_list = [g.id for g in goods]
        # 获取商品上次入库的付款方式
        StockInDocsDetail = models.StockInDocsDetail
        StockInDocs = models.StockInDocs
        detail_ids = session \
            .query(func.max(StockInDocsDetail.id)) \
            .join(StockInDocs, StockInDocsDetail.doc_id == StockInDocs.id) \
            .filter(StockInDocsDetail.goods_id.in_(goods_id_list),
                    StockInDocs.status == 1) \
            .group_by(StockInDocsDetail.goods_id) \
            .all() if goods_id_list else []
        detail_id_list = [d[0] for d in detail_ids]
        details = session \
            .query(StockInDocsDetail) \
            .filter(StockInDocsDetail.id.in_(detail_id_list)) \
            .all() if detail_id_list else []
        map_goods_pay_type = {
            detail.goods_id: detail.pay_type
            for detail in details
        }
        map_stockin_unit = {detail.goods_id: detail.unit for detail in details}

        # 获取商品最新报损的单位
        map_breakage_unit = StockInFunc.get_doc_detail_unit_pub(
            session, models.BreakageDocsDetail, models.BreakageDocs,
            goods_id_list)
        result = {}
        for g in goods:
            detail_name = GoodsFunc.get_goods_detail_name(g)
            g_dict = dict(
                goods_id=g.id,
                img_url=models.AddImgDomain.add_domain(g.img_url),
                detail_name=detail_name,
                storage_unit=g.storage_unit,
                purchase_unit=g.purchase_unit,
                group_id=g.group_id,
                cost_price_quantity=check_float(g.cost_price_quantity / 100),
                cost_price_weight=check_float(
                    g.get_cost_price_weight(weight_unit_text=weight_unit_text)
                    / 100),
                name=g.name,
                supply_type_text=GoodsFunc.get_supply_type(g.supply_type),
                production_place=g.production_place,
                specification=g.specification,
                brand=g.brand,
                commodity_code=g.commodity_code,
                supplier_id=g.shop_supplier_id,
                supplier_name=map_suppliers.get(g.shop_supplier_id, ""),
                goods_type=g.goods_type,
                storage=g.storage / 100,
                storage_num=g.get_storage_num(
                    weight_unit_text=weight_unit_text) / 100,
                commission=check_float(g.commission / 100),
                deposit=g.deposit,
                price=check_float(
                    g.get_price(weight_unit_text=weight_unit_text) / 100),
                unit=g.unit,
                stockin_unit=map_stockin_unit.get(g.id, ""),
                breakage_unit=map_breakage_unit.get(g.id, ""))
            # 联营和自营默认付款方式是赊账代付
            if g.supply_type in (0, 3):
                g_dict["pay_type"] = map_goods_pay_type.get(g.id) or 5
            else:
                g_dict["pay_type"] = 0
            result[g.id] = g_dict
        return result

    @classmethod
    def get_goods_has_supplier_id_list(cls,
                                       session,
                                       shop_id,
                                       shop_supplier_id=0):
        Goods = models.Goods
        goods_id_list = session.query(Goods.id)\
                                .filter_by(shop_id=shop_id)
        if shop_supplier_id:
            goods_id_list = goods_id_list.filter_by(
                shop_supplier_id=shop_supplier_id)
        goods_id_list = goods_id_list.filter(Goods.shop_supplier_id>0,Goods.active>0)\
                                    .all()
        goods_id_list = [x.id for x in goods_id_list]
        return goods_id_list


# 商品批次
class GoodsBatchFunc():

    num_char_consume = ["G", "F", "E", "D", "C", "B", "A"]
    num_char_refund = ["Z", "Y", "X", "W", "V", "U", "T"]
    num_char_ajust = ["N", "M", "L", "K", "J", "I", "H"]
    """ TODO: 后续主货的批次考虑与分货进行合并，主要的区别在于退货品变成了后进先退出 文嘉智"""

    @classmethod
    def get_current_batch(cls, session, goods_id):
        """ 获取商品的当前批次, 即为入库的批次中正在被消耗的记录, 若都被消耗完, 则消耗默认记录

        :param goods_id: 商品ID

        :rtype: obj, `class GoodsBatch`
        """
        batch = session.query(models.GoodsBatch).filter_by(
            goods_id=goods_id, consume_status=1, batch_status=1).first()
        if not batch:
            batch = session.query(models.GoodsBatch).filter_by(
                goods_id=goods_id, consume_status=0,
                batch_status=1).order_by(models.GoodsBatch.id).first()
            if not batch:
                batch = cls.get_default_batch(session, goods_id)
        return batch

    @classmethod
    def get_default_batch(cls, session, goods_id):
        """ 获取商品的默认批次记录 """
        result = session.query(models.GoodsBatch).filter_by(
            goods_id=goods_id, consume_status=-1).first()
        return result

    @classmethod
    def get_batch_num_through_id_list(cls, session, batch_ids):
        batchs = session.query(models.GoodsBatch).filter(
            models.GoodsBatch.id.in_(batch_ids)).all() if batch_ids else []
        return {b.id: b.batch_num for b in batchs}

    @classmethod
    def consume(cls,
                session,
                goods,
                sale_record,
                record_type,
                operator_id,
                storage_record_type,
                is_refund=False,
                is_ajust=False,
                batch_id=None):
        """ 消耗一个商品的库存, 并更新对应的批次状态, 如果是主货还需要生成对应的分货记录, 先限定根据售出单位走 """
        batch_queue = session.query(models.GoodsBatch) \
            .filter_by(goods_id=goods.id, batch_status=1) \
            .filter(models.GoodsBatch.consume_status.in_([0, 1])) \
            .order_by(models.GoodsBatch.id.desc()) \
            .all()

        default_batch = cls.get_default_batch(session, goods.id)
        consume_list = cls._construct_consume_list(
            default_batch, batch_queue, sale_record.commission_mul,
            sale_record.sales_num * 100, goods.unit)

        if not goods.is_master:
            return None

        # 主货品生成分货相关记录
        if is_refund and not is_ajust:
            char_list = cls.num_char_refund[:]
        elif is_ajust:
            char_list = cls.num_char_ajust[:]
        else:
            char_list = cls.num_char_consume[:]

        for consume in consume_list:
            batch = consume["batch"]
            branch_goods = session.query(models.Goods) \
                .filter_by(
                    shop_id=goods.shop_id,
                    master_goods_id=goods.id,
                    shop_supplier_id=batch.supplier_id) \
                .first()
            if not branch_goods:
                continue

            origin_storage, origin_storage_num = OrderFinishFunc.update_goods_storage(
                branch_goods, consume["quantity"], consume["weight"])
            rate = abs(consume["quantity"] / sale_record.commission_mul
                       if consume["quantity"] else consume["weight"] /
                       sale_record.sales_num / 100)
            num = "S" + char_list.pop() + sale_record.get_num()
            new_sale_record = OrderFinishFunc.gen_sales_record_by_origin(
                session,
                sale_record,
                0,
                branch_goods,
                batch.id,
                order=None,
                num=num,
                rate=rate)
            OrderFinishFunc.gen_storage_record(
                session, branch_goods, origin_storage, origin_storage_num,
                new_sale_record, operator_id, storage_record_type)

            # TODO: 暂将销售流水的成本价预留在此处

            # 生成批次消耗记录
            flow = models.GoodsBatchSaleFlow(
                batch_id=batch.id,
                fk_id=new_sale_record.id,
                fk_type=1,
                quantity=new_sale_record.commission_mul,
                weight=new_sale_record.sales_num * 100,
                total_price=new_sale_record.sales_money,
            )
            session.add(flow)

        return None

    @classmethod
    def _construct_consume_list(cls, default_batch, batch_queue, quantity,
                                weight, unit):
        """ 按照批次确定分货品的消耗量, 最早的数据放在队尾

        :rtype: list of dict
        """
        consume_list = []
        while batch_queue:
            current_batch = batch_queue.pop()
            update_quantity, update_weight = cls._cal_consume(
                current_batch, quantity, weight, unit)
            consume_list.insert(
                0,
                dict(
                    quantity=update_quantity,
                    weight=update_weight,
                    batch=current_batch))
            quantity -= update_quantity
            weight -= update_weight
            cls._update_batch_consume(current_batch, update_quantity,
                                      update_weight)
            if not quantity and not weight:
                break
        # 进行精度控制(所有数据都乘了100的整数进行运算)
        if abs(quantity) > 10 or abs(weight) > 10:
            consume_list.insert(
                0, dict(quantity=quantity, weight=weight, batch=default_batch))
            cls._update_batch_consume(default_batch, quantity, weight)
        return consume_list

    @classmethod
    def _cal_consume(cls, batch, quantity, weight, unit):
        """ 更新批次状态, 并计算消耗量, 根据售出单位走, 另外一个单位的数据根据比例扣减 """
        if unit == 1:
            if quantity >= 0:
                batch_left = batch.produce_quantity - batch.consume_quantity
            else:
                batch_left = -batch.consume_quantity
            consume_quantity = cls._cal_update_digit(batch, quantity,
                                                     batch_left)
            consume_weight = weight / quantity * consume_quantity if quantity else 0
        else:
            if weight >= 0:
                batch_left = batch.produce_weight - batch.consume_weight
            else:
                batch_left = -batch.consume_weight
            consume_weight = cls._cal_update_digit(batch, weight, batch_left)
            consume_quantity = quantity / weight * consume_weight if weight else 0
        return consume_quantity, consume_weight

    @classmethod
    def _update_batch_consume(cls, batch, quantity, weight):
        """ 扣减以后更新批次的消耗量 """
        batch.consume_quantity += quantity
        batch.consume_weight += weight
        return None

    @classmethod
    def _cal_update_digit(cls, batch, consume_digit, batch_left):
        """ 计算消耗量

        :rtype: float
        """
        if consume_digit >= 0:
            if consume_digit >= batch_left:
                consume_status = 2
                update_digit = batch_left
            else:
                consume_status = 1
                update_digit = consume_digit
        else:
            if consume_digit >= batch_left:
                consume_status = 1
                update_digit = consume_digit
            else:
                consume_status = 0
                update_digit = batch_left
        cls._update_batch_status(batch, consume_status)
        return update_digit

    @classmethod
    def _update_batch_status(cls, batch, status):
        """ 更新批次消耗状态 """
        batch.consume_status = status

    @classmethod
    def get_branch_goods_id_by_batch(cls, session, goods_list):
        """ 自营货品开票时, 根据先进先出的原则, 获取应该生成开票记录的分货id

        :param goods_list: list of `sa.Goods`

        :rtype: int
        """
        goods_id_list = [g.id for g in goods_list]
        goods_id_query = session \
            .query(models.GoodsBatch.goods_id) \
            .filter(
            models.GoodsBatch.goods_id.in_(goods_id_list),
            models.GoodsBatch.consume_status.in_([0, 1])) \
            .order_by(models.GoodsBatch.id) \
            .first() if goods_id_list else None
        result = goods_id_query.goods_id if goods_id_query else 0
        return result


# 商品分组
class GoodsGroupFunc():
    @classmethod
    def get_group_name_dict(cls, session, group_ids):
        GoodsGroup = models.GoodsGroup
        group_info = session.query(GoodsGroup.id, GoodsGroup.name) \
            .filter(GoodsGroup.id.in_(group_ids)) \
            .all()
        group_name_dict = {i.id: i.name for i in group_info}
        return group_name_dict

    @classmethod
    def get_groups_under_shop(cls, session, shop_id):
        """ 查询店铺下的所有商品分组, {group_id: group_name}形式返回

        :rtype: dict
        """
        groups = session.query(
            models.GoodsGroup.id,
            models.GoodsGroup.name).filter_by(shop_id=shop_id).filter(
                models.GoodsGroup.status.in_([-1, 0])).all()
        result = {g.id: g.name for g in groups}
        return result


# 自营货品主货/分货
class GoodsMasterBranchFunc():
    @classmethod
    def list_goods_id_master(cls, session, shop_id):
        """ 列出所有包含(自营主货/自营没有关联主货的分货)/代卖/联营/货主的货品id

        :rtype: set of int
        """
        query_base = session.query(models.Goods.id).filter_by(shop_id=shop_id)
        # 非自营货品
        query_unself = query_base.filter(
            models.Goods.supply_type.in_([1, 2, 3]))
        # 自营货品
        query_self_master = query_base.filter_by(supply_type=0).filter(
            models.Goods.is_master == 1)
        query_self_unmaster = query_base.filter_by(supply_type=0).filter(
            models.Goods.master_goods_id == 0)

        goods = query_unself.union(query_self_master).union(
            query_self_unmaster).all()
        result = {g.id for g in goods}
        return result

    @classmethod
    def list_goods_id_branch(cls, session, shop_id, master_goods_id_list):
        """ 列出店铺主货对应的所有分货的id

        :param master_goods_id_list: 所有主货的id

        :rtype: set of int
        """
        branch_goods = session.query(models.Goods.id).filter_by(shop_id=shop_id) \
            .filter(models.Goods.master_goods_id.in_(master_goods_id_list)) \
            .all()
        result = {g.id for g in branch_goods}
        return result

    @classmethod
    def list_map_branch_to_master(cls, session, shop_id):
        """ 列出分货品id到主货品id的映射

        :rtype: dict
        """
        goods = session.query(models.Goods) \
            .filter_by(shop_id=shop_id, is_master=0) \
            .filter(models.Goods.master_goods_id != 0) \
            .all()
        result = {g.id: g.master_goods_id for g in goods}
        return result

    @staticmethod
    def get_goods_base_query(session,
                             shop_id,
                             merge_branch_goods=True,
                             only_id=False):
        """ 货品基本查询，自营货品只取主货品 """
        if only_id:
            query_base = session.query(models.Goods.id)
        else:
            query_base = session.query(models.Goods)
        query_base = query_base.filter_by(shop_id=shop_id)
        if merge_branch_goods:
            self_master = query_base.filter(models.Goods.supply_type == 0,
                                            models.Goods.is_master == 1)
            self_sole_master = query_base.filter(
                models.Goods.supply_type == 0,
                models.Goods.master_goods_id == 0, models.Goods.is_master == 0)
            non_self = query_base.filter(models.Goods.supply_type != 0)
            query = self_master.union(non_self).union(self_sole_master)
        else:
            query = query_base
        return query

    @staticmethod
    def apply_filter(base_query, shop_id):
        """ 合并自营货品的供应商，传入基础请求前需要自行 join goods 表 """
        base_query.filter(models.Goods.shop_id == shop_id)
        self_master = base_query.filter(models.Goods.supply_type == 0,
                                        models.Goods.is_master == 1)
        self_sole_master = base_query.filter(models.Goods.supply_type == 0,
                                             models.Goods.master_goods_id == 0,
                                             models.Goods.is_master == 0)
        non_self = base_query.filter(models.Goods.supply_type != 0)
        query = self_master.union(non_self).union(self_sole_master)
        return query

    @classmethod
    def list_branch_goods(cls, session, shop_id, master_goods_id):
        """ 获取一个主货下的所有分货

        :rtype: list of `sa.Goods`
        """
        result = session.query(models.Goods) \
            .filter_by(shop_id=shop_id,
                       master_goods_id=master_goods_id,
                       is_master=0) \
            .all()
        return result

    @classmethod
    def match_branch_goods(cls, session, shop_id, master_goods_id_list):
        """ 获取主货对应的所有分货

        :rtype: dict of `{master_goods_id: [branch_goods,]}`
        """
        branch_goods_list = session.query(models.Goods) \
            .filter(models.Goods.shop_id == shop_id,
                    models.Goods.master_goods_id.in_(master_goods_id_list)) \
            .all()
        branch_goods_dict = defaultdict(list)
        for branch_goods in branch_goods_list:
            branch_goods_dict[branch_goods.master_goods_id].append(
                branch_goods)
        return result


class PubRecordFunc:
    @classmethod
    def get_record_num_from_id_list(cls,
                                    session,
                                    RecordClass,
                                    id_list,
                                    shop_id=None,
                                    goods_id=None):
        """ 根据id列表获取对应的num
        可用于GoodsSalesRecord,
             GoodsStockinRecord,
             GoodsBreakageRecord,
             GoodsInventoryRecord

        :param RecordClass: 被查询的数据库模型类

        :rtype: dict
        """
        base_query = session.query(RecordClass.id, RecordClass.num) \
            .filter(RecordClass.id.in_(id_list))
        if shop_id:
            base_query = base_query.filter_by(shop_id=shop_id)
        if goods_id:
            base_query = base_query.filter_by(goods_id=goods_id)
        return {id_: num for id_, num in base_query.all()}

    @classmethod
    def get_docs_id_from_num_list(cls,
                                  session,
                                  DocClass,
                                  num_list,
                                  shop_id=None):
        """ 根据num列表获取对应的单据id
        用于StockInDocs, BreakageDocs, InventoryDocs

        :param DocClass: 被查询的数据库模型类

        :rtype: dict
        """
        if shop_id:
            query = session.query(DocClass.id, DocClass.num).filter(
                DocClass.num.in_(num_list), DocClass.shop_id == shop_id).all()
        else:
            query = session.query(DocClass.id, DocClass.num).filter(
                DocClass.num.in_(num_list)).all()
        return {num: id_ for id_, num in query}


# 流水小票
class GoodsSaleFunc():
    # 生成票据编号
    @classmethod
    def gen_sale_record_num(cls, session, shop_id, base_time=None):
        if not base_time:
            base_time = datetime.datetime.now()
        key = "sale_record_num:%s:%s" % (shop_id, base_time.strftime("%y%m%d"))
        if redis.get(key):
            return str(redis.incr(key))
        else:
            sale_record = session.query(models.GoodsSalesRecord) \
                .filter_by(shop_id=shop_id)\
                .filter(
                    models.GoodsSalesRecord.bill_date == base_time.date(),
                    models.GoodsSalesRecord.num.op("regexp")("^[0-9]")) \
                .order_by(models.GoodsSalesRecord.id.desc()) \
                .first()
            sale_record_num = str(
                int(sale_record.get_num()) + 1) if sale_record else str(
                    shop_id) + base_time.strftime("%y%m%d") + '00001'
            redis.set(key, sale_record_num, 24 * 3600)
            return sale_record_num

    # 流水基本信息(结算相关)
    @classmethod
    def get_sale_record_basic_info(cls, session, sale_record, display_kg):
        """ 获取小票流水的量和价相关所有信息，主要用于重新开票

        :rtype: dict
        """
        check_float = NumFunc.check_float

        goods = models.Goods.get_by_id(session, sale_record.goods_id)
        sales_num = sale_record.sales_num / 2 \
            if display_kg and goods.unit == 0 else sale_record.sales_num
        fact_price = sale_record.fact_price * 2 \
            if display_kg and goods.unit == 0 else sale_record.fact_price
        result = dict(
            commission_mul=check_float(sale_record.commission_mul / 100),
            sales_num=check_float(sales_num),
            goods_id=sale_record.goods_id,
            commission=check_float(sale_record.commission / 100),
            deposit=check_float(sale_record.deposit_avg / 100),
            fact_price=check_float(fact_price / 100),
            receipt_money=check_float(sale_record.receipt_money / 100),
            shop_supplier_name=sale_record.shop_supplier_name,
            shop_supplier_id=goods.shop_supplier_id,
            commission_total=check_float(sale_record.commission_money / 100),
            pledge_total=check_float(sale_record.deposit_money / 100),
            goods_total=check_float(sale_record.sales_money / 100),
        )
        return result

    # 获取流水详情
    @classmethod
    def get_sale_record_info(cls,
                             session,
                             sale_record,
                             signature_img="",
                             weight_unit_text="斤",
                             print_erasement=False,
                             order_record_num=None,
                             repeat_print=False):
        """ 获取流水详情

        :param order_record_num: 订单号(多品)/流水号(单品), 用于接单的控制

        :rtype: dict
        """
        check_float = NumFunc.check_float
        goods = session.query(
            models.Goods).filter_by(id=sale_record.goods_id).first()
        supplier = session.query(
            models.ShopSupplier).filter_by(id=goods.shop_supplier_id).first()
        supplier_account = AccountFunc.get_account_through_id(
            session, supplier.account_id) if supplier else None
        sale_record_info = {}
        sale_record_info["id"] = sale_record.id
        sale_record_info["shop_id"] = sale_record.shop_id
        sale_record_info["goods_id"] = sale_record.goods_id
        sale_record_info["salesman_id"] = sale_record.salesman_id
        sale_record_info["full_bill_time"] = str(
            sale_record.bill_year) + '-' + str(
                sale_record.bill_month).zfill(2) + '-' + str(
                    sale_record.bill_day).zfill(2) + ' ' + str(
                        sale_record.bill_time)
        sale_record_info["num"] = sale_record.num
        sale_record_info[
            "order_record_num"] = order_record_num if order_record_num else sale_record.num
        sale_record_info["fact_price"] = check_float(
            sale_record.get_fact_price(
                goods.unit, weight_unit_text=weight_unit_text) / 100)
        sale_record_info["sales_num"] = check_float(
            sale_record.get_sales_num(
                goods.unit, weight_unit_text=weight_unit_text))
        sale_record_info["goods_sumup"] = check_float(
            sale_record.fact_price * sale_record.sales_num / 100)
        sale_record_info["commission"] = check_float(
            sale_record.commission / 100)
        sale_record_info["commission_mul"] = check_float(
            sale_record.commission_mul / 100)
        sale_record_info["commission_sumup"] = check_float(
            sale_record.commission * sale_record.commission_mul / 10000)
        sale_record_info["receipt_money"] = check_float(
            sale_record.receipt_money / 100)
        sale_record_info["tare_weight"] = check_float(
            sale_record.get_tare_weight(weight_unit_text=weight_unit_text))
        sale_record_info["gross_weight"] = check_float(
            sale_record.get_gross_weight(weight_unit_text=weight_unit_text))
        sale_record_info["record_type"] = sale_record.record_type
        sale_record_info["order_id"] = sale_record.order_id

        shop_name = session.query(
            models.Shop.shop_name).filter_by(id=sale_record.shop_id).scalar()
        sale_record_info["shop_name"] = shop_name

        # 货品信息
        sale_record_info["goods_name"] = GoodsFunc.get_goods_name_with_attr(
            goods)
        sale_record_info["goods_unit"] = goods.unit
        sale_record_info["goods_unit_text"] = GoodsFunc.getUnit(
            goods.unit, weight_unit_text=weight_unit_text)
        sale_record_info["goods_commodity_code"] = goods.commodity_code or ""
        sale_record_info["supply_type"] = goods.supply_type
        sale_record_info["supply_type_text"] = GoodsFunc.get_supply_type(
            goods.supply_type)

        # 供货商相关
        sale_record_info["shop_supplier_name"] = sale_record.shop_supplier_name
        sale_record_info["active_goods_owner"] \
            = 1 if supplier and supplier.active_goods_owner == 1 else 0
        sale_record_info["supplier_headimg"] \
            = supplier_account.head_imgurl if supplier_account else ""

        sale_record_info["salesman_name"] = AccountFunc.get_account_name(
            session, sale_record.salesman_id)
        sale_record_info["deposit"] = check_float(
            sale_record.deposit_avg / 100)
        sale_record_info["deposit_total"] = check_float(
            sale_record.deposit_money / 100)
        sale_record_info["remark"] = sale_record.remark
        sale_record_info["image_remark"] = sale_record.full_image_remark

        # 送货详情
        courierrecordaccesser = CourierRecordAccesser(session)
        courier_record = courierrecordaccesser.get_courier_record_through_id(
            sale_record.courier_record_id)
        courier_id = 0
        courier_name = ""
        courier_headimgurl = ""
        courier_remark = ""
        courier_image_remark = []
        courier_status_text = ""
        courier_finish_time = ""
        courier_msg_notify = 0
        if courier_record:
            courier_id = courier_record.courier_id
            courier = AccountFunc.get_account_through_id(session, courier_id)
            courier_name = courier.realname or courier.nickname or ""
            courier_headimgurl = courier.headimgurl
            courier_remark = courier_record.remark
            courier_image_remark = courier_record.image_remark.split(
                ",") if courier_record.image_remark else []
            courier_status_text = courier_record.get_status_text
            courier_finish_time = courier_record.finish_time.strftime(
                "%Y-%m-%d %H:%M:%S")
            courier_msg_notify = courier_record.msg_notify
        sale_record_info["courier_id"] = courier_id
        sale_record_info["courier_name"] = courier_name
        sale_record_info["courier_headimgurl"] = courier_headimgurl
        sale_record_info["courier_remark"] = courier_remark
        sale_record_info["courier_image_remark"] = courier_image_remark
        sale_record_info["courier_status_text"] = courier_status_text
        sale_record_info["courier_finish_time"] = courier_finish_time
        sale_record_info["courier_msg_notify"] = courier_msg_notify

        # 单品小票上加上多品订单的备注
        if sale_record.multi_order_id:
            multi_order = session \
                .query(models.Order) \
                .filter_by(id=sale_record.multi_order_id) \
                .first()
            if multi_order and multi_order.remark:
                sale_record_info["remark"] = multi_order.remark
                sale_record_info[
                    "image_remark"] = multi_order.full_image_remark
        sale_record_info["pay_status"] = cls._get_sales_record_pay_status(
            sale_record)
        sale_record_info[
            "pay_status_text"] = cls.get_sales_record_pay_status_text(
                sale_record)
        sale_record_info["printer_num"] = sale_record.printer_num
        sale_record_info["printer_remark"] = sale_record.printer_remark
        sale_record_info["pay_type_text"] = OrderBaseFunc.pay_type_text(
            sale_record.pay_type) if sale_record.order_id else "-"

        sale_record_info["avg_price"] = check_float(
            sale_record.sales_money / sale_record.commission_mul
        ) if goods.unit == 0 and sale_record.commission_mul else 0

        if sale_record.goods_batch_id:
            goods_batch = session \
                .query(models.GoodsBatch) \
                .filter_by(id=sale_record.goods_batch_id) \
                .first()
            sale_record_info["batch_num"] = goods_batch.batch_num
        else:
            sale_record_info["batch_num"] = ""

        if sale_record.accountant_id:
            sale_record_info["accountant_name"] = AccountFunc.get_account_name(
                session, sale_record.accountant_id)
            sale_record_info["accountant_time"] = TimeFunc.time_to_str(
                sale_record.order_time)
            sale_record_info["accountant_id"] = sale_record.accountant_id
        else:
            sale_record_info["accountant_name"] = ""
            sale_record_info["accountant_time"] = ""
            sale_record_info["accountant_id"] = 0

        if sale_record.shop_customer_id:
            debtor = session.query(models.ShopCustomer).filter_by(
                id=sale_record.shop_customer_id).first()
            sale_record_info["shop_customer_id"] = debtor.id
            sale_record_info["shop_customer"] = '%s %s ' % (debtor.name,
                                                            debtor.company)
        else:
            sale_record_info["shop_customer_id"] = 0
            sale_record_info["shop_customer"] = ""

        if sale_record.order_id:
            pay_type = sale_record.pay_type
            order_id = sale_record.order_id
            combine_pay_dict = OrderBaseFunc.multi_combine_pay_type_text(
                session, [order_id])
            pay_type_text = OrderBaseFunc.order_pay_type_text(
                order_id, pay_type, combine_pay_dict)

            if pay_type == 12:
                bank_name = OrderBaseFunc.get_order_bank_name(
                    session, order_id, source="order")
                if bank_name:
                    pay_type_text = "{0}({1})".format(pay_type_text, bank_name)

            sale_record_info["pay_type_text"] = pay_type_text
            sale_record_info[
                "combine_pay_type_text"] = OrderBaseFunc.combine_pay_type_text(
                    session, sale_record.order_id) if pay_type == 10 else ""
            sale_record_info[
                "signature_img"] = signature_img if pay_type == 9 else ""
        else:
            sale_record_info["pay_type_text"] = ""
            sale_record_info["combine_pay_type_text"] = ""
            sale_record_info["signature_img"] = ""

        sale_record_info["origin_pay_type_text"] = ""
        if sale_record.order_id:
            order = session \
                .query(
                    models.Order.fact_total_price,
                    models.Order.erase_money,
                    models.Order.record_type,
                    models.Order.customer_signature) \
                .filter(models.Order.id == sale_record.order_id) \
                .first()
            if order and order.erase_money > 0:
                fact_total_price, erase_money, record_type, *_ = order
                fact_total_price = check_float(
                    (-fact_total_price
                     if record_type == 1 else fact_total_price) / 100)
                erase_money = check_float(
                    (-erase_money if record_type == 1 else erase_money) / 100)
                if print_erasement:
                    sale_record_info["erase_money"] = erase_money
                else:
                    sales_sum_count = session.query(
                        func.count(models.GoodsSalesRecord.id)).filter_by(
                            order_id=sale_record.order_id).scalar() or 0
                    sale_record_info["erase_text"] = "%d个货品应收%s元，合计抹零%s元" % (
                        sales_sum_count, fact_total_price, erase_money)

            # 原始支付方式（仅修改过支付方式的有该值）
            sale_record_info[
                "origin_pay_type_text"] = PaytypeModifyFunc.get_origin_paytype_text(
                    session, sale_record.order_id)

            # 重复打印取签名
            if repeat_print and sale_record.pay_type == 9 and order.customer_signature:
                sale_record_info[
                    "signature_img"] = CustomerSignature.get_signature_image(
                        sale_record.order_id)

        return sale_record_info

    # 多品订单下的单品信息
    @classmethod
    def get_multi_order_info(cls, session, records, order, multi_id=None):
        """ 通过一个多品订单下的所有小票流水信息获取多品订单的信息

        :param records: list of `class GoodsSalesRecord`, 需要注意这些流水必须是在同一个多品订单之下
        :param order: `class Order` 订单对象

        :return goods_list: 多品订单下的所有商品的信息
        :return order_total_dict: 订单的一些汇总信息
        :rtype: tuple
        """
        weight_unit_text = ConfigFunc.get_weight_unit_text(
            session, order.shop_id)
        # 批量获取货品串货调整信息
        origin_record_id_list = {x.id for x in records}
        ajust_record_id_list = SalesRecordAjustFunc.get_multi_ajust_info(
            session, origin_record_id_list)

        detail_record_list = []
        for r in records:
            if r.id in ajust_record_id_list:
                ajust = 1
            else:
                ajust = 0
            record_dict = cls.get_sale_record_info(
                session, r, weight_unit_text=weight_unit_text)
            record_dict["ajust"] = ajust
            detail_record_list.append(record_dict)

        order_total_dict = {}
        for key in [
                "accountant_name", "accountant_time", "full_bill_time",
                "pay_status", "pay_status_text", "printer_num",
                "printer_remark", "salesman_name", "shop_customer",
                "shop_customer_id", "salesman_id", "accountant_id",
                "pay_type_text", "origin_pay_type_text", "courier_id",
                "courier_name", "courier_headimgurl", "courier_remark",
                "courier_image_remark", "courier_status_text",
                "courier_finish_time", "courier_msg_notify"
        ]:
            # 第一条记录即为整个订单的信息, 翻页后为空会报错
            try:
                order_total_dict[key] = detail_record_list[0][key]
            except IndexError:
                return list(), dict()
        order_total_dict["num"] = order.num
        order_total_dict["receipt_money"] = check_float(
            order.total_price / 100)
        order_total_dict["total_commission_mul"] = 0
        order_total_dict["goods_count"] = 0
        order_total_dict["total_goods_sum"] = 0
        order_total_dict["remark"] = order.remark
        order_total_dict["image_remark"] = order.full_image_remark

        if multi_id:
            order_total_dict["multi_id"] = multi_id

        goods_list = []
        for detail in detail_record_list:
            goods_list.append({
                "commission_mul":
                detail["commission_mul"],
                "name":
                detail["goods_name"],
                "record_id":
                detail["id"],
                "sales_num":
                detail["sales_num"] if detail["goods_unit"] == 0 else "-",
                "total_money":
                detail["receipt_money"],
                "unit_price":
                detail["fact_price"],
                "goods_unit_text":
                detail["goods_unit_text"],
                "goods_id":
                detail["goods_id"],
                "num":
                detail["num"],
                "ajust":
                detail["ajust"]
            })
            order_total_dict["total_commission_mul"] += detail[
                "commission_mul"]
            order_total_dict["goods_count"] += 1
            order_total_dict["total_goods_sum"] += \
                (detail["sales_num"] if detail["goods_unit"] == 0 else 0)
        return goods_list, order_total_dict

    @classmethod
    def _get_sales_record_pay_status(cls, record):
        """ 获取小票流水支付状态的状态码
        -1：已作废，1：未结算，2：结算中，3：已结算，4：已退单

        :param record: class `GoodsSalesRecord` object 小票流水

        :rtype: int
        """
        if record.status == -1:
            result = -1
        elif record.status == 1:
            result = 1
        elif record.status == 2:
            result = 2
        elif record.status in (3, 4, 5):
            result = 3
        else:
            result = None
        return result

    @classmethod
    def get_sales_record_pay_status_text(cls, record):
        """ 获取小票流水支付状态的状态文字

        :param record: class `GoodsSalesRecord` object 小票流水

        :rtype: int
        """
        map_pay_status = {
            -1: "已作废",
            1: "未结算",
            2: "结算中",
            3: "已结算",
            4: "已退单",
        }
        result = map_pay_status[cls._get_sales_record_pay_status(record)]
        return result

    # 获取一单多品订单小票相应信息,
    # 2018-07-25 为兼容收银台小票合并打印, 改为同时支持取结算订单, 而非仅一单多品订单, 通过is_merge参数控制
    @classmethod
    def get_temp_sale_order_info(cls,
                                 session,
                                 sale_record_list,
                                 order,
                                 source="",
                                 pay_order=None,
                                 is_merge=False,
                                 repeat_print=False):
        """ 从order上获取相关信息

        :param is_merge: 是否合并打印, 0: 不是, 1: 是
        """
        if is_merge and source != "accountant":
            raise ValueError("仅收银台支持合并打印")
        weight_unit_text = ConfigFunc.get_weight_unit_text(
            session, order.shop_id)
        sale_record_info_list = []
        salesman_name = ""
        shop_name = ""
        total_recipt_money = 0
        total_goods_type_count = 0
        total_commission_mul = 0
        total_goods_type_list = []
        total_remark = set()
        for sale_record in sale_record_list:
            sale_record_info = cls.get_sale_record_info(
                session, sale_record, weight_unit_text=weight_unit_text)
            sale_record_info_list.append(sale_record_info)
            total_remark.add(sale_record_info["remark"])
            if not salesman_name:
                salesman_name = sale_record_info["salesman_name"]
            if not shop_name:
                shop_name = sale_record_info["shop_name"]
            total_goods_type_list.append(sale_record.goods_id)
            total_commission_mul += sale_record_info["commission_mul"]
        total_goods_type_count = len(list(set(total_goods_type_list)))
        total_commission_mul = check_float(total_commission_mul)

        shop_id = order.shop_id
        customer_id = order.debtor_id
        customer_name = ""
        customer_company = ""
        if customer_id:
            customer = ShopCustomerFunc.get_shop_customer_through_id(
                session, shop_id, customer_id)
            customer_name = customer.name
            customer_company = customer.company

        erase_text = ""
        tally_order_id = order.tally_order_id
        # 合并小票打印整个小票的信息
        if is_merge:
            tally_order_id = order.id
        if tally_order_id:
            tally_order = session \
                .query(
                    models.Order.fact_total_price,
                    models.Order.erase_money,
                    models.Order.record_type,
                    models.Order.customer_signature) \
                .filter(models.Order.id == tally_order_id) \
                .first()
            if tally_order and tally_order.erase_money > 0:
                fact_total_price, erase_money, record_type, *_ = tally_order
                fact_total_price = check_float(
                    (-fact_total_price
                     if record_type == 1 else fact_total_price) / 100)
                erase_money = check_float(
                    (-erase_money if record_type == 1 else erase_money) / 100)

                sales_sum_count = session.query(
                    func.count(models.GoodsSalesRecord.id)).filter_by(
                        order_id=tally_order_id).scalar() or 0
                erase_text = "%d个货品应收%s元，合计抹零%s元" % (
                    sales_sum_count, fact_total_price, erase_money)

        if source == "salesman":
            temp_order_info = {
                "num":
                order.num,
                "time":
                TimeFunc.time_to_str(order.create_time),
                "salesman_name":
                salesman_name,
                "shop_name":
                shop_name,
                "total_recipt_money":
                NumFunc.check_float(order.total_price / 100),
                "total_goods_type_count":
                total_goods_type_count,
                "shop_id":
                shop_id,
                "customer_name":
                customer_name,
                "customer_company":
                customer_company,
                "remark":
                order.remark,
                "total_commission_mul":
                total_commission_mul,
                "record_type":
                order.record_type,
                "erase_text":
                erase_text
            }
        else:
            shop_name = session.query(
                models.Shop.shop_name).filter_by(id=shop_id).scalar()
            # 合并打印取所有备注, 用个不常用的符号连起来便于后续处理
            merge_remarks = "^".join(total_remark)
            # 支付类型要用支付订单获取
            pay_type = pay_order.pay_type
            pay_type_text = OrderBaseFunc.pay_type_text(pay_type)
            combine_pay_type_text = OrderBaseFunc.combine_pay_type_text(
                session, pay_order.id) if pay_type == 10 else ""

            if pay_order.pay_type == 12 and pay_order.bank_no:
                bank_name = OrderBaseFunc.get_order_bank_name(
                    session, pay_order.id, source="order")
                pay_type_text = "{0}({1})".format(pay_type_text, bank_name)

            accountant_name = AccountFunc.get_account_name(
                session, pay_order.accountant_id)
            accountant_time = str(pay_order.pay_year) + '-' + str(
                pay_order.pay_month).zfill(2) + '-' + str(
                    pay_order.pay_day).zfill(2) + ' ' + str(pay_order.pay_time)

            temp_order_info = {
                "num":
                order.num,
                "time":
                TimeFunc.time_to_str(order.create_time),
                "accountant_time":
                accountant_time,
                "shop_name":
                shop_name,
                "total_recipt_money":
                NumFunc.check_float(order.fact_total_price / 100),
                "total_goods_type_count":
                total_goods_type_count,
                "shop_id":
                shop_id,
                "pay_type_text":
                pay_type_text,
                "combine_pay_type_text":
                combine_pay_type_text,
                "salesman_name":
                salesman_name,
                "accountant_name":
                accountant_name,
                "customer_name":
                customer_name,
                "customer_company":
                customer_company,
                "remark":
                order.remark or merge_remarks,
                "total_commission_mul":
                total_commission_mul,
                "record_type":
                order.record_type,
                "erase_text":
                erase_text
            }
            # 结算打印签名从缓存中取
            redis_key = "signature_img:{}".format(tally_order_id)
            temp_order_info["signature_img"] = (redis.get(redis_key)
                                                or b"").decode()
            # 重复打印取签名
            if repeat_print and pay_type == 9 and tally_order_id:
                if tally_order.customer_signature:
                    temp_order_info[
                        "signature_img"] = CustomerSignature.get_signature_image(
                            tally_order_id)
        return sale_record_info_list, temp_order_info

    @classmethod
    def get_refund_goods_sale_record(cls, session, origin_record_id):
        '''
            获取退单关联信息
        '''
        refund_goods_sale_record = session.query(models.GoodsSalesRecordRefundInfo) \
            .filter_by(origin_goods_sale_record_id=origin_record_id) \
            .first()
        return refund_goods_sale_record

    @classmethod
    def get_all_sale_record_through_order_id(cls, session, order_id):
        '''
            根据订单ID获取订单对应的所有流水小票
        '''
        record_list = session.query(models.GoodsSalesRecord) \
            .filter_by(order_id=order_id) \
            .filter() \
            .all()
        return record_list

    @classmethod
    def get_sale_record_through_order(cls, session, order_id):
        '''
            根据订单ID获取订单对应非一单多品的单品流水小票
        '''
        GoodsSalesRecord = models.GoodsSalesRecord
        record_list = session.query(GoodsSalesRecord).filter_by(
            order_id=order_id).all()
        return record_list

    @classmethod
    def query_sales_record_by_billing_record(cls,
                                             start_date,
                                             end_date,
                                             session,
                                             page,
                                             shop_id,
                                             salesman_id_list,
                                             accountant_id_list,
                                             pay_type_list,
                                             show_type,
                                             only_refund,
                                             only_paid=True,
                                             only_unpaid=False,
                                             all_page=False,
                                             export=False,
                                             goods_ids=None,
                                             customer_id_list=None):
        """ 通过BillingSalesRecord查询GoodsSalesRecord的列表, 默认全部已结算

        :param start_date: 起止日
        :param end_date: 结束日
        :param session: session
        :param page: 页数
        :param shop_id: 商铺ID
        :param salesman_id_list: 开票员ID，如果指定，根据该开票员过滤
        :param accountant_id_list: 收银员ID，如果指定，根据该结算员过滤
        :param pay_type_list: 支付方式，如果指定，则根据支付方式过滤
        :param show_type: 结账展示方式
        :param only_refund: 是否只看退单
        :param only_paid: 是否只返回已结算商品结算商品
        :param only_unpaid: 是否只返回未结算的票据, 且不包含已作废的票据
        :param all_page: 是否需要返回总页数
        :param export: 是否导出
        :param goods_ids: 货品ID，如果指定，根据货品过滤

        :return: List of SqlAlchemy，及可能的page_sum总页数
        """
        page_size = 20

        BillingSalesRecord = models.BillingSalesRecord
        GoodsSalesRecord = models.GoodsSalesRecord

        # 过滤店铺
        query_base = session.query(models.BillingSalesRecord) \
            .filter_by(shop_id=shop_id)
        # 通过收银信息判断小票是否已结算
        if only_paid and only_unpaid:
            raise ValueError("结算参数错误, only_paid & only_unpaid")
        elif only_paid:
            query_base = query_base.filter(
                BillingSalesRecord.accountant_id > 0)
        elif only_unpaid:
            query_base = query_base.filter(
                BillingSalesRecord.accountant_id == 0)
        # 过滤收银员
        if accountant_id_list:
            query_base = query_base.filter(
                BillingSalesRecord.accountant_id.in_(accountant_id_list))
        # 过滤开票员
        if salesman_id_list:
            query_base = query_base.filter(
                BillingSalesRecord.salesman_id.in_(salesman_id_list))
        # 过滤结算方式
        if pay_type_list:
            query_base = query_base.filter(
                BillingSalesRecord.pay_type.in_(pay_type_list))

        # 扎账日
        if show_type == 1:
            # 开票员今日数据需要展示轧帐以后的，由前端控制
            query_base = query_base.filter(
                BillingSalesRecord.account_period_date >= start_date,
                BillingSalesRecord.account_period_date <= end_date)
        # 自然日
        else:
            query_base = query_base.filter(
                BillingSalesRecord.bill_date >= start_date,
                BillingSalesRecord.bill_date <= end_date)
        # 退单情况过滤, 未支付中已作废情况过滤
        if only_refund or only_unpaid:
            query_base1 = query_base.join(
                GoodsSalesRecord,
                GoodsSalesRecord.id == BillingSalesRecord.record_id).filter(
                    BillingSalesRecord.type_id == 0)
            if only_refund:
                query_base1 = query_base1.filter(
                    GoodsSalesRecord.record_type == 1)
            if only_unpaid:
                query_base1 = query_base1.filter(GoodsSalesRecord.status == 1)

            query_base2 = query_base.join(
                GoodsSalesRecord,
                GoodsSalesRecord.multi_order_id == BillingSalesRecord.
                record_id).filter(BillingSalesRecord.type_id == 1)
            if only_refund:
                query_base2 = query_base2.filter(
                    GoodsSalesRecord.record_type == 1)
            if only_unpaid:
                query_base2 = query_base2.filter(GoodsSalesRecord.status == 1)
            query_base = query_base1.union(query_base2)

        # 获取记录中所有的客户ID/货品ID
        all_single_record_id_list = [
            x.record_id for x in query_base.all() if x.type_id == 0
        ]
        single_goods_customer = []
        if all_single_record_id_list:
            single_goods_customer = session \
                .query(GoodsSalesRecord.goods_id,
                   GoodsSalesRecord.shop_customer_id) \
                .filter(GoodsSalesRecord.id.in_(all_single_record_id_list)) \
                .all()

        all_multi_record_id_list = [
            x.record_id for x in query_base.all() if x.type_id == 1
        ]
        multi_goods_customer = []
        if all_multi_record_id_list:
            multi_goods_customer = session \
                .query(
                    GoodsSalesRecord.goods_id,
                    GoodsSalesRecord.shop_customer_id) \
                .filter(GoodsSalesRecord.multi_order_id.in_(all_multi_record_id_list)) \
                .all()
        origin_record = set(single_goods_customer + multi_goods_customer)

        # 导出不分页
        query_base = query_base.order_by(BillingSalesRecord.id.desc())

        if all_page:
            page_sum = math.ceil(query_base.count() / page_size)

        if not export:
            query_base = query_base.offset(page * page_size).limit(page_size)

        record = query_base.all()

        if record:
            single_record_id_list = [
                x.record_id for x in record if x.type_id == 0
            ]
            multi_record_id_list = [
                x.record_id for x in record if x.type_id == 1
            ]

            # 必须在此处过滤掉single_record_id_list和multi_record_id_list为空的情况
            # 否则mysql5.6解析的过程中会将空查询解析成 1 != 1从而产生慢查询
            record1 = None
            record2 = None
            if single_record_id_list:
                record1 = session.query(GoodsSalesRecord).filter(
                    GoodsSalesRecord.id.in_(single_record_id_list),
                    GoodsSalesRecord.shop_id == shop_id)
            if multi_record_id_list:
                record2 = session.query(GoodsSalesRecord).filter(
                    GoodsSalesRecord.multi_order_id.in_(multi_record_id_list),
                    GoodsSalesRecord.shop_id == shop_id)
                if goods_ids:
                    multi_goods_ids_dcit = {}
                    multi_goods_ids = []
                    new_multi_record_id_list = []
                    for _sale_record in record2.all():
                        _multi_order_id = _sale_record.multi_order_id
                        if _multi_order_id not in multi_goods_ids_dcit:
                            multi_goods_ids_dcit[_multi_order_id] = []
                        multi_goods_ids_dcit[_multi_order_id].append(
                            _sale_record.goods_id)
                    for _multi_order_id in multi_goods_ids_dcit:
                        temp_goods_ids = multi_goods_ids_dcit[_multi_order_id]
                        # 判断goods_id交集
                        if list((set(goods_ids).union(set(temp_goods_ids))) ^ (
                                set(goods_ids) ^ set(temp_goods_ids))):
                            multi_goods_ids += temp_goods_ids
                            new_multi_record_id_list.append(_multi_order_id)

            # 过滤掉货品
            if goods_ids:
                if record1:
                    record1 = record1.filter(
                        GoodsSalesRecord.goods_id.in_(goods_ids))
                if record2:
                    record2 = record2.filter(GoodsSalesRecord.goods_id.in_(multi_goods_ids)) \
                        .filter(GoodsSalesRecord.multi_order_id.in_(new_multi_record_id_list))

            if record1 and record2:
                record = record1.union(record2).order_by(
                    GoodsSalesRecord.id.desc()).all()
            elif record1:
                record = record1.order_by(GoodsSalesRecord.id.desc()).all()
            elif record2:
                record = record2.order_by(GoodsSalesRecord.id.desc()).all()
            else:
                record = []

        if all_page:
            return record, page_sum, origin_record
        else:
            return record

    @classmethod
    def query_sales_record_by_billing_record_new(cls,
                                                 start_date,
                                                 end_date,
                                                 session,
                                                 page,
                                                 shop_id,
                                                 salesman_id_list,
                                                 accountant_id_list,
                                                 pay_type_list,
                                                 show_type,
                                                 only_refund,
                                                 only_paid=True,
                                                 only_unpaid=False,
                                                 all_page=False,
                                                 export=False,
                                                 goods_ids=None,
                                                 customer_id_list=None):
        """ 重构query_sales_record_by_billing_record函数代码
        思路：通过GoodsSalesRecord 获取 Order,然后获取对应的 BillingSalesRecord
        :param start_date: 起止日
        :param end_date: 结束日
        :param session: session
        :param page: 页数
        :param shop_id: 商铺ID
        :param salesman_id_list: 开票员ID，如果指定，根据该开票员过滤
        :param accountant_id_list: 收银员ID，如果指定，根据该结算员过滤
        :param pay_type_list: 支付方式，如果指定，则根据支付方式过滤
        :param show_type: 结账展示方式
        :param only_refund: 是否只看退单
        :param only_paid: 是否只返回已结算商品结算商品
        :param only_unpaid: 是否只返回未结算的票据, 且不包含已作废的票据
        :param all_page: 是否需要返回总页数
        :param export: 是否导出

        :return: List of SqlAlchemy，及可能的page_sum总页数
        """
        # TODO 逐步替换掉query_sales_record_by_billing_record
        page_size = 20
        # 获取所有的客户id和货品id，开票员id，收银员id，继而获得名字
        goods_id_set = set()
        customer_id_set = set()
        accountant_id_set = set()
        salesman_id_set = set()
        # 获取到所有符合要求的票据流水和货品流水及票据流水Id
        billing_record_id_list = list()
        goods_record_dict = dict()
        billing_record_dict = dict()
        multi_record_order_id_set = set()

        goods_name_unit_dict = dict()
        goods_name_dict = dict()
        goods_unit_dict = dict()
        goods_unit_text_dict = dict()
        goods_supply_type_dict = dict()
        goods_supply_type_text_dict = dict()

        BillingSalesRecord = models.BillingSalesRecord
        GoodsSalesRecord = models.GoodsSalesRecord

        # 默认筛选条件 + 数据展示类型的过滤条件
        base_filter_list = [BillingSalesRecord.shop_id == shop_id]
        # 用户选择的过滤方式
        diy_filter_list = []
        goods_diy_filter_list = []

        # 扎账日
        if show_type == 1:
            # 开票员今日数据需要展示轧帐以后的，由前端控制
            base_filter_list += [
                BillingSalesRecord.account_period_date >= start_date,
                BillingSalesRecord.account_period_date <= end_date
            ]
            filter_param_one = GoodsSalesRecord.account_period_date >= start_date
            filter_param_two = GoodsSalesRecord.account_period_date <= end_date
        # 自然日
        else:
            base_filter_list += [
                BillingSalesRecord.bill_date >= start_date,
                BillingSalesRecord.bill_date <= end_date
            ]
            filter_param_one = GoodsSalesRecord.bill_date >= start_date
            filter_param_two = GoodsSalesRecord.bill_date <= end_date

        # 通过收银信息判断小票是否已结算
        filter_param_three = None
        if only_paid and only_unpaid:
            raise ValueError("结算参数错误, only_paid & only_unpaid")
        elif only_paid:
            base_filter_list.append(BillingSalesRecord.accountant_id > 0)
            filter_param_three = and_(
                GoodsSalesRecord.status.in_([3, 4, 5]),
                GoodsSalesRecord.accountant_id > 0)
            if only_refund:
                filter_param_three = GoodsSalesRecord.status.in_(
                    [1, 2, 3, 4, 5])
        elif only_unpaid:
            base_filter_list.append(BillingSalesRecord.accountant_id == 0)
            filter_param_three = and_(
                GoodsSalesRecord.status.in_([1, 2]),
                GoodsSalesRecord.accountant_id <= 0)

        # 联合models.GoodsSalesRecord表，目的是筛选货品和客户, 冗余筛选条件减少GoodsSalesRecord查询量
        first_join = [
            GoodsSalesRecord,
            and_(
                or_(
                    and_(BillingSalesRecord.record_id == GoodsSalesRecord.id,
                         BillingSalesRecord.type_id == 0),
                    and_(
                        BillingSalesRecord.record_id == GoodsSalesRecord.
                        multi_order_id, BillingSalesRecord.type_id == 1)),
                GoodsSalesRecord.shop_id == shop_id, filter_param_one,
                filter_param_two, filter_param_three)
        ]

        # 过滤收银员
        if accountant_id_list:
            diy_filter_list.append(
                BillingSalesRecord.accountant_id.in_(accountant_id_list))
            goods_diy_filter_list.append(
                GoodsSalesRecord.accountant_id.in_(accountant_id_list))
        # 过滤开票员
        if salesman_id_list:
            diy_filter_list.append(
                BillingSalesRecord.salesman_id.in_(salesman_id_list))
            goods_diy_filter_list.append(
                GoodsSalesRecord.salesman_id.in_(salesman_id_list))
        # 过滤结算方式
        if pay_type_list:
            diy_filter_list.append(
                BillingSalesRecord.pay_type.in_(pay_type_list))
            goods_diy_filter_list.append(
                GoodsSalesRecord.pay_type.in_(pay_type_list))

        # 基础查询，沿用以前版本的逻辑, 这一段代码的目的是获取以billing_record的id为主键的字典，
        # 值分别为goods_sale_record和billing_record
        if not (only_unpaid or only_refund or goods_ids or customer_id_list
                or export):
            query_base = session.query(models.BillingSalesRecord) \
                .filter(*base_filter_list)
            temp_result = query_base.all()
            # 获取记录中所有的客户ID/货品ID
            all_single_record_id_list = [
                x.record_id for x in temp_result if x.type_id == 0
            ]
            single_goods_customer = []
            if all_single_record_id_list:
                single_goods_customer = session \
                    .query(GoodsSalesRecord.goods_id,
                           GoodsSalesRecord.shop_customer_id) \
                    .filter(GoodsSalesRecord.id.in_(all_single_record_id_list)) \
                    .all()

            all_multi_record_id_list = [
                x.record_id for x in temp_result if x.type_id == 1
            ]
            multi_goods_customer = []
            if all_multi_record_id_list:
                multi_goods_customer = session \
                    .query(
                    GoodsSalesRecord.goods_id,
                    GoodsSalesRecord.shop_customer_id) \
                    .filter(GoodsSalesRecord.multi_order_id.in_(all_multi_record_id_list)) \
                    .all()
            customer_goods_id_set = set(single_goods_customer +
                                        multi_goods_customer)

            for item in customer_goods_id_set:
                customer_id_set.add(item[1])
                goods_id_set.add(item[0])

            for item in temp_result:
                salesman_id_set.add(item.salesman_id)
                accountant_id_set.add(item.accountant_id)
            if diy_filter_list:
                query_base = query_base.filter(*diy_filter_list).order_by(
                    BillingSalesRecord.id.desc())
            else:
                query_base = query_base.order_by(BillingSalesRecord.id.desc())
            page_sum = math.ceil(query_base.count() / page_size)
            query_base = query_base.offset(page * page_size).limit(page_size)
            billing_records = query_base.all()
            goods_sale_records = []
            return_billing_record_id = []
            if billing_records:
                single_record_id_list = []
                multi_record_id_list = []
                # 获取billing_record字典
                for item in billing_records:
                    if item.type_id == 0:
                        single_record_id_list.append(item.record_id)
                    else:
                        multi_record_id_list.append(item.record_id)
                    return_billing_record_id.append(item.id)
                    billing_record_dict[item.id] = item
                # 必须在此处过滤掉single_record_id_list和multi_record_id_list为空的情况
                # 否则mysql5.6解析的过程中会将空查询解析成 1 != 1从而产生慢查询
                record1 = None
                record2 = None
                if single_record_id_list:
                    record1 = session.query(GoodsSalesRecord).filter(
                        GoodsSalesRecord.id.in_(single_record_id_list),
                        GoodsSalesRecord.shop_id == shop_id)
                if multi_record_id_list:
                    record2 = session.query(GoodsSalesRecord).filter(
                        GoodsSalesRecord.multi_order_id.in_(
                            multi_record_id_list),
                        GoodsSalesRecord.shop_id == shop_id)
                if record1 and record2:
                    goods_sale_records = record1.union(record2).order_by(
                        GoodsSalesRecord.id.desc()).all()
                elif record1:
                    goods_sale_records = record1.order_by(
                        GoodsSalesRecord.id.desc()).all()
                elif record2:
                    goods_sale_records = record2.order_by(
                        GoodsSalesRecord.id.desc()).all()
                # 获取goods_sale_record字典
                for item in goods_sale_records:
                    for temp in billing_records:
                        if temp.type_id == 0 and temp.record_id == item.id or temp.type_id == 1\
                                and temp.record_id == item.multi_order_id:
                            goods_list = goods_record_dict.get(temp.id, list())
                            goods_list.append(item)
                            goods_record_dict[temp.id] = goods_list
                        if temp.type_id:
                            multi_record_order_id_set.add(item.multi_order_id)
        # 要支持货品id或者操作人筛选，那么需要join
        else:
            # 退单情况过滤
            if only_refund:
                base_filter_list.append(GoodsSalesRecord.record_type == 1)
            base_query = session.query(
                BillingSalesRecord,
                GoodsSalesRecord).join(*first_join).filter(*base_filter_list)

            # 如果不是导出，那么需要将所有用户货品的信息返回，不包括用户的筛选条件，导出的话就只用返回需要的部分，就不需要下面的这次查询
            if not export:
                base_result = base_query.all()
                for item in base_result:
                    goods_id_set.add(item[1].goods_id)
                    customer_id_set.add(item[1].shop_customer_id)
                    accountant_id_set.add(item[0].accountant_id)
                    salesman_id_set.add(item[0].salesman_id)
                    billing_record_id = item[0].id
                    if billing_record_id not in billing_record_id_list:
                        billing_record_id_list.append(billing_record_id)
                    goods = item[1]  # type: models.GoodsSalesRecord
                    # 一个票据中可能有多个货品，多品
                    goods_list = goods_record_dict.get(billing_record_id,
                                                       list())
                    goods_list.append(goods)
                    goods_record_dict[billing_record_id] = goods_list
                    billing_record_dict[billing_record_id] = item[0]
                    if item[0].type_id:
                        multi_record_order_id_set.add(item[1].multi_order_id)

            # 过滤客户
            if customer_id_list:
                diy_filter_list.append(
                    GoodsSalesRecord.shop_customer_id.in_(customer_id_list))
            # 过滤货品
            if goods_ids:
                diy_filter_list.append(
                    GoodsSalesRecord.goods_id.in_(goods_ids))

            # 返回的票据流水中的主键列表
            return_billing_record_id = []
            # 只需要获取符合所有筛选条件的资料，和非导出区分的原因是减少查询
            if export:
                all_goods_record = base_query.filter(*diy_filter_list).filter(*goods_diy_filter_list)\
                    .order_by(GoodsSalesRecord.id.desc()).all()
                for item in all_goods_record:
                    goods_id_set.add(item[1].goods_id)
                    customer_id_set.add(item[1].shop_customer_id)
                    accountant_id_set.add(item[0].accountant_id)
                    salesman_id_set.add(item[0].salesman_id)
                    billing_record_id = item[0].id
                    if billing_record_id not in billing_record_id_list:
                        billing_record_id_list.append(billing_record_id)
                    goods = item[1]  # type: models.GoodsSalesRecord
                    # 一个票据中可能有多个货品，多品
                    goods_list = goods_record_dict.get(billing_record_id,
                                                       list())
                    goods_list.append(goods)
                    goods_record_dict[billing_record_id] = goods_list
                    billing_record_dict[billing_record_id] = item[0]
                    if item[0].type_id:
                        multi_record_order_id_set.add(item[1].multi_order_id)
                return_billing_record_id = billing_record_id_list
                page_sum = 1
            else:
                result_query = session.query(BillingSalesRecord.id.distinct()) \
                    .join(*first_join).filter(*base_filter_list).filter(*diy_filter_list).filter(*goods_diy_filter_list)
                page_sum = math.ceil(result_query.count() / page_size)
                all_goods_record = result_query.order_by(GoodsSalesRecord.id.desc())\
                    .offset(page * page_size).limit(page_size).all()
                # 获取到所有符合要求的票据流水和货品流水及票据流水Id
                for item in all_goods_record:
                    return_billing_record_id.append(item[0])
        customer_name_dict = ShopCustomerFunc.get_shop_customer_name_through_id_list(
            session, customer_id_set)
        if 0 in customer_id_set:
            customer_name_dict[0] = "散客"
        account_sales_id_list = list(accountant_id_set) + list(salesman_id_set)
        if account_sales_id_list:
            account_name_dict = AccountFunc\
                .get_account_name_through_id_list(session, account_sales_id_list)
        else:
            account_name_dict = dict()
        if goods_id_set:
            goods_name_unit_dict = GoodsFunc.get_goods_name_unit_through_id_list(
                session, goods_id_set)

        for key, value in goods_name_unit_dict.items():
            goods_name_dict[key] = value.get("name", "")
            goods_unit_dict[key] = value.get("unit", "")
            goods_unit_text_dict[key] = value.get("unit_text", "未知")
            goods_supply_type_dict[key] = value.get("supply_type")
            goods_supply_type_text_dict[key] = value.get("supply_type_text")

        origin_record = {
            "customer_name_dict": customer_name_dict,
            "goods_name_dict": goods_name_dict
        }

        order_num_multi_order_id_map = dict()
        multi_record_order_id_list = list(multi_record_order_id_set)
        # 获取多品订单号
        if multi_record_order_id_list:
            order_num_multi_order_id_map = OrderBaseFunc\
                .get_order_num_through_id_list(session, multi_record_order_id_list)
        # 通过票据流水Id获得对应流水， 绕这个弯是因为如果不以票据流水id为主键，那么出现多品时，分页不准。
        billing_record_list = []
        if return_billing_record_id:
            # 重量显示单位
            weight_unit_text = ConfigFunc.get_weight_unit_text(
                session, shop_id)
            for _id in return_billing_record_id:
                goods_records = goods_record_dict.get(_id)
                billing_record = billing_record_dict.get(
                    _id)  # type: models.BillingSalesRecord
                is_multi = billing_record.type_id
                accounting = 1 if billing_record.account_period_date and \
                                  billing_record.account_period_date > datetime.date.today() else 0
                # 单品
                if is_multi == 0:
                    first_goods_record = goods_records[
                        0]  # type: models.GoodsSalesRecord
                    goods_unit = goods_unit_dict.get(
                        first_goods_record.goods_id)
                    data_dict = {
                        "accountant_date_time": TimeFunc.time_to_str(first_goods_record.order_time, "date") \
                                                   + " " + TimeFunc.time_to_str(first_goods_record.order_time, "time"),
                        "accountant_id": billing_record.accountant_id,
                        "accountant_name": account_name_dict.get(billing_record.accountant_id, ""),

                        "accountant_time": TimeFunc.time_to_str(first_goods_record.order_time, "time"),
                        # "accountant_wximgurl": "-",
                        "accounting": accounting,
                        "batch_num": "-",
                        "bill_time": TimeFunc.time_to_str(billing_record.bill_time, "time"),
                        "bill_time_full": (TimeFunc.time_to_str(billing_record.bill_date, "date")
                                           + " " + TimeFunc.time_to_str(billing_record.bill_time, "time")),
                        "commission_mul": check_float(first_goods_record.get_commission_mul() / 100),
                        "commission_sumup": check_float(first_goods_record.get_commission_money() / 100),
                        "customer_name": customer_name_dict.get(first_goods_record.shop_customer_id),
                        "deposit_total": check_float(first_goods_record.get_deposit_money() / 100),
                        "full_num": first_goods_record.num,  # 注意是否要注意单双品
                        "goods_count": 1,
                        "goods_id": first_goods_record.goods_id,
                        "goods_name": goods_name_dict.get(first_goods_record.goods_id, ""),
                        "goods_sumup": check_float(first_goods_record.get_sales_money() / 100),
                        "goods_supply_type": goods_supply_type_dict.get(first_goods_record.goods_id),
                        "goods_supply_type_text": goods_supply_type_text_dict.get(first_goods_record.goods_id),
                        "goods_unit_text": goods_unit_text_dict.get(first_goods_record.goods_id),
                        "id": first_goods_record.id,
                        "is_after_accounting": accounting,
                        "is_multi": is_multi,
                        "num": first_goods_record.num[-4:],
                        "pay_status": GoodsSaleFunc.get_sales_record_pay_status_text(first_goods_record),
                        "pay_type": billing_record.pay_type,
                        "pay_type_text": first_goods_record.pay_type_dict.get(first_goods_record.pay_type),
                        "receipt_money": check_float(first_goods_record.get_receipt_money() / 100),
                        "record_type": first_goods_record.record_type,
                        "remark": first_goods_record.remark,
                        "sales_num": check_float(
                            first_goods_record.get_sales_num(goods_unit, weight_unit_text=weight_unit_text))
                            if goods_unit == 0 else "-",
                        "salesman_name": account_name_dict.get(billing_record.salesman_id, ""),
                        "salesman_id": billing_record.salesman_id,
                        "supplier_name": "-",
                        "unit_price": check_float(
                            first_goods_record.get_fact_price(goods_unit, weight_unit_text=weight_unit_text) / 100),
                        "weight_unit_text": weight_unit_text,
                        "sort_id": first_goods_record.id  # 只是为了排序使用
                    }
                # 多品
                else:
                    count = len(goods_records)
                    single_multi_list = list()
                    total_sales_num = 0
                    total_commission_mul = 0
                    total_commission_sumup = 0
                    deposit_total = 0
                    total_goods_sumup = 0
                    total_receipt_money = 0
                    # 多品中的货品
                    for item in goods_records:
                        goods_unit = goods_unit_dict.get(item.goods_id)
                        temp_dict = {
                            "accountant_date_time": TimeFunc.time_to_str(item.order_time, "date") \
                                                    + " " + TimeFunc.time_to_str(item.order_time, "time"),
                            "accountant_id": billing_record.accountant_id,
                            "accountant_name": account_name_dict.get(billing_record.accountant_id, ""),

                            "accountant_time": TimeFunc.time_to_str(item.order_time, "time"),
                            # "accountant_wximgurl": "-",
                            "accounting": accounting,
                            "batch_num": "-",
                            "bill_time": TimeFunc.time_to_str(billing_record.bill_time, "time"),
                            "bill_time_full": (TimeFunc.time_to_str(billing_record.bill_date, "date")
                                               + " " + TimeFunc.time_to_str(billing_record.bill_time, "time")),
                            "commission_mul": check_float(item.get_commission_mul() / 100),
                            "commission_sumup": check_float(item.get_commission_money() / 100),
                            "customer_name": customer_name_dict.get(item.shop_customer_id),
                            "deposit_total": check_float(item.get_deposit_money() / 100),
                            "full_num": item.num,  # 注意是否要注意单双品
                            "goods_count": 1,
                            "goods_id": item.goods_id,
                            "goods_name": goods_name_dict.get(item.goods_id, ""),
                            "goods_sumup": check_float(item.get_sales_money() / 100),
                            "goods_supply_type": goods_supply_type_dict.get(item.goods_id),
                            "goods_supply_type_text": goods_supply_type_text_dict.get(item.goods_id),
                            "goods_unit_text": goods_unit_text_dict.get(item.goods_id),
                            "id": item.id,
                            "is_after_accounting": accounting,
                            "is_multi": 0,
                            "num": item.num[-4:],  # 注意是否要注意单双品
                            "pay_status": GoodsSaleFunc.get_sales_record_pay_status_text(item),
                            "pay_type": billing_record.pay_type,
                            "pay_type_text": item.pay_type_dict.get(item.pay_type),
                            "receipt_money": check_float(item.get_receipt_money() / 100),
                            "record_type": item.record_type,
                            "remark": item.remark,
                            "sales_num": check_float(
                                item.get_sales_num(goods_unit, weight_unit_text=weight_unit_text))
                            if goods_unit == 0 else "-",
                            "salesman_name": account_name_dict.get(billing_record.salesman_id, ""),
                            "salesman_id": billing_record.salesman_id,
                            "supplier_name": "-",
                            "unit_price": check_float(
                                item.get_fact_price(goods_unit, weight_unit_text=weight_unit_text) / 100),
                            "weight_unit_text": weight_unit_text
                        }
                        # 求和
                        total_sales_num += check_float(temp_dict["sales_num"])
                        total_commission_mul += check_float(
                            temp_dict["commission_mul"])
                        total_commission_sumup += check_float(
                            temp_dict["commission_sumup"])
                        deposit_total += check_float(
                            temp_dict["deposit_total"])
                        total_goods_sumup += check_float(
                            temp_dict["goods_sumup"])
                        total_receipt_money += check_float(
                            temp_dict["receipt_money"])
                        single_multi_list.append(temp_dict)
                    single_multi_list.sort(key=lambda x: x["id"], reverse=True)
                    # 多品合并部分
                    first_goods_record = goods_records[0]
                    data_dict = {
                        "accountant_date_time": TimeFunc.time_to_str(first_goods_record.order_time, "date") \
                                                + " " + TimeFunc.time_to_str(first_goods_record.order_time, "time"),
                        "accountant_id": billing_record.accountant_id,
                        "accountant_name": account_name_dict.get(billing_record.accountant_id, ""),

                        "accountant_time": TimeFunc.time_to_str(first_goods_record.order_time, "time"),
                        # "accountant_wximgurl": "-",
                        "accounting": accounting,
                        "batch_num": "-",
                        "bill_time": TimeFunc.time_to_str(billing_record.bill_time, "time"),
                        "bill_time_full": (TimeFunc.time_to_str(billing_record.bill_date, "date")
                                           + " " + TimeFunc.time_to_str(billing_record.bill_time, "time")),
                        "commission_mul": total_commission_mul,
                        "commission_sumup": total_commission_sumup,
                        "customer_name": customer_name_dict.get(first_goods_record.shop_customer_id),
                        "deposit_total": deposit_total,
                        "full_num": order_num_multi_order_id_map.get(first_goods_record.multi_order_id, "-"),
                        "goods_count": count,
                        "goods_name": goods_name_dict.get(first_goods_record.goods_id, ""),
                        "goods_sumup": total_goods_sumup,
                        "id": first_goods_record.multi_order_id,
                        "is_after_accounting": accounting,
                        "is_multi": is_multi,
                        "num": first_goods_record.num[-4:],  # 注意是否要注意单双品
                        "pay_status": GoodsSaleFunc.get_sales_record_pay_status_text(first_goods_record),
                        "pay_type": billing_record.pay_type,
                        "pay_type_text": first_goods_record.pay_type_dict.get(first_goods_record.pay_type),
                        "receipt_money": total_receipt_money,
                        "record_type": first_goods_record.record_type,
                        "remark": first_goods_record.remark,
                        "sales_num": total_sales_num,
                        "salesman_name": account_name_dict.get(billing_record.salesman_id, ""),
                        "salesman_id": billing_record.salesman_id,
                        "supplier_name": "-",
                        "single_multi_goods": single_multi_list,
                        "unit_price": "-",
                        "weight_unit_text": weight_unit_text,
                        "sort_id": first_goods_record.id  # 只是为了排序使用
                    }
                billing_record_list.append(data_dict)
        billing_record_list.sort(key=lambda x: x["sort_id"], reverse=True)
        return billing_record_list, page_sum, origin_record

    @classmethod
    def count_unpaid_billing_record_today(cls, session, shop_id):
        """ 查询今日所有未结算的票据流水 """
        params_filter = [
            models.BillingSalesRecord.shop_id == shop_id,
            models.BillingSalesRecord.accountant_id == 0,
            models.BillingSalesRecord.bill_date == datetime.date.today(),
            models.GoodsSalesRecord.status == 1,
        ]
        count_single = session.query(func.count(models.BillingSalesRecord.id)) \
            .join(
                models.GoodsSalesRecord,
                models.GoodsSalesRecord.id == models.BillingSalesRecord.record_id) \
            .filter(models.BillingSalesRecord.type_id == 0, *params_filter) \
            .scalar() or 0
        count_multi = session\
            .query(func.count(func.distinct(models.BillingSalesRecord.record_id))) \
            .join(
                models.GoodsSalesRecord,
                models.GoodsSalesRecord.multi_order_id == models.BillingSalesRecord.record_id) \
            .filter(models.BillingSalesRecord.type_id == 1, *params_filter) \
            .scalar() or 0
        return count_single + count_multi

    @classmethod
    def _parse_single_records(cls,
                              single_record_list,
                              goods_info_dict,
                              session,
                              simplify=False):
        """
        解析单品记录
        :param single_record_list: 单品record list
        :param goods_info_dict:  货品信息字典
        :param session: DB session
        :param simplify: 指定为True时，只返回一部分关键字段属性
        :return: List
        """
        result = []
        current_date = datetime.date.today()
        shop_id = single_record_list[0].shop_id if single_record_list else 0

        # 重量显示单位
        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)

        # 客户信息
        customer_id_list = {x.shop_customer_id for x in single_record_list}
        customer_name_dict = ShopCustomerFunc.get_shop_customer_name_through_id_list(
            session, customer_id_list)

        # 员工信息
        accountant_id_list = {x.accountant_id for x in single_record_list}
        salesman_id_list = {x.salesman_id for x in single_record_list}
        account_id_list = accountant_id_list.union(salesman_id_list)
        staff_account_dict = AccountFunc.get_account_through_id_list(
            session, account_id_list)

        # 组合支付信息
        order_id_list = {x.order_id for x in single_record_list if x.order_id}
        combine_pay_dict = OrderBaseFunc.multi_combine_pay_type_text(
            session, order_id_list)

        # 批量获取串货调整信息
        record_id_list = {x.id for x in single_record_list}
        ajust_record_id_list = SalesRecordAjustFunc.get_multi_ajust_all(
            session, record_id_list)

        # 批次号信息
        batch_id_list = [x.goods_batch_id for x in single_record_list]
        goods_batch_num_dict = GoodsBatchFunc.get_batch_num_through_id_list(
            session, batch_id_list)

        for r in single_record_list:
            default_dict = {
                "name": "unknown",
                "unit": "unknown",
                "supply_type": "unknown",
                "unit_text": "unknown",
                "supply_type_text": "unknown",
            }
            goods_info = goods_info_dict.get(r.goods_id, default_dict)
            # 加入串货调整标志
            ajust = 1 if r.id in ajust_record_id_list else 0
            goods_unit = goods_info["unit"]

            if not simplify:
                data = dict(
                    id=r.id,
                    num=r.num[-4:],
                    bill_time=TimeFunc.time_to_str(r.bill_time, "time"),
                    goods_name=goods_info["name"],
                    commission_mul=check_float(r.get_commission_mul() / 100),
                    # 售出单位为件的不显示重量
                    sales_num=check_float(r.get_sales_num(
                        goods_unit, weight_unit_text=weight_unit_text)) \
                        if goods_unit == 0 else "-",
                    receipt_money=check_float(r.get_receipt_money() / 100),
                    pay_status=GoodsSaleFunc.get_sales_record_pay_status_text(r),
                    record_type=r.record_type,
                    accounting=1 if r.account_period_date and
                                    r.account_period_date > current_date else 0,
                    # 冗余一下，因为接口名字被改了
                    is_after_accounting=1 if r.account_period_date and
                                             r.account_period_date > current_date else 0,
                    goods_count=1,
                    is_multi=0,
                    unit_price=check_float(r.get_fact_price(
                        goods_unit, weight_unit_text=weight_unit_text) / 100),
                    goods_sumup=check_float(r.get_sales_money() / 100),
                    commission_sumup=check_float(r.get_commission_money() / 100),
                    deposit_total=check_float(r.get_deposit_money() / 100),
                    remark=r.remark,
                    bill_time_full=(TimeFunc.time_to_str(r.bill_date, "date") \
                                    + " " + TimeFunc.time_to_str(r.bill_time, "time")),
                    salesman_id=r.salesman_id,
                    goods_id=r.goods_id,
                    full_num=r.num,
                    customer_name=customer_name_dict.get(r.shop_customer_id, "散客"),
                    ajust=ajust,
                    batch_num=goods_batch_num_dict.get(r.goods_batch_id, "") \
                        if r.goods_batch_id else "",
                    supplier_name=r.shop_supplier_name,
                    goods_supply_type=goods_info["supply_type"],
                    goods_supply_type_text=goods_info["supply_type_text"],
                    goods_unit_text=goods_info["unit_text"],
                    weight_unit_text=weight_unit_text,
                )
            else:
                data = dict(
                    id=r.id,
                    commission_mul=check_float((r.get_commission_mul() / 100)),
                    sales_num=check_float(r.get_sales_num(
                        goods_unit, weight_unit_text=weight_unit_text)) \
                        if goods_unit == 0 else "-",
                    reciept_money=check_float(r.get_receipt_money() / 100),
                    goods_name=goods_info["name"],
                    goods_id=r.goods_id,
                    full_num=r.num,
                    record_type=r.record_type,
                    ajust=ajust,
                )
            if r.accountant_id:
                pay_type = r.pay_type
                order_id = r.order_id
                pay_type_text = OrderBaseFunc.order_pay_type_text(
                    order_id, pay_type, combine_pay_dict)
                accountant = staff_account_dict.get(r.accountant_id, None)
                data["accountant_name"] \
                    = accountant.realname or accountant.nickname if accountant else ""
                data[
                    "accountant_wximgurl"] = accountant.head_imgurl_small if accountant.wx_unionid else ""
                data["accountant_time"] = TimeFunc.time_to_str(
                    r.order_time, "time")
                data["accountant_date_time"] = TimeFunc.time_to_str(r.order_time, "date") \
                                               + " " + TimeFunc.time_to_str(r.order_time, "time")
                data["pay_type"] = pay_type
                data["pay_type_text"] = pay_type_text
                data["accountant_id"] = r.accountant_id
            else:
                data["accountant_name"] = "-"
                data["accountant_wximgurl"] = ""
                data["accountant_time"] = "-"
                data["pay_type_text"] = "-"
                data["accountant_date_time"] = "-"

            salesman = staff_account_dict.get(r.salesman_id, None)
            data["salesman_name"] \
                = salesman.realname or salesman.nickname if salesman else ""
            data[
                "salesman_wximgurl"] = salesman.head_imgurl_small if salesman.wx_unionid else ""
            data["salesman_headimg"] = salesman.head_imgurl if salesman else ""

            result.append(data)
        return result

    @classmethod
    def _parse_multi_records(cls,
                             multi_record_list,
                             goods_info_dict,
                             map_multi_orders,
                             session,
                             shop_id=0):
        """
        解析多品订单记录
        :param multi_record_list: 多品record list
        :param goods_info_dict: 货品信息字典
        :param map_multi_orders: 匹配到的多品订单号
        :param session: DB session
        :return: List
        """
        # 多品订单映射到流水记录列表
        map_multi_records = {}
        for record in multi_record_list:
            map_multi_records.setdefault(record.multi_order_id,
                                         []).append(record)

        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)
        result = []
        for multi_records in map_multi_records.values():
            multi_data = {}
            for r in multi_records:
                goods_info = goods_info_dict.get(r.goods_id, {
                    "name": "unknown",
                    "unit": "known"
                })
                multi_data = cls._merge_multi_data(
                    r,
                    multi_data,
                    map_multi_orders,
                    goods_info,
                    session,
                    shop_id=shop_id,
                    weight_unit_text=weight_unit_text)
            result.append(multi_data)
        return result

    @classmethod
    def _merge_multi_data(cls,
                          record,
                          multi_data,
                          map_multi_orders,
                          goods_info,
                          session,
                          shop_id=0,
                          weight_unit_text="斤"):
        """ 合并多品订单, 函数的参数在内部被重写并返回

        :param record: 单条小票流水的id
        :param multi_data: 待进行计算的多品数据字典
        :param map_multi_orders: 多品订单id与订单号的映射，传进来避免多次查询
        :param goods_info: 单条商品的商品名信息
        :param session: DB session

        :rtype: dict
        """
        goods_unit = goods_info["unit"]
        sales_num = NumFunc.check_float(record.get_sales_num(
            goods_unit, weight_unit_text=weight_unit_text)) \
            if goods_unit == 0 else 0
        is_after_accounting = 1 if record.account_period_date and \
                                   record.account_period_date > datetime.date.today() else 0
        multi_order = map_multi_orders[record.multi_order_id]
        multi_data = dict(
            id=record.multi_order_id,
            num=multi_order.num[-4:],
            full_num=multi_order.num,
            bill_time=TimeFunc.time_to_str(record.bill_time, "time"),
            goods_name=goods_info["name"] + " " + multi_data.get(
                "goods_name", ""),
            commission_mul=NumFunc.
            check_float(record.get_commission_mul() / 100 +
                        multi_data.get("commission_mul", 0)),
            # 多品暂不展示重量
            sales_num=NumFunc.check_float(sales_num +
                                          multi_data.get("sales_num", 0)),
            receipt_money=NumFunc.check_float(
                record.get_receipt_money() / 100 +
                multi_data.get("receipt_money", 0)),
            pay_status=GoodsSaleFunc.get_sales_record_pay_status_text(record),
            accounting=is_after_accounting,
            is_after_accounting=is_after_accounting,
            goods_count=1 + multi_data.get("goods_count", 0),
            is_multi=1,
            unit_price="-",
            goods_sumup=NumFunc.check_float(record.get_sales_money() / 100 +
                                            multi_data.get("goods_sumup", 0)),
            commission_sumup=NumFunc.check_float(
                record.get_commission_money() / 100 +
                multi_data.get("commission_sumup", 0)),
            deposit_total=NumFunc.check_float(
                record.get_deposit_money() / 100 +
                multi_data.get("deposit_total", 0)),
            remark=multi_order.remark,
            bill_time_full=(
                TimeFunc.time_to_str(record.bill_date, "date") + " " +
                TimeFunc.time_to_str(record.bill_time, "time")),
            salesman_id=record.salesman_id,
            record_type=record.record_type,
            batch_num="-",
            weight_unit_text=weight_unit_text,
        )
        # 只要获取到开票员
        if not multi_data.get("salesman_name"):
            salesman = AccountFunc.get_account_through_id(
                session, record.salesman_id)
            multi_data["salesman_name"] \
                = salesman.realname or salesman.nickname if salesman else ""
            multi_data[
                "salesman_headimg"] = salesman.head_imgurl if salesman else ""
            multi_data[
                "salesman_wximgurl"] = salesman.head_imgurl_small if salesman.wx_unionid else ""
            if record.accountant_id:
                order_id = multi_order.tally_order_id
                combine_pay_dict = OrderBaseFunc.multi_combine_pay_type_text(
                    session, [order_id])
                pay_type = record.pay_type
                pay_type_text = OrderBaseFunc.order_pay_type_text(
                    order_id, pay_type, combine_pay_dict)
                accountant_object = AccountFunc.get_account_through_id(
                    session, record.accountant_id)
                multi_data[
                    "accountant_name"] = accountant_object.realname or accountant_object.nickname if accountant_object else ""
                multi_data[
                    "accountant_wximgurl"] = accountant_object.head_imgurl_small if accountant_object.wx_unionid else ""
                multi_data["accountant_time"] = TimeFunc.time_to_str(
                    record.order_time, "time")
                multi_data["accountant_date_time"] = \
                    (TimeFunc.time_to_str(record.order_time, "date")
                     + " " + TimeFunc.time_to_str(record.order_time, "time"))
                multi_data["pay_type"] = pay_type
                multi_data["pay_type_text"] = pay_type_text
                multi_data["accountant_id"] = record.accountant_id
            else:
                multi_data["accountant_name"] = "-"
                multi_data["accountant_time"] = "-"
                multi_data["accountant_date_time"] = "-"
                multi_data["accountant_wximgurl"] = ""

        if not multi_data.get("customer_name"):
            customer_name = "散客"
            if record.shop_customer_id:
                shop_customer = ShopCustomerFunc.get_shop_customer_through_id(
                    session, shop_id, record.shop_customer_id)
                if shop_customer:
                    customer_name = shop_customer.name
            multi_data["customer_name"] = customer_name

        return multi_data

    @classmethod
    def list_sales_each_goods_salesman(cls,
                                       session,
                                       statistic_session,
                                       shop_id,
                                       salesman_id,
                                       from_date,
                                       to_date,
                                       page,
                                       data_type=0):
        """ 开票员货品统计, 某段时间内 """
        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)
        page_size = 20
        if data_type == 1:
            sTable = models_account_periods_statistic \
                .AccountPeriodsStatisticSalesGoodsSalesman
        else:
            sTable = models_statistic.StatisticSalesGoodsSalesman
        master_goods_id_set = GoodsMasterBranchFunc.list_goods_id_master(
            session, shop_id)
        columns = StatisticFunc.get_query_param(sTable)
        query_base = statistic_session.query(
            sTable.goods_id,
            # 串货调整会生成负向记录, 影响最终展示的笔数, 求绝对值
            func.sum(sTable.sales_pay_count +
                     2 * sTable.refund_count).label("count"),
            *columns).filter_by(
                shop_id=shop_id, statistic_type=1,
                salesman_id=salesman_id).filter(
                    sTable.goods_id.in_(master_goods_id_set)).having(
                        and_(
                            func.sum(sTable.total_goods_cent) != 0,
                            func.sum(sTable.commission_mul) != 0,
                            func.sum(sTable.sales_pay_count +
                                     2 * sTable.refund_count) != 0))
        if from_date:
            query_base = query_base.filter(sTable.statistic_date >= from_date)
        if to_date:
            query_base = query_base.filter(sTable.statistic_date < to_date)

        count = query_base.group_by(sTable.goods_id).count()
        query_result = query_base.group_by(sTable.goods_id).order_by(
            sTable.goods_id).offset(page_size * page).limit(page_size).all()
        page_sum = math.ceil(count / page_size)

        supplier_dict = SupplierFunc.get_supplier_info(session, shop_id,
                                                       query_result)
        goods_dict = GoodsFunc.get_goods_dict_through_query(
            session, shop_id, query_result)
        result = []
        for q in query_result:
            goods = goods_dict.get(q.goods_id)
            supplier_name = supplier_dict.get(q.supperlier_id, "")
            goods_name = GoodsFunc.get_goods_name_with_attr(goods)
            if goods.unit == 0:
                avg_price = UnitFunc.convert_weight_unit(
                    q.total_goods_cent / q.sales_weigh,
                    weight_unit_text=weight_unit_text,
                    source="price") if q.sales_weigh else 0
            else:
                avg_price = q.total_goods_cent / q.commission_mul if q.commission_mul else 0
            data = {
                "sales_count":
                check_int(q.sales_count),
                # 历史遗留问题, sales_pay_count的名字被占用了
                "sales_pay_count":
                check_int(q.count),
                "refund_count":
                check_int(q.refund_count),
                "commission_mul":
                check_float(q.commission_mul / 100),
                "sales_weigh":
                check_float(
                    UnitFunc.convert_weight_unit(
                        q.sales_weigh,
                        weight_unit_text=weight_unit_text,
                        source="weight") / 100) if goods.unit == 0 else "-",
                "sales_money":
                check_float(q.total_goods_cent / 100),
                "commission_total":
                check_float(q.total_commission / 100),
                "deposit_total":
                check_float(q.deposit_cent / 100),
                "gross_cent":
                check_float(q.gross_cent / 100),
                "goods_id":
                q.goods_id,
                "supplier_id":
                q.supperlier_id,
                "supplier_name":
                supplier_name,
                "goods_name":
                goods_name,
                "avg_price":
                check_float(avg_price),
            }
            result.append(data)
        return result, page_sum

    @classmethod
    def get_sum_query_base(cls, session):
        """ 获取流水表汇总的基础查询

        :rtype: obj, `sa.Query`
        """
        GoodsSalesRecord = models.GoodsSalesRecord
        Goods = models.Goods
        result = session.query(
            func.sum(func.IF(
                GoodsSalesRecord.record_type == 0,
                GoodsSalesRecord.receipt_money,
                -GoodsSalesRecord.receipt_money)).label("receipt_money"),
            func.sum(func.IF(
                GoodsSalesRecord.record_type == 0,
                GoodsSalesRecord.unpayback_money,
                -GoodsSalesRecord.unpayback_money)).label("unpayback_money"),
            func.sum(func.IF(                                                             # 单位为斤或者Kg时才统计，否则为0
                GoodsSalesRecord.record_type == 0,
                func.IF(Goods.unit == 0, GoodsSalesRecord.sales_num, 0),
                func.IF(Goods.unit == 0, -GoodsSalesRecord.sales_num, 0))).label("sales_num"),
            func.sum(func.IF(
                GoodsSalesRecord.record_type == 0,
                GoodsSalesRecord.commission_mul,
                -GoodsSalesRecord.commission_mul)).label("commission_mul"),
            func.sum(func.IF(
                GoodsSalesRecord.record_type == 0,
                GoodsSalesRecord.sales_money,
                -GoodsSalesRecord.sales_money)).label("sales_money"),
            func.sum(func.IF(
                GoodsSalesRecord.record_type == 0,
                GoodsSalesRecord.commission_money,
                -GoodsSalesRecord.commission_money)).label("commission_money"),
            func.sum(func.IF(
                GoodsSalesRecord.record_type == 0,
                GoodsSalesRecord.deposit_money,
                -GoodsSalesRecord.deposit_money)).label("deposit_money"))\
            .join(Goods, Goods.id == GoodsSalesRecord.goods_id)
        return result

    @classmethod
    def list_merged_records(cls,
                            session,
                            shop_id,
                            records,
                            sub_multi=True,
                            sub_single=False):
        """ 将票据流水根据单品和多品详情解析后返回

        :param records: list, GoodsSaleRecord对象的列表
        :param sub_multi: 是否返回多品下的单品信息
        :param sub_single: 是否将单品信息加入单品下(保持与多品的结构统一)

        :return: 票据流水列表
        :rtype: list
        """
        # 查询商品
        goods_id_list = {r.goods_id for r in records}
        goods_info_dict = GoodsFunc.get_goods_name_unit_through_id_list(
            session, goods_id_list)
        order_id_list = {r.multi_order_id for r in records}
        orders = session.query(models.Order).filter(
            models.Order.id.in_(order_id_list)).all()
        # 多品订单id到订单号码及订单备注的映射
        map_multi_orders = {order.id: order for order in orders}

        single_record_list = []
        multi_record_list = []
        for r in records:
            if r.multi_order_id == 0:
                single_record_list.append(r)
            else:
                multi_record_list.append(r)

        # 单品结果列表
        single_result = cls._parse_single_records(single_record_list,
                                                  goods_info_dict, session)
        # 多品结果列表
        multi_result = cls._parse_multi_records(
            multi_record_list,
            goods_info_dict,
            map_multi_orders,
            session,
            shop_id=shop_id)
        if sub_multi:
            # 对每一个多品订单加入对应的单品货品信息
            for multi in multi_result:
                multi["single_multi_goods"] = []
            for record in multi_record_list:
                multi_order_id = record.multi_order_id
                # [record]为单元素列表，解析得到的数据结果也是个单元素列表，取其第一个元素[0]
                multi_single_result = GoodsSaleFunc._parse_single_records(
                    [record], goods_info_dict, session, simplify=False)[0]
                for multi in multi_result:
                    if multi["id"] == multi_order_id:
                        multi["single_multi_goods"].append(multi_single_result)
        if sub_single:
            # 收银台接单保持结构
            for single in single_result:
                tmp = copy.deepcopy(single)
                single["single_multi_goods"] = [tmp]

        result = sorted(
            single_result + multi_result,
            key=lambda r: r["bill_time_full"],
            reverse=True)

        return result

    @classmethod
    def nullify_order(cls, session, order_id, operator_id, shop_id):
        """ 作废一条多品记录

        :param order_id: 订单id
        :param operator_id: 操作人id

        :rtype: tuple
        """
        order = session.query(models.Order).filter_by(
            id=order_id, shop_id=shop_id).first()
        if not order:
            return False, "订单不存在"
        if order.status != 0:
            return False, "订单已付款或已取消，无法作废"
        order.status = -2
        # 订单下的所有小票流水
        records = session.query(models.GoodsSalesRecord).filter_by(
            shop_id=shop_id, multi_order_id=order.id).all()
        record_id_list = []
        for r in records:
            if r.status != 1:
                return False, "订单下存在已结算或已取消流水，无法作废"
            record_id_list.append(r.id)
        if not record_id_list:
            return False, "无法根据多品订单查询到对应的流水, 无法作废"
        return cls.nullify_sales_record(
            session, record_id_list, operator_id, shop_id, is_multi=True)

    @classmethod
    def nullify_sales_record(cls,
                             session,
                             record_ids,
                             operator_id,
                             shop_id,
                             is_multi=False):
        """ 作废一条或多条小票流水记录

        :param record_ids: list, 小票流水的id列表
        :param is_multi: 是否是多品流水
        :param operator_id: 操作人id

        :rtype: tuple
        """
        if not is_multi:
            record = session.query(models.GoodsSalesRecord).filter_by(
                id=record_ids[0], shop_id=shop_id).first()
            if not record:
                return False, "小票流水不存在"
            if record.status != 1:
                return False, "小票流水已支付或已取消, 无法作废"

        session.query(models.GoodsSalesRecord).filter(
            models.GoodsSalesRecord.id.in_(record_ids)).update(
                {
                    "status": -1
                }, synchronize_session=False)

        record_list = []
        multi_id_dict = MultiOrderFunc.get_id_dict_through_sales_record(
            session, record_ids)
        for record_id in record_ids:
            multi_order_id = multi_id_dict.get(record_id, 0)
            record_list.append(
                models.GoodsSalesRecordCancelRecord(
                    sales_record_id=record_id,
                    multi_order_id=multi_order_id,
                    operator_id=operator_id))
        session.add_all(record_list)

        return True, ""

    @classmethod
    def query_and_parse_refunds(cls, session, sales_record_id_list):
        """ 查询并解析小票的的退票信息

        :return: 退单的id, 原单的id, 退单id到原单id的映射
        """
        # 小票退款信息
        refund_infos = session \
            .query(models.GoodsSalesRecordRefundInfo) \
            .filter(models.GoodsSalesRecordRefundInfo.goods_sale_record_id.in_(
                sales_record_id_list)) \
            .all() if sales_record_id_list else []
        refund_record_ids = {r.goods_sale_record_id for r in refund_infos}
        origin_record_ids = {
            r.origin_goods_sale_record_id
            for r in refund_infos
        }
        map_refund_id_to_origin_id = {
            r.goods_sale_record_id: r.origin_goods_sale_record_id
            for r in refund_infos
        }
        return refund_record_ids, origin_record_ids, map_refund_id_to_origin_id

    @classmethod
    def get_record_shown_status_print(cls, record, refund_record_ids,
                                      origin_record_ids):
        """ 获取小票的打印显示状态(退单1, 退单的原单2, 其他0)

        :param refund_record_ids: 退单记录的id
        :param origin_record_ids: 原单记录的id

        :rtype: int
        """
        if record.id in refund_record_ids:
            result = 1
        elif record.id in origin_record_ids:
            result = 2
        else:
            result = 0
        return result

    @classmethod
    def get_allstatus_salerecord_list(cls,
                                      session,
                                      shop_id,
                                      choose_date,
                                      choose_end_date,
                                      goods_id=0,
                                      salesman_id=0,
                                      status=1,
                                      accounting=0,
                                      debt_id=0,
                                      is_branch=0):

        GoodsSalesRecord = models.GoodsSalesRecord
        data_list = []
        # 获取所有主货ID
        master_goods_ids = GoodsMasterBranchFunc.list_goods_id_master(
            session, shop_id)
        if not master_goods_ids:
            return data_list

        # 基础查询

        # 查分货的流水不进行主货的过滤，会传入is_branch和goods_id
        if is_branch:
            query_base = session.query(GoodsSalesRecord) \
                .filter_by(shop_id=shop_id)
        else:
            query_base = session.query(GoodsSalesRecord)\
                                .filter_by(shop_id=shop_id)\
                                .filter(GoodsSalesRecord.goods_id.in_(master_goods_ids))
        if not debt_id:
            if accounting == 1:
                query_base = query_base.filter(
                    GoodsSalesRecord.account_period_date >= choose_date,
                    GoodsSalesRecord.account_period_date <= choose_end_date)
            else:
                query_base = query_base.filter(
                    GoodsSalesRecord.bill_date >= choose_date,
                    GoodsSalesRecord.bill_date <= choose_end_date)
        # 按还款id筛选
        if debt_id:
            # 通过debt_id反查还款id
            pb_order_id = session.query(models.DebtHistory.fk_id)\
                                     .filter_by(id=debt_id,debt_type=1)\
                                     .scalar() or 0
            if not pb_order_id:
                raise ValueError("没有还款记录")
            # 通过还款记录反查流水id
            all_goods_sale_record = session.query(models.GoodsSalesRecordPaybackRecord.goods_sale_record_id)\
                                          .filter_by(pb_order_id=pb_order_id)\
                                          .all()
            goods_sale_record_ids = [
                x.goods_sale_record_id for x in all_goods_sale_record
            ]
            query_base = query_base.filter(
                GoodsSalesRecord.id.in_(goods_sale_record_ids))
        # 按商品id筛选
        if goods_id:
            query_base = query_base.filter_by(goods_id=goods_id)
        # 按开票id筛选
        if salesman_id:
            query_base = query_base.filter_by(salesman_id=salesman_id)
        # 按筛选
        if status:
            if not debt_id:
                if status == 3:
                    status = [3, 5]
                else:
                    status = [status]
                query_base = query_base.filter(
                    GoodsSalesRecord.status.in_(status))

            unpay_salerecord_list = query_base.order_by(
                GoodsSalesRecord.id.desc()).all()
            data_list = OrderSalesRecordFunc.batch_format_order_record_info(
                session, unpay_salerecord_list, return_type="list")
        return data_list

    @classmethod
    def get_cancle_info(cls, slave_session, record_id, multi=False):
        """ 获取作废信息
        """
        GoodsSalesRecordCancelRecord = models.GoodsSalesRecordCancelRecord

        query_base = slave_session.query(GoodsSalesRecordCancelRecord)
        if multi:
            query_base = query_base.filter_by(multi_order_id=record_id)
        else:
            query_base = query_base.filter_by(sales_record_id=record_id)
        cancel_info = query_base.first()

        cancel_text = ""
        if cancel_info:
            operator_name = AccountFunc.get_account_name(
                slave_session, cancel_info.operator_id)
            create_time = TimeFunc.time_to_str(cancel_info.create_time)
            cancel_text = "作废时间：%s，操作人：%s。" % (create_time, operator_name)
        return cancel_text


# 一单多品
class MultiOrderFunc():
    @classmethod
    def get_multi_goods_sale_record_order_id(cls, session, order_id):
        '''
            获取订单对应的一单多品订单以及一单多品对应的流水小票
        '''
        GoodsSalesRecord = models.GoodsSalesRecord
        record_list = session.query(GoodsSalesRecord) \
            .filter_by(order_id=order_id) \
            .filter(GoodsSalesRecord.multi_order_id > 0) \
            .all()

        Order = models.Order
        multi_order_list = session.query(Order) \
            .filter_by(tally_order_id=order_id) \
            .all()
        multi_order_dict = {x.id: x for x in multi_order_list}

        multi_sales_record_dict = {}
        for sales_record in record_list:
            multi_order_id = sales_record.multi_order_id
            if multi_sales_record_dict.get(multi_order_id):
                multi_sales_record_dict[multi_order_id].append(sales_record)
            else:
                multi_sales_record_dict[multi_order_id] = [sales_record]
        return multi_sales_record_dict, multi_order_dict

    @classmethod
    def get_id_dict_through_sales_record(cls, session, record_id_list):
        GoodsSalesRecord = models.GoodsSalesRecord
        multi_id_list = session.query(
            GoodsSalesRecord.id, GoodsSalesRecord.multi_order_id).filter(
                GoodsSalesRecord.id.in_(record_id_list),
                GoodsSalesRecord.multi_order_id > 0).all()
        multi_id_dict = {x.id: x.multi_order_id for x in multi_id_list}
        return multi_id_dict


# 赊账
class DebtFunc():
    @classmethod
    def update_debt(cls,
                    session,
                    debt_type,
                    debt_value,
                    debtor_id,
                    shop_id,
                    operator_id,
                    fk_id,
                    remark="",
                    account_period_date=None,
                    create_time=None):
        dict_debt_type = {
            'pay_withcredit': 0,
            'payback': 1,
            'refund': 2,
            'history_debt': 3
        }
        debt_type_int = dict_debt_type[debt_type]
        debtor = session.query(
            models.ShopCustomer).filter_by(id=debtor_id).first()
        shop = session.query(
            models.Shop).filter_by(id=shop_id).with_lockmode('update').first()
        if debt_type_int == 0:
            debtor.debt_rest += debt_value
            shop.shop_debt += debt_value
        elif debt_type_int == 1:
            debtor.debt_rest -= debt_value
            debtor.payback_sum += debt_value
            shop.shop_debt -= debt_value
        elif debt_type_int == 2:
            debtor.debt_rest -= debt_value
            shop.shop_debt -= debt_value
        elif debt_type_int == 3:
            debtor.debt_rest += debt_value
            shop.shop_debt += debt_value
        # if debtor.debt_rest<0:
        #     debtor.debt_rest = 0
        # if  shop.shop_debt <0:
        #     shop.shop_debt = 0
        debt_history = models.DebtHistory(
            debtor_id=debtor_id,
            shop_id=shop_id,
            operator_id=operator_id,
            debt_type=debt_type_int,
            debt_value=debt_value,
            debt_rest=debtor.debt_rest,
            shop_debt=shop.shop_debt,
            fk_id=fk_id,
            remark=remark)
        if create_time:
            debt_history.create_time = create_time
        if debt_type_int == 3:
            debt_history.unpayback_money = debt_value
        session.add(debt_history)
        session.flush()
        if account_period_date:
            debt_history.account_period_date = account_period_date
        return debt_history.id


# 收银员
class AccoutantFunc():
    @classmethod
    def get_current_work_record(cls, session, accountant_id, shop_id):
        work_record = session.query(models.AccountantWorkRecord).filter_by(accountant_id=accountant_id,
                                                                           shop_id=shop_id). \
            order_by(models.AccountantWorkRecord.onduty_time.desc()).first()
        if not work_record or (work_record and work_record.offduty_time):
            work_record = models.AccountantWorkRecord(
                accountant_id=accountant_id, shop_id=shop_id)
            session.add(work_record)
            try:
                hr = session.query(models.HireLink).filter_by(
                    account_id=accountant_id, shop_id=shop_id).first()
                session.commit()
            except Exception as e:
                return None
        return work_record

    @classmethod
    def get_timebefore_newest_work_record(cls, session, accountant_id, shop_id,
                                          off_datetime):
        work_record = session\
            .query(models.AccountantWorkRecord)\
            .filter_by(accountant_id=accountant_id, shop_id=shop_id)\
            .filter(models.AccountantWorkRecord.offduty_time <= off_datetime)\
            .order_by(models.AccountantWorkRecord.offduty_time.desc())\
            .first()
        return work_record

    @classmethod
    def update_work_record(cls,
                           session,
                           accountant_id,
                           shop_id,
                           pay_cent,
                           pay_type,
                           fk_id=0,
                           is_reset=False,
                           work_record=None,
                           paytype_modify=False):
        """ 更新收银员交接班记录

        :param paytype_modify 支付方式修改中含条码支付的不更新交接班记录
        """
        # 撤销笔数需要算减法
        count = 1 if not is_reset else -1
        if not work_record:
            work_record = session.query(models.AccountantWorkRecord) \
                .filter_by(accountant_id=accountant_id, shop_id=shop_id) \
                .order_by(models.AccountantWorkRecord.onduty_time.desc()) \
                .first()
            if not work_record or (work_record and work_record.offduty_time):
                work_record = models.AccountantWorkRecord(
                    accountant_id=accountant_id, shop_id=shop_id)
                session.add(work_record)
                session.flush()

        if pay_type in [
                "order_cash", "order_pos", "order_alipay", "order_wx",
                "order_cheque", "order_remittance"
        ]:
            if pay_type == "order_cash":
                work_record.order_cash_cent += pay_cent
                work_record.order_cash_count += count
            elif pay_type == "order_pos":
                work_record.order_pos_cent += pay_cent
                work_record.order_pos_count += count
            elif pay_type == "order_alipay":
                work_record.order_ali_cent += pay_cent
                work_record.order_ali_count += count
            elif pay_type == "order_wx":
                work_record.order_wx_cent += pay_cent
                work_record.order_wx_count += count
            elif pay_type == "order_cheque":
                work_record.order_cheque_cent += pay_cent
                work_record.order_cheque_count += count
            elif pay_type == "order_remittance":
                work_record.order_remittance_cent += pay_cent
                work_record.order_remittance_count += count
            work_record.order_total_cent += pay_cent
            work_record.order_total_count += count
        if pay_type in [
                "pb_cash", "pb_pos", "pb_alipay", "pb_wx", "pb_cheque",
                "pb_remittance"
        ]:
            if pay_type == "pb_cash":
                work_record.pb_cash_cent += pay_cent
                work_record.pb_cash_count += count
            elif pay_type == "pb_pos":
                work_record.pb_pos_cent += pay_cent
                work_record.pb_pos_count += count
            elif pay_type == "pb_alipay":
                work_record.pb_ali_cent += pay_cent
                work_record.pb_ali_count += count
            elif pay_type == "pb_wx":
                work_record.pb_wx_cent += pay_cent
                work_record.pb_wx_count += count
            elif pay_type == "pb_cheque":
                work_record.pb_cheque_cent += pay_cent
                work_record.pb_cheque_count += count
            elif pay_type == "pb_remittance":
                work_record.pb_remittance_cent += pay_cent
                work_record.pb_remittance_count += count
            work_record.pb_total_cent += pay_cent
            work_record.pb_total_count += count
        if pay_type == "pb_combine":
            order_money = OrderBaseFunc.get_order_money(
                session, fk_id, source_type=2)
            pb_total_pay_cent = 0
            cash_cent = order_money.cash_cent if not is_reset else -order_money.cash_cent
            pos_cent = order_money.pos_cent if not is_reset else -order_money.pos_cent
            tag_ali_cent = order_money.tag_ali_cent if not is_reset else -order_money.tag_ali_cent
            tag_wx_cent = order_money.tag_wx_cent if not is_reset else -order_money.tag_wx_cent
            cheque_cent = order_money.cheque_cent if not is_reset else -order_money.cheque_cent
            remittance_cent = order_money.remittance_cent if not is_reset else -order_money.remittance_cent
            if cash_cent:
                work_record.pb_combine_cash_cent += cash_cent
                work_record.pb_combine_cash_count += count
                pb_total_pay_cent += cash_cent
            if pos_cent:
                work_record.pb_combine_pos_cent += pos_cent
                work_record.pb_combine_pos_count += count
                pb_total_pay_cent += pos_cent
            if order_money.barcode_ali_cent:
                work_record.pb_combine_ali_cent += pay_cent
                work_record.pb_combine_ali_count += count
                pb_total_pay_cent += pay_cent
            if tag_ali_cent:
                work_record.pb_combine_ali_cent += tag_ali_cent
                work_record.pb_combine_ali_count += count
                pb_total_pay_cent += tag_ali_cent
            if order_money.barcode_wx_cent:
                work_record.pb_combine_wx_cent += pay_cent
                work_record.pb_combine_wx_count += count
                pb_total_pay_cent += pay_cent
            if tag_wx_cent:
                work_record.pb_combine_wx_cent += tag_wx_cent
                work_record.pb_combine_wx_count += count
                pb_total_pay_cent += tag_wx_cent
            if cheque_cent:
                work_record.pb_combine_cheque_cent += cheque_cent
                work_record.pb_combine_cheque_count += count
                pb_total_pay_cent += cheque_cent
            if remittance_cent:
                work_record.pb_combine_remittance_cent += remittance_cent
                work_record.pb_combine_remittance_count += count
                pb_total_pay_cent += remittance_cent
            work_record.pb_total_cent += pb_total_pay_cent
            work_record.pb_total_count += count
        if pay_type in [
                "refund_cash", "refund_pos", "refund_alipay", "refund_wx",
                "refund_cheque", "refund_remittance"
        ]:
            if pay_type == "refund_cash":
                work_record.refund_cash_cent += pay_cent
                work_record.refund_cash_count += count
            elif pay_type == "refund_pos":
                work_record.refund_pos_cent += pay_cent
                work_record.refund_pos_count += count
            elif pay_type == "refund_alipay":
                work_record.refund_ali_cent += pay_cent
                work_record.refund_ali_count += count
            elif pay_type == "refund_wx":
                work_record.refund_wx_cent += pay_cent
                work_record.refund_wx_count += count
            elif pay_type == "refund_cheque":
                work_record.refund_cheque_cent += pay_cent
                work_record.refund_cheque_count += count
            elif pay_type == "refund_remittance":
                work_record.refund_remittance_cent += pay_cent
                work_record.refund_remittance_count += count
            work_record.refund_total_cent += pay_cent
            work_record.refund_total_count += count
        if pay_type == "order_credit":
            work_record.order_credit_cent += pay_cent
            work_record.order_credit_count += count
        if pay_type == "refund_credit":
            work_record.refund_credit_cent += pay_cent
            work_record.refund_credit_count += count
        if pay_type == "order_combine":
            total_pay_cent = 0

            order_money = OrderBaseFunc.get_order_money(
                session, fk_id, source_type=1)
            cash_cent = order_money.cash_cent
            pos_cent = order_money.pos_cent
            tag_ali_cent = order_money.tag_ali_cent
            tag_wx_cent = order_money.tag_wx_cent
            cheque_cent = order_money.cheque_cent
            remittance_cent = order_money.remittance_cent

            if is_reset:
                cash_cent = -cash_cent
                pos_cent = -pos_cent
                tag_ali_cent = -tag_ali_cent
                tag_wx_cent = -tag_wx_cent
                cheque_cent = -cheque_cent
                remittance_cent = -remittance_cent

            if order_money.cash_cent:
                work_record.order_combine_cash_cent += cash_cent
                work_record.order_combine_cash_count += count
                total_pay_cent += cash_cent
            if order_money.pos_cent:
                work_record.order_combine_pos_cent += pos_cent
                work_record.order_combine_pos_count += count
                total_pay_cent += pos_cent
            if order_money.barcode_ali_cent and not paytype_modify:
                work_record.order_combine_ali_cent += pay_cent
                work_record.order_combine_ali_count += count
                total_pay_cent += pay_cent
            if order_money.tag_ali_cent:
                work_record.order_combine_ali_cent += tag_ali_cent
                work_record.order_combine_ali_count += count
                total_pay_cent += tag_ali_cent
            if order_money.barcode_wx_cent and not paytype_modify:
                work_record.order_combine_wx_cent += pay_cent
                work_record.order_combine_wx_count += count
                total_pay_cent += pay_cent
            if order_money.tag_wx_cent:
                work_record.order_combine_wx_cent += tag_wx_cent
                work_record.order_combine_wx_count += count
                total_pay_cent += tag_wx_cent
            if order_money.cheque_cent:
                work_record.order_combine_cheque_cent += cheque_cent
                work_record.order_combine_cheque_count += 1
                total_pay_cent += cheque_cent
            if order_money.remittance_cent:
                work_record.order_combine_remittance_cent += remittance_cent
                work_record.order_combine_remittance_count += 1
                total_pay_cent += remittance_cent
            work_record.order_total_cent += total_pay_cent
            work_record.order_total_count += count

        if pay_type in [
                "deposit_refund_cash", "deposit_refund_pos",
                "deposit_refund_alipay", "deposit_refund_wx",
                "deposit_refund_cheque", "deposit_refund_remittance"
        ]:
            if pay_type == "deposit_refund_cash":
                work_record.deposit_refund_cash_cent += pay_cent
                work_record.deposit_refund_cash_count += count
            if pay_type == "deposit_refund_pos":
                work_record.deposit_refund_pos_cent += pay_cent
                work_record.deposit_refund_pos_count += count
            if pay_type == "deposit_refund_alipay":
                work_record.deposit_refund_ali_cent += pay_cent
                work_record.deposit_refund_ali_count += count
            if pay_type == "deposit_refund_wx":
                work_record.deposit_refund_wx_cent += pay_cent
                work_record.deposit_refund_wx_count += count
            if pay_type == "deposit_refund_cheque":
                work_record.deposit_refund_cheque_cent += pay_cent
                work_record.deposit_refund_cheque_count += count
            if pay_type == "deposit_refund_remittance":
                work_record.deposit_refund_remittance_cent += pay_cent
                work_record.deposit_refund_remittance_count += count
            work_record.deposit_refund_cent += pay_cent
            work_record.deposit_refund_count += count

        if pay_type in [
                "salary_cash", "salary_pos", "salary_alipay", "salary_wx",
                "salary_cheque", "salary_remittance"
        ]:
            if pay_type == "salary_cash":
                work_record.payout_cash_cent += pay_cent
                work_record.payout_cash_count += count
            if pay_type == "salary_pos":
                work_record.payout_pos_cent += pay_cent
                work_record.payout_pos_count += count
            if pay_type == "salary_alipay":
                work_record.payout_ali_cent += pay_cent
                work_record.payout_ali_count += count
            if pay_type == "salary_wx":
                work_record.payout_wx_cent += pay_cent
                work_record.payout_wx_count += count
            if pay_type == "salary_cheque":
                work_record.payout_cheque_cent += pay_cent
                work_record.payout_cheque_count += count
            if pay_type == "salary_remittance":
                work_record.payout_remittance_cent += pay_cent
                work_record.payout_remittance_count += count
            work_record.payout_cent += pay_cent
            work_record.payout_count += count

        if pay_type in [
                "payout_cash", "payout_pos", "payout_alipay", "payout_wx",
                "payout_cheque", "payout_remittance"
        ]:
            if pay_type == "payout_cash":
                work_record.payout_cash_cent += pay_cent
                work_record.payout_cash_count += count
            if pay_type == "payout_pos":
                work_record.payout_pos_cent += pay_cent
                work_record.payout_pos_count += count
            if pay_type == "payout_alipay":
                work_record.payout_ali_cent += pay_cent
                work_record.payout_ali_count += count
            if pay_type == "payout_wx":
                work_record.payout_wx_cent += pay_cent
                work_record.payout_wx_count += count
            if pay_type == "payout_cheque":
                work_record.payout_cheque_cent += pay_cent
                work_record.payout_cheque_count += count
            if pay_type == "payout_remittance":
                work_record.payout_remittance_cent += pay_cent
                work_record.payout_remittance_count += count
            work_record.payout_cent += pay_cent
            work_record.payout_count += count
        if pay_type in [
                "supplier_borrow_cash", "supplier_borrow_pos",
                "supplier_borrow_alipay", "supplier_borrow_wx",
                "supplier_borrow_cheque", "supplier_borrow_remittance"
        ]:
            if pay_type == "supplier_borrow_cash":
                work_record.supplier_borrow_cash_cent += pay_cent
                work_record.supplier_borrow_cash_count += count
            if pay_type == "supplier_borrow_pos":
                work_record.supplier_borrow_pos_cent += pay_cent
                work_record.supplier_borrow_pos_count += count
            if pay_type == "supplier_borrow_alipay":
                work_record.supplier_borrow_ali_cent += pay_cent
                work_record.supplier_borrow_ali_count += count
            if pay_type == "supplier_borrow_wx":
                work_record.supplier_borrow_wx_cent += pay_cent
                work_record.supplier_borrow_wx_count += count
            if pay_type == "supplier_borrow_cheque":
                work_record.supplier_borrow_cheque_cent += pay_cent
                work_record.supplier_borrow_cheque_count += count
            if pay_type == "supplier_borrow_remittance":
                work_record.supplier_borrow_remittance_cent += pay_cent
                work_record.supplier_borrow_remittance_count += count
            work_record.supplier_borrow_cent += pay_cent
            work_record.supplier_borrow_count += count

        if pay_type == "deposit":
            work_record.deposit_cent += pay_cent
            work_record.deposit_count += count
        if pay_type == "order_refund_deposit":
            work_record.deposit_cent -= pay_cent
            work_record.deposit_count -= count

        if pay_type in [
                "other_income_cash", "other_income_pos", "other_income_alipay",
                "other_income_wx", "other_income_cheque",
                "other_income_remittance"
        ]:
            if pay_type == "other_income_cash":
                work_record.other_income_cash_cent += pay_cent
                work_record.other_income_cash_count += count
            if pay_type == "other_income_pos":
                work_record.other_income_pos_cent += pay_cent
                work_record.other_income_pos_count += count
            if pay_type == "other_income_alipay":
                work_record.other_income_ali_cent += pay_cent
                work_record.other_income_ali_count += count
            if pay_type == "other_income_wx":
                work_record.other_income_wx_cent += pay_cent
                work_record.other_income_wx_count += count
            if pay_type == "other_income_cheque":
                work_record.other_income_cheque_cent += pay_cent
                work_record.other_income_cheque_count += count
            if pay_type == "other_income_remittance":
                work_record.other_income_remittance_cent += pay_cent
                work_record.other_income_remittance_count += count
            work_record.other_income_cent += pay_cent
            work_record.other_income_count += count

        if pay_type in [
                "waste_selling_income", "waste_selling_income_cash",
                "waste_selling_income_pos", "waste_selling_income_alipay",
                "waste_selling_income_wx", "waste_selling_income_cheque",
                "waste_selling_income_remittance"
        ]:
            if pay_type == "waste_selling_income_cash":
                work_record.waste_selling_income_cash_cent += pay_cent
                work_record.waste_selling_income_cash_count += count
            if pay_type == "waste_selling_income_pos":
                work_record.waste_selling_income_pos_cent += pay_cent
                work_record.waste_selling_income_pos_count += count
            if pay_type == "waste_selling_income_alipay":
                work_record.waste_selling_income_ali_cent += pay_cent
                work_record.waste_selling_income_ali_count += count
            if pay_type == "waste_selling_income_wx":
                work_record.waste_selling_income_wx_cent += pay_cent
                work_record.waste_selling_income_wx_count += count
            if pay_type == "waste_selling_income_cheque":
                work_record.waste_selling_income_cheque_cent += pay_cent
                work_record.waste_selling_income_cheque_count += count
            if pay_type == "waste_selling_income_remittance":
                work_record.waste_selling_income_remittance_cent += pay_cent
                work_record.waste_selling_income_remittance_count += count
            work_record.waste_selling_income_cent += pay_cent
            work_record.waste_selling_income_count += count

        if pay_type in [
                "order_erase_cash", "order_erase_pos", "order_erase_alipay",
                "order_erase_wx", "order_erase_combine", "order_erase_cheque",
                "order_erase_remittance"
        ]:
            if pay_type == "order_erase_cash":
                work_record.erase_cash_cent += pay_cent
                work_record.erase_cash_count += count
            if pay_type == "order_erase_pos":
                work_record.erase_pos_cent += pay_cent
                work_record.erase_pos_count += count
            if pay_type == "order_erase_alipay":
                work_record.erase_ali_cent += pay_cent
                work_record.erase_ali_count += count
            if pay_type == "order_erase_wx":
                work_record.erase_wx_cent += pay_cent
                work_record.erase_wx_count += count
            if pay_type == "order_erase_combine":
                work_record.erase_combine_cent += pay_cent
                work_record.erase_combine_count += count
            if pay_type == "order_erase_cheque":
                work_record.erase_cheque_cent += pay_cent
                work_record.erase_cheque_count += count
            if pay_type == "order_erase_remittance":
                work_record.erase_remittance_cent += pay_cent
                work_record.erase_remittance_count += count
            work_record.erase_total_cent += pay_cent
            work_record.erase_total_count += count

        if pay_type in [
                "order_fee_cash", "order_fee_pos", "order_fee_alipay",
                "order_fee_wx", "order_fee_cheque", "order_fee_remittance"
        ]:
            if pay_type == "order_fee_cash":
                work_record.order_fee_cash_cent += pay_cent
                work_record.order_fee_cash_count += count
            if pay_type == "order_fee_pos":
                work_record.order_fee_pos_cent += pay_cent
                work_record.order_fee_pos_count += count
            if pay_type == "order_fee_alipay":
                work_record.order_fee_ali_cent += pay_cent
                work_record.order_fee_ali_count += count
            if pay_type == "order_fee_wx":
                work_record.order_fee_wx_cent += pay_cent
                work_record.order_fee_wx_count += count
            if pay_type == "order_fee_cheque":
                work_record.order_fee_cheque_cent += pay_cent
                work_record.order_fee_cheque_count += count
            if pay_type == "order_fee_remittance":
                work_record.order_fee_remittance_cent += pay_cent
                work_record.order_fee_remittance_count += count
            work_record.order_fee_cent += pay_cent
            work_record.order_fee_count += count

    @classmethod
    def get_word_record_data(cls, work_record):
        if work_record.offduty_time:
            delta_day = (
                work_record.offduty_time - work_record.onduty_time).days
            delta_seconds = (
                work_record.offduty_time - work_record.onduty_time).seconds
        else:
            delta_day = (
                datetime.datetime.now() - work_record.onduty_time).days
            delta_seconds = (
                datetime.datetime.now() - work_record.onduty_time).seconds
        delta_hour = delta_seconds // 3600
        delta_minute = (delta_seconds - delta_hour * 3600) // 60
        work_time = str(delta_day) + '天' if delta_day > 0 else ''
        work_time += str(delta_hour) + '小时' if delta_hour > 0 else ''
        work_time += str(delta_minute) + '分钟' if delta_minute > 0 else ''
        work_time = work_time if work_time else '不足1分钟'
        data = {}
        data['onduty_time'] = work_record.onduty_time.strftime(
            "%Y-%m-%d %H:%M:%S")
        data['offduty_time'] = work_record.offduty_time.strftime(
            "%Y-%m-%d %H:%M:%S") if work_record.offduty_time else '--'
        data['work_time'] = work_time
        data['now_time'] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")

        # 订单收银
        data['order_total_money'] = check_float(
            work_record.order_total_cent / 100)
        data['order_total_count'] = work_record.order_total_count
        data['order_cash_money'] = check_float(
            work_record.order_cash_cent / 100 +
            work_record.order_combine_cash_cent / 100)
        data[
            'order_cash_count'] = work_record.order_cash_count + work_record.order_combine_cash_count
        data['order_pos_money'] = check_float(
            work_record.order_pos_cent / 100 +
            work_record.order_combine_pos_cent / 100)
        data[
            'order_pos_count'] = work_record.order_pos_count + work_record.order_combine_pos_count
        data['order_ali_money'] = check_float(
            work_record.order_ali_cent / 100 +
            work_record.order_combine_ali_cent / 100)
        data[
            'order_ali_count'] = work_record.order_ali_count + work_record.order_combine_ali_count
        data['order_wx_money'] = check_float(
            work_record.order_wx_cent / 100 +
            work_record.order_combine_wx_cent / 100)
        data[
            'order_wx_count'] = work_record.order_wx_count + work_record.order_combine_wx_count
        data['order_cheque_money'] = check_float(
            work_record.order_cheque_cent / 100 +
            work_record.order_combine_cheque_cent / 100)
        data[
            'order_cheque_count'] = work_record.order_cheque_count + work_record.order_combine_cheque_count
        data['order_remittance_money'] = check_float(
            work_record.order_remittance_cent / 100 +
            work_record.order_combine_remittance_cent / 100)
        data[
            'order_remittance_count'] = work_record.order_remittance_count + work_record.order_combine_remittance_count
        data['order_bank_money'] = check_float(data['order_pos_money'] +
                                               data['order_remittance_money'])
        data['order_bank_count'] = data['order_pos_count'] + data[
            'order_remittance_count']

        # 赊账
        data['order_credit_money'] = check_float(
            work_record.order_credit_cent / 100)
        data['order_credit_count'] = work_record.order_credit_count

        # 赊账退还
        data['refund_credit_money'] = check_float(
            work_record.refund_credit_cent / 100)
        data['refund_credit_count'] = work_record.refund_credit_count

        # 还款记录
        data['pb_total_money'] = check_float(work_record.pb_total_cent / 100)
        data['pb_total_count'] = work_record.pb_total_count
        data['pb_cash_money'] = check_float(
            work_record.pb_cash_cent / 100 +
            work_record.pb_combine_cash_cent / 100)
        data[
            'pb_cash_count'] = work_record.pb_cash_count + work_record.pb_combine_cash_count
        data['pb_pos_money'] = check_float(
            work_record.pb_pos_cent / 100 +
            work_record.pb_combine_pos_cent / 100)
        data[
            'pb_pos_count'] = work_record.pb_pos_count + work_record.pb_combine_pos_count
        data['pb_ali_money'] = check_float(
            work_record.pb_ali_cent / 100 +
            work_record.pb_combine_ali_cent / 100)
        data[
            'pb_ali_count'] = work_record.pb_ali_count + work_record.pb_combine_ali_count
        data['pb_wx_money'] = check_float(work_record.pb_wx_cent / 100 +
                                          work_record.pb_combine_wx_cent / 100)
        data[
            'pb_wx_count'] = work_record.pb_wx_count + work_record.pb_combine_wx_count
        data['pb_cheque_money'] = check_float(
            work_record.pb_cheque_cent / 100 +
            work_record.pb_combine_cheque_cent / 100)
        data[
            'pb_cheque_count'] = work_record.pb_cheque_count + work_record.pb_combine_cheque_count
        data['pb_remittance_money'] = check_float(
            work_record.pb_remittance_cent / 100 +
            work_record.pb_combine_remittance_cent / 100)
        data[
            'pb_remittance_count'] = work_record.pb_remittance_count + work_record.pb_combine_remittance_count
        data['pb_bank_money'] = check_float(data['pb_pos_money'] +
                                            data['pb_remittance_money'])
        data['pb_bank_count'] = data['pb_pos_count'] + data[
            'pb_remittance_count']

        # 退单记录
        data['refund_total_money'] = check_float(
            work_record.refund_total_cent / 100)
        data['refund_total_count'] = work_record.refund_total_count
        data['refund_cash_money'] = check_float(
            work_record.refund_cash_cent / 100)
        data['refund_cash_count'] = work_record.refund_cash_count
        data['refund_pos_money'] = check_float(
            work_record.refund_pos_cent / 100)
        data['refund_pos_count'] = work_record.refund_pos_count
        data['refund_ali_money'] = check_float(
            work_record.refund_ali_cent / 100)
        data['refund_ali_count'] = work_record.refund_ali_count
        data['refund_wx_money'] = check_float(work_record.refund_wx_cent / 100)
        data['refund_wx_count'] = work_record.refund_wx_count
        data['refund_cheque_money'] = check_float(
            work_record.refund_cheque_cent / 100)
        data['refund_cheque_count'] = work_record.refund_cheque_count
        data['refund_remittance_money'] = check_float(
            work_record.refund_remittance_cent / 100)
        data['refund_remittance_count'] = work_record.refund_remittance_count
        data['refund_bank_money'] = check_float(
            data['refund_pos_money'] + data['refund_remittance_money'])
        data['refund_bank_count'] = data['refund_pos_count'] + data[
            'refund_remittance_count']

        # 收银台支出
        data['payout_total_money'] = check_float(
            work_record.deposit_refund_cent / 100 +
            work_record.payout_cent / 100 +
            work_record.supplier_borrow_cent / 100)
        data[
            'payout_total_count'] = work_record.deposit_refund_count + work_record.payout_count + work_record.supplier_borrow_count
        data['payout_cash_money'] = check_float(
            work_record.deposit_refund_cash_cent / 100 +
            work_record.payout_cash_cent / 100 +
            work_record.supplier_borrow_cash_cent / 100)
        data[
            'payout_cash_count'] = work_record.deposit_refund_cash_count + work_record.payout_cash_count + work_record.supplier_borrow_cash_count
        data['payout_pos_money'] = check_float(
            work_record.deposit_refund_pos_cent / 100 +
            work_record.payout_pos_cent / 100 +
            work_record.supplier_borrow_pos_cent / 100)
        data[
            'payout_pos_count'] = work_record.deposit_refund_pos_count + work_record.payout_pos_count + work_record.supplier_borrow_pos_count
        data['payout_ali_money'] = check_float(
            work_record.deposit_refund_ali_cent / 100 +
            work_record.payout_ali_cent / 100 +
            work_record.supplier_borrow_ali_cent / 100)
        data[
            'payout_ali_count'] = work_record.deposit_refund_ali_count + work_record.payout_ali_count + work_record.supplier_borrow_ali_count
        data['payout_wx_money'] = check_float(
            work_record.deposit_refund_wx_cent / 100 +
            work_record.payout_wx_cent / 100 +
            work_record.supplier_borrow_wx_cent / 100)
        data[
            'payout_wx_count'] = work_record.deposit_refund_wx_count + work_record.payout_wx_count + work_record.supplier_borrow_wx_count
        data['payout_cheque_money'] = check_float(
            work_record.deposit_refund_cheque_cent / 100 +
            work_record.payout_cheque_cent / 100 +
            work_record.supplier_borrow_cheque_cent / 100)
        data[
            'payout_cheque_count'] = work_record.deposit_refund_cheque_count + work_record.payout_cheque_count + work_record.supplier_borrow_cheque_count
        data['payout_remittance_money'] = check_float(
            work_record.deposit_refund_remittance_cent / 100 +
            work_record.payout_remittance_cent / 100 +
            work_record.supplier_borrow_remittance_cent / 100)
        data[
            'payout_remittance_count'] = work_record.deposit_refund_remittance_count + work_record.payout_remittance_count + work_record.supplier_borrow_remittance_count

        data['payout_bank_money'] = check_float(
            data['payout_pos_money'] + data['payout_remittance_money'])
        data['payout_bank_count'] = data['payout_pos_count'] + data[
            'payout_remittance_count']

        # 收银台收入
        data['income_total_money'] = check_float(
            work_record.other_income_cent / 100 +
            work_record.waste_selling_income_cent / 100)
        data[
            'income_total_count'] = work_record.other_income_count + work_record.waste_selling_income_count
        data['income_cash_money'] = check_float(
            work_record.other_income_cash_cent / 100 +
            work_record.waste_selling_income_cash_cent / 100)
        data[
            'income_cash_count'] = work_record.other_income_cash_count + work_record.waste_selling_income_cash_count
        data['income_pos_money'] = check_float(
            work_record.other_income_pos_cent / 100 +
            work_record.waste_selling_income_pos_cent / 100)
        data[
            'income_pos_count'] = work_record.other_income_pos_count + work_record.waste_selling_income_pos_count
        data['income_ali_money'] = check_float(
            work_record.other_income_ali_cent / 100 +
            work_record.waste_selling_income_ali_cent / 100)
        data[
            'income_ali_count'] = work_record.other_income_ali_count + work_record.waste_selling_income_ali_count
        data['income_wx_money'] = check_float(
            work_record.other_income_wx_cent / 100 +
            work_record.waste_selling_income_wx_cent / 100)
        data[
            'income_wx_count'] = work_record.other_income_wx_count + work_record.waste_selling_income_wx_count
        data['income_cheque_money'] = check_float(
            work_record.other_income_cheque_cent / 100 +
            work_record.waste_selling_income_cheque_cent / 100)
        data[
            'income_cheque_count'] = work_record.other_income_cheque_count + work_record.waste_selling_income_cheque_count
        data['income_remittance_money'] = check_float(
            work_record.other_income_remittance_cent / 100 +
            work_record.waste_selling_income_remittance_cent / 100)
        data[
            'income_remittance_count'] = work_record.other_income_remittance_count + work_record.waste_selling_income_remittance_count
        data['income_bank_money'] = check_float(
            data['income_pos_money'] + data['income_remittance_money'])
        data['income_bank_count'] = data['income_pos_count'] + data[
            'income_remittance_count']

        # 现金清点金额
        cash_expect_money = check_float(
            (work_record.order_cash_cent + work_record.pb_cash_cent +
             work_record.order_combine_cash_cent +
             work_record.pb_combine_cash_cent + work_record.order_fee_cash_cent
             + work_record.other_income_cash_cent + work_record.
             waste_selling_income_cash_cent - work_record.refund_cash_cent -
             work_record.deposit_refund_cash_cent - work_record.
             payout_cash_cent - work_record.supplier_borrow_cash_cent) / 100)
        cash_check_money = check_float(work_record.cash_check_cent / 100)

        data['cash_expect_money'] = cash_expect_money
        data['cash_check_money'] = cash_check_money
        data['cash_discrepancy_money'] = check_float(cash_check_money -
                                                     cash_expect_money)

        # 微信/支付宝/POS清点金额
        data['wx_expect_money'] = check_float(
            (work_record.order_wx_cent + work_record.pb_wx_cent) / 100)
        data['ali_expect_money'] = check_float(
            (work_record.order_ali_cent + work_record.pb_ali_cent) / 100)
        data['pos_expect_money'] = check_float(
            (work_record.order_pos_cent + work_record.pb_pos_cent) / 100)

        # 押金金额
        data['deposit_money'] = check_float(work_record.deposit_cent / 100)
        data['deposit_count'] = work_record.deposit_count

        # 退押金金额
        data['deposit_refund_money'] = check_float(
            work_record.deposit_refund_cent / 100)
        data['deposit_refund_count'] = work_record.deposit_refund_count

        # 押金收支出
        data['deposit_spend'] = check_float(data['deposit_money'] -
                                            data['deposit_refund_money'])

        # 抹零金额
        data['erase_money'] = check_float(work_record.erase_total_cent / 100)
        data['erase_count'] = work_record.erase_total_count
        data['erase_cash_money'] = check_float(
            work_record.erase_cash_cent / 100)
        data['erase_cash_count'] = work_record.erase_cash_count
        data['erase_pos_money'] = check_float(work_record.erase_pos_cent / 100)
        data['erase_pos_count'] = work_record.erase_pos_count
        data['erase_ali_money'] = check_float(work_record.erase_ali_cent / 100)
        data['erase_ali_count'] = work_record.erase_ali_count
        data['erase_wx_money'] = check_float(work_record.erase_wx_cent / 100)
        data['erase_wx_count'] = work_record.erase_wx_count
        data['erase_combine_money'] = check_float(
            work_record.erase_combine_cent / 100)
        data['erase_combine_count'] = work_record.erase_combine_count
        data['erase_cheque_money'] = check_float(
            work_record.erase_cheque_cent / 100)
        data['erase_cheque_count'] = work_record.erase_cheque_count
        data['erase_remittance_money'] = check_float(
            work_record.erase_remittance_cent / 100)
        data['erase_remittance_count'] = work_record.erase_remittance_count
        data['erase_bank_money'] = check_float(data['erase_pos_money'] +
                                               data['erase_remittance_money'])
        data['erase_bank_count'] = data['erase_pos_count'] + data[
            'erase_remittance_count']

        # 手续费
        data['order_fee_money'] = check_float(work_record.order_fee_cent / 100)
        data['order_fee_count'] = work_record.order_fee_count
        data['order_fee_cash_money'] = check_float(
            work_record.order_fee_cash_cent / 100)
        data['order_fee_cash_count'] = work_record.order_fee_cash_count
        data['order_fee_pos_money'] = check_float(
            work_record.order_fee_pos_cent / 100)
        data['order_fee_pos_count'] = work_record.order_fee_pos_count
        data['order_fee_ali_money'] = check_float(
            work_record.order_fee_ali_cent / 100)
        data['order_fee_ali_count'] = work_record.order_fee_ali_count
        data['order_fee_wx_money'] = check_float(
            work_record.order_fee_wx_cent / 100)
        data['order_fee_wx_count'] = work_record.order_fee_wx_count
        data['order_fee_cheque_money'] = check_float(
            work_record.order_fee_cheque_cent / 100)
        data['order_fee_cheque_count'] = work_record.order_fee_cheque_count
        data['order_fee_remittance_money'] = check_float(
            work_record.order_fee_remittance_cent / 100)
        data[
            'order_fee_remittance_count'] = work_record.order_fee_remittance_count
        data['order_fee_bank_money'] = check_float(
            data['order_fee_pos_money'] + data['order_fee_remittance_money'])
        data['order_fee_bank_count'] = data['order_fee_pos_count'] + data[
            'order_fee_remittance_count']

        # 实收资金
        data['total_money'] = check_float(
            (work_record.order_total_cent + work_record.pb_total_cent +
             work_record.other_income_cent +
             work_record.waste_selling_income_cent + work_record.order_fee_cent
             - work_record.refund_total_cent - work_record.deposit_refund_cent
             - work_record.payout_cent - work_record.supplier_borrow_cent) /
            100)
        data['total_count'] = data['order_total_count'] \
                              + data['pb_total_count'] \
                              + data['payout_total_count'] \
                              + data['income_total_count'] \
                              + data['order_fee_count'] \
                              - data['refund_total_count']
        data['total_cash_money'] = check_float(
            (work_record.order_cash_cent + work_record.order_combine_cash_cent
             + work_record.pb_cash_cent + work_record.pb_combine_cash_cent +
             work_record.other_income_cash_cent +
             work_record.waste_selling_income_cash_cent +
             work_record.order_fee_cash_cent - work_record.refund_cash_cent -
             work_record.deposit_refund_cash_cent - work_record.
             payout_cash_cent - work_record.supplier_borrow_cash_cent) / 100)
        data['total_cash_count'] = data['order_cash_count'] \
                                   + data['pb_cash_count'] \
                                   + data['payout_cash_count'] \
                                   + data['income_cash_count'] \
                                   + data['order_fee_cash_count'] \
                                   - data['refund_cash_count']

        data['total_pos_money'] = check_float(
            (work_record.order_pos_cent + work_record.order_combine_pos_cent +
             work_record.pb_pos_cent + work_record.pb_combine_pos_cent +
             work_record.other_income_pos_cent +
             work_record.waste_selling_income_pos_cent +
             work_record.order_fee_pos_cent - work_record.refund_pos_cent -
             work_record.deposit_refund_pos_cent - work_record.payout_pos_cent
             - work_record.supplier_borrow_pos_cent) / 100)
        data['total_pos_count'] = data['order_pos_count'] \
                                    + data['pb_pos_count'] \
                                    + data['income_pos_count'] \
                                    + data['payout_pos_count'] \
                                    + data['order_fee_pos_count'] \
                                    - data['refund_pos_count']
        data['total_ali_money'] = check_float(
            (work_record.order_ali_cent + work_record.order_combine_ali_cent +
             work_record.pb_ali_cent + work_record.pb_combine_ali_cent +
             work_record.other_income_ali_cent +
             work_record.waste_selling_income_ali_cent +
             work_record.order_fee_ali_cent - work_record.refund_ali_cent -
             work_record.deposit_refund_ali_cent - work_record.payout_ali_cent
             - work_record.supplier_borrow_ali_cent) / 100)
        data['total_ali_count'] = data['order_ali_count'] \
                                  + data['pb_ali_count'] \
                                  + data['income_ali_count'] \
                                  + data['payout_ali_count'] \
                                  + data['order_fee_ali_count'] \
                                  - data['refund_ali_count']
        data['total_wx_money'] = check_float(
            (work_record.order_wx_cent + work_record.order_combine_wx_cent +
             work_record.pb_wx_cent + work_record.pb_combine_wx_cent +
             work_record.other_income_wx_cent +
             work_record.waste_selling_income_wx_cent +
             work_record.order_fee_wx_cent - work_record.refund_wx_cent -
             work_record.deposit_refund_wx_cent - work_record.payout_wx_cent -
             work_record.supplier_borrow_wx_cent) / 100)
        data['total_wx_count'] = data['order_wx_count'] \
                                 + data['pb_wx_count'] \
                                 + data['income_wx_count'] \
                                 + data['payout_wx_count'] \
                                 + data['order_fee_wx_count'] \
                                 - data['refund_wx_count']

        data['total_cheque_money'] = check_float(
            (work_record.order_cheque_cent +
             work_record.order_combine_cheque_cent + work_record.pb_cheque_cent
             + work_record.pb_combine_cheque_cent +
             work_record.other_income_cheque_cent +
             work_record.waste_selling_income_cheque_cent +
             work_record.order_fee_cheque_cent - work_record.refund_cheque_cent
             - work_record.deposit_refund_cheque_cent -
             work_record.payout_cheque_cent -
             work_record.supplier_borrow_cheque_cent) / 100)
        data['total_cheque_count'] = data['order_cheque_count'] \
                                     + data['pb_cheque_count'] \
                                     + data['income_cheque_count'] \
                                     + data['payout_cheque_count'] \
                                     + data['order_fee_cheque_count'] \
                                     - data['refund_cheque_count']
        data['total_remittance_money'] = check_float(
            (work_record.order_remittance_cent + work_record.
             order_combine_remittance_cent + work_record.pb_remittance_cent +
             work_record.pb_combine_remittance_cent +
             work_record.other_income_remittance_cent +
             work_record.waste_selling_income_remittance_cent + work_record.
             order_fee_remittance_cent - work_record.refund_remittance_cent -
             work_record.deposit_refund_remittance_cent -
             work_record.payout_remittance_cent -
             work_record.supplier_borrow_remittance_cent) / 100)
        data['total_remittance_count'] = data['order_remittance_count'] \
                                         + data['pb_remittance_count'] \
                                         + data['income_remittance_count'] \
                                         + data['payout_remittance_count'] \
                                         + data['order_fee_remittance_count'] \
                                         - data['refund_remittance_count']
        data['total_bank_money'] = check_float(data['total_pos_money'] +
                                               data['total_remittance_money'])
        data['total_bank_count'] = data['total_pos_count'] + data[
            'total_remittance_count']
        return data

    @classmethod
    def get_paytype_through_fund_flow(self, fund_flow, source_type):
        pay_type_dict = {
            1: "cash",
            2: "pos",
            3: "wx",
            4: "alipay",
            5: "wx",
            6: "alipay",
            7: "wx",
            8: "alipay",
            9: "credit",
            10: "combine",
            11: "cheque",
            12: "remittance",
        }
        pay_type_text = pay_type_dict.get(fund_flow.fund_type, "")
        pay_type_text = "%s_%s" % (source_type, pay_type_text)
        return pay_type_text

    @classmethod
    def get_word_record_deposit_date(cls, work_record):
        data = {}
        # 押金金额
        data['deposit_money'] = check_float(work_record.deposit_cent / 100)
        data['deposit_count'] = work_record.deposit_count

        # 退押金金额
        data['deposit_refund_money'] = check_float(
            work_record.deposit_refund_cent / 100)
        data['deposit_refund_count'] = work_record.deposit_refund_count
        return data


# 订单
class OrderBaseFunc():
    pay_type_dict = {
        1: "现金",
        2: "pos刷卡",
        3: "条码收款|微信",
        4: "条码收款|支付宝",
        5: "二维码收款|微信",
        6: "二维码收款|支付宝",
        7: "客户扫码付|微信",
        8: "客户扫码付|支付宝",
        9: "赊账",
        10: "组合支付",
        11: "支票",
        12: "汇款转账",
    }

    pay_type_column_dict = {
        "cash_cent": 1,
        "pos_cent": 2,
        "barcode_wx_cent": 3,
        "barcode_ali_cent": 4,
        "tag_wx_cent": 7,
        "tag_ali_cent": 8,
        "credit_cent": 9,
        "cheque_cent": 11,
        "remittance_cent": 12
    }

    @classmethod
    def gen_order_num(cls, session, shop_id, source="order"):
        """ 生成单据号

        单号前缀
            10:结算 order
            11:还款 pb_order
            12:入库 stockin_order
            13:报损 breakage_order
            15:盘点 inventory_order
            16:调货 allocate_order
        """
        if source == "pb_order":
            key = "pb_order_num:{}".format(shop_id)
            models_order = models.PbOrder
            prefix = "11"
        elif source == "multi_order":
            key = "multi_order_num:{}".format(shop_id)
            models_order = models.Order
            prefix = ""
        elif source == "stockin_order":
            key = "stockin_order:{}".format(shop_id)
            models_order = models.GoodsStockinRecord
            prefix = "12"
        elif source == "breakage_order":
            key = "breakage_order:{}".format(shop_id)
            models_order = models.GoodsBreakageRecord
            prefix = "13"
        elif source == "inventory_order":
            key = "inventory_order:{}".format(shop_id)
            models_order = models.GoodsInventoryRecord
            prefix = "15"
        elif source == "allocate_order":
            key = "allocate_order:{}".format(shop_id)
            models_order = models.StorageAllocate
            prefix = "16"
        else:
            key = "order_num:{}".format(shop_id)
            models_order = models.Order
            prefix = "10"
        if redis.get(key):
            # TODO:这里强行检查单号长度来插入新的订单前缀，留到2018-10-14 by yy
            order_num = redis.get(key).decode("utf8")
            if (source == "multi_order"
                    and len(order_num) != 9) or (source != "multi_order"
                                                 and len(order_num) != 11):
                redis.delete(key)
                return None
            # TODO END

            return str(redis.incr(key))
        else:
            if source == "allocate_order":
                filter_by_param = {"apply_shop_id": shop_id}
            elif source == "multi_order":
                filter_by_param = {"shop_id": shop_id, "multi_goods": 1}
            elif source == "order":
                filter_by_param = {"shop_id": shop_id, "multi_goods": 0}
            else:
                filter_by_param = {"shop_id": shop_id}

            latest_order_num = session.query(models_order.num) \
                .filter_by(**filter_by_param) \
                .order_by(models_order.id.desc()) \
                .first()

            # TODO 单号过渡 留到2018-10-14 by yy
            if source == "multi_order":
                max_order_num = session \
                    .query(
                        models_order.num,
                        models_order.multi_goods) \
                    .filter_by(shop_id=shop_id) \
                    .filter(func.length(models_order.num) == 9) \
                    .order_by(models_order.id.desc()) \
                    .limit(1) \
                    .first()
                if max_order_num and max_order_num.multi_goods == 0:
                    latest_order_num = max_order_num

            # 初始化单号
            init_order_num = "{prefix}{shop_id}000001" \
                .format(
                prefix=prefix,
                shop_id=shop_id
            )
            if latest_order_num:
                latest_order_num = latest_order_num.num

                # TODO:这里强行检查单号长度来插入新的订单前缀，留到2018-10-14 by yy
                if source != "multi_order" and len(latest_order_num) == 9:
                    latest_order_num = prefix + latest_order_num
                if source == "multi_order" and len(latest_order_num) == 11:
                    latest_order_num = latest_order_num[2:]
                # TODO END

            order_num = str(int(latest_order_num) +
                            1) if latest_order_num else init_order_num
            redis.set(key, order_num)
            return order_num

    # 生成单号相关的先放在一起
    @classmethod
    def gen_short_num(cls, session, shop_id, source="payout"):
        """ 生成短单号，主要适用的业务场景是业务的产生不频繁，
        且用户需要手动数据单号来查找，简化用户输入
        生成规则: 五位 最小: 10001, 最大: 99999
        """
        if source == "payout":
            key = "payout_num:{}".format(shop_id)
        else:
            raise ValueError("source not supported")
        if redis.get(key):
            return str(redis.incr(key))
        else:
            db_model = session.query(
                models.PayoutShortNum.num).filter_by(shop_id=shop_id).order_by(
                    desc(models.PayoutShortNum.id)).first()
            num = "10001"
            if db_model and db_model.num:
                num = str(int(db_model.num) + 1)
            redis.set(key, num)
            return num

    @classmethod
    def get_db_order(cls,
                     session,
                     shop_id,
                     front_end_pay_id,
                     last_update_timestamp,
                     if_pborder=False,
                     if_lock=False):
        if if_pborder:
            order_redis_key = "pb_order_frontid_updatetime:%d:%s:%d" % (
                shop_id, front_end_pay_id, last_update_timestamp)
            models_order = models.PbOrder
        else:
            order_redis_key = "order_frontid_updatetime:%d:%s:%d" % (
                shop_id, front_end_pay_id, last_update_timestamp)
            models_order = models.Order
        if redis.get(order_redis_key):
            order_id = int(redis.get(order_redis_key).decode('utf-8'))
            order = session.query(models_order).filter_by(id=order_id)
            if if_lock:
                order = order.with_lockmode("update").first()
            else:
                order = order.first()
            return order
        else:
            return None

    @classmethod
    def create_db_order(cls,
                        session,
                        shop_id,
                        accountant_id,
                        debtor_id,
                        front_end_pay_id,
                        if_pborder=False,
                        multi=False):
        if if_pborder:
            models_order = models.PbOrder
            source = "pb_order"
        else:
            models_order = models.Order
            if multi:
                source = "multi_order"
            else:
                source = "order"
        add_order = False
        for i in range(20):
            # 生成订单号
            order = models_order(
                shop_id=shop_id,
                accountant_id=accountant_id,
                debtor_id=debtor_id,
                frontend_num=front_end_pay_id,
                num=cls.gen_order_num(session, shop_id, source=source))
            try:
                session.add(order)
                session.flush()
                add_order = True
                break
            except Exception as e:
                log_msg("order_create_fail", str(e))
                add_order = False
        if not add_order:
            raise ValueError("订单生成20次重试都失败，请检查")
        return order

    @classmethod
    def redis_save_order(cls,
                         shop_id,
                         front_end_pay_id,
                         last_update_timestamp,
                         order_id,
                         if_pborder=False):
        """用于在redis中缓存支付结果
        """
        if if_pborder:
            order_redis_key = "pb_order_frontid_updatetime:%d:%s:%d" % (
                shop_id, front_end_pay_id, last_update_timestamp)
        else:
            order_redis_key = "order_frontid_updatetime:%d:%s:%d" % (
                shop_id, front_end_pay_id, last_update_timestamp)
        redis.set(order_redis_key, order_id, 60 * 60)

    @classmethod
    def redis_save_order_lock(cls,
                              shop_id,
                              front_end_pay_id,
                              last_update_timestamp,
                              if_pborder=False):
        """防止并发生成订单
        """
        if if_pborder:
            order_lock_redis_key = "pb_order_frontid_updatetime_lock:%d:%s:%d" % (
                shop_id, front_end_pay_id, last_update_timestamp)
        else:
            order_lock_redis_key = "order_frontid_updatetime_lock:%d:%s:%d" % (
                shop_id, front_end_pay_id, last_update_timestamp)
        redis_set_res = redis.setnx(order_lock_redis_key, "1")
        if redis_set_res:
            redis.expire(order_lock_redis_key, 15)
            return True
        else:
            return False

    # ret:True,对应的票据list
    #     False,失败原因
    # order为结算订单ID
    @classmethod
    def finish_order(cls, session, order, total_fee=0, if_pborder=False):
        order_finish_redis_tag = 'finish_order:%s' % order.num
        order_finish_redis_tag_twice_verify = 'finish_order_twice_verify:%s' % order.num
        # print(order_finish_redis_tag)
        if if_pborder:
            order_finish_redis_tag = 'pb_' + order_finish_redis_tag
            order_finish_redis_tag_twice_verify = 'pb_' + order_finish_redis_tag_twice_verify
        if not redis.setnx(order_finish_redis_tag, '1'):
            return False, "订单已经成功完成"
        redis.expire(order_finish_redis_tag, 15)

        if total_fee == 0:
            total_fee = order.fact_total_price
        order.status = 2

        # 订单类型(仅结算订单有类型区分)
        if if_pborder:
            order_record_type = 0
        else:
            order_record_type = order.record_type

        now = datetime.datetime.now()
        account_period_date = now.date()
        if AccountingPeriodFunc.get_accounting_time(session, order.shop_id):
            account_period_date = account_period_date + datetime.timedelta(1)

        t_year, t_month, t_day, t_week = TimeFunc.get_date_split(now)
        order.pay_year = t_year
        order.pay_month = t_month
        order.pay_day = t_day
        order.pay_week = t_week
        order.pay_time = now.strftime('%H:%M:%S')
        order.account_period_date = account_period_date
        sale_record_ids = []
        deposit_cent = 0
        customer_id = order.debtor_id
        shop_id = order.shop_id
        order_id = order.id

        if if_pborder:
            # 还款订单，更新对应的还款记录
            GoodsSalesRecordPaybackRecord = models.GoodsSalesRecordPaybackRecord
            HistoryPaybackRecord = models.HistoryPaybackRecord
            GoodsSalesRecord = models.GoodsSalesRecord
            DebtHistory = models.DebtHistory

            # 处理小票还款记录状态
            session.query(GoodsSalesRecordPaybackRecord).filter_by(
                pb_order_id=order_id).update({
                    "status": 1
                })

            # 处理小票剩余未还款金额
            goods_sale_record_id_list = session.query(GoodsSalesRecordPaybackRecord.goods_sale_record_id,
                                                      GoodsSalesRecordPaybackRecord.payback_cent) \
                .filter_by(pb_order_id=order_id) \
                .all()
            payback_record_dict = {
                x.goods_sale_record_id: x.payback_cent
                for x in goods_sale_record_id_list
            }
            record_list = session.query(GoodsSalesRecord) \
                .filter(GoodsSalesRecord.id.in_(payback_record_dict.keys())) \
                .all()
            for record in record_list:
                goods_sale_record_id = record.id
                record.unpayback_money -= payback_record_dict.get(
                    goods_sale_record_id, 0)

            # 处理历史欠款还款记录状态
            session.query(HistoryPaybackRecord).filter_by(
                pb_order_id=order_id).update({
                    "status": 1
                })

            # 处理历史欠款剩余未还款金额
            debt_record_id_list = session.query(HistoryPaybackRecord.debt_history_id,
                                                      HistoryPaybackRecord.payback_cent) \
                .filter_by(pb_order_id=order_id) \
                .all()
            payback_record_dict = {
                x.debt_history_id: x.payback_cent
                for x in debt_record_id_list
            }
            record_list = session.query(DebtHistory) \
                .filter(DebtHistory.id.in_(payback_record_dict.keys())) \
                .all()
            customer = ShopCustomerFunc.get_shop_customer_through_id(
                session, shop_id, customer_id)
            for record in record_list:
                debt_record_id = record.id
                _payback_money = payback_record_dict.get(debt_record_id, 0)
                record.unpayback_money -= _payback_money
                customer.history_debt_rest -= _payback_money

        else:
            # 非还款订单，更新对应的票据状态
            GoodsSalesRecord = models.GoodsSalesRecord
            record_list = session.query(GoodsSalesRecord) \
                .filter_by(order_id=order_id).all()

            if not len(record_list):
                return False, "订单异常"

            goods_id_list = [r.goods_id for r in record_list]
            Goods = models.Goods
            goods_list = session.query(Goods) \
                .filter(Goods.id.in_(goods_id_list)).all()
            goods_dict = {g.id: g for g in goods_list}

            storage_record_list = []
            purchase_ids = []
            for record in record_list:
                storage_record = cls.update_sales_record(
                    session, shop_id, record, order, goods_dict,
                    account_period_date)
                purchase_ids.append(record.purchase_id)
                sale_record_ids.append(record.id)
                storage_record_list.append(storage_record)
                deposit_cent += record.deposit_money

            session.add_all(storage_record_list)
            session.flush()

            billing_query = session.query(models.BillingSalesRecord) \
                .filter(models.BillingSalesRecord.shop_id == shop_id)
            # 检查商品中是否有一单多品商品，一单多品订单要绑定结算订单ID
            multi_order_id_list = set(
                x.multi_order_id for x in record_list if x.multi_order_id)
            if multi_order_id_list:
                session.query(models.Order) \
                    .filter(models.Order.id.in_(multi_order_id_list)) \
                    .update({"tally_order_id": order.id,
                             "accountant_id": order.accountant_id,
                             "debtor_id": customer_id,
                             "pay_year": t_year,
                             "pay_month": t_month,
                             "pay_day": t_day,
                             "pay_week": t_week,
                             "pay_time": now.strftime('%H:%M:%S'),
                             "status": -1,
                             "account_period_date": account_period_date
                             },
                            synchronize_session=False)
                billing_query.filter(
                    models.BillingSalesRecord.record_id.in_(multi_order_id_list)) \
                    .filter(models.BillingSalesRecord.type_id == 1) \
                    .update({"accountant_id": order.accountant_id,
                             "account_period_date": account_period_date,
                             "pay_type": order.pay_type},
                            synchronize_session=False)
            # 获取结算后的单品货品ID列表：
            single_order_id_list = set(x.id for x in record_list)
            if single_order_id_list:
                billing_query.filter(
                    models.BillingSalesRecord.record_id.in_(single_order_id_list)) \
                    .filter(models.BillingSalesRecord.type_id == 0) \
                    .update({"accountant_id": order.accountant_id,
                             "account_period_date": account_period_date,
                             "pay_type": order.pay_type},
                            synchronize_session=False)

        # 处理抹零记录
        order_type = 1 if if_pborder else 0
        if order.erase_money:
            order_erase = session.query(models.OrderErase).filter_by(
                order_id=order.id, order_type=order_type).first()
            if order_erase:
                order_erase.status = 2
                order_erase.account_period_date = account_period_date
        # 处理手续费记录
        if order.fee:
            order_fee = session.query(models.OrderFee).filter_by(
                order_id=order.id, order_type=order_type).first()
            if order_fee:
                order_fee.status = 2
                order_fee.account_period_date = account_period_date
                order_fee.pay_type = order.pay_type

                # 创建手续费资金流水记录 TODO 组合支付可以收手续费之后需要处理组合手续费 FundFlow 创建
                fee_order_type = 1 if if_pborder else 0
                cls.add_order_fundflow(
                    session,
                    shop_id,
                    order.pay_type,
                    10,
                    order_fee.fee,
                    order.id,
                    order.accountant_id,
                    fee_order_type=fee_order_type)

        if if_pborder:
            debt_type = 'payback'
            flow_source_type = 2
            deposit_record_type = ""
            pay_type_text = cls.get_paytype_through_order(
                order, source_type="pb")
        elif order_record_type == 1:
            debt_type = 'refund'
            flow_source_type = 3
            deposit_record_type = "order_refund_deposit"
            pay_type_text = cls.get_refund_pay_type_text(order.pay_type)
        else:
            debt_type = 'pay_withcredit'
            flow_source_type = 1
            deposit_record_type = "deposit"
            pay_type_text = cls.get_paytype_through_order(
                order, source_type="order")

        # 赊账订单与还款订单,债务更新
        shop_id, fk_id, accountant_id = order.shop_id, order.id, order.accountant_id
        if (not if_pborder and order.pay_type == 9) or if_pborder:
            # 还款使用订单金额计算
            debt_value, operator_id = order.total_price, accountant_id
            DebtFunc.update_debt(
                session,
                debt_type,
                debt_value,
                customer_id,
                shop_id,
                operator_id,
                fk_id,
                account_period_date=account_period_date)
        # 有实际入账的,资金流水表
        if if_pborder or order.pay_type != 9:
            fund_type, fund_value, accountant_id = order.pay_type, total_fee, accountant_id

            # 组合支付需生成两条流水记录
            if fund_type == 10:
                cls.add_combine_order_fundflow(session, shop_id,
                                               flow_source_type, fund_value,
                                               order.id, accountant_id)
            else:
                cls.add_order_fundflow(session, shop_id, fund_type,
                                       flow_source_type, fund_value, fk_id,
                                       accountant_id)
            session.flush()

        # 非还款订单
        if not if_pborder:
            # 单独更新一下押金
            if deposit_cent:
                AccoutantFunc.update_work_record(session, accountant_id,
                                                 shop_id, deposit_cent,
                                                 deposit_record_type, fk_id)

            # 每一个订单对应的所有流水应该是相同的采购员(如果有采购员的话)
            if purchase_ids and len(set(purchase_ids)) > 1:
                return False, "该订单对应的所有流水包含不同的采购员"
            purchase_id = purchase_ids[0] if purchase_ids else 0
            # 更新用户消费信息(仅记录消费订单)
            if customer_id:
                if order_record_type == 0:
                    update_purchase_money = order.fact_total_price
                    # 更新用户积分
                    customer = ShopCustomerFunc.get_shop_customer_through_id(
                        session, shop_id, customer_id)
                    ShopCustomerFunc.purchase_point(session, shop_id, customer,
                                                    order, accountant_id)
                else:
                    update_purchase_money = -order.fact_total_price
                ShopCustomerFunc.update_purchase_info(
                    session,
                    customer_id,
                    shop_id,
                    purchase_money=update_purchase_money,
                    purchase_id=purchase_id)

        # 更新收银员抹零记录
        if order.erase_money:
            erase_pay_type_text = cls.get_paytype_through_order(
                order, source_type="order_erase")
            AccoutantFunc.update_work_record(session, accountant_id, shop_id,
                                             order.erase_money,
                                             erase_pay_type_text, fk_id)

        # 更新手续费记录
        if order.fee:
            fee_pay_type_text = cls.get_paytype_through_order(
                order, source_type="order_fee")
            AccoutantFunc.update_work_record(session, accountant_id, shop_id,
                                             order.fee, fee_pay_type_text,
                                             fk_id)

        # 收银员收银记录
        AccoutantFunc.update_work_record(session, accountant_id, shop_id,
                                         total_fee, pay_type_text, fk_id)

        #  二次检查当前订单是否已经完成
        if not redis.setnx(order_finish_redis_tag_twice_verify, '1'):
            return False, "订单已经成功完成"
        redis.expire(order_finish_redis_tag_twice_verify, 5 * 60)

        # XXX：处理前端单号检查，感觉不够保险，有更好的办法要改掉 by yy 2018-10-11
        try:
            front_num_redis_key = "front_num:{shop_id}:{front_num}:{fact_total_price}"\
                .format(shop_id=shop_id, front_num=order.frontend_num, fact_total_price=order.fact_total_price)
            redis.delete(front_num_redis_key)
        except:
            pass
        return True, sale_record_ids

    @classmethod
    def update_sales_record(cls,
                            session,
                            shop_id,
                            record,
                            order,
                            goods_dict,
                            account_period_date,
                            order_time=None,
                            recover_storage=True):
        """ 结算过程中更新流水记录，并生成库存变更记录

        :param record: obj, `class sa.GoodsSalesRecord`
        :param order: obj, `class sa.Order`
        :param goods_dict: 货品信息汇总字典
        :param account_period_date: 帐期日
        :param order_time: 结算时间, 默认为现在，手工下单稍有区别

        :rtype: obj, `class sa.GoodsStorageRecord`
        """
        goods_id = record.goods_id
        record_type = record.record_type
        commission_mul = record.commission_mul
        sales_num = record.sales_num * 100
        storage_type = 2
        is_refund = False
        if record_type == 1:
            commission_mul = -commission_mul
            sales_num = -sales_num
            storage_type = 3
            is_refund = True

        # 更新货品销售记录
        record.status = 3
        record.accountant_id = order.accountant_id
        record.account_period_date = account_period_date
        record.order_time = order_time if order_time else datetime.datetime.now(
        )
        record.pay_type = order.pay_type
        record.shop_customer_id = order.debtor_id
        if record.pay_type == 9:
            record.unpayback_money = record.receipt_money

        storage_record = None
        goods = goods_dict.get(goods_id)
        GoodsBatchService.consume(
            session,
            goods,
            operator_id=record.salesman_id,
            sale_record=record,
            is_refund=is_refund,
            recover_storage=recover_storage,
        )

        if recover_storage:
            # 更新货品库存
            origin_storage = goods.storage
            goods.storage = goods.storage - commission_mul
            origin_storage_num = goods.storage_num
            goods.storage_num = goods.storage_num - sales_num

            # 更新商品的库存版本
            GoodsFunc.set_goods_storage_version(goods_id)

            record.storage = goods.storage
            record.storage_num = goods.storage_num

            # 创建库存流水记录
            storage_record = models.GoodsStorageRecord(
                shop_id=shop_id,
                goods_id=goods_id,
                type=storage_type,
                fk_id=record.id,
                operater_id=order.accountant_id,
                origin_storage=origin_storage,
                now_storage=goods.storage,
                origin_storage_num=origin_storage_num,
                now_storage_num=goods.storage_num,
            )
        return storage_record

    # 收银台结算完成之后发送短信
    @classmethod
    def send_message_after_settlement(cls,
                                      session,
                                      order,
                                      if_pborder,
                                      is_refund=0):
        customer_id = order.debtor_id
        shop_id = order.shop_id
        accountant_id = order.accountant_id
        salersman_settlement_send_message_key = "salersman_settlement_send_message_key:%d" % (
            order.id)
        errmsg, notify_phone = "not send", ""
        if customer_id:
            customer = ShopCustomerFunc.get_shop_customer_through_id(
                session, shop_id, customer_id)
            accountant_name = session.query(
                models.Accountinfo.realname,
                models.Accountinfo.nickname).filter_by(
                    id=accountant_id).first()
            shop_name = session.query(
                models.Shop.shop_name).filter_by(id=shop_id).scalar() or ""
            customer_name = customer.name
            notify_phone = customer.phone
            time = TimeFunc.time_to_str(datetime.datetime.now())
            accountant_name = accountant_name[0] or accountant_name[1]
            total_price = check_float(order.total_price / 100)
            debt_rest = check_float(customer.debt_rest / 100)
            point_display = session.query(
                models.Config.point_display).filter_by(
                    id=shop_id).scalar() or 0
            # 过滤特殊字符
            shop_name = shop_name.replace("【", "").replace("】", "")
            customer_name = customer_name.replace("【", "").replace("】", "")
            accountant_name = accountant_name.replace("【", "").replace("】", "")

            # 微信模板消息
            if if_pborder:
                type = 1
            elif order.pay_type == 9:
                if is_refund:
                    type = 2
                else:
                    type = 3
            else:
                if is_refund:
                    type = 4
                else:
                    type = 5
            account = AccountFunc.get_account_by_phone(session, customer.phone)
            touser = account.wx_openid if account else ""
            if touser:
                send(
                    CustomerFlow,
                    customer_name,
                    shop_id,
                    shop_name,
                    type,
                    time,
                    accountant_name=accountant_name,
                    total_price=total_price,
                    point_display=point_display,
                    debt_rest=debt_rest,
                    purchase_times=customer.purchase_times,
                    point=customer.point,
                    touser=touser)

            if redis.get(salersman_settlement_send_message_key):
                # 给客户发送短信
                from libs.yunpian import send_yunpian_accountant_settlement_payback, \
                    send_yunpian_accountant_settlement_credit, \
                    send_yunpian_accountant_settlement_normal, \
                    send_yunpian_credit_refund, \
                    send_yunpian_refund

                if notify_phone:
                    if if_pborder:
                        # 发送还款短信
                        errmsg = send_yunpian_accountant_settlement_payback(
                            notify_phone, customer_name, shop_name,
                            total_price, debt_rest, time, accountant_name)
                    elif order.pay_type == 9:
                        # 发送赊账退单短信
                        if is_refund:
                            errmsg = send_yunpian_credit_refund(
                                notify_phone, customer_name, shop_name,
                                total_price, debt_rest, time, accountant_name)
                        # 发送赊账短信
                        else:
                            errmsg = send_yunpian_accountant_settlement_credit(
                                notify_phone, customer_name, shop_name,
                                total_price, debt_rest, time, accountant_name)
                    else:
                        # 发送退单短信
                        if is_refund:
                            errmsg = send_yunpian_refund(
                                notify_phone, customer_name, shop_name,
                                total_price, time, accountant_name)
                        # 发送正常结算短信
                        # 查询店铺是否开启积分功能
                        else:
                            errmsg = send_yunpian_accountant_settlement_normal(
                                notify_phone, customer_name, shop_name,
                                customer.purchase_times, total_price,
                                customer.point, time, accountant_name,
                                point_display)
        if errmsg != True:
            # BUG-收银台短信发送失败信息跟踪
            log_msg(
                "accountant_message_error",
                str([
                    customer_id,
                    redis.get(salersman_settlement_send_message_key),
                    notify_phone, errmsg,
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]), "%Y-%m")
        redis.delete(salersman_settlement_send_message_key)
        return

    # 收银台退单完成之后发送短信
    @classmethod
    def send_message_after_refund(cls, session, order, send_sms):
        if not order.debtor_id:
            return

        from libs.yunpian import send_yunpian_refund, send_yunpian_credit_refund
        customer = ShopCustomerFunc.get_shop_customer_through_id(
            session, order.shop_id, order.debtor_id)
        accountant_name = session.query(models.Accountinfo.realname, models.Accountinfo.nickname) \
            .filter_by(id=order.accountant_id) \
            .first()
        shop_name = session.query(models.Shop.shop_name) \
                        .filter_by(id=order.shop_id) \
                        .scalar() or ""
        send_time = TimeFunc.time_to_str(datetime.datetime.now())
        accountant_name = accountant_name[0] or accountant_name[1]
        total_price = check_float(order.fact_total_price / 100)
        debt_rest = check_float(customer.debt_rest / 100)

        # 过滤特殊字符
        shop_name = shop_name.replace("【", "").replace("】", "")
        customer_name = customer.name.replace("【", "").replace("】", "")
        accountant_name = accountant_name.replace("【", "").replace("】", "")
        account = AccountFunc.get_account_by_phone(session, customer.phone)
        touser = account.wx_openid if account else ""

        if order.pay_type == 9:
            type = 2
        else:
            type = 4
        if touser:
            send(
                CustomerFlow,
                customer_name,
                order.shop_id,
                shop_name,
                type,
                send_time,
                accountant_name=accountant_name,
                total_price=total_price,
                debt_rest=debt_rest,
                touser=touser)

        if send_sms:

            if customer.phone:
                if order.pay_type == 9:
                    errmsg = send_yunpian_credit_refund(
                        customer.phone, customer_name, shop_name, total_price,
                        debt_rest, send_time, accountant_name)
                else:
                    errmsg = send_yunpian_refund(customer.phone, customer_name,
                                                 shop_name, total_price,
                                                 send_time, accountant_name)
            else:
                errmsg = "no phone registered"

            if isinstance(errmsg, str) and errmsg:
                log_msg(
                    "refund_sms_error",
                    str([order.debtor_id, customer.phone, errmsg, send_time]),
                    "%Y-%m")

    #组合支付流水记录
    @classmethod
    def add_combine_order_fundflow(cls, session, shop_id, source_type,
                                   args_fund_value, order_id, accountant_id):
        '''
            条码支付的流水值是取自支付回调中实际取得的值需单独处处理（fund_type为3/4的情况使用的是回调值）
        '''
        order_money = cls.get_order_money(
            session, order_id, source_type=source_type)

        cash_cent = order_money.cash_cent
        pos_cent = order_money.pos_cent
        tag_ali_cent = order_money.tag_ali_cent
        tag_wx_cent = order_money.tag_wx_cent
        if cash_cent:
            fund_type = 1
            fund_value = cash_cent
            cls.add_order_fundflow(session, shop_id, fund_type, source_type,
                                   fund_value, order_id, accountant_id)
        if pos_cent:
            fund_type = 2
            fund_value = pos_cent
            cls.add_order_fundflow(session, shop_id, fund_type, source_type,
                                   fund_value, order_id, accountant_id)
        if order_money.barcode_wx_cent:
            fund_type = 3
            fund_value = args_fund_value
            cls.add_order_fundflow(session, shop_id, fund_type, source_type,
                                   fund_value, order_id, accountant_id)
        if order_money.barcode_ali_cent:
            fund_type = 4
            fund_value = args_fund_value
            cls.add_order_fundflow(session, shop_id, fund_type, source_type,
                                   fund_value, order_id, accountant_id)
        if tag_wx_cent:
            fund_type = 7
            fund_value = tag_wx_cent
            cls.add_order_fundflow(session, shop_id, fund_type, source_type,
                                   fund_value, order_id, accountant_id)
        if tag_ali_cent:
            fund_type = 8
            fund_value = tag_ali_cent
            cls.add_order_fundflow(session, shop_id, fund_type, source_type,
                                   fund_value, order_id, accountant_id)

    @classmethod
    def get_order_money(cls, session, order_id, source_type=1):
        """
            source_type 1:结算 2:还款
        """
        if source_type == 1:
            models_name = models.OrderMoney
        else:
            models_name = models.PbOrderMoney
        order_money = session.query(models_name) \
            .filter_by(id=order_id) \
            .first()
        return order_money

    @classmethod
    def get_order_money_through_id_list(cls,
                                        session,
                                        order_id_list,
                                        source_type="order"):
        if order_id_list:
            if source_type == "order":
                models_name = models.OrderMoney
            else:
                models_name = models.PbOrderMoney
            order_money_list = session.query(models_name) \
                .filter(models_name.id.in_(order_id_list)) \
                .all()
        else:
            order_money_list = []
        return order_money_list

    # 添加流水
    @classmethod
    def add_order_fundflow(cls,
                           session,
                           shop_id,
                           fund_type,
                           source_type,
                           fund_value,
                           fk_id,
                           accountant_id,
                           create_date_time=None,
                           account_period_date=None,
                           fee_order_type=0):
        if not create_date_time:
            create_date_time = datetime.datetime.now()
        t_year, t_month, t_day, t_week = TimeFunc.get_date_split(
            create_date_time)

        if not account_period_date:
            account_period_date = TimeFunc.time_to_str(create_date_time,
                                                       'date')
            if AccountingPeriodFunc.get_accounting_time(session, shop_id):
                account_period_date = TimeFunc.time_to_str(
                    create_date_time + datetime.timedelta(1), 'date')
        fund_flow_new = models.FundFlow(
            shop_id=shop_id,
            create_year=t_year,
            create_month=t_month,
            create_day=t_day,
            create_week=t_week,
            create_time=create_date_time.strftime('%H:%M:%S'),
            create_date=create_date_time.date(),
            fund_type=fund_type,
            source_type=source_type,
            fund_value=fund_value,
            fk_id=fk_id,
            accountant_id=accountant_id,
            account_period_date=account_period_date,
            fee_order_type=fee_order_type)
        session.add(fund_flow_new)
        session.flush()

    # 获取订单条码支付类型
    @classmethod
    def get_order_online_type(self,
                              session,
                              order,
                              if_front=False,
                              if_pborder=False):
        pay_type = ''

        if if_front:
            pay_type_wx = "wx"
            pay_type_ali = "ali"
        else:
            pay_type_wx = "010"
            pay_type_ali = "020"

        if order.pay_type == 3:
            pay_type = pay_type_wx
        elif order.pay_type == 4:
            pay_type = pay_type_ali
        elif order.pay_type == 10:
            order_id = order.id
            if if_pborder:
                OrderMoney = models.PbOrderMoney
            else:
                OrderMoney = models.OrderMoney
            order_money = session.query(OrderMoney) \
                .filter_by(id=order_id) \
                .first()
            if order_money.barcode_wx_cent:
                pay_type = pay_type_wx
            if order_money.barcode_ali_cent:
                pay_type = pay_type_ali
        return pay_type

    # 获取订单条码支付金额
    @classmethod
    def get_order_barcode_price(self, session, order, if_pborder=False):
        order_barcode_price = 0
        if order.pay_type in [3, 4]:
            order_barcode_price = order.fact_total_price
        elif order.pay_type == 10:
            order_id = order.id
            if if_pborder:
                OrderMoney = models.PbOrderMoney
            else:
                OrderMoney = models.OrderMoney
            order_money = session.query(OrderMoney) \
                .filter_by(id=order_id) \
                .first()
            if order_money.barcode_wx_cent:
                order_barcode_price = order_money.barcode_wx_cent
            if order_money.barcode_ali_cent:
                order_barcode_price = order_money.barcode_ali_cent
        return order_barcode_price

    @classmethod
    def get_pay_type_text(cls, pay_type, source_type="order"):
        pay_type_dict = {
            1: "cash",
            2: "pos",
            3: "wx",
            4: "alipay",
            5: "wx",
            6: "alipay",
            7: "wx",
            8: "alipay",
            9: "credit",
            10: "combine",
            11: "cheque",
            12: "remittance",
        }
        pay_type_text = pay_type_dict.get(pay_type, "")
        pay_type_text = "%s_%s" % (source_type, pay_type_text)
        return pay_type_text

    @classmethod
    def get_paytype_through_order(cls, order, source_type="order"):
        return cls.get_pay_type_text(order.pay_type, source_type)

    # 返回:
    #   类型说明:0-支付查询中|1-支付出错|2-支付成功
    #   字典说明:out_trade_no,channel_trade_no必有的；0/1,+msg;2,+total_fee
    @classmethod
    def barpay_lcsw_order(cls,
                          session,
                          shop_id,
                          auth_code,
                          order,
                          if_pborder=False):
        ret_dict = {}
        lc_merchant_no, lc_terminal_id, lc_access_token = ShopFunc.get_lcpay_account(
            session, shop_id)
        pay_type = cls.get_order_online_type(
            session, order, if_pborder=if_pborder)
        create_time = order.create_time.strftime("%Y%m%d%H%M%S")
        if if_pborder:
            order_num = 'p' + order.num
        else:
            order_num = order.num
        shop_name = session.query(
            models.Shop.shop_name).filter_by(id=shop_id).scalar()
        order_barcode_price = cls.get_order_barcode_price(
            session, order, if_pborder=if_pborder)
        parameters = LcswPay.getBarcodePayParas(
            pay_type, auth_code, order_num, create_time,
            str(order_barcode_price), shop_name, lc_merchant_no,
            lc_terminal_id, lc_access_token)
        # parameters = LcswPay.getBarcodePayParas(pay_type,auth_code,order_num,create_time,"1",shop_name,lc_merchant_no,lc_terminal_id,lc_access_token)
        headers = {'content-type': 'application/json'}
        log_date = datetime.datetime.now().strftime('%Y%m%d')
        log_msg('lc_bar_parameters/%s' % log_date,
                'order_num:%s' % (order_num))
        log_msg('lc_bar_parameters/%s' % log_date,
                'datetime:%s' % (str(datetime.datetime.now())))
        log_msg_dict('lc_bar_parameters/%s' % log_date, parameters)
        r = requests.post(
            LCSW_HANDLE_HOST + '/pay/100/barcodepay',
            data=json.dumps(parameters),
            verify=False,
            headers=headers)
        try:
            res_dict = json.loads(r.text)
        except Exception as e:
            ret_dict["out_trade_no"] = ''
            ret_dict["channel_trade_no"] = ''
            ret_dict['msg'] = "支付结果未知,点击结算完成按钮重试"
            log_msg('lc_bar_error', '返回内容为【%s】' % str(r.text))
            from handlers.celery_print import send_servererror_msg
            time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            send_servererror_msg.apply_async(
                args=[
                    "oAOOiwFYvm8judvvfLr2bteSI9oQ", None, "条码支付失败", "接口返回有误",
                    time_now, ""
                ],
                countdown=0)
            return 0, ret_dict
        log_msg('lc_bar_res/%s' % log_date, 'order_num:%s' % (order_num))
        log_msg('lc_bar_res/%s' % log_date,
                'datetime:%s' % (str(datetime.datetime.now())))
        log_msg_dict('lc_bar_res/%s' % log_date, res_dict)
        ret_dict["out_trade_no"] = res_dict.get('out_trade_no', '')
        ret_dict["channel_trade_no"] = res_dict.get('channel_trade_no', '')
        # 响应码：01成功 ，02失败，响应码仅代表通信状态，不代表业务结果
        if res_dict['return_code'] == '02':
            ret_dict['msg'] = res_dict['return_msg']
            return 1, ret_dict
        else:
            if res_dict['attach'] != 'SENGUOPRODUCT':
                ret_dict['msg'] = '附加信息有误'
                return 1, ret_dict
            key_sign = res_dict['key_sign']
            str_sign = LcswPay.getStrForSignOfBarcodeRet(res_dict)
            if key_sign != hashlib.md5(
                    str_sign.encode('utf-8')).hexdigest().lower():
                ret_dict['msg'] = "签名有误"
                return 1, ret_dict
            else:
                # 业务结果：01成功 02失败 ，03支付中
                result_code = res_dict['result_code']
                if result_code == '02':
                    ret_dict['msg'] = res_dict['return_msg']
                    return 1, ret_dict
                else:
                    # 支付中
                    if result_code == '03':
                        ret_dict['msg'] = "支付查询中"
                        return 0, ret_dict
                    else:
                        ret_dict['total_fee'] = int(res_dict['total_fee'])
                        return 2, ret_dict

    @classmethod
    def set_query_base(cls, redis_query_key, redis_query_value, redis_stop_key,
                       redis_stop_value):
        redis.set(redis_query_key, redis_query_value, 3600)
        redis.set(redis_stop_key, redis_stop_value, 3600)

    # 返回:
    #   类型说明:0-支付查询中|1-支付出错|2-支付成功
    #   字典说明:out_trade_no,channel_trade_no必有的；0/1,+msg;2,+total_fee
    @classmethod
    def query_lcsw_order(cls, session, shop_id, order, if_pborder=False):
        ret_dict = {}
        lc_merchant_no, lc_terminal_id, lc_access_token = ShopFunc.get_lcpay_account(
            session, shop_id)
        pay_type = cls.get_order_online_type(
            session, order, if_pborder=if_pborder)
        order_num = order.num
        create_time = order.create_time.strftime("%Y%m%d%H%M%S")
        if if_pborder:
            pay_trace = 'p' + order_num
            order_query_num = 'p' + order_num + 'q1'
        else:
            pay_trace = order_num
            order_query_num = order_num + 'q1'
        out_trade_no = order.lc_out_trade_no
        if out_trade_no:
            parameters = LcswPay.getQueryParas(pay_type, order_query_num,
                                               out_trade_no, lc_merchant_no,
                                               lc_terminal_id, lc_access_token)
        else:
            parameters = LcswPay.getQueryParas(
                pay_type,
                order_query_num,
                out_trade_no,
                lc_merchant_no,
                lc_terminal_id,
                lc_access_token,
                pay_trace=pay_trace,
                pay_time=create_time)
        headers = {'content-type': 'application/json'}
        r = requests.post(
            LCSW_HANDLE_HOST + '/pay/100/query',
            data=json.dumps(parameters),
            verify=False,
            headers=headers)
        res_dict = json.loads(r.text)
        log_date = datetime.datetime.now().strftime('%Y%m%d')
        log_msg_dict('lc_bar_query_res/%s' % log_date, res_dict)
        ret_dict["out_trade_no"] = res_dict.get('out_trade_no', '')
        ret_dict["channel_trade_no"] = res_dict.get('channel_trade_no', '')
        # 响应码：01成功 ，02失败，响应码仅代表通信状态，不代表业务结果
        if res_dict['return_code'] == '02':
            ret_dict['msg'] = res_dict['return_msg']
            return 1, ret_dict
        else:
            if res_dict['attach'] != 'SENGUOPRODUCT':
                ret_dict['msg'] = '附加信息有误'
                return 1, ret_dict
            key_sign = res_dict['key_sign']
            str_sign = LcswPay.getStrForSignOfQueryRet(res_dict)
            if key_sign != hashlib.md5(
                    str_sign.encode('utf-8')).hexdigest().lower():
                ret_dict['msg'] = "签名有误"
                return 1, ret_dict
            else:
                # 业务结果：01成功 02失败 ，03支付中
                result_code = res_dict['result_code']
                if result_code == '02':
                    ret_dict['msg'] = res_dict['return_msg']
                    return 1, ret_dict
                else:
                    # 支付中
                    if result_code == '03':
                        ret_dict['msg'] = "支付查询中"
                        return 0, ret_dict
                    else:
                        ret_dict['total_fee'] = res_dict['total_fee']
                        return 2, ret_dict

    # 返回:
    #   类型说明:1-退款失败|2-退款成功
    #   字典说明:out_trade_no,out_refund_no必有的；1,+msg;2,+total_fee
    @classmethod
    def refund_lcsw_order(cls, session, shop_id, order, refund_fee):
        ret_dict = {}
        lc_merchant_no, lc_terminal_id, lc_access_token = ShopFunc.get_lcpay_account(
            session, shop_id)
        if order.pay_type == 3:
            pay_type = '010'
        else:
            pay_type = '020'
        order_num = order.num
        create_time = order.create_time.strftime("%Y%m%d%H%M%S")
        if if_pborder:
            pay_trace = 'p' + order_num
            order_refund_num = 'p' + order_num + 'r1'
        else:
            pay_trace = order_num
            order_refund_num = order_num + 'r1'
        out_trade_no = order.lc_out_trade_no
        if out_trade_no:
            parameters = LcswPay.getRefundParas(
                pay_type, order_refund_num, out_trade_no, str(refund_fee),
                lc_merchant_no, lc_terminal_id, lc_access_token)
        else:
            parameters = LcswPay.getRefundParas(
                pay_type,
                order_refund_num,
                out_trade_no,
                str(refund_fee),
                lc_merchant_no,
                lc_terminal_id,
                lc_access_token,
                pay_trace=pay_trace,
                pay_time=create_time)
        headers = {'content-type': 'application/json'}
        r = requests.post(
            LCSW_HANDLE_HOST + '/pay/100/refund',
            data=json.dumps(parameters),
            verify=False,
            headers=headers)
        res_dict = json.loads(r.text)
        ret_dict["out_trade_no"] = res_dict.get('out_trade_no', '')
        ret_dict["out_refund_no"] = res_dict.get('out_refund_no', '')
        # 响应码：01成功 ，02失败，响应码仅代表通信状态，不代表业务结果
        if res_dict['return_code'] == '02':
            ret_dict['msg'] = res_dict['return_msg']
            return 1, ret_dict
        else:
            key_sign = res_dict['key_sign']
            str_sign = LcswPay.getStrForSignOfRefundRet(res_dict)
            if key_sign != hashlib.md5(
                    str_sign.encode('utf-8')).hexdigest().lower():
                ret_dict['msg'] = "签名有误"
                return 1, ret_dict
            else:
                # 业务结果：01成功 02失败
                result_code = res_dict['result_code']
                if result_code == '02':
                    ret_dict['msg'] = res_dict['return_msg']
                    return 1, ret_dict
                else:
                    ret_dict['recall'] = res_dict['recall']
                    return 2, ret_dict

    @classmethod
    def encode_dict(cls, params):
        import six
        if six.PY3:
            from urllib.parse import parse_qs, urlparse, unquote
        else:
            from urlparse import parse_qs, urlparse, unquote
        return {
            k: six.u(v).encode('utf-8') if isinstance(v, str) else
            v.encode('utf-8') if isinstance(v, six.string_types) else v
            for k, v in six.iteritems(params)
        }

    @classmethod
    def wappay_url_lcsw(cls, session, shop_id, shop_name, total_fee,
                        wap_order):
        try:
            from urllib import urlencode
        except ImportError:
            from urllib.parse import urlencode
        lc_merchant_no, lc_terminal_id, lc_access_token = ShopFunc.get_lcpay_account(
            session, shop_id)
        terminal_trace = "%dw%d" % (shop_id, wap_order.id)
        headers = {'content-type': 'application/json'}
        parameters = LcswPay.getWapPayParas(lc_merchant_no, lc_terminal_id,
                                            terminal_trace, total_fee,
                                            shop_name, lc_access_token)
        pay_url = LCSW_HANDLE_HOST + '/open/wap/110/pay?' + urlencode(
            cls.encode_dict(parameters))
        return pay_url

    # 支付类型
    @classmethod
    def pay_type_text(cls, pay_type):
        text = cls.pay_type_dict.get(pay_type, "")
        return text

    # 组合支付类型
    @classmethod
    def combine_pay_type_text(cls, session, order_id, source_type="order"):
        """ source_type order: 订单结算 """
        pay_dict = cls.combine_pay_type_detail_through_id_list(
            session, [order_id], source_type=source_type)
        pay_type_dict = pay_dict[order_id]
        pay_type_text_list = []
        for pay_type in pay_type_dict:
            pay_money = pay_type_dict[pay_type]
            pay_type_text_list.append("%s:%s元" % (pay_type, pay_money))
        pay_type_text = "+".join(pay_type_text_list)
        return pay_type_text

    @classmethod
    def split_order_money(cls, order_money):
        """ 拆分组合支付的支付方式 """
        if not order_money:
            return {}
        result = {}
        if order_money.cash_cent:
            result[1] = order_money.cash_cent
        if order_money.pos_cent:
            result[2] = order_money.pos_cent
        if order_money.barcode_wx_cent:
            result[3] = order_money.barcode_wx_cent
        if order_money.barcode_ali_cent:
            result[4] = order_money.barcode_ali_cent
        if order_money.tag_wx_cent:
            result[7] = order_money.tag_wx_cent
        if order_money.tag_ali_cent:
            result[8] = order_money.tag_ali_cent
        if order_money.credit_cent:
            result[9] = order_money.credit_cent
        if order_money.cheque_cent:
            result[11] = order_money.cheque_cent
        if order_money.remittance_cent:
            result[12] = order_money.remittance_cent
        return result

    # 判断显示为普通支付类型or组合支付类型
    @classmethod
    def order_pay_type_text(cls, order_id, pay_type, combine_pay_dict):
        pay_type_text = "-"
        if order_id:
            if pay_type == 10:
                pay_type_text = combine_pay_dict.get(order_id, "")
            else:
                pay_type_text = OrderBaseFunc.pay_type_text(pay_type)
        return pay_type_text

    @classmethod
    def multi_combine_pay_type_text(cls,
                                    session,
                                    order_id_list,
                                    source_type="order"):
        """ 批量获取组合支付类型

        :param order_id_list 结算/还款订单ID
        :param source_type order/pb_order 结算/还款
        """
        pay_dict = cls.combine_pay_type_detail_through_id_list(
            session, order_id_list, source_type=source_type)
        pay_type_text_list = []
        combine_pay_dict = dict()
        for order_id in pay_dict:
            pay_type_dict = pay_dict[order_id]
            pay_list = [
                "%s:%s元" % (pay_type, pay_money)
                for pay_type, pay_money in pay_type_dict.items()
            ]
            pay_text = "+".join(pay_list)
            combine_pay_dict[order_id] = pay_text
        return combine_pay_dict

    # 批量获取组合支付详情
    @classmethod
    def combine_pay_type_detail_through_id_list(cls,
                                                session,
                                                order_id_list,
                                                return_type="text",
                                                source_type="order"):
        order_money_list = cls.get_order_money_through_id_list(
            session, order_id_list, source_type=source_type)

        check_float = NumFunc.check_float
        pay_dict = {}
        pay_type_column_dict = cls.pay_type_column_dict
        for order_money in order_money_list:
            order_id = order_money.id

            if return_type == "text":
                pay_dict[order_id] = {}
            else:
                pay_dict[order_id] = []
            order_money_dict = order_money.all_props()

            for column_name in pay_type_column_dict:
                column_code = pay_type_column_dict[column_name]
                if order_money_dict.get(column_name, 0):
                    money = check_float(order_money_dict[column_name] / 100)
                    if return_type == "text":
                        pay_dict[order_id][cls.pay_type_text(
                            column_code)] = money
                    else:
                        pay_dict[order_id].append({column_code: money})
        return pay_dict

    # 退单支付类型
    @classmethod
    def get_refund_pay_type_text(self, refund_pay_type):
        refund_pay_type_dict = {
            1: "refund_cash",
            2: "refund_pos",
            3: "refund_wx",
            4: "refund_alipay",
            7: "refund_wx",
            8: "refund_alipay",
            9: "refund_credit",
            11: "refund_cheque",
            12: "refund_remittance"
        }
        refund_pay_type_text = refund_pay_type_dict.get(refund_pay_type, "")
        return refund_pay_type_text

    # 判断一个单号是否是订单号
    @classmethod
    def check_if_order_through_num(self, record_num, shop_id=0):
        shop_id_str = str(shop_id)
        shop_id_len = len(shop_id_str)
        if len(record_num[shop_id_len:]) == 6:
            return True
        else:
            return False

    @classmethod
    def get_order_throuth_id(cls, session, order_id):
        '''
            根据订单ID获取订单
        '''
        order = session.query(models.Order) \
            .filter_by(id=order_id) \
            .first()
        return order

    @classmethod
    def get_order_dict_throuth_id_list(cls, session, order_id_list):
        '''
            根据订单ID列表获取订单字典
        '''
        if order_id_list:
            order_list = session.query(models.Order) \
                .filter(models.Order.id.in_(order_id_list)) \
                .all()
        else:
            order_list = []
        order_dict = {x.id: x for x in order_list}
        return order_dict

    @classmethod
    def get_order_num_throuth_id(cls, session, order_id):
        '''
            根据订单ID获取订单num
        '''
        order_num = session.query(models.Order.num) \
                        .filter_by(id=order_id) \
                        .scalar() or ""
        return order_num

    @classmethod
    def get_order_through_num(cls, session, order_num, shop_id=0):
        '''
            根据订单号获取订单num
        '''
        order = session.query(models.Order) \
            .filter_by(num=order_num, shop_id=shop_id) \
            .first()
        return order

    @classmethod
    def get_order_num_through_id_list(cls, session, order_id_list):
        '''
            使用订单ID列表获取订单号字典
        '''
        if order_id_list:
            Order = models.Order
            order_num_list = session.query(Order.id, Order.num) \
                .filter(Order.id.in_(order_id_list)) \
                .all()
        else:
            order_num_list = []
        order_num_dict = {x.id: x.num for x in order_num_list}
        return order_num_dict

    @classmethod
    def get_signature_image(cls, order_id):
        """ 临时从服务器上获取签名图片 """
        signature_image = ""
        try:
            filename = "/home/monk/pf_signatures/OrderPaidWithCredit/%s.png" % order_id
            #filename = "/Users/yy/Desktop/%s.png" % order_id
            import base64
            with open(filename, 'rb') as file_object:
                signature_image = base64.b64encode(file_object.read())
                signature_image = signature_image.decode()
        except:
            pass
        return signature_image

    @classmethod
    def get_order_bank_name(cls, session, order_id, source="order"):
        if source == "order":
            models_name = models.Order
        else:
            models_name = models.PbOrder

        bank_no = session.query(models_name.bank_no) \
                      .filter_by(id=order_id, pay_type=12).scalar() or 0

        bank_name = ""
        if bank_no:
            bank_name = LcBankFunc.get_bank_name(session, bank_no)
        return bank_name

    @classmethod
    def check_pb_order_reset(cls, session, order_id_list):
        query_data = session \
            .query(models.PbOrder, models.PbOrderMoney) \
            .outerjoin(models.PbOrderMoney, models.PbOrder.id == models.PbOrderMoney.id) \
            .filter(models.PbOrder.id.in_(order_id_list)) \
            .all()
        reset_dict = {}
        for pborder, pborder_money in query_data:
            pay_type = pborder.pay_type
            can_be_reset = 1
            if pay_type in [3, 4]:
                can_be_reset = 0
            if pay_type == 10 and \
                pborder_money and \
                (pborder_money.barcode_wx_cent > 0 or
                    pborder_money.barcode_ali_cent > 0):
                can_be_reset = 0
            reset_dict[pborder.id] = can_be_reset
        return reset_dict


# 订单及小票信息展示
class OrderSalesRecordFunc():
    @classmethod
    def batch_format_order_record_info(self,
                                       session,
                                       sale_records,
                                       return_type="dict",
                                       assign_accounting_date=None,
                                       attach_refund=False,
                                       get_wximgurl=False):
        ''' 批量处理订单和小票的数据

        :param attach_refund: 附加退款单的信息, 用于打印时隐藏数据

        :param get_wximgurl: 需要返回用户微信头像时为真, 如果不使用默认参数，改为所有接口都返回，会影响另外七个接口，都会带上wximgurl

        :return: 返回格式有list和dict两种
        '''
        order_id_list = [x.order_id for x in sale_records]
        accountant_id_list = [x.accountant_id for x in sale_records]
        customer_id_list = [x.shop_customer_id for x in sale_records]
        goods_id_list = [x.goods_id for x in sale_records]
        salesman_id_list = [x.salesman_id for x in sale_records]
        batch_id_list = [x.goods_batch_id for x in sale_records]
        account_id_list = accountant_id_list + salesman_id_list

        # 退款信息的准备
        if attach_refund:
            if return_type == "dict":
                raise ValueError("暂时不支持返回字典")
            sales_record_id_list = [x.id for x in sale_records]
            refund_record_ids, origin_record_ids, map_refund_id_to_origin_id \
                = GoodsSaleFunc.query_and_parse_refunds(session, sales_record_id_list)
            map_origin_id_to_refund = {}

        order_num_dict = OrderBaseFunc.get_order_num_through_id_list(
            session, order_id_list)
        # 加上判断，如果需要返回wximgurl则返回，目的是不影响其它接口的返回结果
        if not get_wximgurl:
            staff_name_dict = AccountFunc.get_account_name_through_id_list(
                session, account_id_list)
        else:
            staff_name_dict, staff_wximgurl_dict = AccountFunc.\
                get_account_name_wximg_through_id_list(session, account_id_list)
        customer_name_dict = ShopCustomerFunc.get_shop_customer_name_through_id_list(
            session, customer_id_list)
        goods_name_unit_dict = GoodsFunc.get_goods_name_unit_through_id_list(
            session, goods_id_list)
        group_id_dict = GoodsFunc.get_group_id_dict_with_goods_ids(
            session, goods_id_list)
        group_name_dict = GoodsGroupFunc.get_group_name_dict(
            session,
            list(
                set([
                    group_id_dict.get(goods_id) for goods_id in group_id_dict
                ])))

        goods_batch_num_dict = GoodsBatchFunc.get_batch_num_through_id_list(
            session, batch_id_list)
        combine_pay_dict = OrderBaseFunc.multi_combine_pay_type_text(
            session, order_id_list)
        multi_order_id_list = {r.multi_order_id for r in sale_records}
        order_dict = OrderBaseFunc.get_order_dict_throuth_id_list(
            session, multi_order_id_list)
        supplytype_goodstype_dict = GoodsFunc.get_goods_supplytype_goodstype_through_id_list(
            session, goods_id_list)
        if return_type == "dict":
            record_data = {}
        else:
            record_data = []
        shop_id = sale_records[0].shop_id if sale_records else 0
        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)
        for record in sale_records:
            record_id = record.id
            accountant_id = record.accountant_id
            accountant_name = staff_name_dict.get(accountant_id,
                                                  "收银员%d" % accountant_id)
            shop_customer_id = record.shop_customer_id
            debtor_name = customer_name_dict.get(shop_customer_id, "散客")
            order_id = record.order_id
            order_num = order_num_dict.get(order_id, "")

            pay_time = TimeFunc.time_to_str(record.order_time)
            goods_id = record.goods_id
            goods_info = goods_name_unit_dict.get(goods_id, {})
            goods_name = goods_info["name"] if goods_info else ""
            group_id = group_id_dict.get(goods_id, "")
            group_name = group_name_dict.get(group_id, "")
            goods_unit = GoodsFunc.getUnit(
                goods_info["unit"],
                weight_unit_text=weight_unit_text) if goods_info else ""
            salesman_id = record.salesman_id
            salesman_name = staff_name_dict.get(salesman_id,
                                                "开票员%d" % salesman_id)
            sales_time = str(record.bill_year) + '-' + str(
                record.bill_month).zfill(2) + '-' + str(
                    record.bill_day).zfill(2) + ' ' + str(record.bill_time)
            commission = record.commission
            commission_mul = record.commission_mul
            deposit_avg = record.deposit_avg
            deposit_total = record.deposit_money
            receipt_money = record.receipt_money
            shop_supplier_name = record.shop_supplier_name if goods_info and goods_info[
                "supply_type"] > 0 else ""
            print_text = record.printer_remark
            printer_num = record.printer_num
            sales_money = check_float(record.sales_money / 100)
            commission_money = check_float(record.commission_money / 100)
            record_type = record.record_type
            goods_name_with_supplier = goods_name
            sales_num = check_float(
                record.get_sales_num(
                    goods_info["unit"], weight_unit_text=weight_unit_text))
            unpayback_money = record.unpayback_money
            goods_type, supply_type = "", ""
            if goods_id in supplytype_goodstype_dict:
                goods_type = supplytype_goodstype_dict[goods_id]["goods_type"]
                supply_type = supplytype_goodstype_dict[goods_id][
                    "supply_type"]

            # 支付方式
            pay_type = record.pay_type
            pay_type_text = OrderBaseFunc.order_pay_type_text(
                order_id, pay_type, combine_pay_dict)

            # 获取小票已还款金额
            payback_money = 0
            if pay_type == 9 and unpayback_money >= 0 and record.status in [
                    3, 5
            ]:
                payback_money = check_float(
                    (receipt_money - unpayback_money) / 100)

            # 处理历史数据异常
            if record_type == 0 and unpayback_money < 0:
                unpayback_money = 0
            # 处理一下退单数据的显示
            if record_type == 1:
                deposit_total = -deposit_total
                receipt_money = -receipt_money
                sales_money = -sales_money
                commission_money = -commission_money
                commission_mul = -commission_mul
                payback_money = -payback_money
                unpayback_money = -unpayback_money

            # 扎账数据的显示
            accounting = 0
            if record.account_period_date:
                assign_accounting_date = datetime.datetime.now().date()
                if assign_accounting_date and (
                        record.account_period_date -
                        assign_accounting_date).days == 1:
                    accounting = 1

            # 扎帐日期
            account_period_date = record.account_period_date.strftime(
                "%Y-%m-%d")

            # 备注处理(一单多品的单品流水显示一单多品的备注)
            if record.multi_order_id:
                multi_order = order_dict[record.multi_order_id]
                remark = multi_order.remark
            else:
                remark = record.remark

            record_info = {}
            record_info["num"] = record.get_num()  # 流水号
            record_info["order_num"] = order_num  # 订单号
            record_info["status"] = record.status  # 流水状态
            record_info["id"] = record.id  # 流水ID
            record_info["receipt_money"] = check_float(
                receipt_money / 100)  # 票据金额
            record_info["shop_supplier_name"] = shop_supplier_name  # 供货商名称
            record_info["only_goods_name"] = goods_name  # 商品名称
            record_info[
                "goods_name"] = goods_name_with_supplier  # 商品名称（带供货商名称）
            record_info["goods_unit"] = goods_unit  # 商品单位
            record_info["group_id"] = group_id  # 商品分类id
            record_info["group_name"] = group_name  # 商品分类名称
            record_info["fact_price"] = check_float(
                record.get_fact_price(
                    goods_info["unit"], weight_unit_text=weight_unit_text) /
                100)  # 单价
            record_info["sales_num"] = "-" if goods_info[
                "unit"] == 1 else sales_num  # 已售重量
            record_info["gross_weight"] = check_float(
                record.get_gross_weight(
                    weight_unit_text=weight_unit_text))  # 毛重
            record_info["tare_weight"] = check_float(
                record.get_tare_weight(
                    weight_unit_text=weight_unit_text))  # 皮重
            record_info["storage"] = check_float(record.storage / 100)  # 剩余件数
            record_info["storage_num"] = check_float(
                record.get_storage_num(weight_unit_text=weight_unit_text) /
                100)  # 剩余重量
            record_info["commission_mul"] = check_float(
                commission_mul / 100)  # 已售件数
            record_info["commission"] = check_float(commission / 100)  # 行费
            record_info["commission_total"] = commission_money  # 行费小计
            record_info["goods_total"] = sales_money  # 货品小计
            record_info["salesman_id"] = salesman_id  # 开票员ID
            record_info["salesman_name"] = salesman_name  # 开票员姓名
            record_info["sales_time"] = sales_time  # 开票时间
            record_info["print"] = print_text  # 打印机备注
            record_info["accountant_id"] = accountant_id  # 收银员ID
            record_info["accountant_name"] = accountant_name  # 收银员姓名
            record_info["pay_time"] = pay_time  # 支付时间
            record_info["deposit"] = check_float(deposit_avg / 100)  # 押金
            record_info["deposit_total"] = check_float(
                deposit_total / 100)  # 押金小计
            record_info["goods_id"] = goods_id  # 商品ID
            record_info["pay_type"] = pay_type  # 支付类型
            record_info["pay_type_text"] = pay_type_text  # 支付方式类型文本描述
            record_info["debtor_id"] = shop_customer_id  # 客户ID
            record_info["debtor_name"] = debtor_name  # 客户姓名
            record_info["accounting"] = accounting  # 是否扎账
            record_info["account_period_date"] = account_period_date  # 扎帐日期
            record_info["record_type"] = record_type  # 流水类型
            record_info["remark"] = remark  # 备注
            record_info["payback_money"] = payback_money  # 已还款金额
            record_info["unpayback_money"] = check_float(unpayback_money / 100)
            record_info["batch_num"] = goods_batch_num_dict.get(record.goods_batch_id, "") \
                if record.goods_batch_id else ""  # 批次号
            record_info["goods_type"] = goods_type
            record_info["supply_type"] = supply_type
            record_info["create_time"] = TimeFunc.time_to_str(
                record.create_time)
            record_info["order_time"] = TimeFunc.time_to_str(record.order_time)
            # 还款状态
            payback_status = ""
            if record_type == 1:
                payback_status = "已退单"
            else:
                if record_info["unpayback_money"] <= 0:
                    payback_status = "已还清"
                elif record_info["unpayback_money"] < record_info[
                        "receipt_money"]:
                    payback_status = "部分还款"
                elif record_info["unpayback_money"] == record_info[
                        "receipt_money"]:
                    payback_status = "未还款"
                else:
                    payback_status = "其他"
            record_info["payback_status"] = payback_status
            avg_price = 0
            if goods_info["unit"] == 0 and commission_mul:
                avg_price = check_float(sales_money / (commission_mul / 100))
            record_info["avg_price"] = avg_price

            if attach_refund:
                record_info[
                    "shown_status_print"] = GoodsSaleFunc.get_record_shown_status_print(
                        record, refund_record_ids, origin_record_ids)
                # 处理原单到其退票单的映射
                if record.id in refund_record_ids:
                    origin_id = map_refund_id_to_origin_id.get(record.id)
                    map_origin_id_to_refund[origin_id] = record_info

            if return_type == "dict":
                record_data[record.id] = record_info
            else:
                record_data.append(record_info)

        if attach_refund:
            # 循环第二次处理退单数据的原单展示
            for data in record_data:
                if data["shown_status_print"] == 2:
                    refund_data = map_origin_id_to_refund.get(data["id"])
                    if not refund_data:
                        data["shown_status_print"] = 0
                    elif refund_data["goods_total"] + data["goods_total"] == 0:
                        data["shown_status_print"] = 1
                    else:
                        data["refund_data"] = refund_data
        # 处理是否返回微信头像
        if get_wximgurl:
            if not return_type == "dict":
                for data in record_data:
                    data["accountant_wximgurl"] = staff_wximgurl_dict.get(
                        data.get("accountant_id", 0), "")
                    data["salesman_wximgurl"] = staff_wximgurl_dict.get(
                        data.get("salesman_id", 0), "")
            else:
                for data in record_data.values():
                    data["accountant_wximgurl"] = staff_wximgurl_dict.get(
                        data.get("accountant_id", 0), "")
                    data["salesman_wximgurl"] = staff_wximgurl_dict.get(
                        data.get("salesman_id", 0), "")

        return record_data


# 库存变更记录展示与导出
class StorageRecordFunc():
    @classmethod
    def batch_format_storage_record_info(cls,
                                         session,
                                         record_list,
                                         shop_id,
                                         goods_id,
                                         weight_unit_text,
                                         return_type="list"):
        '''
            批量处理库存变更记录的数据
            :param record_list: 查询出来的记录列表
            :param shop_id: 店铺id
            :param goods_id: 货品id
            :param weight_unit_text: 店铺设置的重量单位文字
            :param return_type:返回参数可以有两种，list和dict
        '''
        account_ids = {x.operater_id for x in record_list}
        account_dict, account_wximgurl_dict = AccountFunc.get_account_name_wximg_through_id_list(
            session, account_ids)

        # 获取GoodsSalesRecord.num
        fk_ids = {i.fk_id for i in record_list if i.type in (2, 3, 5)}
        sale_records = PubRecordFunc.get_record_num_from_id_list(
            session, models.GoodsSalesRecord, fk_ids, shop_id, goods_id)
        # 获取GoodsStockinRecord.num
        fk_ids = {i.fk_id for i in record_list if i.type in (1, 4)}
        stockin_records = PubRecordFunc.get_record_num_from_id_list(
            session, models.GoodsStockinRecord, fk_ids, shop_id, goods_id)
        fk_nums = [val for key, val in stockin_records.items()]
        stockin_records_num = PubRecordFunc.get_docs_id_from_num_list(
            session, models.StockInDocs, fk_nums, shop_id)
        # 获取GoodsBreakageRecord.num
        fk_ids = {i.fk_id for i in record_list if i.type in (6, 7)}
        breakage_records = PubRecordFunc.get_record_num_from_id_list(
            session, models.GoodsBreakageRecord, fk_ids, shop_id, goods_id)
        fk_nums = [val for key, val in breakage_records.items()]
        breakage_records_num = PubRecordFunc.get_docs_id_from_num_list(
            session, models.BreakageDocs, fk_nums, shop_id)
        # 获取StorageAllocate.num
        fk_ids = {i.fk_id for i in record_list if i.type in (8, 9, 10)}
        allocate_records = PubRecordFunc.get_record_num_from_id_list(
            session, models.StorageAllocate, fk_ids)
        fk_nums = [val for key, val in allocate_records.items()]
        allocate_records_num = PubRecordFunc.get_docs_id_from_num_list(
            session, models.StorageAllocate, fk_nums)
        # 获取GoodInventoryRecord.num
        fk_ids = {i.fk_id for i in record_list if i.type == 11}
        inventory_records = PubRecordFunc.get_record_num_from_id_list(
            session, models.GoodsInventoryRecord, fk_ids, shop_id, goods_id)
        fk_nums = [val for key, val in inventory_records.items()]
        inventory_records_num = PubRecordFunc.get_docs_id_from_num_list(
            session, models.InventoryDocs, fk_nums, shop_id)

        # 构造返回数据
        if return_type == "dict":
            record_data = {}
        else:
            record_data = []
        for record in record_list:
            # 获取关联单号
            if record.type in (2, 3, 5):
                foreign_record_num = sale_records.get(record.fk_id, "")
                foreign_doc_id = 0
            elif record.type in (1, 4):
                foreign_record_num = stockin_records.get(record.fk_id, "")
                foreign_doc_id = stockin_records_num.get(foreign_record_num)
            elif record.type in (6, 7):
                foreign_record_num = breakage_records.get(record.fk_id, "")
                foreign_doc_id = breakage_records_num.get(foreign_record_num)
            elif record.type in (8, 9, 10):
                foreign_record_num = allocate_records.get(record.fk_id, "")
                foreign_doc_id = allocate_records_num.get(foreign_record_num)
            elif record.type == 11:
                foreign_record_num = inventory_records.get(record.fk_id, "")
                foreign_doc_id = inventory_records_num.get(foreign_record_num)
            else:
                foreign_record_num = ""
                foreign_doc_id = 0
            # 组装
            data = dict(
                type_id=record.type,
                type_text=record.type_text,
                foreign_record_num=foreign_record_num,
                create_time=TimeFunc.time_to_str(record.create_time),
                mul=check_float(
                    (record.now_storage - record.origin_storage) / 100),
                num=check_float(
                    (record.get_now_storage_num(
                        weight_unit_text=weight_unit_text) -
                     record.get_origin_storage_num(
                         weight_unit_text=weight_unit_text)) / 100),
                now_storage=check_float(record.now_storage / 100),
                now_storage_num=check_float(
                    record.get_now_storage_num(
                        weight_unit_text=weight_unit_text) / 100),
                operator_id=record.operater_id,
                operator_name=account_dict.get(record.operater_id, ""),
                operator_wximgurl=account_wximgurl_dict.get(
                    record.operater_id, ""),
                foreign_doc_id=foreign_doc_id,
                weight_unit=weight_unit_text,
            )
            if return_type == "dict":
                record_data[record.id] = data
            else:
                record_data.append(data)
        return record_data

    @classmethod
    def format_export_storage_record_body(cls, session, record_list, shop_id,
                                          goods_id, weight_unit_text):
        '''构造某货品库存变更记录的导出excel格式'''
        data_content = []
        data_dict = StorageRecordFunc \
            .batch_format_storage_record_info(session, record_list, shop_id, goods_id, weight_unit_text,
                                              return_type="dict")
        data_content.append(['变更类型', '时间', '操作人', '库存变更数量', '变更后库存', '相关单号'])
        for storage_record in record_list:
            record_info = data_dict.get(storage_record.id)
            if record_info["type_id"] in (1, 4):
                foreign_record_num_info = '入库单号：' + record_info[
                    "foreign_record_num"]
            elif record_info["type_id"] in (6, 7):
                foreign_record_num_info = '报损单号：' + record_info[
                    "foreign_record_num"]
            elif record_info["type_id"] in (8, 9, 10):
                foreign_record_num_info = '调货单号：' + record_info[
                    "foreign_record_num"]
            elif record_info["type_id"] in (11, ):
                foreign_record_num_info = '盘点单号：' + record_info[
                    "foreign_record_num"]
            else:
                foreign_record_num_info = record_info["foreign_record_num"]
            data_content.append([
                record_info["type_text"],
                record_info["create_time"],
                record_info["operator_name"],
                str(record_info["mul"]) + '件/' + str(record_info["num"]) +
                weight_unit_text,
                str(record_info["now_storage"]) + '件/' + str(
                    record_info["now_storage_num"]) + weight_unit_text,
                foreign_record_num_info,
            ])
        return data_content

    # @classmethod
    # def get_storage_left_data(cls, args, session, shop_id, master_goods_ids=None):
    #     """ 对账中心-货品进销存获取剩余库存数据
    #
    #     :param master_goods_ids: 主货品id列表, 传递, 表示获取分货品的统计数据
    #     """
    #     date = args['date']
    #     end_date = args.get("end_date", "")
    #     data_type = args.get("data_type", 0)
    #     statistic_type = args.get("statistic_type", 1)
    #     goods_ids = args.get("goods_ids", [])
    #     supplier_ids = args.get("supplier_ids",[])
    #     supply_types = args.get("supply_types", [])
    #     goods_types = args.get("goods_types", [])
    #     group_ids = args.get("group_ids", [])
    #
    #     # 日期区间筛选
    #     start_date = TimeFunc.check_input_date(date, statistic_type, return_type="date")
    #
    #     if end_date:
    #         end_date = TimeFunc.check_input_date(end_date, statistic_type, return_type="date")
    #     else:
    #         end_date = start_date
    #
    #     # 剩余库存特殊处理，只有日期区间为同一天才有剩余库存的意义
    #     storage_left_data = None
    #     if end_date == start_date:
    #         if data_type == 1:
    #             choose_time = AccountingPeriodFunc.get_accounting_time(
    #                 session, shop_id, start_date)
    #         if data_type == 0 or not choose_time:
    #             choose_time = start_date + datetime.timedelta(days=1)
    #         # 查询出符合条件所有的库存记录id
    #         stoarge_id_querys = session\
    #             .query(models.Goods.id,
    #                    func.max(models.GoodsStorageRecord.id).label("record_id"))\
    #             .join(models.GoodsStorageRecord,
    #                   models.GoodsStorageRecord.goods_id == models.Goods.id)\
    #             .filter(models.Goods.shop_id == shop_id,
    #                     models.GoodsStorageRecord.create_time <= choose_time)\
    #             .group_by(models.Goods.id)\
    #             .all()
    #         record_id_list = [i.record_id for i in stoarge_id_querys]
    #         query_base = session \
    #             .query(
    #                 models.Goods,
    #                 models.GoodsStorageRecord.now_storage,
    #                 models.GoodsStorageRecord.now_storage_num) \
    #             .join(
    #                 models.GoodsStorageRecord,
    #                 models.GoodsStorageRecord.goods_id == models.Goods.id)
    #         filter_params_record = [
    #             models.GoodsStorageRecord.id.in_(record_id_list)
    #         ]
    #         if master_goods_ids:
    #             branch_goods_id_set = GoodsMasterBranchFunc.list_goods_id_branch(
    #                 session, shop_id, master_goods_ids)
    #             filter_params_record.append(
    #                 models.GoodsStorageRecord.goods_id.in_(branch_goods_id_set))
    #         else:
    #             master_goods_id_set = GoodsMasterBranchFunc.list_goods_id_master(
    #                 session, shop_id)
    #             filter_params_record.append(
    #                 models.GoodsStorageRecord.goods_id.in_(master_goods_id_set))
    #
    #         # 过滤货品ID
    #         if goods_ids:
    #             filter_params_record.append(
    #                 models.GoodsStorageRecord.goods_id.in_(goods_ids))
    #
    #         # 过滤分组ID
    #         if group_ids:
    #             goods_in_group = session\
    #                 .query(models.Goods)\
    #                 .filter(models.Goods.group_id.in_(group_ids),
    #                         models.Goods.shop_id==shop_id)\
    #                 .all()
    #             filter_params_record.append(
    #                 models.GoodsStorageRecord.goods_id.in_([x.id for x in goods_in_group]))
    #
    #         # 过滤供应商ID
    #         if supplier_ids:
    #             supplier_goods_id_list = SupplierFunc.get_supplier_goods_id_through_id(session, shop_id, supplier_ids)
    #             filter_params_record.append(
    #                 models.GoodsStorageRecord.goods_id.in_(supplier_goods_id_list))
    #
    #         # 过滤销售类型
    #         if supply_types:
    #             supply_goods_id_list = GoodsFunc.get_goods_through_supply_type(session, shop_id, supply_types)
    #             filter_params_record.append(
    #                 models.GoodsStorageRecord.goods_id.in_(supply_goods_id_list))
    #
    #         # 过滤货品类型
    #         if goods_types:
    #             type_goods_id_list = GoodsFunc.get_goods_through_goods_type(session, shop_id, goods_types)
    #             filter_params_record.append(
    #                 models.GoodsStorageRecord.goods_id.in_(type_goods_id_list))
    #
    #         storage_left_data = query_base \
    #             .filter(*filter_params_record) \
    #             .all()
    #     return storage_left_data

    @classmethod
    def get_storage_data(cls,
                         args,
                         session,
                         statistic_session,
                         shop_id,
                         master_goods_ids=None):
        date = args['date']
        end_date = args.get("end_date", "")
        data_type = args.get("data_type", 0)
        statistic_type = args.get("statistic_type", 1)
        goods_ids = args.get("goods_ids", [])
        supplier_ids = args.get("supplier_ids", [])
        supply_types = args.get("supply_types", [])
        goods_types = args.get("goods_types", [])
        group_ids = args.get("group_ids", [])

        GoodsStorageRecord = models.GoodsStorageRecord
        # 日期区间筛选
        start_date = TimeFunc.check_input_date(
            date, statistic_type, return_type="date")

        if end_date:
            end_date = TimeFunc.check_input_date(
                end_date, statistic_type, return_type="date")
        else:
            end_date = start_date

        # 区分自然日/扎账日
        if data_type == 1:
            StatisticGoodsStorage = models_account_periods_statistic.AccountPeriodsStatisticGoodsStorage
        else:
            StatisticGoodsStorage = models_statistic.StatisticGoodsStorage

        # 过滤条件
        filter_type = StatisticFunc.get_filter_type(statistic_type)
        filter_date = StatisticFunc.get_filter_date(statistic_type, start_date)

        columns = StatisticFunc.get_query_param(StatisticGoodsStorage)

        if statistic_type == 1:
            query_base = statistic_session\
                .query(
                    StatisticGoodsStorage.goods_id,
                    StatisticGoodsStorage.begin_storage,
                    StatisticGoodsStorage.end_storage,
                    StatisticGoodsStorage.begin_storage_num,
                    StatisticGoodsStorage.end_storage_num,
                    *columns)\
                .filter_by(shop_id=shop_id, statistic_type=filter_type)\
                .group_by(StatisticGoodsStorage.goods_id)
            storage_data_ret = query_base\
                .filter(
                    StatisticGoodsStorage.statistic_date >= start_date,
                    StatisticGoodsStorage.statistic_date <= end_date)

        elif statistic_type == 3:
            query_base = statistic_session\
                .query(
                    StatisticGoodsStorage.goods_id,
                    StatisticGoodsStorage.begin_storage,
                    StatisticGoodsStorage.end_storage,
                    StatisticGoodsStorage.begin_storage_num,
                    StatisticGoodsStorage.end_storage_num,
                    *columns)\
                .filter_by(shop_id=shop_id, statistic_type=filter_type)\
                .group_by(StatisticGoodsStorage.goods_id)
            storage_data_ret = query_base.filter_by(**filter_date)

        # 暂时只考虑年统计
        else:
            query_base = statistic_session\
                .query(
                    StatisticGoodsStorage.goods_id,
                    *columns)\
                .group_by(StatisticGoodsStorage.goods_id)\
                .filter_by(shop_id=shop_id, statistic_type=filter_type)
            storage_data_ret = query_base.filter_by(**filter_date)

        storage_data_ret = storage_data_ret.filter(
            or_(StatisticGoodsStorage.stockin_record != 0,
                StatisticGoodsStorage.sales_record != 0,
                StatisticGoodsStorage.breakage_record != 0,
                StatisticGoodsStorage.allocate_record != 0,
                StatisticGoodsStorage.inventory_record != 0,
                StatisticGoodsStorage.clearing_record != 0))
        if master_goods_ids:
            branch_goods_id_set = GoodsMasterBranchFunc.list_goods_id_branch(
                session, shop_id, master_goods_ids)
            storage_data_ret = storage_data_ret.filter(
                StatisticGoodsStorage.goods_id.in_(branch_goods_id_set))
        else:
            master_goods_id_set = GoodsMasterBranchFunc.list_goods_id_master(
                session, shop_id)
            storage_data_ret = storage_data_ret.filter(
                StatisticGoodsStorage.goods_id.in_(master_goods_id_set))

        # 过滤货品ID
        if goods_ids:
            storage_data_ret = storage_data_ret.filter(
                StatisticGoodsStorage.goods_id.in_(goods_ids))

        # 过滤分组ID
        if group_ids:
            goods_in_group = session\
                .query(models.Goods)\
                .filter(models.Goods.group_id.in_(group_ids),
                        models.Goods.shop_id == shop_id)\
                .all()
            storage_data_ret = storage_data_ret\
                .filter(StatisticGoodsStorage.goods_id.in_([x.id for x in goods_in_group]))
        # 过滤供应商ID
        if supplier_ids:
            supplier_goods_id_list = SupplierFunc.get_supplier_goods_id_through_id(
                session, shop_id, supplier_ids)
            storage_data_ret = storage_data_ret\
                .filter(StatisticGoodsStorage.goods_id.in_(supplier_goods_id_list))
        # 过滤销售类型
        if supply_types:
            supply_goods_id_list = GoodsFunc.get_goods_through_supply_type(
                session, shop_id, supply_types)
            storage_data_ret = storage_data_ret\
                .filter(StatisticGoodsStorage.goods_id.in_(supply_goods_id_list))
        # 过滤货品类型
        if goods_types:
            type_goods_id_list = GoodsFunc.get_goods_through_goods_type(
                session, shop_id, goods_types)
            storage_data_ret = storage_data_ret\
                .filter(StatisticGoodsStorage.goods_id.in_(type_goods_id_list))

        storage_data = storage_data_ret.all()

        # 如果是区间统计，还需要更新一下期初期末库存
        storage_range_dict = {}
        if statistic_type == 1 and start_date != end_date:
            goods_id_list = {data.goods_id for data in storage_data}
            storage_range_dict = StorageRecordFunc.get_begin_end_storage(
                args, session, shop_id, goods_id_list)

        # 年统计的期初库存和期末库存
        goods_storage_dict = {}
        if statistic_type == 4:
            goods_id_list = {data.goods_id for data in storage_data}
            goods_storage_dict = StorageRecordFunc.get_begin_end_storage(
                args, session, shop_id, goods_id_list)

        # 年统计里storage_data没有储存期初和期末库存，而是放在goods_storage_dict里面
        # 区间筛选中，需要更新的期末库存放在storage_range_dict里面
        return storage_data, goods_storage_dict, storage_range_dict

    @classmethod
    def get_begin_end_storage(cls, args, session, shop_id, goods_id_list):
        date = args['date']
        end_date = args.get("end_date", "")
        statistic_type = args.get("statistic_type", 1)
        data_type = args.get("data_type", 0)

        GoodsStorageRecord = models.GoodsStorageRecord
        # 日期区间筛选

        start_date, end_date = TimeFunc.get_to_date_by_from_date(
            date, end_date, statistic_type)

        if data_type == 1:
            # 前面的日期处理给end_date加了一天，换算扎帐日要把这一天减去
            end_date = end_date - datetime.timedelta(days=1)
            start_date, end_date = TimeFunc.transfer_range_normal_to_accounting(
                session, shop_id, start_date.date(), end_date.date())

        # 查询出符合条件所有的库存记录id
        end_stoarge_id_querys = session \
            .query(GoodsStorageRecord.goods_id,
                   func.max(GoodsStorageRecord.id).label("record_id")) \
            .with_hint(GoodsStorageRecord, "FORCE INDEX(ix_shop_time_goods)") \
            .filter(GoodsStorageRecord.shop_id == shop_id,
                    GoodsStorageRecord.goods_id.in_(goods_id_list),
                    GoodsStorageRecord.create_time < end_date) \
            .group_by(GoodsStorageRecord.goods_id) \
            .all()
        end_id_list = [i.record_id for i in end_stoarge_id_querys]

        begin_stoarge_id_querys = session \
            .query(GoodsStorageRecord.goods_id,
                   func.max(GoodsStorageRecord.id).label("record_id")) \
            .with_hint(GoodsStorageRecord, "FORCE INDEX(ix_shop_time_goods)") \
            .filter(GoodsStorageRecord.shop_id == shop_id,
                    GoodsStorageRecord.goods_id.in_(goods_id_list),
                    GoodsStorageRecord.create_time < start_date) \
            .group_by(GoodsStorageRecord.goods_id) \
            .all()

        begin_id_list = [i.record_id for i in begin_stoarge_id_querys]

        end_storage_list = session \
            .query(
            GoodsStorageRecord.goods_id,
            GoodsStorageRecord.now_storage,
            GoodsStorageRecord.now_storage_num) \
            .filter(GoodsStorageRecord.id.in_(end_id_list)).all() if end_id_list else []
        begin_storage_list = session \
            .query(
            GoodsStorageRecord.goods_id,
            GoodsStorageRecord.now_storage,
            GoodsStorageRecord.now_storage_num) \
            .filter(GoodsStorageRecord.id.in_(begin_id_list)).all() if begin_id_list else []

        storage_dict = {}
        # 注意：存在有当天是第一次更改库存的货品，因此需要处理这部分goods_id对应的数据
        for goods_id, begin_storage, begin_storage_num in begin_storage_list:
            storage_dict[goods_id] = [begin_storage, begin_storage_num]
        for goods_id, now_storage, now_storage_num in end_storage_list:
            if not storage_dict.get(goods_id):
                storage_dict[goods_id] = [0, 0]
            storage_dict[goods_id].extend([now_storage, now_storage_num])

        return storage_dict

    @classmethod
    def get_extra_storage_data(cls, args, session, shop_id, storage_data,
                               data_sales_ret):
        # 有一种情况，某货品没有任何库存更改，但是有未付款件数，会显示在data_sales_ret中
        # 但是库存变更统计表里是没有的，因此选择现查库存
        goods_id_list_stocksales = {data.goods_id for data in data_sales_ret}
        goods_id_list_storage = {data.goods_id for data in storage_data}
        goods_to_query = goods_id_list_stocksales - goods_id_list_storage

        extra_storage_dict = StorageRecordFunc.get_begin_end_storage(
            args, session, shop_id, goods_to_query)

        return extra_storage_dict

    @classmethod
    def get_storage_allocate_end_time_by_storage_allocate_id(
            cls, session, storage_allocate_id):
        """获取库存调度的完成时间"""
        GoodsStorageRecord = models.GoodsStorageRecord
        filter_list = [
            GoodsStorageRecord.fk_id == storage_allocate_id,
            GoodsStorageRecord.type.in_([8, 9, 10])
        ]
        result = session.query(GoodsStorageRecord.create_time)\
            .filter(*filter_list)\
            .order_by(GoodsStorageRecord.create_time.desc()).first()
        if result:
            return TimeFunc.time_to_str(result.create_time)
        return ""


# 货款结算记录展示与导出
class SupplierFlowFunc():
    @classmethod
    def batch_format_supplier_flow_info(cls,
                                        session,
                                        shop_id,
                                        flow_list,
                                        return_type="list",
                                        data_type=0):
        '''
            批量处理贷款结算记录的数据
            :param shop_id: 店铺id
            :param flow_list: 贷款结算流水的列表
            :param return_type: 返回参数可以有两种，list和dict
        '''
        # 查询操作员信息
        operater_id_list = {x.operater_id for x in flow_list}
        operater_name_wximgurl_list = AccountFunc.get_account_name_wximg_through_id_list(
            session, operater_id_list)
        operater_name_dict, operater_wximgurl_dict = operater_name_wximgurl_list[
            0], operater_name_wximgurl_list[1]
        # 查询供货商信息
        supplier_id_list = {x.shop_supplier_id for x in flow_list}
        supplier_info_dict = SupplierFunc.get_shop_supplier_base_info(
            session, shop_id, supplier_id_list)
        # 查询商品信息，不查询预支记录的商品
        clearing_id_list = {x.fk_id for x in flow_list if x.flow_type == 0}
        clearing_goods_name_dict = cls._get_multi_clearing_goods(
            session, clearing_id_list)
        accounting_time = AccountingPeriodFunc.get_accounting_time(
            session, shop_id) if data_type == 1 else None
        # 查询货款结算手续费
        procedure_dict = cls._get_clearing_procedure_fee(
            session, clearing_id_list)

        # 构造返回数据
        if return_type == "dict":
            record_data = {}
        else:
            record_data = []
        for flow in flow_list:
            supplier_info = supplier_info_dict.get(
                flow.shop_supplier_id, dict(name="", phone="", wximgurl=""))
            flow_id = flow.fk_id
            data = {
                "supplier_id":
                flow.shop_supplier_id,
                "supplier_name":
                supplier_info["name"],
                "supplier_phone":
                supplier_info["phone"],
                "supplier_wximgurl":
                supplier_info["wximgurl"],
                "create_date":
                TimeFunc.time_to_str(flow.create_date),
                "flow_value":
                check_float(flow.flow_value / 100),
                "operater_id":
                flow.operater_id,
                "operater_name":
                operater_name_dict.get(flow.operater_id, ""),
                "operater_wximgurl":
                operater_wximgurl_dict.get(flow.operater_id, ""),
                "flow_id":
                flow_id,
                "goods_id":
                flow.goods_id or 0,
                "goods_list":
                clearing_goods_name_dict.get(flow_id, []),
                "sms_sent":
                flow.sms_sent,
                "is_after_accounting":
                1 if accounting_time and flow.create_date > accounting_time
                else 0,
                "procedure_fee":
                procedure_dict.get(flow_id) if flow.flow_type == 0 else 0
            }
            if return_type == "dict":
                record_data[flow.id] = data
            else:
                record_data.append(data)
        return record_data

    # 获取供应商结算货品
    @classmethod
    def _get_multi_clearing_goods(cls, session, clearing_id_list):
        """根据结算ID批量获取货品信息"""
        ShopSupplierClearingGoods = models.ShopSupplierClearingGoods

        clearing_id_list = session \
            .query(ShopSupplierClearingGoods.clearing_id, ShopSupplierClearingGoods.goods_id) \
            .filter(ShopSupplierClearingGoods.clearing_id.in_(clearing_id_list)).all()

        clearing_goods_id_list = {x.goods_id for x in clearing_id_list}
        goods_name_dict = GoodsFunc.get_goods_name_through_id_list(
            session, clearing_goods_id_list)

        clearing_goods_name_dict = collections.defaultdict(list)
        for clearing_id, goods_id in clearing_id_list:
            goods_name = goods_name_dict.get(goods_id, "")
            clearing_goods_name_dict[clearing_id].append({
                "goods_id":
                goods_id,
                "goods_name":
                goods_name
            })
        return clearing_goods_name_dict

    @classmethod
    def _get_clearing_procedure_fee(cls, session, clearing_id_list):
        """根据结算ID批量获取结算手续费"""
        procedure_list = session \
            .query(
                models.ShopSupplierClearing.id,
                models.ShopSupplierClearing.procedure_cent) \
            .filter(models.ShopSupplierClearing.id.in_(clearing_id_list))\
            .all()
        procedure_dict = {
            x.id: check_float(x.procedure_cent / 100)
            for x in procedure_list
        }
        return procedure_dict

    @classmethod
    def _get_sum_clearing_procedure_fee(cls, session, clearing_id_list):
        """根据结算ID获取累计供应商结算手续费"""
        clearing_id_list = {x.fk_id for x in clearing_id_list}
        if clearing_id_list:
            procedure_sum = session \
                .query(func.sum(models.ShopSupplierClearing.procedure_cent)) \
                .filter(models.ShopSupplierClearing.id.in_(clearing_id_list))\
                .scalar() or 0
            procedure_sum = check_float(procedure_sum / 100)
        else:
            procedure_sum = 0
        return procedure_sum

    @classmethod
    def format_export_supplier_flow_body(cls, session, shop_id, flow_list):
        '''构造某货品贷款结算记录的导出excel格式'''
        data_content = []
        data_dict = SupplierFlowFunc.batch_format_supplier_flow_info(
            session, shop_id, flow_list, return_type="dict")
        data_content.append(['时间', '金额', '经办人'])
        for flow in flow_list:
            record_info = data_dict.get(flow.id)
            data_content.append([
                record_info["create_date"],
                str(record_info["flow_value"]) + '元',
                record_info["operater_name"],
            ])
        return data_content

    @classmethod
    def batch_format_borrowing_flow_info(cls,
                                         session,
                                         shop_id,
                                         flows_borrowing_records,
                                         data_type=0):
        """ 构造预支流水的返回字典 """

        # 查询操作员信息
        operater_id_list = {
            flow.operater_id
            for flow, _ in flows_borrowing_records
        }
        operator_name_dict, operator_wximgurl_dict = AccountFunc.\
            get_account_name_wximg_through_id_list(session, operater_id_list)
        # 查询供货商信息
        supplier_id_list = {
            flow.shop_supplier_id
            for flow, _ in flows_borrowing_records
        }
        supplier_info_dict = SupplierFunc.get_shop_supplier_base_info(
            session, shop_id, supplier_id_list)
        # 查询商品信息
        # goods_id_list = {x.goods_id for x in borrowing_records if x.goods_id}
        # goods_name_dict = GoodsFunc.get_goods_name_through_id_list(session, goods_id_list)
        accounting_time = AccountingPeriodFunc.get_accounting_time(
            session, shop_id) if data_type == 1 else None

        data_list = []
        for flow, record in flows_borrowing_records:
            supplier_info = supplier_info_dict.get(
                flow.shop_supplier_id, dict(name="", phone="", wximgurl=""))
            cancelable = record.create_date.date() == datetime.date.today(
            ) and record.status == 0
            data_list.append({
                "borrowing_id":
                record.id,
                "supplier_id":
                flow.shop_supplier_id,
                "supplier_name":
                supplier_info["name"],
                "supplier_phone":
                supplier_info["phone"],
                "supplier_wximgurl":
                supplier_info["wximgurl"],
                "create_date":
                TimeFunc.time_to_str(flow.create_date),
                "flow_id":
                flow.id,
                "flow_type":
                flow.flow_type,
                "flow_type_text":
                flow.flow_type_text,
                "flow_value":
                check_float(flow.flow_value / 100),
                "operater_id":
                record.operater_id,
                "operater_name":
                operator_name_dict.get(record.operater_id, ""),
                "operator_wximgurl":
                operator_wximgurl_dict.get(record.operater_id, ""),
                "borrow_type":
                record.borrow_type,
                "borrow_type_text":
                record.borrow_type_text,
                "borrow_pay_type":
                record.borrow_pay_type,
                "borrow_pay_type_text":
                record.borrow_pay_type_text,
                "reason":
                record.reason,
                "borrowing_status":
                record.status,
                "cancelable":
                cancelable,
                "num":
                record.num,
                "sms_sent":
                flow.sms_sent,
                "is_after_accounting":
                1 if accounting_time and flow.create_date > accounting_time
                else 0,
            })
        return data_list

    @classmethod
    def format_export_clearing_flow_body(cls, data_list):
        data_content = []
        data_content.append(
            ["时间", "供货商", "结款货品", "结算金额", "手续费", "经办人", "短信通知"])
        for data in data_list:
            data_content.append([
                data["create_date"],
                data["supplier_name"],
                ", ".join(g["goods_name"] for g in data["goods_list"]),
                data["flow_value"],
                data["procedure_fee"],
                data["operater_name"],
                "已发送" if data["sms_sent"] else "未发送",
            ])
        return data_content

    @classmethod
    def format_export_borrowing_flow_body(cls, data_list):
        data_content = []
        data_content.append(
            ["时间", "供货商", "预支类型", "预支金额", "付款方式", "经办人", "短信通知", "备注"])
        for data in data_list:
            data_content.append([
                data["create_date"],
                data["supplier_name"],
                data["borrow_type_text"],
                data["flow_value"],
                data["borrow_pay_type_text"],
                data["operater_name"],
                "已发送" if data["sms_sent"] else "未发送",
                data["reason"],
            ])
        return data_content

    @classmethod
    def format_export_payout_statistic_body(cls, data_list):
        data_content = []
        data_content.append(
            ["时间", "货款结算笔数", "货款结算金额", "供货商预支笔数", "供货商预支金额", "支出总额"])
        for data in data_list:
            data_content.append([
                data["date"],
                data["clear_count"],
                data["clear_money"],
                data["borrow_count"],
                data["borrow_money"],
                data["payout_money"],
            ])
        return data_content

    @classmethod
    def format_export_supplier_money_statistic_body(cls, data_list):
        data_content = []
        data_content.append([
            "供货商ID", "供货商", "应结货款", "已结货款", "未结货款", "预支总额", "已结预支", "未结预支",
            "期末货款"
        ])
        for data in data_list:
            supplier_info = data["supplier_info"]
            data_content.append([
                supplier_info["supplier_id"],
                supplier_info["name"] + "/" + supplier_info["phone"],
                data["goods_money"],
                data["goods_money_cleared"],
                data["goods_money_unclearing"],
                data["borrow_money"],
                data["borrow_money_cleared"],
                data["borrow_money_unclearing"],
                data["goods_money_unclearing_end_date"],
            ])
        return data_content


# 采购入库记录展示与导出
class StockinHistoryFunc():
    @classmethod
    def batch_format_stockin_history_info(cls,
                                          session,
                                          flows,
                                          weight_unit_text,
                                          map_groups_id_name,
                                          return_type="list"):
        '''
            批量处理采购入库记录的数据
            :param flows: 采购入库流水的列表
            :param weight_unit_text: 店铺设置的重量单位描述
            :param map_groups_id_name: 分组id到name的映射
            :param return_type: 返回参数可以有两种，list和dict
        '''
        if return_type == "dict":
            data_list = {}
        else:
            data_list = []
        goods_id_list = [flow[0].goods_id for flow in flows]
        goods_supplier_name_dict = GoodsFunc.get_goods_and_supplier_names(
            session, goods_id_list)
        account_ids = [f.operator_id for f in flows]
        map_account_name, map_account_wximgurl = AccountFunc.\
            get_account_name_wximg_through_id_list(session, account_ids)
        goods_dict = GoodsFunc.get_map_id_goods(session, goods_id_list)
        for flow in flows:
            doc_detail = flow[0]
            goods_supplier_name = goods_supplier_name_dict[doc_detail.goods_id]
            storage_unit = goods_dict[doc_detail.goods_id].storage_unit
            storage_unit_text = "件" if storage_unit == 1 else weight_unit_text
            purchase_unit = goods_dict[doc_detail.goods_id].purchase_unit
            purchase_unit_text = "件" if purchase_unit == 1 else weight_unit_text
            data = dict(
                id=doc_detail.id,
                doc_id=flow.doc_id,
                goods_id=doc_detail.goods_id,
                operate_date=flow.operate_date.strftime("%Y-%m-%d %H:%M"),
                receive_date=flow.receive_date.strftime("%Y-%m-%d")
                if flow.receive_date else "",
                num=flow.num,
                goods_group_name=map_groups_id_name[flow.group_id],
                goods_supply_type=flow.supply_type,
                goods_name=goods_supplier_name["goods_name"],
                supplier_name=goods_supplier_name["supplier_name"],
                supplier_id=goods_supplier_name["supplier_id"],
                quantity=check_float(doc_detail.quantity / 100),
                weight=check_float(doc_detail.get_weight(
                    weight_unit_text=weight_unit_text) / 100),
                total_price=check_float(doc_detail.total_price / 100),
                remark=doc_detail.remark,
                status=flow.status,
                hand_num=flow.hand_num,
                batch_num=doc_detail.goods_batch.batch_num \
                    if doc_detail.goods_batch else "",
                quantity_unit_price=check_float(
                    doc_detail.total_price / doc_detail.quantity)
                if doc_detail.quantity else "",
                weight_unit_price=check_float(
                    doc_detail.total_price / doc_detail.get_weight(
                        weight_unit_text=weight_unit_text))
                if doc_detail.weight else "",
                pay_type=doc_detail.pay_type,
                pay_type_text=doc_detail.pay_type_text,
                operator_id = flow.operator_id,
                operator_name=map_account_name.get(flow.operator_id, ""),
                operator_wximgurl=map_account_wximgurl.get(flow.operator_id, ""),
                weight_unit=weight_unit_text,
                unit=doc_detail.unit,
                storage_unit=storage_unit,
                storage_unit_text=storage_unit_text,
                purchase_unit=purchase_unit,
                purchase_unit_text=purchase_unit_text,
            )
            if return_type == "dict":
                data_list[doc_detail.id] = data
            else:
                data_list.append(data)
        return data_list

    @classmethod
    def format_export_stockin_history_body(
            cls, session, flows, weight_unit_text, map_groups_id_name):
        '''构造某货品采购入库记录的导出excel格式'''
        data_content = []
        data_dict = StockinHistoryFunc \
            .batch_format_stockin_history_info(session, flows, weight_unit_text, map_groups_id_name, return_type="dict")
        data_content.append([
            '序号', '入库时间', '到货时间', '单据号', '件数', '重量', '采购价/件',
            '采购价/%s' % weight_unit_text, '费用/元', '批次号', '操作员', '备注'
        ])
        i = 1
        for flow in flows:
            record_info = data_dict.get(flow[0].id)
            if record_info["status"] == 2:
                num = record_info["num"] + '（已撤销）'
            else:
                num = record_info["num"]
            data_content.append([
                i,
                record_info["operate_date"],
                record_info["receive_date"],
                num,
                record_info["quantity"],
                record_info["weight"],
                record_info["quantity_unit_price"],
                record_info["weight_unit_price"],
                record_info["total_price"],
                record_info["batch_num"],
                record_info["operator_name"],
                record_info["remark"],
            ])
            i += 1
        return data_content


# 报损记录展示与导出
class BreakageHistoryFunc():
    @classmethod
    def batch_format_breakage_history_info(cls,
                                           flows,
                                           weight_unit_text,
                                           return_type="list"):
        '''
            批量处理报损记录的数据
            :param flows: 报损记录的流水列表
            :param weight_unit_text: 店铺设置的重量单位描述
            :param return_type: 返回参数可以有两种，list和dict
        '''
        if return_type == "dict":
            data_list = {}
        else:
            data_list = []
        for flow in flows:
            doc_detail = flow[0]
            data = dict(
                doc_id=doc_detail.doc_id,
                operate_date=flow.operate_date.strftime("%Y-%m-%d %H:%M"),
                num=flow.num,
                quantity=check_float(doc_detail.quantity / 100),
                weight=check_float(
                    doc_detail.get_weight(weight_unit_text=weight_unit_text) /
                    100),
                cost_price_quantity=check_float(
                    doc_detail.cost_price_quantity / 100),
                cost_price_weight=check_float(
                    doc_detail.get_cost_price_weight(
                        weight_unit_text=weight_unit_text) / 100),
                total_price=check_float(doc_detail.total_price / 100),
                remark=doc_detail.remark,
                status=flow.status,
                operator_nickname=flow.operator_nickname,
                unit=doc_detail.unit)
            if hasattr(flow, "wx_unionid"):
                data["operator_wximgurl"] = models.AddImgDomain. \
                                        add_domain_headimgsmall(flow.headimgurl) if flow.wx_unionid else ""
            if return_type == "dict":
                data_list[flow[0].id] = data
            else:
                data_list.append(data)
        return data_list

    @classmethod
    def format_export_breakage_history_bodt(cls, flows, weight_unit_text):
        '''构造某货品报损记录的导出excel格式'''
        data_content = []
        data_dict = BreakageHistoryFunc \
            .batch_format_breakage_history_info(flows, weight_unit_text, return_type="dict")
        data_content.append([
            '序号', '报损时间', '单据号', '件数', '重量', '报损价/件',
            '报损价/%s' % weight_unit_text, '费用/元', '操作员', '备注'
        ])
        i = 1
        for flow in flows:
            record_info = data_dict.get(flow[0].id)
            data_content.append([
                i,
                record_info["operate_date"],
                record_info["num"],
                record_info["quantity"],
                record_info["weight"],
                record_info["cost_price_quantity"],
                record_info["cost_price_weight"],
                record_info["total_price"],
                record_info["operator_nickname"],
                record_info["remark"],
            ])
            i += 1
        return data_content


# 流水导出
class SalesRecordExportFunc():
    @classmethod
    def format_export_body(cls, session, shop_id, salerecord_list):

        # 店铺是否开启去皮功能
        config = ShopFunc.get_config(session, shop_id)
        distinguish_weight = config.distinguish_weight

        data_content = []
        record_data_dict = OrderSalesRecordFunc.batch_format_order_record_info(
            session, salerecord_list)

        fee_text = ConfigFunc.get_fee_text(session, shop_id)
        fee_text_total = '{}小计'.format(fee_text)
        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)
        if distinguish_weight:
            data_content.append(['自然日期', '扎账日期', '流水号', '分组名', '货品名', '供货商', '单价', '单位', \
                                 '重量/{}'.format(weight_unit_text), '毛重', '皮重', '件数', fee_text, '押金', '货品小计', \
                                 '押金小计', fee_text_total, '票据金额', '支付方式', '开票员', '开票时间', \
                                 '收银员', '收款时间', '客户信息', '对应订单', '备注', '剩余件数', '剩余重量'])
            for sale_record in salerecord_list:
                record_info = record_data_dict.get(sale_record.id)
                data_content.append([
                    TimeFunc.time_to_str(sale_record.order_time, "date"),
                    TimeFunc.time_to_str(sale_record.account_period_date,
                                         "date"), "'" + record_info["num"],
                    record_info["group_name"], record_info["only_goods_name"],
                    record_info["shop_supplier_name"],
                    record_info["fact_price"], record_info["goods_unit"],
                    record_info["sales_num"], record_info["gross_weight"],
                    record_info["tare_weight"], record_info["commission_mul"],
                    record_info["commission"], record_info["deposit"],
                    record_info["goods_total"], record_info["deposit_total"],
                    record_info["commission_total"],
                    record_info["receipt_money"], record_info["pay_type_text"],
                    record_info["salesman_name"], record_info["sales_time"],
                    record_info["accountant_name"], record_info["pay_time"],
                    record_info["debtor_name"], record_info["order_num"],
                    record_info["remark"], record_info["storage"],
                    record_info["storage_num"]
                ])

            if not config.enable_deposit:
                index_deposit = data_content[0].index("押金")
                for data in data_content:
                    data.pop(index_deposit)
                index_deposit_total = data_content[0].index("押金小计")
                for data in data_content:
                    data.pop(index_deposit_total)

            if not config.enable_commission:
                index_commission = data_content[0].index(fee_text)
                for data in data_content:
                    data.pop(index_commission)
                index_commission_total = data_content[0].index(fee_text_total)
                for data in data_content:
                    data.pop(index_commission_total)
        else:
            data_content.append(['自然日期', '扎账日期', '流水号', '分组名', '货品名', '供货商', '单价', '单位', \
                                 '重量/{}'.format(weight_unit_text), '件数', fee_text, '押金', '货品小计', \
                                 '押金小计', fee_text_total, '票据金额', '支付方式', '开票员', '开票时间', \
                                 '收银员', '收款时间', '客户信息', '对应订单', '备注', '剩余件数', '剩余重量'])
            for sale_record in salerecord_list:
                record_info = record_data_dict.get(sale_record.id)
                data_content.append([
                    TimeFunc.time_to_str(sale_record.order_time, "date"),
                    TimeFunc.time_to_str(sale_record.account_period_date,
                                         "date"), "'" + record_info["num"],
                    record_info["group_name"], record_info["only_goods_name"],
                    record_info["shop_supplier_name"],
                    record_info["fact_price"], record_info["goods_unit"],
                    record_info["sales_num"], record_info["commission_mul"],
                    record_info["commission"], record_info["deposit"],
                    record_info["goods_total"], record_info["deposit_total"],
                    record_info["commission_total"],
                    record_info["receipt_money"], record_info["pay_type_text"],
                    record_info["salesman_name"], record_info["sales_time"],
                    record_info["accountant_name"], record_info["pay_time"],
                    record_info["debtor_name"], record_info["order_num"],
                    record_info["remark"], record_info["storage"],
                    record_info["storage_num"]
                ])

            if not config.enable_deposit:
                index_deposit = data_content[0].index("押金")
                for data in data_content:
                    data.pop(index_deposit)
                index_deposit_total = data_content[0].index("押金小计")
                for data in data_content:
                    data.pop(index_deposit_total)

            if not config.enable_commission:
                index_commission = data_content[0].index(fee_text)
                for data in data_content:
                    data.pop(index_commission)
                index_commission_total = data_content[0].index(fee_text_total)
                for data in data_content:
                    data.pop(index_commission_total)
        return data_content

    @classmethod
    def format_export_goods_sale_history(cls, session, shop_id,
                                         salerecord_list, export_item_list):
        """货品流水自定义导出指定字段"""
        # 店铺是否开启去皮功能
        config = ShopFunc.get_config(session, shop_id)
        distinguish_weight = config.distinguish_weight

        data_content = []
        fee_text = ConfigFunc.get_fee_text(session, shop_id)
        fee_text_total = '{}小计'.format(fee_text)
        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)

        item_en_name_dict = {
            1: 'num',
            2: 'only_goods_name',
            3: 'shop_supplier_name',
            4: 'fact_price',
            5: 'sales_num',
            6: 'commission_mul',
            7: 'commission',
            8: 'deposit',
            9: 'goods_total',
            10: 'deposit_total',
            11: 'commission_total',
            12: 'receipt_money',
            13: 'salesman_name',
            14: 'sales_time',
            15: 'accountant_name',
            16: 'pay_time',
            17: 'debtor_name',
            18: 'order_num',
            19: 'pay_type',
            20: 'remark',
            21: 'batch_num',
            22: 'group_name',
            23: "gross_weight",
            24: "tare_weight"
        }
        item_cn_name_dict = {
            1: "流水号",
            2: "货品名",
            3: "供货商",
            4: "单价",
            5: '重量/{}'.format(weight_unit_text),
            6: "件数",
            7: fee_text,
            8: "押金",
            9: "货品小计",
            10: "押金小计",
            11: fee_text_total,
            12: "票据金额",
            13: "开票员",
            14: "开票时间",
            15: "收银员",
            16: "收款时间",
            17: "客户信息",
            18: "对应订单",
            19: "支付方式",
            20: "备注",
            21: "批次号",
            22: "分组名",
            23: "皮重",
            24: "毛重"
        }
        headline_list = ["序号", '自然日期', '扎账日期']
        # if distinguish_weight:
        #     export_item_list += [23, 24]  # 加入皮重和毛重导出, 暂时不需要，可能以后会用到
        for _key in export_item_list:
            if not distinguish_weight and _key in [23, 24]:
                continue
            else:
                key_name = item_cn_name_dict.get(_key, "未知")
            headline_list.append(key_name)

        data_content.append(headline_list)
        i = 1
        for sale_record in salerecord_list:
            order_time = sale_record["order_time"]
            if isinstance(order_time, str):
                order_time = order_time.split(" ")[0]
            else:
                order_time = ""
            _content_list = [i, order_time, sale_record["account_period_date"]]
            for _key in export_item_list:
                if not distinguish_weight and _key in [23, 24]:
                    continue
                # 支付方式
                if _key == 19:
                    _content_list.append(sale_record["pay_type_text"])
                    continue
                _content_list.append(sale_record[item_en_name_dict[int(_key)]])
            i += 1
            data_content.append(_content_list)
        return data_content

    # 自定义获取流水指定条目（以及导出）
    @classmethod
    def format_export_body_custom(cls,
                                  session,
                                  shop_id,
                                  salerecord_list,
                                  export_item_list,
                                  data_sum,
                                  have_headline=False,
                                  filter_config=None):
        """

        :param session:
        :param shop_id:
        :param salerecord_list:
        :param export_item_list:
        :param data_sum:
        :param have_headline: 需要导出时为True
        :param filter_config:
        :return:
        """
        # 店铺是否开启去皮功能
        config = ShopFunc.get_config(session, shop_id)
        distinguish_weight = config.distinguish_weight

        data_content = []
        record_data_dict = OrderSalesRecordFunc.batch_format_order_record_info(
            session, salerecord_list, get_wximgurl=True)
        SaleExportFilterConfig = models.SaleExportFilterConfig()

        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)
        item_cn_name_dict = SaleExportFilterConfig.get_item_cn_name_dict(
            weight_unit_text=weight_unit_text)
        item_en_name_dict = SaleExportFilterConfig.item_en_name_dict

        if not export_item_list:
            export_item_list = list(
                SaleExportFilterConfig.get_item_cn_name_dict(
                    weight_unit_text=weight_unit_text).keys())
        if have_headline:
            # 筛选条件顺便一起导出
            Accountinfo = models.Accountinfo
            HireLink = models.HireLink
            GoodsGroup = models.GoodsGroup
            # 获取开票员
            hire_link_base = session.query(Accountinfo.id, Accountinfo.realname, Accountinfo.nickname) \
                .join(HireLink, Accountinfo.id == HireLink.account_id) \
                .filter(HireLink.shop_id == shop_id)
            all_salesman = hire_link_base.filter(
                HireLink.active_salesman == 1).all()
            salesman_dict = {
                x.id: x.realname or x.nickname
                for x in all_salesman
            }

            # 获取收银员
            all_accountant = hire_link_base.filter(
                HireLink.active_accountant == 1).all()
            accountant_dict = {
                x.id: x.realname or x.nickname
                for x in all_accountant
            }

            # 获取付款方式
            pay_type_dict = models.GoodsSalesRecord().pay_type_dict

            # 获取商品分组
            all_group = session.query(
                GoodsGroup.id,
                GoodsGroup.name).filter_by(shop_id=shop_id).all()
            group_dict = {x.id: x.name for x in all_group}

            filter_config_content = ["筛选条件"]

            goods_type_dict = {0: "无", 1: "国产", 2: "出口"}
            text_contet_list = []
            goods_type_list = filter_config.get("goods_type_list", [])
            for _id in goods_type_list:
                text_contet_list.append(goods_type_dict.get(_id, "未知"))
            if text_contet_list:
                filter_config_content.append(
                    "货品类型：%s" % (",".join(text_contet_list)))

            text_contet_list = []
            group_id_list = filter_config.get("group_id_list", [])
            for _id in group_id_list:
                text_contet_list.append(group_dict.get(_id, "未知"))
            if text_contet_list:
                filter_config_content.append(
                    "分组类型：%s" % (",".join(text_contet_list)))

            supply_type_dict = {0: "自营", 1: "代卖", 2: "货主", 3: "联营"}
            text_contet_list = []
            supply_type_list = filter_config.get("supply_type_list", [])
            for _id in supply_type_list:
                text_contet_list.append(supply_type_dict.get(_id, "未知"))
            if text_contet_list:
                filter_config_content.append(
                    "销售类型：%s" % (",".join(text_contet_list)))

            text_contet_list = []
            accountant_id_list = filter_config.get("accountant_id_list", [])
            for _id in accountant_id_list:
                text_contet_list.append(accountant_dict.get(_id, "未知"))
            if text_contet_list:
                filter_config_content.append(
                    "收银员：%s" % (",".join(text_contet_list)))

            text_contet_list = []
            salesman_id_list = filter_config.get("salesman_id_list", [])
            for _id in salesman_id_list:
                text_contet_list.append(salesman_dict.get(_id, "未知"))
            if text_contet_list:
                filter_config_content.append(
                    "开票员：%s" % (",".join(text_contet_list)))

            customer_type_dict = {0: "散客"}
            text_contet_list = []
            customer_type_list = filter_config.get("customer_type_list", [])
            for _id in customer_type_list:
                text_contet_list.append(customer_type_dict.get(_id, "固定客户"))
            if text_contet_list:
                filter_config_content.append(
                    "客户类型：%s" % (",".join(text_contet_list)))

            text_contet_list = []
            pay_type_list = filter_config.get("pay_type_list", [])
            for _id in pay_type_list:
                text_contet_list.append(pay_type_dict.get(_id, "未知"))
            if text_contet_list:
                filter_config_content.append(
                    "支付方式：%s" % (",".join(text_contet_list)))

            filter_config_content.append(
                "票据金额：%s到%s" % (filter_config.get("receipt_min_money", 0),
                                filter_config.get("receipt_max_money", "无")))

            sale_unit_dict = {0: "{}".format(weight_unit_text), 1: "件"}
            text_contet_list = []
            sale_unit_list = filter_config.get("sale_unit_list", [])
            for _id in sale_unit_list:
                text_contet_list.append(sale_unit_dict.get(_id, "固定客户"))
            if text_contet_list:
                filter_config_content.append(
                    "售卖单位：%s" % (",".join(text_contet_list)))

            filter_config_content.append(
                "收银时间：%s到%s" % (filter_config.get("cash_time_start", "无"),
                                filter_config.get("cash_time_end", "无")))
            filter_config_content.append(
                "开票时间：%s到%s" % (filter_config.get("ticket_time_start", "无"),
                                filter_config.get("ticket_time_end", "无")))
            data_content.append(filter_config_content)
            data_content.append([])
            data_content.append([])
            headline_list = ["序号"]
            sum_list = ["累计"]

            fee_text = ConfigFunc.get_fee_text(session, shop_id)

            for _key in export_item_list:
                if not distinguish_weight and _key in [9, 10]:
                    continue
                if not config.enable_deposit and _key in [13, 15]:
                    continue
                if not config.enable_commission and _key in [12, 16]:
                    continue
                if _key == 12:
                    key_name = fee_text
                elif _key == 16:
                    key_name = "{}小计".format(fee_text)
                else:
                    key_name = item_cn_name_dict.get(_key, "未知")
                headline_list.append(key_name)
                sum_list.append(
                    data_sum.get(item_en_name_dict.get(_key, ""), ""))

            data_content.append(headline_list)
            data_content.append(sum_list)
            i = 1
            for sale_record in salerecord_list:
                record_info = record_data_dict.get(sale_record.id)
                _content_list = [i]
                for _key in export_item_list:
                    if _key == 1:
                        _content_list.append(
                            TimeFunc.time_to_str(sale_record.order_time))
                        continue
                    if _key == 2:
                        _content_list.append(
                            TimeFunc.time_to_str(
                                sale_record.account_period_date))
                        continue
                    if not config.enable_deposit and _key in [13, 15]:
                        continue
                    if not config.enable_commission and _key in [12, 16]:
                        continue
                    if not distinguish_weight and _key in [9, 10]:
                        continue
                    if _key == 18:
                        _content_list.append(record_info["pay_type_text"])
                        continue
                    _content_list.append(record_info[item_en_name_dict[_key]])
                i += 1
                data_content.append(_content_list)
        else:
            data_content = OrderSalesRecordFunc.batch_format_order_record_info(
                session,
                salerecord_list,
                return_type='list',
                get_wximgurl=True)
        return data_content, export_item_list


# 订单打印
class OrderPrintBaseFunc():
    @classmethod
    def celery_order_print(cls,
                           session,
                           shop_id,
                           order_id,
                           user_id=0,
                           receipt_config=None):
        if cls._check_salesman_settlement(order_id):
            # 手机直接收款不取传过来的receipt_config，直接去数据库取
            need_print = ShopFunc.check_salesman_need_print(session, shop_id)
        else:
            need_print = ShopFunc.check_cashier_need_print(
                session, shop_id, receipt_config=receipt_config)
        if need_print:
            from handlers.celery_print import print_order
            print_order(order_id, user_id, receipt_config=receipt_config)

    @classmethod
    def order_print(cls,
                    session,
                    order,
                    user_id=0,
                    async=False,
                    receipt_config=None,
                    repeat_print=False):
        order_id = order.id
        shop_id = order.shop_id
        notice_txt = ""
        if not user_id:
            user_id = order.accountant_id
        if not receipt_config:
            receipt_config = ShopFunc.get_receipt_config(session, shop_id)

        if async and cls._check_salesman_settlement(order_id):
            # 手机直接收款不取传过来的receipt_config，直接去数据库取
            need_print = ShopFunc.check_salesman_need_print(session, shop_id)
        else:
            need_print = ShopFunc.check_cashier_need_print(
                session, shop_id, receipt_config=receipt_config)

        # 记录收银台开始打印时间
        log_dict = {}
        log_dict["order_id"] = order_id
        log_dict["start_time"] = TimeFunc.time_to_str(datetime.datetime.now())
        log_dict["need_print"] = need_print

        if need_print:
            scene = 2
            source = "accountant"

            if async and cls._check_salesman_settlement(order_id):
                scene = 1
                source = "salesman_async"
                cls._delete_salesman_settlement(order_id)
                log_dict["salesman_async"] = 1

            wireless_print_type, wireless_print_num, wireless_print_key, _ = WirelessPrintFunc.get_hirelink_printer(
                session, shop_id, user_id, scene=scene)
            print_dict = {
                "wireless_print_type": wireless_print_type,
                "wireless_print_num": wireless_print_num,
                "wireless_print_key": wireless_print_key
            }
            # 记录打印机
            log_dict["wireless_print_num"] = wireless_print_num

            # 如果订单内只有一张小票，强制走原单打印模式, 并在结束后恢复控制打印模式的状态
            if cls._check_order_records_single(
                    session, order_id) or source == "salesman_async":
                origin_merge_print_accountant = receipt_config.merge_print_accountant
                origin_merge_print_goods = receipt_config.merge_print_goods
                receipt_config.merge_print_accountant = 0
                receipt_config.merge_print_goods = 0
            # 合并打印模式
            if (receipt_config.cashier_accountant_receipt
                    and receipt_config.merge_print_accountant == 1) or (
                        receipt_config.cashier_goods_receipt
                        and receipt_config.merge_print_goods == 1):
                cls._merge_sale_record_print(
                    session,
                    order,
                    receipt_config,
                    print_dict,
                    repeat_print=repeat_print)
            # 原单打印模式
            notice_txt = cls._multi_sale_record_print(
                session,
                receipt_config,
                shop_id,
                user_id,
                print_dict,
                order=order,
                source=source,
                repeat_print=repeat_print)
            # 恢复打印模式设置
            if cls._check_order_records_single(
                    session, order_id) or source == "salesman_async":
                receipt_config.merge_print_accountant = origin_merge_print_accountant
                receipt_config.merge_print_goods = origin_merge_print_goods
            session.commit()

            # 设置订单已打印标识，避免重复发送打印请求
            order_redis_key = "async_order_print_mark:{}".format(order_id)
            redis.set(order_redis_key, "1", 3600)

        log_date = datetime.datetime.now().strftime('%Y%m%d')
        log_msg_dict("async_order_print/%d/%s" % (shop_id, log_date), log_dict)
        return notice_txt

    # 检查订单是否为一单多品小票订单
    @classmethod
    def _check_order_if_temp_order(cls, session, order_id):
        Order = models.Order
        order = session.query(Order).filter_by(
            tally_order_id=order_id, multi_goods=1).first()
        if order:
            return True
        else:
            return False

    # 检查一个订单内是否只包含一条流水
    @classmethod
    def _check_order_records_single(cls, session, order_id):
        record_count = session.query(func.count(
            models.GoodsSalesRecord.id)).filter_by(
                order_id=order_id).scalar() or 0
        if record_count > 1:
            return False
        return True

    # 检查是否为开票助手收款
    @classmethod
    def _check_salesman_settlement(cls, order_id):
        salersman_settlement_print_key = "salersman_settlement_print:%d" % order_id
        if redis.get(salersman_settlement_print_key):
            return True
        else:
            return False

    # 检查是否为开票助手收款
    @classmethod
    def _delete_salesman_settlement(cls, order_id):
        salersman_settlement_print_key = "salersman_settlement_print:%d" % order_id
        redis.delete(salersman_settlement_print_key)

    @classmethod
    def sale_record_print(cls,
                          session,
                          record_list,
                          receipt_config,
                          shop_id,
                          user_id,
                          print_dict,
                          source="",
                          local_print=0,
                          repeat_print=False):
        '''
            单品小票打印
        '''
        wireless_print_type = int(print_dict["wireless_print_type"])
        wireless_print_num = print_dict["wireless_print_num"]
        shop = ShopFunc.get_shop(session, shop_id)
        wpp = WirelessPrintFunc(wireless_print_type, shop=shop)
        accounting_time = AccountingPeriodFunc.get_accounting_time(
            session, shop_id)

        notice_txt, print_body = cls.single_sale_record_print(
            session,
            wpp,
            record_list,
            receipt_config,
            shop_id,
            user_id,
            wireless_print_num,
            source,
            accounting_time,
            is_hand_print=True,
            local_print=local_print,
            repeat_print=repeat_print)
        session.commit()
        return notice_txt, print_body

    @classmethod
    def single_sale_record_print(cls,
                                 session,
                                 wpp,
                                 record_list,
                                 receipt_config,
                                 shop_id,
                                 user_id,
                                 wireless_print_num,
                                 source="",
                                 accounting_time=None,
                                 is_hand_print=False,
                                 local_print=0,
                                 repeat_print=False):
        '''
            单品小票打印,以slice_count为切分点发送打印请求
        '''
        slice_count = 3
        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)
        # 查询签名照片缓存
        order_id = record_list[0].order_id
        redis_key = "signature_img:{}".format(order_id)
        signature_img = (redis.get(redis_key) or b"").decode()

        # 查询订单流水数，用于判断是否打印抹零信息
        order_ids = [record.order_id for record in record_list]
        record_counts = session.query(
            models.Order.id, func.count(models.GoodsSalesRecord.id)).join(
                models.GoodsSalesRecord,
                models.Order.id == models.GoodsSalesRecord.order_id).filter(
                    models.Order.id.in_(order_ids)).group_by(
                        models.Order.id).all()
        record_count_dict = {data[0]: data[1] for data in record_counts}

        fee_text = ConfigFunc.get_fee_text(session, shop_id)

        # 将所有需要打印的小票放入打印数组中
        print_body_list = []
        index = 0
        for valid_sale_record in record_list:
            # 结算中只有一个流水的才打印抹零信息
            print_erasement = record_count_dict.get(valid_sale_record.order_id,
                                                    0) == 1
            sale_record_info = GoodsSaleFunc.get_sale_record_info(
                session,
                valid_sale_record,
                signature_img,
                weight_unit_text=weight_unit_text,
                print_erasement=print_erasement,
                repeat_print=repeat_print)
            index += 1
            if valid_sale_record.multi_order_id == 0:
                content_body_list = wpp.get_sale_record_print_body(
                    sale_record_info,
                    receipt_config,
                    shop_id=shop_id,
                    accounting_time=accounting_time,
                    source=source,
                    force_accountant=True,
                    force_goods=True,
                    is_hand_print=is_hand_print,
                    fee_text=fee_text,
                    index=index)
            else:
                content_body_list = wpp.get_sale_record_print_body(
                    sale_record_info,
                    receipt_config,
                    shop_id=shop_id,
                    accounting_time=accounting_time,
                    source=source,
                    is_hand_print=is_hand_print,
                    fee_text=fee_text,
                    index=index)
            print_body_list.extend(content_body_list)

        print_body_content = ""
        print_body_len = len(print_body_list)
        if_print_success, notice_txt = True, ""

        if not local_print == 1:
            for print_body in print_body_list:
                # 取出小票下标用于计算切分点
                record_index = print_body_list.index(print_body) + 1
                print_body_content += print_body

                # 不是slice_count的倍数并且不是最后一张小票的需要加上切刀
                if record_index % slice_count != 0 and record_index != print_body_len:
                    print_body_content += wpp.cut()

                # 是slice_count的倍数或者是最后一张小票需要发送打印请求，发送打印请求后需要清空打印字符串防止重复打印
                if record_index % slice_count == 0 or record_index == print_body_len:
                    if_print_success, error_txt = wpp.send_print_request(
                        print_body_content, wireless_print_num)
                    # 清空打印字符串以便进行下一次打印
                    print_body_content = ""
                    notice_txt += error_txt

        cls.single_sale_record_print_log(
            session,
            shop_id,
            record_list,
            user_id,
            if_print_success,
            notice_txt,
            wireless_print_num,
            source=source)
        return notice_txt, print_body_list

    @classmethod
    def _multi_sale_record_print(cls,
                                 session,
                                 receipt_config,
                                 shop_id,
                                 user_id,
                                 print_dict,
                                 order=None,
                                 source="",
                                 repeat_print=False):
        '''
            收银台结算后打印一单多品订单场景比较复杂
            场景1:单品小票
            场景2:单品小票+一单多品小票
            场景3:一单多品小票+一单多品小票
            一单多品收银台出票数量为单张模式时一单多品要打印一单多品的格式，单品小票打印单品小票格式
        '''
        wireless_print_type = int(print_dict["wireless_print_type"])
        wireless_print_num = print_dict["wireless_print_num"]
        shop = ShopFunc.get_shop(session, shop_id)
        wpp = WirelessPrintFunc(wireless_print_type, shop=shop)
        accounting_time = AccountingPeriodFunc.get_accounting_time(
            session, shop_id)
        if_print_success, notice_txt = True, ""
        order_id = order.id

        fee_text = ConfigFunc.get_fee_text(session, shop_id)

        # 一单多品打印
        multi_sales_record_dict, multi_order_dict = MultiOrderFunc.get_multi_goods_sale_record_order_id(
            session, order_id)
        for multi_order_id in multi_sales_record_dict:
            multi_record_list = multi_sales_record_dict[multi_order_id]
            multi_order = multi_order_dict[multi_order_id]
            sale_record_info_list, temp_order_info = GoodsSaleFunc.get_temp_sale_order_info(
                session,
                multi_record_list,
                multi_order,
                source=source,
                pay_order=order,
                repeat_print=repeat_print)
            _, content_body_list = wpp.get_multi_sale_record_print_body(
                sale_record_info_list,
                temp_order_info,
                receipt_config,
                accounting_time=accounting_time,
                source=source,
                fee_text=fee_text)

            for content_body in content_body_list:
                if_print_success, error_txt = wpp.send_print_request(
                    content_body, wireless_print_num)
                notice_txt += error_txt

        # 单品小票打印
        # 兼容手机直接收款
        if not (source == "salesman_async" and multi_order_dict):
            single_record_list = GoodsSaleFunc.get_sale_record_through_order(
                session, order_id)
            if single_record_list:
                error_txt, _ = cls.single_sale_record_print(
                    session,
                    wpp,
                    single_record_list,
                    receipt_config,
                    shop_id,
                    user_id,
                    wireless_print_num,
                    source,
                    accounting_time,
                    repeat_print=repeat_print)
                notice_txt += error_txt

        # 订单打印日志
        cls.multi_sale_record_print_log(
            session,
            shop_id,
            order_id,
            user_id,
            if_print_success,
            notice_txt,
            wireless_print_num,
            source=source)

        return notice_txt

    @classmethod
    def _merge_sale_record_print(cls,
                                 session,
                                 order,
                                 receipt_config,
                                 print_dict,
                                 repeat_print=False):
        """ 收银台流水合并打印 """
        source = "accountant"
        wireless_print_type = int(print_dict["wireless_print_type"])
        wireless_print_num = print_dict["wireless_print_num"]
        shop = ShopFunc.get_shop(session, order.shop_id)
        wpp = WirelessPrintFunc(wireless_print_type, shop=shop)

        sale_record_list = GoodsSaleFunc.get_sale_record_through_order(
            session, order.id)
        sale_record_info_list, temp_order_info = GoodsSaleFunc.get_temp_sale_order_info(
            session,
            sale_record_list,
            order,
            source=source,
            pay_order=order,
            is_merge=True,
            repeat_print=repeat_print)

        fee_text = ConfigFunc.get_fee_text(session, order.shop_id)

        accounting_time = AccountingPeriodFunc.get_accounting_time(
            session, order.shop_id)
        _, content_body_list = wpp.get_merge_sale_record_print_body(
            sale_record_info_list,
            temp_order_info,
            receipt_config,
            accounting_time=accounting_time,
            source=source,
            fee_text=fee_text)

        notice_txt = ""
        for content_body in content_body_list:
            if_print_success, error_txt = wpp.send_print_request(
                content_body, wireless_print_num)
            notice_txt += error_txt

        # 订单打印日志
        cls.multi_sale_record_print_log(
            session,
            order.shop_id,
            order.id,
            order.accountant_id,
            if_print_success,
            notice_txt,
            wireless_print_num,
            source=source)

        return notice_txt

    @classmethod
    def multi_sale_record_print_log(cls,
                                    session,
                                    shop_id,
                                    order_id,
                                    user_id,
                                    print_result,
                                    error_txt,
                                    printer_num,
                                    source=""):
        if source == "accountant":
            source_type = 2
        else:
            source_type = 1

        TempOrderPrintLog = models.TempOrderPrintLog
        print_log = TempOrderPrintLog(
            shop_id=shop_id,
            order_id=order_id,
            user_id=user_id,
            print_result=print_result,
            error_txt=error_txt,
            source_type=source_type,
            printer_num=printer_num)
        session.add(print_log)
        session.flush()

    @classmethod
    def single_sale_record_print_log(cls,
                                     session,
                                     shop_id,
                                     sale_record_list,
                                     user_id,
                                     print_result,
                                     error_txt,
                                     printer_num,
                                     source=""):
        if source == "accountant":
            source_type = 2
        else:
            source_type = 1

        GoodsSalesRecordPrintLog = models.GoodsSalesRecordPrintLog

        log_list = []
        for sale_record in sale_record_list:
            sales_record_id = sale_record.id
            order_id = sale_record.order_id
            print_log = GoodsSalesRecordPrintLog(
                shop_id=shop_id,
                sales_record_id=sales_record_id,
                order_id=order_id,
                user_id=user_id,
                print_result=print_result,
                error_txt=error_txt,
                source_type=source_type,
                printer_num=printer_num)
            log_list.append(print_log)
        session.add_all(log_list)
        session.flush()

    @classmethod
    def get_print_times(self, session, sales_record_id):
        """获取单品小票打印次数"""
        print_times = session \
            .query(func.count(models.GoodsSalesRecordPrintLog.id)) \
            .filter_by(sales_record_id=sales_record_id, source_type=2) \
            .scalar() or 0
        return print_times

    @classmethod
    def get_first_record_id(cls, session, order_id):
        """获取订单的第一条流水ID"""
        first_record_id = session\
            .query(models.GoodsSalesRecord.id)\
            .filter_by(order_id=order_id).first()
        first_record_id = first_record_id.id
        return first_record_id

    @classmethod
    def get_order_print_times(self, session, order_id):
        """获取订单打印次数"""
        print_times = session \
            .query(func.count(models.RecordPrintLog.id)) \
            .filter_by(fk_id=order_id, print_type=8, source_type=1) \
            .scalar() or 0
        print_times += 1
        return print_times


# 返回print_body方法
class OrderPrintContentFunc():
    @classmethod
    def order_print_body(cls,
                         session,
                         shop_id,
                         order,
                         source,
                         local_print,
                         if_pborder=False,
                         pb_print=0,
                         receipt_config=None,
                         repeat_print=False):
        """
        主要兼容pos和蓝牙两种无线打印以及本地80打印返回print_body
        :param order: 订单
        :param source: 请求来源（accountant 收银台; salesman 开票助手; salesman_async 开票助手直接收款）
        :param local_print: 来源为accountant时，0-无线打印 1-本地打印；来源为开票助手，1-pos机打印 2-蓝牙打印
        :param if_pborder: 是否为还款订单
        :param pb_print: 0-还款订单不打印 1-还款订单无线打印 2-还款订单本地打印
        """
        print_body = ""
        if not receipt_config:
            receipt_config = ShopFunc.get_receipt_config(session, shop_id)
        shop = ShopFunc.get_shop(session, shop_id)
        accounting_time = AccountingPeriodFunc.get_accounting_time(
            session, shop_id)
        order_id = order.id
        fee_text = ConfigFunc.get_fee_text(session, shop_id)
        print_body_list_content = []
        is_hand_print = False
        # 用来区分小票生成条形码
        index = 0
        # 结算订单print_body
        if not if_pborder:
            # 开票员手机直接收款打印小票（默认打印开票员的小票模板）
            if source == "accountant" and local_print == 1:
                wireless_print_type = 4
            elif source == 'salesman_async':
                is_hand_print = True
                if not local_print in (1, 2):
                    return print_body
                if local_print == 1:
                    wireless_print_type = "smposprint"
                else:
                    wireless_print_type = 3
            else:
                return print_body

            wpp = WirelessPrintFunc(wireless_print_type, shop=shop)
            multi_sales_record_dict, multi_order_dict = MultiOrderFunc.get_multi_goods_sale_record_order_id(
                session, order_id)

            # 如果订单内只有一张小票，强制走原单打印模式, 并在结束后恢复控制打印模式的状态
            if OrderPrintBaseFunc._check_order_records_single(
                    session, order_id) or source == "salesman_async":
                origin_merge_print_accountant = receipt_config.merge_print_accountant
                origin_merge_print_goods = receipt_config.merge_print_goods
                receipt_config.merge_print_accountant = 0
                receipt_config.merge_print_goods = 0
            # 合并打印模式
            if (receipt_config.cashier_accountant_receipt
                    and receipt_config.merge_print_accountant == 1) or (
                        receipt_config.cashier_goods_receipt
                        and receipt_config.merge_print_goods == 1):
                index += 1
                sale_record_list = GoodsSaleFunc.get_sale_record_through_order(
                    session, order.id)
                sale_record_info_list, temp_order_info = GoodsSaleFunc.get_temp_sale_order_info(
                    session,
                    sale_record_list,
                    order,
                    source=source,
                    pay_order=order,
                    is_merge=True,
                    repeat_print=repeat_print)
                _, content_body_list = wpp.get_merge_sale_record_print_body(
                    sale_record_info_list,
                    temp_order_info,
                    receipt_config,
                    accounting_time=accounting_time,
                    source=source,
                    fee_text=fee_text,
                    index=index)
                print_body_list_content.extend(content_body_list)
            # 原单打印模式
            # 一单多品打印
            for multi_order_id in multi_sales_record_dict:
                index += 1
                multi_record_list = multi_sales_record_dict[multi_order_id]
                multi_order = multi_order_dict[multi_order_id]
                sale_record_info_list, temp_order_info = GoodsSaleFunc.get_temp_sale_order_info(
                    session,
                    multi_record_list,
                    multi_order,
                    source=source,
                    pay_order=order,
                    repeat_print=repeat_print)
                content_body, content_body_list = wpp.get_multi_sale_record_print_body(
                    sale_record_info_list,
                    temp_order_info,
                    receipt_config,
                    accounting_time=accounting_time,
                    source=source,
                    fee_text=fee_text,
                    index=index)
                print_body_list_content.extend(content_body_list)

            # 单品小票打印
            # 兼容手机直接收款
            if not (source == "salesman_async" and multi_order_dict):
                index = 0
                single_record_list = GoodsSaleFunc.get_sale_record_through_order(
                    session, order_id)
                if single_record_list:
                    weight_unit_text = ConfigFunc.get_weight_unit_text(
                        session, shop_id)
                    # 查询签名照片缓存
                    order_id = single_record_list[0].order_id
                    redis_key = "signature_img:{}".format(order_id)
                    signature_img = (redis.get(redis_key) or b"").decode()

                    # 查询订单流水数，用于判断是否打印抹零信息
                    order_ids = [
                        record.order_id for record in single_record_list
                    ]
                    record_counts = session \
                        .query(
                            models.Order.id,
                            func.count(models.GoodsSalesRecord.id)) \
                        .join(
                            models.GoodsSalesRecord,
                            models.Order.id == models.GoodsSalesRecord.order_id) \
                        .filter(models.Order.id.in_(order_ids)) \
                        .group_by(models.Order.id) \
                        .all()
                    record_count_dict = {
                        data[0]: data[1]
                        for data in record_counts
                    }
                    for valid_sale_record in single_record_list:
                        # 结算中只有一个流水的才打印抹零信息
                        index += 1
                        print_erasement = record_count_dict.get(
                            valid_sale_record.order_id, 0) == 1
                        sale_record_info = GoodsSaleFunc.get_sale_record_info(
                            session,
                            valid_sale_record,
                            signature_img,
                            weight_unit_text=weight_unit_text,
                            print_erasement=print_erasement,
                            repeat_print=repeat_print)
                        if valid_sale_record.multi_order_id == 0:
                            content_body_list = wpp.get_sale_record_print_body(
                                sale_record_info,
                                receipt_config,
                                shop_id=shop_id,
                                accounting_time=accounting_time,
                                source=source,
                                force_accountant=True,
                                force_goods=True,
                                fee_text=fee_text,
                                is_hand_print=is_hand_print,
                                index=index)
                        else:
                            content_body_list = wpp.get_sale_record_print_body(
                                sale_record_info,
                                receipt_config,
                                shop_id=shop_id,
                                accounting_time=accounting_time,
                                source=source,
                                fee_text=fee_text,
                                is_hand_print=is_hand_print,
                                index=index)
                        print_body_list_content.extend(content_body_list)
            # 恢复打印模式设置
            if OrderPrintBaseFunc._check_order_records_single(
                    session, order_id) or source == "salesman_async":
                receipt_config.merge_print_accountant = origin_merge_print_accountant
                receipt_config.merge_print_goods = origin_merge_print_goods

            if print_body_list_content and wireless_print_type != 4:
                count_index = 1
                for print_body_content in print_body_list_content:
                    add_cut = "" if count_index == len(
                        print_body_list_content) else wpp.cut()
                    print_body += (print_body_content + add_cut)
                    count_index += 1
            elif print_body_list_content and wireless_print_type == 4:
                print_body = []
                print_body.extend(print_body_list_content)

            if local_print == 2:
                print_body = wpp.sgbluetooth(print_body)
        # 还款订单打印小票
        elif if_pborder and pb_print == 2:
            if source == "accountant" and local_print == 1:
                wireless_print_type = 4
            else:
                return print_body

            wpp = WirelessPrintFunc(wireless_print_type, shop=shop)
            pb_order_info = PbOrderPrintBaseFunc.get_pb_order_info(
                session, order)
            print_body = []
            content_body = wpp.get_payback_print_body(pb_order_info)
            print_body.append(content_body)

        return print_body


# 还款凭证打印
class PbOrderPrintBaseFunc():
    @classmethod
    def celery_pb_order_print(cls, pb_order_id, user_id=0):
        from handlers.celery_print import print_pb_order
        print_pb_order.delay(pb_order_id, user_id)

    @classmethod
    def pb_order_print(cls, session, pb_order, user_id=0):
        shop_id = pb_order.shop_id
        shop = ShopFunc.get_shop(session, shop_id)
        if not user_id:
            user_id = pb_order.accountant_id

        wireless_print_type, wireless_print_num, wireless_print_key, _ = WirelessPrintFunc.get_hirelink_printer(
            session, shop_id, user_id, scene=2)
        wireless_print_type = int(wireless_print_type)
        if wireless_print_type not in [2, 3]:
            return "请设置无线打印机"

        wpp = WirelessPrintFunc(wireless_print_type, shop=shop)
        pb_order_info = cls.get_pb_order_info(session, pb_order)
        content_body = wpp.get_payback_print_body(pb_order_info)
        if_print_success, error_txt = wpp.send_print_request(
            content_body, wireless_print_num)

        RecordPrintFunc.record_print_log(
            session,
            shop_id,
            pb_order.id,
            user_id,
            if_print_success,
            error_txt,
            wireless_print_num,
            print_type=1,
            source_type=1)

        if if_print_success:
            order_redis_key = "async_pb_order_print_mark:{}".format(
                pb_order.id)
            redis.set(order_redis_key, "1", 3600)
        return error_txt

    @classmethod
    def get_pb_order_info(cls, session, pb_order):
        shop_id = pb_order.shop_id
        accountant_id = pb_order.accountant_id

        shop_name = session.query(
            models.Shop.shop_name).filter_by(id=pb_order.shop_id).scalar()
        shop_customer = session.query(models.ShopCustomer) \
            .filter_by(id=pb_order.debtor_id, shop_id=shop_id) \
            .first()
        accountant = AccountFunc.get_account_through_id(session, accountant_id)

        combine_pay_dict = OrderBaseFunc.multi_combine_pay_type_text(
            session, [pb_order.id], source_type="pb_order")
        pay_type_text = OrderBaseFunc.order_pay_type_text(
            pb_order.id, pb_order.pay_type, combine_pay_dict)
        if pb_order.pay_type == 12 and pb_order.bank_no:
            bank_name = OrderBaseFunc.get_order_bank_name(
                session, pb_order.id, source="pb_order")
            pay_type_text = "{0}({1})".format(pay_type_text, bank_name)

        pb_order_info = {}
        pb_order_info["shop_name"] = shop_name
        pb_order_info["money"] = NumFunc.check_float(
            pb_order.total_price / 100)
        pb_order_info["erase_money"] = NumFunc.check_float(
            pb_order.erase_money / 100)
        pb_order_info["fact_total_price"] = NumFunc.check_float(
            pb_order.fact_total_price / 100)
        pb_order_info["pay_type_text"] = pay_type_text
        pb_order_info["time"] = TimeFunc.splice_time(
            pb_order.pay_year, pb_order.pay_month, pb_order.pay_day,
            pb_order.pay_time)
        pb_order_info["debtor_name"] = shop_customer.name
        pb_order_info["debtor_phone"] = shop_customer.phone
        pb_order_info["debtor_number"] = shop_customer.number
        pb_order_info["debtor_rest"] = NumFunc.check_float(
            shop_customer.debt_rest / 100)
        pb_order_info[
            "accountant_name"] = accountant.realname or accountant.nickname
        return pb_order_info


# 无线打印
class WirelessPrintFunc():
    def __init__(self, print_type, _paper_width=32, shop=None):

        # 打印机类型
        if print_type == "smposprint":
            self._type = print_type
            _cashier_accountant_space = (_paper_width - 8) * " "
            _cashier_goods_space = (_paper_width - 20) * " "
            _salesman_customer_space = (_paper_width - 16) * " "
            _salesman_accountant_space = (_paper_width - 8) * " "
        else:
            print_type_dict = {
                1: "ylyprint",
                2: "feprint",
                3: "sgprint",
                4: "80localprint"
            }
            self._type = print_type_dict.get(print_type, "sgprint")
            paper_width_dict = {1: 32, 2: 48, 3: 48, 4: 48}
            _paper_width = paper_width_dict.get(print_type, 32)
            # 收银台空格
            cashier_accountant_space_dict = {
                1: (_paper_width - 8) * " ",
                2: (_paper_width - 8) * " ",
                3: (_paper_width - 8) * " ",
                4: (_paper_width - 8) * "&nbsp;"
            }
            cashier_goods_space_dict = {
                1: (_paper_width - 20) * " ",
                2: (_paper_width - 20) * " ",
                3: (_paper_width - 20) * " ",
                4: (_paper_width - 6) * "&nbsp;"
            }
            # 开票助手空格
            salesman_customer_space_dict = {
                1: (_paper_width - 16) * " ",
                2: (_paper_width - 16) * " ",
                3: (_paper_width - 16) * " ",
                4: (_paper_width - 16) * " "
            }
            salesman_accountant_space_dict = {
                1: (_paper_width - 8) * " ",
                2: (_paper_width - 8) * " ",
                3: (_paper_width - 8) * " ",
                4: (_paper_width - 8) * " "
            }
            _cashier_accountant_space = cashier_accountant_space_dict.get(
                print_type, (_paper_width - 8) * " ")
            _cashier_goods_space = cashier_goods_space_dict.get(
                print_type, (_paper_width - 20) * " ")
            _salesman_customer_space = salesman_customer_space_dict.get(
                print_type, (_paper_width - 16) * " ")
            _salesman_accountant_space = salesman_accountant_space_dict.get(
                print_type, (_paper_width - 8) * " ")

        # 纸宽
        self._paper_width = _paper_width

        # 会计联、客户联和提货联的间距
        self._cashier_accountant_space = _cashier_accountant_space
        self._cashier_goods_space = _cashier_goods_space
        self._salesman_customer_space = _salesman_customer_space
        self._salesman_accountant_space = _salesman_accountant_space

        # 打印机默认字号
        if self._type in ["feprint", "sgprint"]:
            self.default_size = 0
        else:
            self.default_size = 1

        if shop and shop.is_trial:
            self.support_text = self.line_break("技术支持：森果  服务热线：400-027-0135") \
                                + self.float_middle("系统未缴全款")  # 底部技术支持文字
        else:
            self.support_text = "技术支持：森果  服务热线：400-027-0135"  # 底部技术支持文字

    @classmethod
    def get_hirelink_printer(cls, session, shop_id, account_id, scene=1):
        '''获取员工当前绑定打印机
            scene 1:开票员  2:收银员
        '''
        if scene == 1:
            printer_key = "printer:%s:%s" % (shop_id, account_id)
        elif scene == 2:
            printer_key = "accountant_printer:%s:%s" % (shop_id, account_id)
        elif scene == 3:
            printer_key = "admin_printer:%s:%s" % (shop_id, account_id)
        printer = redis.get(printer_key)
        if printer:
            printer = printer.decode('utf-8')
            wireless_print_type, wireless_print_num, wireless_print_key, printer_remark = printer.split(
                "|")
        else:
            wireless_print_type, wireless_print_num, wireless_print_key, printer_remark = "0", "", "", ""
            if scene == 1:
                printer_id = session \
                    .query(models.HireLink.printer_id) \
                    .filter_by(shop_id=shop_id, account_id=account_id) \
                    .scalar()
            elif scene == 2:
                printer_id = session \
                    .query(models.HireLink.accountant_printer_id) \
                    .filter_by(shop_id=shop_id, account_id=account_id) \
                    .scalar()
            elif scene == 3:
                printer_id = session \
                    .query(models.HireLink.admin_printer_id) \
                    .filter_by(shop_id=shop_id, account_id=account_id)\
                    .scalar()
            if printer_id:
                # 检查打印机的状态和打印机被分配的状态
                print_data = session.query(models.Printer) \
                    .join(
                        models.PrinterAssignLink,
                        models.PrinterAssignLink.printer_id == models.Printer.id) \
                    .filter(
                        models.Printer.id == printer_id,
                        models.PrinterAssignLink.account_id == account_id,
                        models.Printer.status == 1,
                        models.PrinterAssignLink.status == 1) \
                    .first()
                if print_data:
                    wireless_print_type = str(print_data.receipt_type)
                    wireless_print_num = print_data.wireless_print_num
                    wireless_print_key = print_data.wireless_print_key
                    printer_remark = print_data.remark
                    redis.set(
                        printer_key,
                        wireless_print_type + "|" + wireless_print_num + "|" +
                        wireless_print_key + "|" + printer_remark, 24 * 3600)
        return wireless_print_type, wireless_print_num, wireless_print_key, printer_remark

    def line_break(self, str_data):
        """换行"""
        _type = self._type
        if _type == "ylyprint":
            return "{}\r\n".format(str_data)
        elif _type == "feprint":
            return "{}<BR>".format(str_data)
        elif _type == "sgprint":
            return "{}\n".format(str_data)
        elif _type == "smposprint":
            return "{}\n".format(str_data)
        elif _type == "80localprint":
            if not str_data:
                return "</br>"
            if not re.search('<p', str_data):
                return "<p style='display:block;'>{}</p>".format(str_data)
            _, num = re.search('style=', str_data).span()
            str_data = "<p style='display:block;" + str_data[num + 1:]
            return str_data

    def bottom_line_break(self):
        """横线"""
        _type = self._type
        if _type == "ylyprint":
            return "--------------------------------\r\n"
        elif _type == "feprint":
            return "------------------------------------------------<BR>"
        elif _type == "sgprint":
            return "------------------------------------------------\n"
        elif _type == "smposprint":
            return "----------------\n"
        elif _type == "80localprint":
            return "<p style='overflow:hidden;width:100%;height: 20px;'>------------------------------------------------------------</p>"

    def float_middle(self, str_data, bytes_len=0):
        """居中"""
        _type = self._type
        _paper_width = self._paper_width
        if _type == "ylyprint":
            # 32字节
            left_len = (_paper_width - bytes_len) // 2
            right_len = (_paper_width - left_len - bytes_len)
            return left_len * ' ' + str_data + right_len * ' '
        elif _type == "feprint":
            return "<C>{}</C>".format(str_data)
        elif _type == "sgprint":
            return "<C>{}</C>".format(str_data)
        elif _type == 'smposprint':
            return "##1{}\n".format(str_data)
        elif _type == "80localprint":
            if not re.search('<p', str_data):
                return "<p style='text-align:center;'>{}</p>".format(str_data)
            _, num = re.search('style=', str_data).span()
            str_data = "<p style='text-align:center;" + str_data[num + 1:]
            return str_data

    def float_right(self, str_data, bytes_len=0):
        """右对齐"""
        _type = self._type
        _paper_width = self._paper_width
        if _type == "ylyprint":
            # 32个字节
            return (_paper_width - bytes_len) * ' ' + str_data
        elif _type == "feprint":
            return "<RIGHT>{}</RIGHT>".format(str_data)
        elif _type == "sgprint":
            return "<R>{}</R>".format(str_data)
        elif _type == "smposprint":
            return "##2{}\n".format(str_data)
        elif _type == "80localprint":
            if not re.search('<p', str_data):
                return "<p style='text-align:right;'>{}</p>".format(str_data)
            _, num = re.search('style=', str_data).span()
            str_data = "<p style='text-align:right;" + str_data[num + 1:]
            return str_data

    def zoom_in(self, str_data, zoom_type=2):
        """字号缩放

        zoom_type:缩放类型:0.不缩放,1.宽度放大,2.高度放大,3.宽度和高度都放大
        """
        _type = self._type
        if _type == "ylyprint":
            return "@@2" + str_data
        elif _type == "feprint":
            if zoom_type == 1:
                return "<W>{}</W>".format(str_data)
            elif zoom_type == 2:
                return "<B>{}</B>".format(str_data)
            elif zoom_type == 3:
                return "<DB>{}</DB>".format(str_data)
            else:
                return str_data
        elif _type == "sgprint":
            if zoom_type == 1:
                return "<FS1>{}</FS1>".format(str_data)
            elif zoom_type == 2:
                return "<FS2>{}</FS2>".format(str_data)
            elif zoom_type == 3:
                return "<FS3>{}</FS3>".format(str_data)
            else:
                return str_data
        elif _type == "smposprint":
            if zoom_type == 1:
                return "@@1{}".format(str_data)
            elif zoom_type == 2:
                return "@@2{}".format(str_data)
            elif zoom_type == 3:
                return "@@3{}".format(str_data)
            else:
                return str_data
        elif _type == "80localprint":
            if zoom_type == 1:
                return "<p style='font-size:8pt;'>{}</p>".format(str_data)
            elif zoom_type == 2:
                return "<p style='font-size:16pt;'>{}</p>".format(str_data)
            elif zoom_type == 3:
                return "<p style='font-size:24pt;'>{}</p>".format(str_data)
            else:
                return str_data

        # size是几表示对应几个普通的汉字大小
        # for size in [3,4,5,6,7,8]:
        #     b_start = [29,33,17*(size-1)]
        #     b_end = [13,10,27,64]
        #     temp_str = ''.join([chr(c) for c in b_start])+'012'+''.join([chr(c) for c in b_end])

    def barcode(self, str_data, type=None, index=None):
        """条码"""
        _type = self._type
        if _type == "ylyprint":
            return "<b>{}</b>".format(str_data)
        elif _type == "feprint":
            length = len(str_data)
            b = [27, 100, 2, 29, 72, 50, 29, 104, 80, 29, 119, 2, 29, 107]
            b[5] = 48  # 48隐藏下方的号码,49展示上方的号码,50展示下方的号码
            b[8] = 100  # 调节条码高度,0x50~0x64,没有给固定的，应该想多少都差不多
            b.append(73)  # code128
            b.append(length + 2)
            b.append(123)
            b.append(66)
            i = 0
            while i < length:
                b.append(ord(str_data[i]))
                i += 1
            strcode = ''.join([chr(c) for c in b])
            return strcode
        elif _type == 'sgprint':
            return "\n<BR>{}</BR>".format(str_data)
        elif _type == "smposprint":
            return "%%1{}\n".format(str_data)
        elif _type == "80localprint":
            return "<div id='order_{0}_barcode_{1}' style='font-size:16pt;text-align:center;margin:2px auto;'>{2}</div>".format(
                type, index, str_data)

    def fill_box(self, content, box_len, content_len=0, float_dir='right'):
        """指定字符串和盒子长度，以及对齐方式和字符串长度

        left:文字左对齐填充空格
        right:文字右对齐填充空格
        """
        if not content_len:
            content_len = len(content)
        if content_len >= box_len:
            return content
        else:
            left_len = 0
            right_len = 0
            if float_dir == 'right':
                left_len = box_len - content_len
            elif float_dir == 'left':
                right_len = box_len - content_len
            else:
                left_len = (box_len - content_len) // 2
                right_len = box_len - content_len - left_len
            return ' ' * left_len + content + ' ' * right_len

    def fill_box_to_float_right(self, left_content, box_len, right_content):
        """指定左边字符串和盒子长度，以及右边字符串长度　使得右边字符串靠右
        """
        left_len = len(left_content) + \
                   CharFunc.get_contain_chinese_number(left_content)
        right_len = len(right_content) + \
                    CharFunc.get_contain_chinese_number(right_content)
        if left_len % box_len + right_len <= box_len:
            # 同行
            space_count = box_len - (left_len % box_len + right_len)
        else:
            # 只能两行
            space_count = box_len * 2 - (left_len % box_len + right_len)
        if self._type == "80localprint":
            return "<p><span style='display:inline-block;width:75%;'>{}</span><span style='display:inline-block;width:25%;text-align:right;vertical-align:top;'>{}</span></p>".format(
                left_content, right_content)
        return left_content + ' ' * space_count + right_content

    def cut(self):
        """切刀"""
        _type = self._type
        if _type == "ylyprint":
            return 32 * "-"
        elif _type == "feprint":
            return self.line_break('') * 3 + "<CUT>"
        elif _type == "sgprint":
            return "<CUT>"
        else:
            return ""

    def qrcode(self, str_data):
        """二维码"""
        _type = self._type
        if _type == "ylyprint":
            return "<q>{}</q>".format(str_data)
        elif _type == "feprint":
            return "<QR>{}</QR>".format(str_data)
        elif _type == "sgprint":
            return "<QR>{}</QR>".format(str_data)
        elif _type == "smposprint":
            return "%%2{}".format(str_data)
        else:
            return str_data

    def bold(self, str_data):
        """加粗"""
        _type = self._type
        if _type == "ylyprint":
            return str_data
        elif _type == "feprint":
            return "<BOLD>{}</BOLD>".format(str_data)
        elif _type == "sgprint":
            return "<FB>{}</FB>".format(str_data)
        elif _type == "80localprint":
            if not re.search('<p', str_data):
                return "<p style='font-weight:bold;'>{}</p>".format(str_data)
            _, num = re.search('style=', str_data).span()
            str_data = "<p style='font-weight:bold;" + str_data[num + 1:]
            return str_data
        else:
            return "@@1{}".format(str_data)

    def img(self, img_base64):
        """图片"""
        _type = self._type
        if _type == "feprint":
            return "<LOGO>"
        elif _type == "sgprint":
            return "<IMG>{}</IMG>".format(img_base64)
        elif _type == "80localprint":
            return "<img src='{}' style=''>".format(img_base64)
        else:
            return img_base64

    def signature(self, img_base64):
        """签名"""
        _type = self._type
        if _type == "feprint":
            return "<LOGO>"
        elif _type == "sgprint":
            return "<SIG>{}</SIG>".format(img_base64)
        elif _type == "80localprint":
            return "<img src='{}' style=''>".format(img_base64)
        else:
            return img_base64

    def color_invert(self, str_data, cancel=False):
        """黑白反显

        为了不影响下一个字符串，在反显前后各加了一个取消
        """
        if self._type == "smposprint":
            return "::{}".format(str_data)
        elif self._type == "80localprint":
            if not re.search('<p', str_data):
                return "<p style='background-color:#000;-webkit-print-color-adjust: exact;color:#fff;'>{}</p>".format(
                    str_data)
            _, num = re.search('style=', str_data).span()
            str_data = "<p style='background-color:#000;-webkit-print-color-adjust: exact;color:#fff;" + str_data[
                num + 1:]
            return str_data
        else:
            length = len(str_data)
            b = [29, 66, 2]
            b += [29, 66, 1]
            i = 0
            while i < length:
                b.append(ord(str_data[i]))
                i += 1
            b += [29, 66, 2]
            str_data = ''.join([chr(c) for c in b])
            return str_data

    def inline_block(self, str_data):
        if not re.search('<p', str_data):
            return "<p style='display:inline-block;min-width: 40%;box-sizing:border-box;'>{}</p>".format(
                str_data)
        _, num = re.search('style=', str_data).span()
        str_data = "<p style='display:inline-block;min-width: 40%;box-sizing:border-box;" + str_data[
            num + 1:]
        return str_data

    def _ylyprint(self, content_body, machine_code, mkey):
        """易连云无线打印

        content_body:打印内容
        machine_code:打印机终端号
        mkey:打印机密钥
        """
        partner = '1693'  # 用户ID
        apikey = '664466347d04d1089a3d373ac3b6d985af65d78e'  # API密钥
        timenow = str(int(time.time()))  # 当前时间戳
        if machine_code and mkey:
            sign = apikey + 'machine_code' + machine_code + \
                   'partner' + partner + 'time' + timenow + mkey  # 生成的签名加密
            sign = hashlib.md5(sign.encode("utf-8")).hexdigest().upper()
        else:
            return False, "易联云打印失败，请检查打印机终端号是否正确设置"
        data = {
            "partner": partner,
            "machine_code": machine_code,
            "content": content_body,
            "time": timenow,
            "sign": sign
        }
        r = requests.post("http://open.10ss.net:8888", data=data)
        try:
            text = int(eval(r.text)["state"])
        except BaseException:
            return False, "易联云打印接口返回异常，请稍后重试"
        if text == 1:
            return True, ""
        elif text in [3, 4]:
            return False, "易联云打印失败，请在店铺设置中检查打印机终端号是否正确设置"
        else:
            return False, "易联云打印失败，错误代码：%d" % text

    def _feprint(self, content_body, machine_code):
        """飞鹅云无线打印

        content_body:打印内容
        machine_code:打印机终端号
        """
        URL = "http://api.feieyun.cn/Api/Open/"  # 不需要修改
        USER = "senguo@senguo.cc"
        UKEY = "WAhPxdfWIgLd3FYW"
        STIME = str(int(time.time()))
        signature = hashlib.sha1(
            (USER + UKEY + STIME).encode("utf-8")).hexdigest()
        data = {
            'user': USER,
            'sig': signature,
            'stime': STIME,
            'apiname': 'Open_printMsg',  # 固定值,不需要修改
            'sn': machine_code,
            'content': content_body,
            'times': '1'  # 打印联数
        }
        response = requests.post(URL, data=data, timeout=30)
        code = response.status_code  # 响应状态码
        if code == 200:
            ret_dict = json.loads(response.text)
            log_date = datetime.datetime.now().strftime('%Y%m%d')
            if ret_dict["ret"] != 0:
                log_msg_dict('fe_ret_error/%s' % log_date, ret_dict, False)
                return False, "FE云打印出错，接口返回--" + ret_dict["msg"]
            return True, ""
        else:
            return False, "FE云打印服务器出错，请联系森果客服人员"

    def _sgprint(self, content_body, machine_code):
        """森果云无线打印

        content_body:打印内容
        machine_code:打印机终端号
        """
        import base64
        from libs.aliyun_IoT import IoTClient, PubRequest
        from libs.escpos.parser import Parser
        # 附加单据号防止重复打印
        receipt_number = "<N>" + str(int(time.time() * 1000)) + "</N>"
        content_body = receipt_number + content_body + "<CUT>"
        content = base64.b64encode(Parser.parse(content_body)).decode()
        request = PubRequest.PubRequest()
        request.set_accept_format('json')
        request.set_ProductKey('9t2pDhYz0IS')
        request.set_TopicFullName('/9t2pDhYz0IS/%s/data' % machine_code)
        request.set_MessageContent(content)
        request.set_Qos(1)
        r = IoTClient.do_action_with_exception(request)

        # 加日志观察一下 add by yy 2018-6-1
        try:
            time_now = datetime.datetime.now()
            log_dict = {}
            log_dict["time"] = time_now.strftime('%Y-%m-%d %H:%M:%s')
            log_dict["content"] = content_body
            log_date = time_now.strftime('%Y%m%d')
            log_msg_dict('sgprint/%s/%s' % (machine_code, log_date), log_dict)
        except BaseException:
            pass

        return True, ""

    def sgbluetooth(self, content_body):
        import base64
        from libs.escpos.parser import Parser
        content = base64.b64encode(
            Parser.parse(content_body + "<CUT>")).decode()
        return content

    @classmethod
    def _fequery(cls, machine_code):
        """飞鹅云无线打印查询

        machine_code:打印机终端号
        """
        URL = "http://api.feieyun.cn/Api/Open/"  # 不需要修改
        USER = "senguo@senguo.cc"
        UKEY = "WAhPxdfWIgLd3FYW"
        STIME = str(int(time.time()))
        signature = hashlib.sha1(
            (USER + UKEY + STIME).encode("utf-8")).hexdigest()
        data = {
            'user': USER,
            'sig': signature,
            'stime': STIME,
            'apiname': 'Open_queryPrinterStatus',  # 固定值,不需要修改
            'sn': machine_code
        }
        response = requests.post(URL, data=data, timeout=30)
        code = response.status_code  # 响应状态码
        if code == 200:
            ret_dict = json.loads(response.text)
            if ret_dict["ret"] != 0:
                err_txt = ret_dict["data"] if ret_dict["data"] else ret_dict[
                    "msg"]
                return False, "FE打印机状态有误,接口返回--" + err_txt
            return True, ""
        else:
            return False, "FE云打印服务器出错,请联系森果客服人员"

    @classmethod
    def _feadd(cls, machine_code, mkey):
        """飞鹅云无线打印机添加

        machine_code:打印机终端号
        """
        URL = "http://api.feieyun.cn/Api/Open/"  # 不需要修改
        USER = "senguo@senguo.cc"
        UKEY = "WAhPxdfWIgLd3FYW"
        STIME = str(int(time.time()))
        signature = hashlib.sha1(
            (USER + UKEY + STIME).encode("utf-8")).hexdigest()
        params = {
            'user': USER,
            'sig': signature,
            'stime': STIME,
            'apiname': 'Open_printerAddlist',  # 固定值,不需要修改
            'printerContent': "%s#%s# # " % (machine_code, mkey)
        }
        response = requests.post(URL, data=params, timeout=30)
        code = response.status_code  # 响应状态码
        if code == 200:
            ret_dict = json.loads(response.text)
            if ret_dict["ret"] != 0:
                err_txt = ret_dict["data"] if ret_dict["data"] else ret_dict[
                    "msg"]
                return False, "FE打印机状态有误,接口返回--" + err_txt
            else:
                bind_ok = ret_dict["data"]["ok"]
                if bind_ok:
                    return True, ""
                else:
                    bind_no = ret_dict["data"]["no"][0]
                    if bind_no.find("错误：已被添加过") == -1:
                        return False, bind_no
                    else:
                        return True, ""
        else:
            return False, "FE云打印服务器出错,请联系森果客服人员"

    def send_print_request(self,
                           content_body,
                           wireless_print_num,
                           wireless_print_key=""):
        """向各个打印平台发送打印请求
        """
        print_type = self._type
        if print_type == "ylyprint":
            if_print_success, error_txt = self._ylyprint(
                content_body, wireless_print_num, wireless_print_key)
        elif print_type == "feprint":
            if_print_success, error_txt = self._feprint(
                content_body, wireless_print_num)
        else:
            if_print_success, error_txt = self._sgprint(
                content_body, wireless_print_num)
        return if_print_success, error_txt

    def get_sale_record_print_body(self,
                                   sale_record_info,
                                   receipt_config,
                                   sales_num_total="",
                                   commission_mul_total="",
                                   shop_id=0,
                                   accounting_time=None,
                                   source="salesman",
                                   force_accountant=False,
                                   force_goods=False,
                                   is_hand_print=False,
                                   fee_text="",
                                   index=None):
        """单品小票打印

        开票处打印采用一次请求打印多个票据（至多三张，使用的是拼接好的字符串）
        收银台打印则是将多个票据以3的倍数为切分点分多次打印（一次请求至多三张，使用的是字符串数组）

        sale_record_info:小票内容
        receipt_config:小票设置
        sales_num_total:重量累计
        commission_mul_total:件数累计
        shop_id:店铺ID
        accounting_time:扎账时间
        source:打印来源
        force_accountant: 强制打印会计联, 用来解决单品无法打印问题
        force_goods: 强制打印货物联, 用来解决单品无法打印问题
        is_hand_print: 手工打印小票, 收银台查单
        """
        _paper_width = self._paper_width
        line_break = self.line_break
        cut = self.cut
        zoom_in = self.zoom_in
        default_size = self.default_size
        _type = self._type
        float_middle = self.float_middle
        float_right = self.float_right
        color_invert = self.color_invert

        if accounting_time:
            accounting_title = "已扎账，此单计入明日账"
        else:
            accounting_title = ""

        if source == "salesman":
            content_body = ""
            if receipt_config.salesman_customer_receipt:
                temp_name = "salesman_customer_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                salesman_customer_title = '[客户联]' + self._salesman_customer_space + '提货撕毁'
                content_body += float_middle(
                    zoom_in(accounting_title, default_size))
                content_body += line_break(
                    zoom_in(salesman_customer_title, default_size))
                content_body += self.format_receipt_temp(
                    sale_record_info,
                    receip_temp,
                    temp_name,
                    sales_num_total=sales_num_total,
                    commission_mul_total=commission_mul_total,
                    receipt_config=receipt_config,
                    fee_text=fee_text)
                if receipt_config.salesman_accountant_receipt or receipt_config.salesman_goods_receipt:
                    content_body += cut()

            if receipt_config.salesman_accountant_receipt:
                temp_name = "salesman_accountant_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                salesman_accountant_title = color_invert(
                    '[会计联]' + self._salesman_accountant_space)
                content_body += float_middle(
                    zoom_in(accounting_title, default_size))
                content_body += line_break(
                    zoom_in(salesman_accountant_title, default_size))
                content_body += self.format_receipt_temp(
                    sale_record_info,
                    receip_temp,
                    temp_name,
                    receipt_config=receipt_config,
                    fee_text=fee_text)
                if receipt_config.salesman_goods_receipt:
                    content_body += cut()

            if receipt_config.salesman_goods_receipt:
                temp_name = "salesman_goods_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                salesman_goods_title = '[货物联]'
                content_body += float_middle(
                    zoom_in(accounting_title, default_size))
                content_body += line_break(
                    zoom_in(salesman_goods_title, default_size))
                content_body += self.format_receipt_temp(
                    sale_record_info,
                    receip_temp,
                    temp_name,
                    receipt_config=receipt_config,
                    fee_text=fee_text)

            return content_body
        elif source == "salesman_async":
            content_body_list = []
            if receipt_config.salesman_async_customer_receipt:
                temp_name = "salesman_async_customer_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                salesman_customer_title = '[客户联]' + self._salesman_customer_space + '提货撕毁'
                content_body_customer = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_customer += line_break(
                    zoom_in(salesman_customer_title, default_size))
                content_body_customer += self.format_receipt_temp(
                    sale_record_info,
                    receip_temp,
                    temp_name,
                    sales_num_total=sales_num_total,
                    commission_mul_total=commission_mul_total,
                    receipt_config=receipt_config,
                    fee_text=fee_text)
                content_body_list.append(content_body_customer)

            if receipt_config.salesman_async_accountant_receipt:
                temp_name = "salesman_async_accountant_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                salesman_accountant_title = color_invert(
                    '[会计联]' + self._salesman_accountant_space)
                content_body_accountant = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_accountant += line_break(
                    zoom_in(salesman_accountant_title, default_size))
                content_body_accountant += self.format_receipt_temp(
                    sale_record_info,
                    receip_temp,
                    temp_name,
                    receipt_config=receipt_config,
                    fee_text=fee_text)
                content_body_list.append(content_body_accountant)

            if receipt_config.salesman_async_goods_receipt:
                temp_name = "salesman_async_goods_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                salesman_goods_title = '[货物联]'
                content_body_goods = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_goods += line_break(
                    zoom_in(salesman_goods_title, default_size))
                content_body_goods += self.format_receipt_temp(
                    sale_record_info,
                    receip_temp,
                    temp_name,
                    receipt_config=receipt_config,
                    fee_text=fee_text)
                content_body_list.append(content_body_goods)
            return content_body_list
        else:
            content_body_list = []
            if receipt_config.cashier_accountant_receipt and \
                    (receipt_config.merge_print_accountant == 0 and
                     (receipt_config.multi_sale_print_accountant == 1 or
                      force_accountant) or is_hand_print):
                temp_name = "cashier_accountant_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                cashier_accountant_title = color_invert(
                    '[会计联]' + self._cashier_accountant_space)
                content_body_accountant = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_accountant += line_break(
                    zoom_in(cashier_accountant_title, default_size))
                content_body_accountant += self.format_receipt_temp(
                    sale_record_info,
                    receip_temp,
                    temp_name,
                    receipt_config=receipt_config,
                    fee_text=fee_text,
                    index=index)
                content_body_list.append(content_body_accountant)

            if receipt_config.cashier_goods_receipt and \
                    (receipt_config.merge_print_goods == 0 and
                     (receipt_config.multi_sale_print_goods == 1 or
                      force_goods) or is_hand_print):
                temp_name = "cashier_goods_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                if shop_id in [116, 105]:
                    cashier_goods_title = float_middle(zoom_in('[客户提货联]', 2))
                    cashier_goods_title += float_right(
                        zoom_in('提货撕毁', default_size))
                else:
                    cashier_goods_title = '[客户提货联]' + self._cashier_goods_space + '提货撕毁'
                content_body_goods = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_goods += line_break(
                    zoom_in(cashier_goods_title, default_size))
                content_body_goods += self.format_receipt_temp(
                    sale_record_info,
                    receip_temp,
                    temp_name,
                    receipt_config=receipt_config,
                    fee_text=fee_text,
                    index=index)
                content_body_list.append(content_body_goods)

            return content_body_list

    def format_receipt_temp(self,
                            sale_record_info,
                            receip_temp,
                            temp_name,
                            sales_num_total="",
                            commission_mul_total="",
                            receipt_config=None,
                            fee_text="",
                            index=None):
        """格式化单品小票样式

        sale_record_info:小票内容
        receip_temp:小票模版
        temp_name:小票模版名称
        sales_num_total:重量累计
        commission_mul_total:件数累计
        receipt_config:小票设置
        """
        line_break = self.line_break
        barcode = self.barcode
        qrcode = self.qrcode
        bold = self.bold
        zoom_in = self.zoom_in
        _type = self._type
        bottom_line_break = self.bottom_line_break
        float_middle = self.float_middle
        default_size = self.default_size
        img = self.img

        shop_name = sale_record_info["shop_name"]
        sale_record_num = sale_record_info["num"]
        goods_name = sale_record_info["goods_name"]
        goods_unit = sale_record_info["goods_unit"]
        goods_unit_text = sale_record_info["goods_unit_text"]
        fact_price = sale_record_info["fact_price"]
        sales_num = sale_record_info["sales_num"]
        goods_sumup = sale_record_info["goods_sumup"]
        commission = sale_record_info["commission"]
        commission_mul = sale_record_info["commission_mul"]
        commission_sumup = sale_record_info["commission_sumup"]
        receipt_money = sale_record_info["receipt_money"]
        salesman_name = sale_record_info["salesman_name"]
        salesman_time = sale_record_info["full_bill_time"]
        accountant_name = sale_record_info["accountant_name"]
        accountant_time = sale_record_info["accountant_time"]
        pay_type_text = sale_record_info["pay_type_text"]
        combine_pay_type_text = sale_record_info["combine_pay_type_text"]
        deposit_avg = sale_record_info["deposit"]
        deposit_sum = sale_record_info["deposit_total"]
        shop_supplier_name = sale_record_info["shop_supplier_name"]
        gross_weight = sale_record_info["gross_weight"]
        tare_weight = sale_record_info["tare_weight"]
        shop_customer = sale_record_info["shop_customer"]
        record_type = sale_record_info["record_type"]
        remark = sale_record_info["remark"]
        signature_img = sale_record_info["signature_img"]
        receipt_text = receipt_config.receipt_text
        receipt_img = receipt_config.receipt_img
        shop_id = sale_record_info["shop_id"]
        commodity_code = sale_record_info["goods_commodity_code"]
        batch_num = sale_record_info["batch_num"]
        erase_money = sale_record_info.get("erase_money", 0)
        erase_text = sale_record_info.get("erase_text", "")

        if receipt_config.supplier_print and shop_supplier_name:
            goods_name = goods_name + "/%s" % shop_supplier_name

        # 重量累计
        if sales_num_total:
            sales_num_total += "="

        # 件数累计
        if commission_mul_total:
            commission_mul_total += "="

        # 基本参数
        stream_num_text = "流水号：%s" % sale_record_num
        goods_name_text = "货品：%s" % goods_name
        if batch_num:
            batch_num_text = "批次号：%s" % batch_num
        else:
            batch_num_text = ""

        # 退款单将金额标为负数
        if record_type == 1:
            goods_sumup = -goods_sumup
            commission_sumup = -commission_sumup
            deposit_sum = -deposit_sum
            receipt_money = -receipt_money
            commission_mul = -commission_mul

        if goods_unit == 0:
            if tare_weight:
                sales_num_text = "净重：{sales_num}{goods_unit_text} 皮重：{tare_weight}{goods_unit_text} 毛重：{gross_weight}{goods_unit_text}" \
                    .format(sales_num=sales_num,
                            tare_weight=tare_weight,
                            gross_weight=gross_weight,
                            goods_unit_text=goods_unit_text)
                sales_num_total_text = sales_num_text
            else:
                sales_num_text = "重量：%s%s" % (sales_num, goods_unit_text)
                sales_num_total_text = "重量：%s%s%s" % (
                    sales_num_total, sales_num, goods_unit_text)
        else:
            sales_num_text = ""
            sales_num_total_text = ""

        commission_mul_text = "件数：%s件" % (commission_mul)
        commission_mul_total_text = "件数：%s%s件" % (commission_mul_total,
                                                  commission_mul)

        price_text = "单价：%s元/%s" % (fact_price, goods_unit_text)
        goods_sum_text = "货品小计：%s元" % goods_sumup

        if commission:
            commission_text = "%s：%s元/件" % (fee_text, commission)
            commission_sum_text = "%s小计：%s元" % (fee_text, commission_sumup)
        else:
            commission_text = ""
            commission_sum_text = ""

        if deposit_avg:
            deposit_avg_text = "押金：%s元/件" % deposit_avg

        else:
            deposit_avg_text = ""

        if deposit_sum:
            deposit_sum_text = "押金小计：%s元" % deposit_sum
        else:
            deposit_sum_text = ""

        if erase_money:
            erase_money_text = "抹零金额：%s元" % erase_money
            actual_money_text = "实收金额：%s元" % (receipt_money - erase_money)
        else:
            erase_money_text = ""
            actual_money_text = ""

        receipt_money_text = "票据总金额：%s元" % receipt_money
        salesman_text = "开票信息：%s  %s" % (salesman_name, salesman_time)

        if accountant_time:
            accountant_text = "收银信息：%s  %s" % (accountant_name,
                                               accountant_time)
            pay_text = pay_type_text
        else:
            accountant_text = ""
            pay_text = ""

        if erase_text:
            erase_text = float_middle(zoom_in(erase_text, default_size))

        tec_service_text = float_middle(
            zoom_in(self.support_text, default_size))
        if temp_name == 'cashier_accountant_receipt_temp' and index:
            barcode = float_middle(
                barcode(sale_record_num, "accountant", index))
        elif temp_name == 'cashier_goods_receipt_temp' and index:
            barcode = float_middle(barcode(sale_record_num, "goods", index))
        else:
            barcode = float_middle(barcode(sale_record_num))
        record_num_qrcode = float_middle(qrcode(sale_record_num))
        code = self.get_dot_body(sale_record_num)

        # 客户信息
        shop_customer_text = "客户信息：%s" % shop_customer if shop_customer else ""

        # 货号
        goods_code_text = "货号：%s" % commodity_code

        # 嘉兴市场个性化
        jiaxing_customize_title = "果品质量安全可溯源管理供货凭证"

        content_body = ""

        # 退款单增加标识
        if record_type == 1:
            content_body += float_middle(zoom_in('退款单', 3))

        # 备注
        remark_text = ""
        if remark:
            remark_text = "备注：%s" % remark

        last_newline = 1
        last_print = 1
        for receipt_dict in receip_temp:
            name = receipt_dict.get("name", "")
            receipt_print = NumFunc.check_int(receipt_dict.get("print", 0))
            position = receipt_dict.get("position", "left")
            size = receipt_dict.get("size", 1)
            newline = NumFunc.check_int(receipt_dict.get("newline", 1))

            last_content_body = content_body
            if receipt_print:
                # 店铺名称
                if name == "shop_name":
                    content_body += self.format_str_position_size(
                        shop_name,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                    if shop_id in [105, 193, 224]:
                        content_body += line_break("")
                        content_body += float_middle(
                            zoom_in(jiaxing_customize_title, 1))
                # 条形码
                if name == "barcode":
                    if _type == "smposprint" and temp_name in [
                            "salesman_goods_receipt_temp",
                            "salesman_async_goods_receipt_temp"
                    ]:
                        content_body += record_num_qrcode
                    else:
                        content_body += barcode
                # 货品名
                if name == "goods_name":
                    content_body += self.format_str_position_size(
                        goods_name_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline,
                        action=name)
                    if shop_id in [105, 193, 224] and commodity_code:
                        content_body += self.format_str_position_size(
                            goods_code_text,
                            position,
                            size,
                            newline,
                            last_newline=last_newline,
                            action="")
                # 件数
                if name == "piece":
                    if temp_name in [
                            "salesman_customer_receipt_temp",
                            "salesman_async_customer_receipt_temp"
                    ]:
                        content_body += self.format_str_position_size(
                            commission_mul_total_text,
                            position,
                            size,
                            newline,
                            last_newline=last_newline,
                            action=name)
                    else:
                        content_body += self.format_str_position_size(
                            commission_mul_text,
                            position,
                            size,
                            newline,
                            last_newline=last_newline,
                            action=name)
                # 流水号
                if name == "sale_number":
                    content_body += self.format_str_position_size(
                        stream_num_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)

                # 批次号
                if name == "batch_num":
                    content_body += self.format_str_position_size(
                        batch_num_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)

                # 重量
                if name == "weight" and sales_num_text:
                    if temp_name in [
                            "salesman_customer_receipt_temp",
                            "salesman_async_customer_receipt_temp"
                    ]:
                        content_body += self.format_str_position_size(
                            sales_num_total_text,
                            position,
                            size,
                            newline,
                            last_newline=last_newline)
                    else:
                        content_body += self.format_str_position_size(
                            sales_num_text,
                            position,
                            size,
                            newline,
                            last_newline=last_newline)
                # 单价
                if name == "price":
                    content_body += self.format_str_position_size(
                        price_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                # 行费
                if name == "commission" and commission_text:
                    content_body += self.format_str_position_size(
                        commission_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                # 押金
                if name == "deposit" and deposit_avg_text:
                    content_body += self.format_str_position_size(
                        deposit_avg_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                # 货品小计
                if name == "total_goods_sale":
                    content_body += self.format_str_position_size(
                        goods_sum_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                # 行费小计
                if name == "total_commission" and commission_sum_text:
                    content_body += self.format_str_position_size(
                        commission_sum_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                # 押金小计
                if name == "total_deposit" and deposit_sum_text:
                    content_body += self.format_str_position_size(
                        deposit_sum_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                # 票据金额
                if name == "receipt_money":
                    if last_newline:
                        content_body += bottom_line_break()
                    content_body += self.format_str_position_size(
                        receipt_money_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                    # 抹零信息和票据金额放在一起
                    if erase_money_text:
                        content_body += self.format_str_position_size(
                            erase_money_text,
                            position,
                            size,
                            newline,
                            last_newline=last_newline)
                        content_body += self.format_str_position_size(
                            actual_money_text,
                            position,
                            size,
                            newline,
                            last_newline=last_newline)
                # 开票信息
                if name == "salesman_info":
                    content_body += self.format_str_position_size(
                        salesman_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                # 收银信息
                if name == "accountant_info":
                    content_body += self.format_str_position_size(
                        accountant_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                # 支付方式
                if name == "pay_info":
                    # 组合支付方式
                    if combine_pay_type_text:
                        content_body += self.format_str_position_size(
                            combine_pay_type_text,
                            position,
                            size,
                            newline,
                            last_newline=last_newline)
                    else:
                        content_body += self.format_str_position_size(
                            pay_text,
                            position,
                            size,
                            newline,
                            last_newline=last_newline)
                # 货品识别码
                if name == "code":
                    content_body += line_break('')
                    content_body += code
                    content_body += bottom_line_break()
                # 客户信息
                if name == "shop_customer" and shop_customer_text:
                    content_body += self.format_str_position_size(
                        shop_customer_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                # 客户签名
                # if name == "signature_img" and signature_img:
                #     content_body += line_break("")
                #     content_body += self.signature(signature_img)
                # 附加文字
                if name == "receipt_text" and receipt_text:
                    content_body += self.format_str_position_size(
                        receipt_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)
                # 附加图片
                if name == "receipt_img" and receipt_img:
                    content_body += line_break('')
                    content_body += self.img(receipt_img)
                # 备注
                if name == "remark" and remark_text:
                    content_body += self.format_str_position_size(
                        remark_text,
                        position,
                        size,
                        newline,
                        last_newline=last_newline)

            # 如果一列有两个条目,上一个条目有值而第二个条目没有值的情况下如果第二个条目需要换行则这里需要强制处理成换行
            if last_content_body == content_body and last_print and not last_newline:
                content_body = line_break(content_body)

            # 这里给last_print赋值是为了给下一次循环使用
            if last_content_body == content_body:
                last_print = 0
            else:
                last_print = 1

            # 如果本次没有打印出内容并且last_newline为换行则本次也记做换行
            if last_content_body == content_body and last_newline:
                last_newline = 1
            else:
                last_newline = newline

        # 抹零合计
        if erase_text:
            content_body += erase_text

        # TODO 签名目前没有模版设置，暂时先放在这里，如果加上了模版设置，这里要去掉
        if signature_img and _type in ["sgprint", "80localprint"]:
            content_body += line_break("")
            content_body += self.signature(signature_img)

        content_body += tec_service_text
        return content_body

    def format_str_position_size(self,
                                 text,
                                 position,
                                 size,
                                 newline,
                                 last_newline=1,
                                 action="",
                                 last_string=""):
        """格式化字符串大小及位置

        :param str text:字符串内容
        :param str position:对齐方式
        :param int size:字号
        :param int newline:是否换行
        :param int last_newline:上一行文字是否换行
        :param str action:条目名称
        :param str last_string:上一行文字
        """
        float_middle = self.float_middle
        float_right = self.float_right
        zoom_in = self.zoom_in
        line_break = self.line_break
        _type = self._type
        default_size = self.default_size
        fill_box = self.fill_box
        size = NumFunc.check_int(size)
        newline = NumFunc.check_int(newline)
        origin_text = text
        if action in ["goods_name", "piece"
                      ] and position == "center" and "：" in text:
            text = text.split("：")[1]
        if size == 2:
            text = zoom_in(text, 2)
        elif size == 3:
            text = zoom_in(text, 3)
        else:
            text = zoom_in(text, default_size)
        if newline:
            if last_newline:
                if position == "center":
                    text = float_middle(text)
                elif position == "right":
                    text = float_right(text)
                else:
                    text = line_break(text)
            else:
                if _type == '80localprint':
                    text = self.inline_block(text)
                else:
                    if last_string:
                        content_len = len(
                            origin_text) + CharFunc.get_contain_chinese_number(
                                origin_text)
                        last_content_len = len(
                            last_string) + CharFunc.get_contain_chinese_number(
                                last_string)
                        content_box_len = self._paper_width - last_content_len - 28
                        if content_box_len <= 4:
                            text = "  " + text
                        text = line_break(
                            fill_box(
                                text,
                                content_box_len,
                                content_len,
                                float_dir="right"))
                    else:
                        text = line_break(text)
        else:
            if _type in ["feprint", "sgprint"]:
                if size == 1:
                    content_len = len(origin_text)
                else:
                    # 遇到不换行的保留一个最小长度以维持格式对齐
                    content_len = (len(origin_text) + CharFunc.
                                   get_contain_chinese_number(origin_text)) * 2
                text = fill_box(text, 20, content_len, float_dir="left")
            elif _type == '80localprint':
                text = self.inline_block(text)
            else:
                text += "  "
        return text

    def get_dot_body(self, sale_record_num):
        """根据流水号获取识别码

        sale_record_num:流水号
        """
        float_middle = self.float_middle
        bold = self.bold
        _type = self._type
        zoom_in = self.zoom_in

        if _type in ["feprint", "sgprint"]:
            last_four_num = sale_record_num[-4:]
            chinese_str = "龘龘龘龘"
            first_chinese_array = chinese_array(
                int(last_four_num[0]), chinese_str[0])
            second_chinese_array = chinese_array(
                int(last_four_num[1]), chinese_str[1])
            third_chinese_array = chinese_array(
                int(last_four_num[2]), chinese_str[2])
            forth_chinese_array = chinese_array(
                int(last_four_num[3]), chinese_str[3])
            tmp_content_body = ""
            for i in range(len(first_chinese_array)):
                tmp_content1 = "" + \
                               first_chinese_array[i] + second_chinese_array[i] + third_chinese_array[i] + \
                               forth_chinese_array[i] + ""
                tmp_content_body += float_middle(bold(tmp_content1))
        elif _type == "80localprint":
            return float_middle(bold(zoom_in(sale_record_num[-4:], 3)))
        else:
            last_four_num = sale_record_num[-3:]
            chinese_str = "龘龘龘"
            first_chinese_array = chinese_array(
                int(last_four_num[0]), chinese_str[0])
            second_chinese_array = chinese_array(
                int(last_four_num[1]), chinese_str[1])
            third_chinese_array = chinese_array(
                int(last_four_num[2]), chinese_str[2])
            tmp_content_body = ""
            for i in range(len(first_chinese_array)):
                tmp_content1 = "" + \
                               first_chinese_array[i] + second_chinese_array[i] + third_chinese_array[i] + ""
                tmp_content_body += zoom_in(tmp_content1, 1) + "\n"

        return tmp_content_body

    def get_multi_sale_record_print_body(self,
                                         sale_record_info_list,
                                         temp_order_info,
                                         receipt_config,
                                         accounting_time=None,
                                         source="salesman",
                                         fee_text="",
                                         collect_multi_info=None,
                                         index=None):
        """一单多品小票打印

        receip_temp:小票模版
        sale_record_info_list:一单多品多个流水信息
        temp_order_info:一单多品订单信息
        receipt_config:小票设置
        accounting_time:扎账时间
        source:打印来源
        """
        line_break = self.line_break
        float_middle = self.float_middle
        zoom_in = self.zoom_in
        cut = self.cut
        _paper_width = self._paper_width
        default_size = self.default_size
        color_invert = self.color_invert

        shop_id = receipt_config.id

        if accounting_time:
            accounting_title = "已扎账，此单计入明日账"
        else:
            accounting_title = ""

        content_body = ""  # 用于一次发送单个请求
        content_body_list = []  # 用于发送多次请求（存放着每张小票的字符串）
        if source == "salesman":
            if receipt_config.salesman_customer_receipt:
                # 客户联
                temp_name = "multi_salesman_customer_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                content_main_body = self.format_multi_receipt_temp(
                    sale_record_info_list,
                    temp_order_info,
                    receip_temp,
                    source=source,
                    receipt_config=receipt_config,
                    fee_text=fee_text,
                    collect_multi_info=collect_multi_info,
                    temp_name=temp_name)
                content_body_data = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_data += line_break(
                    zoom_in('[客户联]' + self._salesman_customer_space + '提货撕毁',
                            default_size))
                content_body_data += content_main_body
                content_body += content_body_data
                content_body_list.append(content_body_data)
                if receipt_config.salesman_accountant_receipt or receipt_config.salesman_goods_receipt:
                    content_body += cut()

            if receipt_config.salesman_accountant_receipt:
                # 会计联
                temp_name = "multi_salesman_accountant_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                content_main_body = self.format_multi_receipt_temp(
                    sale_record_info_list,
                    temp_order_info,
                    receip_temp,
                    source=source,
                    receipt_config=receipt_config,
                    fee_text=fee_text,
                    temp_name=temp_name)
                content_body_data = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_data += line_break(
                    zoom_in(
                        color_invert('[会计联]' +
                                     self._salesman_accountant_space),
                        default_size))
                content_body_data += content_main_body
                content_body += content_body_data
                content_body_list.append(content_body_data)
                if receipt_config.salesman_goods_receipt:
                    content_body += cut()

            if receipt_config.salesman_goods_receipt:
                # 货物联
                temp_name = "multi_salesman_goods_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                content_main_body = self.format_multi_receipt_temp(
                    sale_record_info_list,
                    temp_order_info,
                    receip_temp,
                    source=source,
                    receipt_config=receipt_config,
                    fee_text=fee_text,
                    temp_name=temp_name)
                content_body_data = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_data += line_break(zoom_in('[货物联]', default_size))
                content_body_data += content_main_body
                content_body += content_body_data
                content_body_list.append(content_body_data)
        elif source == "salesman_async":
            if receipt_config.salesman_async_customer_receipt:
                # 客户联
                temp_name = "multi_salesman_async_customer_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                content_main_body = self.format_multi_receipt_temp(
                    sale_record_info_list,
                    temp_order_info,
                    receip_temp,
                    source=source,
                    receipt_config=receipt_config,
                    fee_text=fee_text,
                    collect_multi_info=collect_multi_info,
                    temp_name=temp_name)
                content_body_data = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_data += line_break(
                    zoom_in('[客户联]' + self._salesman_customer_space + '提货撕毁',
                            default_size))
                content_body_data += content_main_body
                content_body += content_body_data
                content_body_list.append(content_body_data)
                if receipt_config.salesman_accountant_receipt or receipt_config.salesman_goods_receipt:
                    content_body += cut()

            if receipt_config.salesman_async_accountant_receipt:
                # 会计联
                temp_name = "multi_salesman_async_accountant_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                content_main_body = self.format_multi_receipt_temp(
                    sale_record_info_list,
                    temp_order_info,
                    receip_temp,
                    source=source,
                    receipt_config=receipt_config,
                    fee_text=fee_text,
                    temp_name=temp_name)
                content_body_data = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_data += line_break(
                    zoom_in(
                        color_invert('[会计联]' +
                                     self._salesman_accountant_space),
                        default_size))
                content_body_data += content_main_body
                content_body += content_body_data
                content_body_list.append(content_body_data)
                if receipt_config.salesman_goods_receipt:
                    content_body += cut()

            if receipt_config.salesman_async_goods_receipt:
                # 货物联
                temp_name = "multi_salesman_async_goods_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                content_main_body = self.format_multi_receipt_temp(
                    sale_record_info_list,
                    temp_order_info,
                    receip_temp,
                    source=source,
                    receipt_config=receipt_config,
                    fee_text=fee_text,
                    temp_name=temp_name)
                content_body_data = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_data += line_break(zoom_in('[货物联]', default_size))
                content_body_data += content_main_body
                content_body += content_body_data
                content_body_list.append(content_body_data)
        else:
            if receipt_config.cashier_accountant_receipt and \
                    receipt_config.multi_sale_print_accountant == 0 and \
                    receipt_config.merge_print_accountant == 0:
                # 会计联
                temp_name = "multi_cashier_accountant_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                content_main_body = self.format_multi_receipt_temp(
                    sale_record_info_list,
                    temp_order_info,
                    receip_temp,
                    source=source,
                    receipt_config=receipt_config,
                    fee_text=fee_text,
                    temp_name=temp_name,
                    index=index)
                content_body_data = float_middle(
                    zoom_in(accounting_title, default_size))

                content_body_data += line_break(
                    zoom_in(
                        color_invert('[会计联]' + self._cashier_accountant_space),
                        default_size))
                content_body_data += content_main_body
                content_body += content_body_data
                content_body_list.append(content_body_data)
                if receipt_config.cashier_goods_receipt:
                    content_body += cut()

            if receipt_config.cashier_goods_receipt and \
                    receipt_config.multi_sale_print_goods == 0 and \
                    receipt_config.merge_print_goods == 0:
                # 提货联
                temp_name = "multi_cashier_goods_receipt_temp"
                receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                    temp_name, shop_id)
                content_main_body = self.format_multi_receipt_temp(
                    sale_record_info_list,
                    temp_order_info,
                    receip_temp,
                    source=source,
                    receipt_config=receipt_config,
                    fee_text=fee_text,
                    temp_name=temp_name,
                    index=index)
                content_body_data = float_middle(
                    zoom_in(accounting_title, default_size))
                content_body_data += line_break(
                    zoom_in('[客户提货联]' + self._cashier_goods_space + '提货撕毁',
                            default_size))
                content_body_data += content_main_body
                content_body += content_body_data
                content_body_list.append(content_body_data)

        return content_body, content_body_list

    def get_merge_sale_record_print_body(self,
                                         sale_record_info_list,
                                         temp_order_info,
                                         receipt_config,
                                         accounting_time=None,
                                         source="accountant",
                                         fee_text="",
                                         index=None):
        """ 收银台小票合并成一张打印

        receip_temp:小票模版
        sale_record_info_list:一单多品多个流水信息
        temp_order_info:一单多品订单信息
        receipt_config:小票设置
        accounting_time:扎账时间
        """
        line_break = self.line_break
        float_middle = self.float_middle
        zoom_in = self.zoom_in
        cut = self.cut
        _paper_width = self._paper_width
        default_size = self.default_size
        color_invert = self.color_invert

        shop_id = receipt_config.id

        if accounting_time:
            accounting_title = "已扎账，此单计入明日账"
        else:
            accounting_title = ""

        content_body = ""  # 用于一次发送单个请求
        content_body_list = []  # 用于发送多次请求（存放着每张小票的字符串）
        if receipt_config.cashier_accountant_receipt and \
                receipt_config.merge_print_accountant == 1:
            # 会计联
            temp_name = "multi_cashier_accountant_receipt_temp"
            receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                temp_name, shop_id)
            content_main_body = self.format_multi_receipt_temp(
                sale_record_info_list,
                temp_order_info,
                receip_temp,
                source=source,
                receipt_config=receipt_config,
                fee_text=fee_text,
                temp_name=temp_name,
                index=index)
            content_body_data = float_middle(
                zoom_in(accounting_title, default_size))
            content_body_data += line_break(
                zoom_in(
                    color_invert('[会计联]' + self._cashier_accountant_space),
                    default_size))
            content_body_data += content_main_body
            content_body += content_body_data
            content_body_list.append(content_body_data)
            if receipt_config.cashier_goods_receipt:
                content_body += cut()

        if receipt_config.cashier_goods_receipt and \
                receipt_config.merge_print_goods == 1:
            # 提货联
            temp_name = "multi_cashier_goods_receipt_temp"
            receip_temp = ReceiptTemplateFunc.get_receipt_temp_detail(
                temp_name, shop_id)
            content_main_body = self.format_multi_receipt_temp(
                sale_record_info_list,
                temp_order_info,
                receip_temp,
                source=source,
                receipt_config=receipt_config,
                fee_text=fee_text,
                temp_name=temp_name,
                index=index)
            content_body_data = float_middle(
                zoom_in(accounting_title, default_size))
            content_body_data += line_break(
                zoom_in('[客户提货联]' + self._cashier_goods_space + '提货撕毁',
                        default_size))
            content_body_data += content_main_body
            content_body += content_body_data
            content_body_list.append(content_body_data)

        return content_body, content_body_list

    def format_multi_receipt_temp(self,
                                  sale_record_info_list,
                                  temp_order_info,
                                  receip_temp,
                                  source="salesman",
                                  receipt_config=None,
                                  fee_text="",
                                  collect_multi_info=None,
                                  temp_name="",
                                  index=None):
        """格式化多品小票格式"""
        line_break = self.line_break
        float_middle = self.float_middle
        float_right = self.float_right
        zoom_in = self.zoom_in
        barcode = self.barcode
        qrcode = self.qrcode
        bold = self.bold
        _paper_width = self._paper_width
        _type = self._type
        bottom_line_break = self.bottom_line_break
        default_size = self.default_size

        check_int = NumFunc.check_int
        shop_name = temp_order_info["shop_name"]
        salesman_name = temp_order_info.get("salesman_name", "")
        multi_order_num = temp_order_info["num"]
        time = temp_order_info["time"]
        accountant_time = temp_order_info.get("accountant_time", "")
        total_recipt_money = temp_order_info["total_recipt_money"]
        total_goods_type_count = temp_order_info["total_goods_type_count"]
        total_commission_mul = temp_order_info["total_commission_mul"]
        shop_id = temp_order_info["shop_id"]
        accountant_name = temp_order_info.get("accountant_name", "")
        pay_type_text = temp_order_info.get("pay_type_text", "")
        combine_pay_type_text = temp_order_info.get("combine_pay_type_text",
                                                    "")
        customer_name = temp_order_info["customer_name"]
        customer_company = temp_order_info["customer_company"]
        signature_img = temp_order_info.get("signature_img", "")
        remark_list = temp_order_info["remark"].split("^")
        record_type = temp_order_info["record_type"]
        erase_text = temp_order_info.get("erase_text", "")

        if temp_name == 'multi_cashier_accountant_receipt_temp' and index:
            temp_barcode = float_middle(
                barcode(multi_order_num, "accountant", index))
        elif temp_name == 'multi_cashier_goods_receipt_temp' and index:
            temp_barcode = float_middle(
                barcode(multi_order_num, "goods", index))
        else:
            temp_barcode = float_middle(barcode(multi_order_num))
        sale_record_len = len(sale_record_info_list)
        receipt_text = receipt_config.receipt_text

        # 退款单将总计标为负数
        if record_type == 1:
            total_recipt_money = -total_recipt_money
            total_commission_mul = -total_commission_mul

        goods_type_text = "种类:%s" % total_goods_type_count
        total_money_text = "总价:%s元" % total_recipt_money
        total_commission_mul_text = "总件数:%s件" % total_commission_mul

        # 客户信息
        customer_text = ""
        if customer_name or customer_company:
            customer_text = "客户:%s %s" % (customer_name, customer_company)

        # 备注
        remark_text_list = []
        for remark in remark_list:
            if remark:
                remark_text_list.append("备注：%s" % remark)

        # 支付信息
        accountant_text = ""
        pay_text = ""
        if source in ["accountant", "salesman_async"]:
            if combine_pay_type_text:
                pay_text = combine_pay_type_text
            else:
                pay_text = pay_type_text

            accountant_text = "收银信息：%s  %s" % (accountant_name,
                                               accountant_time)
            pay_text = "支付方式：%s" % pay_text

        # 一单多品订单号
        multi_order_num = "订单号：" + multi_order_num

        # 取货码
        pick_num = multi_order_num[-4:]

        # 开票员信息
        salesman_text = "开票信息：%s  %s" % (salesman_name, time)

        # 技术支持
        tec_service_text = float_middle(
            zoom_in(self.support_text, default_size))

        # 嘉兴市场个性化
        jiaxing_customize_title = "果品质量安全可溯源管理供货凭证"

        # 取出货品模版和备注模板
        receipt_goods_list = {}
        receipt_remark = {}
        for receipt_dict in receip_temp:
            name = receipt_dict.get("name", "")
            if name == "goods_list":
                receipt_goods_list = receipt_dict["style"]
            if name == "remark":
                receipt_remark = receipt_dict
            else:
                continue

        # 组合备注信息
        remark_content_body = ""
        for remark_text in remark_text_list:
            position = receipt_remark.get("position", "left")
            size = receipt_remark.get("size", 1)
            newline = NumFunc.check_int(receipt_remark.get("newline", 1))
            remark_content_body += self.format_str_position_size(
                remark_text, position, size, newline, last_newline=1)

        # 组合商品信息字符串
        goods_content_body = ""
        for sale_record_info in sale_record_info_list:
            sale_record_num = sale_record_info["num"]
            goods_name = sale_record_info["goods_name"]
            goods_unit = sale_record_info["goods_unit"]
            goods_unit_text = sale_record_info["goods_unit_text"]
            fact_price = sale_record_info["fact_price"]
            sales_num = sale_record_info["sales_num"]
            gross_weight = sale_record_info["gross_weight"]
            tare_weight = sale_record_info["tare_weight"]
            goods_sumup = sale_record_info["goods_sumup"]
            commission = sale_record_info["commission"]
            commission_mul = sale_record_info["commission_mul"]
            commission_sumup = sale_record_info["commission_sumup"]
            receipt_money = sale_record_info["receipt_money"]
            deposit_avg = sale_record_info["deposit"]
            deposit_sum = sale_record_info["deposit_total"]
            shop_supplier_name = sale_record_info["shop_supplier_name"]
            commodity_code = sale_record_info["goods_commodity_code"]
            record_type = sale_record_info["record_type"]
            batch_num = sale_record_info["batch_num"]

            sales_num_total = ""
            commission_mul_total = ""
            if collect_multi_info and collect_multi_info.get(sale_record_num):
                multi_info = collect_multi_info[sale_record_num]
                sales_num_total = multi_info.get("sales_num_total", "")
                commission_mul_total = multi_info.get("commission_mul_total",
                                                      "")

            # 重量累计
            if sales_num_total:
                sales_num_total += "="

            # 件数累计
            if commission_mul_total:
                commission_mul_total += "="

            # 退款单将金额标为负数
            if record_type == 1:
                goods_sumup = -goods_sumup
                commission_sumup = -commission_sumup
                deposit_sum = -deposit_sum
                receipt_money = -receipt_money
                commission_mul = -commission_mul
                sales_num = -sales_num

            if receipt_config.supplier_print and shop_supplier_name:
                goods_name = goods_name + "/%s" % shop_supplier_name

            receipt_money_text = "%s元" % receipt_money
            price_text = "单价：%s元/%s" % (fact_price, goods_unit_text)
            goods_sum_text = "货品小计：%s元" % goods_sumup
            sales_num_text = "重量：%s%s" % (sales_num, goods_unit_text)
            commission_mul_text = "%s件" % (commission_mul)

            # 重量显示处理
            if goods_unit == 1:
                sales_num_text = ""
            else:
                if tare_weight:
                    sales_num_text = "净重：{sales_num}{goods_unit_text} 皮重：{tare_weight}{goods_unit_text} 毛重：{gross_weight}{goods_unit_text}" \
                        .format(sales_num=sales_num,
                                tare_weight=tare_weight,
                                gross_weight=gross_weight,
                                goods_unit_text=goods_unit_text)

            # 件数显示处理
            if temp_name in [
                    "multi_salesman_customer_receipt_temp",
                    "multi_salesman_async_customer_receipt_temp"
            ]:
                if commission_mul_total:
                    commission_mul_text = "%s%s件" % (commission_mul_total,
                                                     commission_mul)
                if sales_num_total:
                    sales_num_text = "重量：%s%s%s" % (sales_num_total, sales_num,
                                                    goods_unit_text)

            if batch_num:
                batch_num_text = "批次号：%s" % batch_num
            else:
                batch_num_text = ""

            deposit_text = ""
            deposit_sum_text = ""
            if deposit_avg:
                deposit_text = "押金：%s元/件" % deposit_avg
            if deposit_sum:
                deposit_sum_text = "押金小计：%s元" % deposit_sum

            commission_text = ""
            commission_sum_text = ""
            if commission:
                commission_text = "%s：%s元/件" % (fee_text, commission)
                commission_sum_text = "%s小计：%s元" % (fee_text, commission_sumup)

            # 货号
            goods_code_text = "货号：%s" % commodity_code

            goods_last_newline = 1
            goods_last_print = 1
            goods_last_string = ""
            # 货品名/件数/流水号/重量/单价/行费/押金/货品小计/行费小计/押金小计/票据金额
            receipt_goods_key_dict = {
                "goods_name": goods_name,
                "piece": commission_mul_text,
                "sale_number": sale_record_num,
                "batch_num": batch_num_text,
                "weight": sales_num_text,
                "price": price_text,
                "commission": commission_text,
                "deposit": deposit_text,
                "total_goods_sale": goods_sum_text,
                "total_commission": commission_sum_text,
                "total_deposit": deposit_sum_text,
                "receipt_money": receipt_money_text
            }
            for receipt_goods_dict in receipt_goods_list:
                name = receipt_goods_dict.get("name", "")
                receipt_print = check_int(receipt_goods_dict.get("print", 0))
                position = receipt_goods_dict.get("position", "left")
                size = receipt_goods_dict.get("size", 1)
                newline = check_int(receipt_goods_dict.get("newline", 1))

                last_goods_content_body = goods_content_body
                if receipt_print:
                    goods_print_text = receipt_goods_key_dict.get(name)
                    if goods_print_text:
                        goods_content_body += self.format_str_position_size(
                            goods_print_text,
                            position,
                            size,
                            newline,
                            last_newline=goods_last_newline,
                            last_string=goods_last_string)
                        if name == "goods_name" and shop_id in [
                                105, 193, 224
                        ] and commodity_code:
                            goods_content_body += self.format_str_position_size(
                                goods_code_text,
                                position,
                                size,
                                newline,
                                last_newline=goods_last_newline,
                                action="",
                                last_string=goods_last_string)

                        goods_last_string = goods_print_text

                # 如果一列有两个条目,上一个条目有值而第二个条目没有值的情况下如果第二个条目需要换行则这里需要强制处理成换行
                if last_goods_content_body == goods_content_body and goods_last_print and not goods_last_newline:
                    goods_content_body = line_break(goods_content_body)

                # 这里给last_print赋值是为了给下一次循环使用
                if last_goods_content_body == goods_content_body:
                    goods_last_print = 0
                else:
                    goods_last_print = 1

                # 如果本次没有打印出内容并且last_newline为换行则本次也记做换行
                if last_goods_content_body == goods_content_body and goods_last_newline:
                    goods_last_newline = 1
                else:
                    goods_last_newline = newline

        # 组合小票信息
        last_newline = 1
        last_print = 1
        content_body = ""

        # 退款单增加标识
        if record_type == 1:
            content_body += float_middle(zoom_in('退款单', 3))

        receipt_main_key_dict = {
            "shop_name": shop_name,
            "barcode": temp_barcode,
            "code": pick_num,
            "order_num": multi_order_num,
            "category": goods_type_text,
            "total_money": total_money_text,
            "salesman_info": salesman_text,
            "accountant_info": accountant_text,
            "pay_info": pay_text,
            "erase_text": erase_text,
            "shop_customer": customer_text,
            "receipt_text": receipt_text,
            # "remark": remark_text,
            "total_commission_mul": total_commission_mul_text
        }
        for receipt_dict in receip_temp:
            last_content_body = content_body

            name = receipt_dict.get("name", "")
            if name == "goods_list":
                content_body += bottom_line_break()
                content_body += goods_content_body
                content_body += bottom_line_break()
                last_newline = 1
                last_print = 1
            elif name == "remark":
                content_body += remark_content_body
                if remark_content_body:
                    last_newline = 1
                    last_print = 1
            else:
                receipt_print = NumFunc.check_int(receipt_dict.get("print", 0))
                position = receipt_dict.get("position", "left")
                size = receipt_dict.get("size", 1)
                newline = NumFunc.check_int(receipt_dict.get("newline", 1))

                if receipt_print:
                    main_print_text = receipt_main_key_dict.get(name)
                    if main_print_text:
                        content_body += self.format_str_position_size(
                            main_print_text,
                            position,
                            size,
                            newline,
                            last_newline=last_newline)
                        if name == "shop_name" and shop_id in [105, 193, 224]:
                            content_body += line_break("")
                            content_body += float_middle(
                                zoom_in(jiaxing_customize_title, 1))

            # 如果一列有两个条目,上一个条目有值而第二个条目没有值的情况下如果第二个条目需要换行则这里需要强制处理成换行
            if last_content_body == content_body and last_print and not last_newline:
                content_body = line_break(content_body)

            # 这里给last_print赋值是为了给下一次循环使用
            if last_content_body == content_body:
                last_print = 0
            else:
                last_print = 1

            # 如果本次没有打印出内容并且last_newline为换行则本次也记做换行
            if last_content_body == content_body and last_newline:
                last_newline = 1
            else:
                last_newline = newline

        # TODO 签名目前没有模版设置，暂时先放在这里，如果加上了模版设置，这里要去掉
        if signature_img and _type in ["sgprint", "80localprint"]:
            content_body += line_break("")
            content_body += self.signature(signature_img)

        content_body += tec_service_text
        return content_body

    def get_payback_print_body(self, pb_order_info):
        """还款凭证小票"""
        line_break = self.line_break
        float_middle = self.float_middle
        zoom_in = self.zoom_in

        shop_name = pb_order_info["shop_name"]
        money = pb_order_info["money"]
        erase_money = pb_order_info["erase_money"]
        fact_total_price = pb_order_info["fact_total_price"]
        pay_type = pb_order_info["pay_type_text"]
        time = pb_order_info["time"]
        debtor_name = pb_order_info["debtor_name"]
        debtor_phone = pb_order_info["debtor_phone"]
        debtor_number = pb_order_info["debtor_number"]
        debtor_rest = pb_order_info["debtor_rest"]
        accountant_name = pb_order_info["accountant_name"]

        title = "还款凭证"
        shop_name_text = "店铺：%s" % (shop_name)
        money_text = "还款金额：%s元" % (money)
        erase_money_text = "抹零金额：%s元" % (erase_money)
        fact_total_price_text = "实收金额：%s元" % (fact_total_price)
        pay_type_text = "付款方式：%s" % (pay_type)
        time_text = "还款时间：%s" % (time)
        customer_name_text = "客户姓名：%s" % (debtor_name)
        phone_text = "手机号：%s" % (debtor_phone)
        number_text = "客户编号：%s" % (debtor_number)
        rest_money_text = "还款后还欠：%s元" % (debtor_rest)
        accountant_name_text = "收银员：%s" % (accountant_name)

        customer_info_text = ""
        if debtor_phone:
            customer_info_text = phone_text + " "
        if debtor_number:
            customer_info_text += number_text

        content_body = float_middle(zoom_in(title, 2))
        content_body += line_break("")
        content_body += line_break(zoom_in(shop_name_text, 2))
        content_body += line_break(zoom_in(money_text, 2))
        if erase_money:
            content_body += line_break(zoom_in(erase_money_text, 2))
        content_body += line_break(zoom_in(fact_total_price_text, 2))
        content_body += line_break(zoom_in(pay_type_text, 2))
        content_body += line_break(zoom_in(time_text, 2))
        content_body += line_break(zoom_in(customer_name_text, 2))
        content_body += line_break(zoom_in(customer_info_text, 2))
        content_body += line_break(zoom_in(rest_money_text, 2))
        content_body += zoom_in(accountant_name_text, 2)
        content_body += line_break("")

        content_body += float_middle(self.support_text)

        return content_body

    def get_work_record_print_body(self,
                                   config,
                                   record_data,
                                   shop_name="",
                                   accountant_name="",
                                   for_qingdao_qianyuan=False,
                                   total_cent=0,
                                   count=0):
        """交接班记录小票

        record_data:交接班记录
        shop_name:店铺名称
        accountant_name:收银员名称
        """
        _paper_width = self._paper_width
        line_break = self.line_break
        float_middle = self.float_middle
        zoom_in = self.zoom_in

        title = "交班小票"
        shop_name_text = "店铺：%s" % (shop_name)
        accountant_name = "收银员：%s" % (accountant_name)
        onduty_time = "上班时间：%s" % (record_data["onduty_time"])
        print_time = "打印时间：%s" % (TimeFunc.time_to_str(
            datetime.datetime.now()))
        work_time = "收银时长：%s" % (record_data["work_time"])
        order_total_money = "订单收款：%s元" % (record_data["order_total_money"])
        order_cash_money = "现金：%s元  %s笔" % (record_data["order_cash_money"],
                                            record_data["order_cash_count"])
        order_bank_money = "银行卡：%s元  %s笔" % (record_data["order_bank_money"],
                                             record_data["order_bank_count"])
        order_cheque_money = "支票：%s元  %s笔" % (
            record_data["order_cheque_money"],
            record_data["order_cheque_count"])
        order_wx_money = "微信：%s元  %s笔" % (record_data["order_wx_money"],
                                          record_data["order_wx_count"])
        order_ali_money = "支付宝：%s元  %s笔" % (record_data["order_ali_money"],
                                            record_data["order_ali_count"])

        pb_total_money = "还款：%s元" % (record_data["pb_total_money"])
        pb_cash_money = "现金：%s元  %s笔" % (record_data["pb_cash_money"],
                                         record_data["pb_cash_count"])
        pb_bank_money = "银行卡：%s元  %s笔" % (record_data["pb_bank_money"],
                                          record_data["pb_bank_count"])
        pb_cheque_money = "支票：%s元  %s笔" % (record_data["pb_cheque_money"],
                                           record_data["pb_cheque_count"])
        pb_wx_money = "微信：%s元  %s笔" % (record_data["pb_wx_money"],
                                       record_data["pb_wx_count"])
        pb_ali_money = "支付宝：%s元  %s笔" % (record_data["pb_ali_money"],
                                         record_data["pb_ali_count"])

        refund_total_money = "退单：%s元" % (record_data["refund_total_money"])
        refund_cash_money = "现金：%s元  %s笔" % (record_data["refund_cash_money"],
                                             record_data["refund_cash_count"])
        refund_bank_money = "银行卡：%s元  %s笔" % (record_data['refund_bank_money'],
                                              record_data['refund_bank_count'])
        refund_wx_money = "微信：%s元  %s笔" % (record_data["refund_wx_money"],
                                           record_data["refund_wx_count"])
        refund_ali_money = "支付宝：%s元  %s笔" % (record_data["refund_ali_money"],
                                             record_data["refund_ali_count"])
        refund_cheque_money = "支票：%s元  %s笔" % (
            record_data["refund_cheque_money"],
            record_data["refund_cheque_count"])

        erase_total_money = "抹零：%s元" % (record_data["erase_money"])
        erase_cash_money = "现金：%s元  %s笔" % (record_data["erase_cash_money"],
                                            record_data["erase_cash_count"])
        erase_bank_money = "银行卡：%s元  %s笔" % (record_data["erase_bank_money"],
                                             record_data["erase_bank_count"])
        erase_cheque_money = "支票：%s元  %s笔" % (
            record_data["erase_cheque_money"],
            record_data["erase_cheque_count"])
        erase_wx_money = "微信：%s元  %s笔" % (record_data["erase_wx_money"],
                                          record_data["erase_wx_count"])
        erase_ali_money = "支付宝：%s元  %s笔" % (record_data["erase_ali_money"],
                                            record_data["erase_ali_count"])
        erase_combine_money = "组合支付：%s元  %s笔" % (
            record_data["erase_combine_money"],
            record_data["erase_combine_count"])

        payout_total_money = "收银台支出：%s元" % (record_data["payout_total_money"])
        income_total_money = "额外收入：%s元" % (record_data["income_total_money"])

        order_fee_money = "手续费：%s元 %s笔" % (record_data["order_fee_money"],
                                           record_data["order_fee_count"])
        order_fee_cash_money = "现金：%s元  %s笔" % (
            record_data["order_fee_cash_money"],
            record_data["order_fee_cash_count"])
        order_fee_bank_money = "银行卡：%s元  %s笔" % (
            record_data["order_fee_bank_money"],
            record_data["order_fee_bank_count"])
        order_fee_cheque_money = "支票：%s元  %s笔" % (
            record_data["order_fee_cheque_money"],
            record_data["order_fee_cheque_count"])
        order_fee_wx_money = "微信：%s元  %s笔" % (
            record_data["order_fee_wx_money"],
            record_data["order_fee_wx_count"])
        order_fee_ali_money = "支付宝：%s元  %s笔" % (
            record_data["order_fee_ali_money"],
            record_data["order_fee_ali_count"])

        total_money = "实收资金：%s元" % (record_data["total_money"])
        total_cash_money = "现金：%s元  %s笔" % (record_data["total_cash_money"],
                                            record_data["total_cash_count"])
        total_bank_money = "银行卡：%s元  %s笔" % (record_data["total_bank_money"],
                                             record_data["total_bank_count"])
        total_cheque_money = "支票：%s元  %s笔" % (
            record_data["total_cheque_money"],
            record_data["total_cheque_count"])
        total_wx_money = "微信：%s元  %s笔" % (record_data["total_wx_money"],
                                          record_data["total_wx_count"])
        total_ali_money = "支付宝：%s元  %s笔" % (record_data["total_ali_money"],
                                            record_data["total_ali_count"])
        total_deposit_money = "押金收支：%s元" % (record_data["deposit_spend"])

        order_credit_money = "赊账：%s元  %s笔" % (
            record_data["order_credit_money"],
            record_data["order_credit_count"])
        refund_credit_money = "赊账退还：%s元  %s笔" % (
            record_data["refund_credit_money"],
            record_data["refund_credit_count"])

        content_body = ""
        content_body = float_middle(zoom_in(title, 2))
        content_body += line_break("")
        content_body += line_break(shop_name_text)
        content_body += line_break(accountant_name)
        content_body += line_break(onduty_time)
        content_body += line_break(work_time)
        content_body += line_break(print_time)
        content_body += line_break("")

        content_body += line_break(zoom_in(order_total_money, 2))
        if record_data["order_cash_money"]:
            content_body += line_break(order_cash_money)
        if record_data["order_bank_money"]:
            content_body += line_break(order_bank_money)
        if record_data["order_cheque_money"]:
            content_body += line_break(order_cheque_money)
        if record_data["order_wx_money"]:
            content_body += line_break(order_wx_money)
        if record_data["order_ali_money"]:
            content_body += line_break(order_ali_money)
        content_body += line_break("")

        content_body += line_break(zoom_in(pb_total_money, 2))
        if record_data["pb_cash_money"]:
            content_body += line_break(pb_cash_money)
        if record_data["pb_bank_money"]:
            content_body += line_break(pb_bank_money)
        if record_data["pb_cheque_money"]:
            content_body += line_break(pb_cheque_money)
        if record_data["pb_wx_money"]:
            content_body += line_break(pb_wx_money)
        if record_data["pb_ali_money"]:
            content_body += line_break(pb_ali_money)
        content_body += line_break("")

        content_body += line_break(zoom_in(refund_total_money, 2))
        if record_data["refund_cash_money"]:
            content_body += line_break(refund_cash_money)
        if record_data["refund_bank_money"]:
            content_body += line_break(refund_bank_money)
        if record_data["refund_cheque_money"]:
            content_body += line_break(refund_cheque_money)
        if record_data["refund_wx_money"]:
            content_body += line_break(refund_wx_money)
        if record_data["refund_ali_money"]:
            content_body += line_break(refund_ali_money)
        content_body += line_break("")

        content_body += line_break(zoom_in(erase_total_money, 2))
        if record_data["erase_cash_money"]:
            content_body += line_break(erase_cash_money)
        if record_data["erase_bank_money"]:
            content_body += line_break(erase_bank_money)
        if record_data["erase_cheque_money"]:
            content_body += line_break(erase_cheque_money)
        if record_data["erase_wx_money"]:
            content_body += line_break(erase_wx_money)
        if record_data["erase_ali_money"]:
            content_body += line_break(erase_ali_money)
        if record_data["erase_combine_money"]:
            content_body += line_break(erase_combine_money)
        content_body += line_break("")

        content_body += line_break(zoom_in(payout_total_money, 2))
        content_body += line_break(zoom_in(income_total_money, 2))
        # =============================================
        # TODO 收银员供货商借支总额 for 青岛谦源，临时使用，正式加上供货商借支显示之后删除此段代码
        if for_qingdao_qianyuan:
            content_body += line_break(
                zoom_in("供货商预支：{}元 {}笔".format(total_cent, count), 2))
        # =============================================
        content_body += line_break("")

        content_body += line_break(zoom_in(order_fee_money, 2))
        if record_data["order_fee_cash_money"]:
            content_body += line_break(order_fee_cash_money)
        if record_data["order_fee_bank_money"]:
            content_body += line_break(order_fee_bank_money)
        if record_data["order_fee_cheque_money"]:
            content_body += line_break(order_fee_cheque_money)
        if record_data["order_fee_wx_money"]:
            content_body += line_break(order_fee_wx_money)
        if record_data["order_fee_ali_money"]:
            content_body += line_break(order_fee_ali_money)

        content_body += line_break(zoom_in(total_money, 2))
        if record_data["total_cash_money"]:
            content_body += line_break(total_cash_money)
        if record_data["total_bank_money"]:
            content_body += line_break(total_bank_money)
        if record_data["total_cheque_money"]:
            content_body += line_break(total_cheque_money)
        if record_data["total_wx_money"]:
            content_body += line_break(total_wx_money)
        if record_data["total_ali_money"]:
            content_body += line_break(total_ali_money)
        content_body += line_break("")

        # 如果店铺押金设置开关没有打开，则小票打印中不显示押金项
        if config.enable_deposit:
            content_body += line_break(zoom_in(total_deposit_money, 2))
            content_body += line_break("")

        content_body += line_break(zoom_in(order_credit_money, 2))
        content_body += line_break(zoom_in(refund_credit_money, 2))
        content_body += line_break("")

        content_body += float_middle(self.support_text)

        return content_body

    def get_borrowing_print_body(self, record_data):
        """预支单小票"""
        line_break = self.line_break
        float_middle = self.float_middle
        float_right = self.float_right
        zoom_in = self.zoom_in
        fill_box = self.fill_box
        color_invert = self.color_invert
        cut = self.cut

        shop_name = record_data["shop_name"]
        supplier_name = record_data["supplier_name"]
        supplier_phone = record_data["supplier_phone"]
        borrow_money = record_data["borrow_money"]
        up_borrow_money = record_data["up_borrow_money"]
        reason = record_data["reason"]
        time = record_data["time"]
        operater_name = record_data["operater_name"]
        signature_img = record_data["signature_img"]
        borrow_type_text = record_data["borrow_type_text"]
        borrow_pay_type_text = record_data["borrow_pay_type_text"]
        num = record_data["num"] or "-"

        title = "%s预支单" % (shop_name)
        supplier_name = "预支人：%s" % supplier_name
        supplier_phone = "手机号：%s" % supplier_phone
        num = "预支单号：%s" % num
        borrow_money = "预支金额：%s元" % borrow_money
        up_borrow_money = "大写：%s" % up_borrow_money
        borrow_type = "预支类型：%s" % borrow_type_text
        pay_type = "支付方式：%s" % borrow_pay_type_text
        reason = "备注：%s" % reason
        time = "预支时间：%s" % time
        operater_name = "经办人：%s" % operater_name
        sign = "老板签字："

        content_body = float_middle(zoom_in(title, 2))
        content_body += line_break("")
        content_body += line_break(zoom_in(supplier_name, 2))
        content_body += line_break(zoom_in(supplier_phone, 2))
        content_body += line_break(zoom_in(num, 2))
        content_body += line_break(zoom_in(borrow_money, 2))
        content_body += line_break(zoom_in(up_borrow_money, 2))
        content_body += line_break(zoom_in(borrow_type, 2))
        content_body += line_break(zoom_in(pay_type, 2))
        content_body += line_break(zoom_in(reason, 2))
        content_body += line_break(time)
        content_body += line_break(operater_name)
        content_body += line_break(zoom_in(sign, 2))

        accountant_content_body = line_break(
            zoom_in(
                color_invert('[会计联]' + self._cashier_accountant_space),
                self.default_size))
        accountant_content_body += content_body
        if signature_img:
            accountant_content_body += self.signature(signature_img)
        accountant_content_body += line_break("")
        accountant_content_body += float_middle(self.support_text)
        accountant_content_body += cut()

        supplier_content_body = line_break(zoom_in("[货主联]", self.default_size))
        supplier_content_body += content_body
        if signature_img:
            supplier_content_body += self.signature(signature_img)
        supplier_content_body += line_break("")
        supplier_content_body += float_middle(self.support_text)
        print_body = accountant_content_body + supplier_content_body
        print_body_list = [accountant_content_body, supplier_content_body]

        return print_body, print_body_list

    def get_deposit_print_body(self, record_data):
        """客户退押金单小票"""
        line_break = self.line_break
        float_middle = self.float_middle
        float_right = self.float_right
        zoom_in = self.zoom_in
        fill_box = self.fill_box
        color_invert = self.color_invert
        cut = self.cut

        shop_name = record_data["shop_name"]
        customer_name = record_data["customer_name"]
        customer_phone = record_data["customer_phone"]
        refund_money = record_data["refund_money"]
        up_refund_money = record_data["up_refund_money"]
        remark = record_data["remark"]
        time = record_data["time"]
        operater_name = record_data["operater_name"]
        # signature_img = record_data["signature_img"]
        pay_type_text = record_data["pay_type_text"]
        num = record_data["num"] or "-"

        title = "%s" % (shop_name)
        name = "退押金单"
        customer_name = "客户：%s" % customer_name
        customer_phone = "手机号：%s" % customer_phone
        num = "退押金单号：%s" % num
        refund_money = "退押金金额：%s元" % refund_money
        up_refund_money = "大写：%s" % up_refund_money
        pay_type = "支付方式：%s" % pay_type_text
        remark = "备注：%s" % remark
        time = "退押金时间：%s" % time
        operater_name = "经办人：%s" % operater_name
        # sign = "老板签字："

        content_body = float_middle(zoom_in(title, 2))
        content_body += line_break(float_middle(zoom_in(name, 2)))
        content_body += line_break("")
        content_body += line_break(zoom_in(customer_name, 2))
        content_body += line_break(zoom_in(customer_phone, 2))
        content_body += line_break(zoom_in(num, 2))
        content_body += line_break(zoom_in(refund_money, 2))
        content_body += line_break(zoom_in(up_refund_money, 2))
        content_body += line_break(zoom_in(pay_type, 2))
        content_body += line_break(zoom_in(remark, 2))
        content_body += line_break(time)
        content_body += line_break(operater_name)
        # content_body += line_break(zoom_in(sign, 2))

        accountant_content_body = line_break(
            zoom_in(
                color_invert('[会计联]' + self._cashier_accountant_space),
                self.default_size))
        accountant_content_body += content_body
        # if signature_img:
        #     accountant_content_body += self.signature(signature_img)
        accountant_content_body += line_break("")
        accountant_content_body += float_middle(self.support_text)
        accountant_content_body += cut()

        supplier_content_body = line_break(zoom_in("[客户联]", self.default_size))
        supplier_content_body += content_body
        # if signature_img:
        #     supplier_content_body += self.signature(signature_img)
        supplier_content_body += line_break("")
        supplier_content_body += float_middle(self.support_text)
        print_body = accountant_content_body + supplier_content_body
        print_body_list = [accountant_content_body, supplier_content_body]

        return print_body, print_body_list

    def get_salary_print_body(self, record_data):
        """员工工资支出单小票"""
        line_break = self.line_break
        float_middle = self.float_middle
        float_right = self.float_right
        zoom_in = self.zoom_in
        fill_box = self.fill_box
        color_invert = self.color_invert
        cut = self.cut

        shop_name = record_data["shop_name"]
        staff_name = record_data["staff_name"]
        staff_phone = record_data["staff_phone"]
        salary_money = record_data["salary_money"]
        up_salary_money = record_data["up_salary_money"]
        remark = record_data["remark"]
        time = record_data["time"]
        operater_name = record_data["operater_name"]
        # signature_img = record_data["signature_img"]
        pay_type_text = record_data["pay_type_text"]
        num = record_data["num"] or "-"

        title = "%s" % (shop_name)
        name = "员工工资单"
        staff_name = "员工：%s" % staff_name
        staff_phone = "手机号：%s" % staff_phone
        num = "工资单号：%s" % num
        salary_money = "工资金额：%s元" % salary_money
        up_salary_money = "大写：%s" % up_salary_money
        pay_type = "支付方式：%s" % pay_type_text
        remark = "备注：%s" % remark
        time = "工资支出时间：%s" % time
        operater_name = "经办人：%s" % operater_name
        # sign = "老板签字："

        content_body = float_middle(zoom_in(title, 2))
        content_body += line_break(float_middle(zoom_in(name, 2)))
        content_body += line_break("")
        content_body += line_break(zoom_in(staff_name, 2))
        content_body += line_break(zoom_in(staff_phone, 2))
        content_body += line_break(zoom_in(num, 2))
        content_body += line_break(zoom_in(salary_money, 2))
        content_body += line_break(zoom_in(up_salary_money, 2))
        content_body += line_break(zoom_in(pay_type, 2))
        content_body += line_break(zoom_in(remark, 2))
        content_body += line_break(time)
        content_body += line_break(operater_name)
        # content_body += line_break(zoom_in(sign, 2))

        accountant_content_body = line_break(
            zoom_in(
                color_invert('[会计联]' + self._cashier_accountant_space),
                self.default_size))
        accountant_content_body += content_body
        # if signature_img:
        #     accountant_content_body += self.signature(signature_img)
        accountant_content_body += line_break("")
        accountant_content_body += float_middle(self.support_text)
        accountant_content_body += cut()

        supplier_content_body = line_break(zoom_in("[员工联]", self.default_size))
        supplier_content_body += content_body
        # if signature_img:
        #     supplier_content_body += self.signature(signature_img)
        supplier_content_body += line_break("")
        supplier_content_body += float_middle(self.support_text)
        print_body = accountant_content_body + supplier_content_body
        print_body_list = [accountant_content_body, supplier_content_body]

        return print_body, print_body_list

    def get_extra_print_body(self, record_data):
        """其他支出单小票"""
        line_break = self.line_break
        float_middle = self.float_middle
        float_right = self.float_right
        zoom_in = self.zoom_in
        fill_box = self.fill_box
        color_invert = self.color_invert
        cut = self.cut

        shop_name = record_data["shop_name"]
        extra_money = record_data["extra_money"]
        up_extra_money = record_data["up_extra_money"]
        reason = record_data["reason"]
        remark = record_data["remark"]
        time = record_data["time"]
        operater_name = record_data["operater_name"]
        # signature_img = record_data["signature_img"]
        pay_type_text = record_data["pay_type_text"]
        num = record_data["num"] or "-"

        title = "%s" % (shop_name)
        name = "其他支出单"
        num = "其他支出单号：%s" % num
        extra_money = "其他支出金额：%s元" % extra_money
        up_extra_money = "大写：%s" % up_extra_money
        pay_type = "支付方式：%s" % pay_type_text
        reason = "支出原因：%s" % reason
        remark = "备注：%s" % remark
        time = "支出时间：%s" % time
        operater_name = "经办人：%s" % operater_name
        # sign = "老板签字："

        content_body = float_middle(zoom_in(title, 2))
        content_body += line_break(float_middle(zoom_in(name, 2)))
        content_body += line_break("")
        content_body += line_break(zoom_in(num, 2))
        content_body += line_break(zoom_in(extra_money, 2))
        content_body += line_break(zoom_in(up_extra_money, 2))
        content_body += line_break(zoom_in(pay_type, 2))
        content_body += line_break(zoom_in(reason, 2))
        content_body += line_break(zoom_in(remark, 2))
        content_body += line_break(time)
        content_body += line_break(operater_name)
        # content_body += line_break(zoom_in(sign, 2))

        accountant_content_body = line_break(
            zoom_in(
                color_invert('[会计联]' + self._cashier_accountant_space),
                self.default_size))
        accountant_content_body += content_body
        # if signature_img:
        #     accountant_content_body += self.signature(signature_img)
        accountant_content_body += line_break("")
        accountant_content_body += float_middle(self.support_text)
        accountant_content_body += cut()

        supplier_content_body = line_break(zoom_in("[副联]", self.default_size))
        supplier_content_body += content_body
        # if signature_img:
        #     supplier_content_body += self.signature(signature_img)
        supplier_content_body += line_break("")
        supplier_content_body += float_middle(self.support_text)
        print_body = accountant_content_body + supplier_content_body
        print_body_list = [accountant_content_body, supplier_content_body]

        return print_body, print_body_list

    def get_clearing_print_body(self, record_data):
        """货款结算小票"""
        line_break = self.line_break
        float_middle = self.float_middle
        float_right = self.float_right
        zoom_in = self.zoom_in
        bottom_line_break = self.bottom_line_break

        shop_name = record_data["shop_name"]
        supplier_name = record_data["supplier_name"]
        supplier_phone = record_data["supplier_phone"]
        goods_name = record_data["goods_name"]
        clear_commission_mul = record_data["clear_commission_mul"]
        clear_sales_num = record_data["clear_sales_num"]
        clear_sales_money = record_data["clear_sales_money"]
        procedure_money = record_data["procedure_money"]
        supplier_borrow_money = record_data["supplier_borrow_money"]
        unload_money = record_data["unload_money"]
        delivery_money = record_data["delivery_money"]
        park_money = record_data["park_money"]
        other_money = record_data["other_money"]
        business_money = record_data["business_money"]
        freeze_momey = record_data["freeze_momey"]
        tricycle_money = record_data["tricycle_money"]
        load_money = record_data["load_money"]
        overweight_money = record_data["overweight_money"]
        total_payout = record_data["total_payout"]
        clearing_money = record_data["clearing_money"]
        pay_type = record_data["pay_type_text"]
        remark = record_data["remark"]
        time = record_data["time"]
        operater_id = record_data["operater_id"]
        operater_name = record_data["operater_name"]
        rate_type_text = record_data["rate_type_text"]

        title = "供货商结款单"
        supplier_name_text = "供货商：%s/%s" % (supplier_name, supplier_phone)
        goods_name_text = "货品：%s" % goods_name
        clear_sales_money_text = "货款总额：%s元" % clear_sales_money
        clear_sales_num_text = "重量：%s" % clear_sales_num
        clear_commission_mul_text = "件数：%s" % clear_commission_mul

        total_payout_text = "扣费合计：%s元" % total_payout
        rate_type_text = "费率：%s" % rate_type_text if rate_type_text != "-" else ""
        procedure_money_text = "手续费：%s元" % procedure_money
        supplier_borrow_money_text = "货主借支：%s" % supplier_borrow_money
        unload_money_text = "卸车费：%s" % unload_money
        delivery_money_text = "运费：%s" % delivery_money
        park_money_text = "停车费：%s" % park_money
        other_money_text = "其他预支：%s" % other_money
        business_money_text = "工商费：%s" % business_money
        freeze_momey_text = "冷库费：%s" % freeze_momey
        tricycle_money_text = "三轮车费：%s" % tricycle_money
        load_money_text = "装车费：%s" % load_money
        overweight_money_text = "超重罚款：%s" % overweight_money

        clearing_money = "结算金额：%s元" % clearing_money
        pay_type_text = "付款方式：%s" % pay_type
        remark_text = "备注：%s" % remark
        draw_operator = "领款人："

        operater_name = "结算人：%s" % operater_name
        time_text = "结算时间：%s" % time
        print_time_text = "打印时间：%s" % TimeFunc.time_to_str(
            datetime.datetime.now())

        content_body = float_middle(zoom_in(shop_name, 2))
        content_body += line_break(float_middle(zoom_in(title, 2)))
        content_body += bottom_line_break()

        content_body += line_break(zoom_in(supplier_name_text, 2))
        content_body += line_break(zoom_in(goods_name_text, 2))
        content_body += line_break(zoom_in(clear_sales_money_text, 2))
        content_body += line_break(zoom_in(clear_sales_num_text, 2))
        content_body += line_break(zoom_in(clear_commission_mul_text, 2))

        content_body += line_break(zoom_in(total_payout_text, 2))

        if rate_type_text:
            content_body += line_break(zoom_in(rate_type_text, 1))
        if procedure_money:
            content_body += line_break(zoom_in(procedure_money_text, 1))
        if supplier_borrow_money:
            content_body += line_break(zoom_in(supplier_borrow_money_text, 1))
        if unload_money:
            content_body += line_break(zoom_in(unload_money_text, 1))
        if delivery_money:
            content_body += line_break(zoom_in(delivery_money_text, 1))
        if park_money:
            content_body += line_break(zoom_in(park_money_text, 1))
        if other_money:
            content_body += line_break(zoom_in(other_money_text, 1))
        if business_money:
            content_body += line_break(zoom_in(business_money_text, 1))
        if freeze_momey:
            content_body += line_break(zoom_in(freeze_momey_text, 1))
        if tricycle_money:
            content_body += line_break(zoom_in(tricycle_money_text, 1))
        if load_money:
            content_body += line_break(zoom_in(load_money_text, 1))
        if overweight_money:
            content_body += line_break(zoom_in(overweight_money_text, 1))

        content_body += bottom_line_break()

        content_body += line_break(zoom_in(clearing_money, 2))
        content_body += line_break(zoom_in(pay_type_text, 2))
        if remark:
            content_body += line_break(zoom_in(remark_text, 2))
        content_body += line_break(zoom_in(draw_operator, 2))
        content_body += bottom_line_break()

        content_body += line_break(zoom_in(operater_name, 1))
        content_body += line_break(zoom_in(time_text, 1))
        content_body += line_break(zoom_in(print_time_text, 1))
        content_body += line_break("")

        if self._type == "80localprint":
            content_body += float_middle(zoom_in(self.support_text, 1))
        else:
            content_body += float_middle(self.support_text)

        return content_body

    def get_stockin_receipt_print_body(self, record_data):
        """入库单打印"""
        line_break = self.line_break
        float_middle = self.float_middle
        float_right = self.float_right
        zoom_in = self.zoom_in
        cut = self.cut

        title = "{shop_name}收货单"
        num = "收货单号：{num}"
        supplier = "供货商：{supplier_name}/{supplier_phone}"
        goods = "货品名：{goods_name}"
        quantity = "到货件数：{quantity}件"
        # 这里的斤暂时不用了，函数没有地方被调用
        weight = "到货重量：{weight}斤"
        operator = "操作人：{operator_name}"
        time = "打印时间：{time}"
        sign = "老板签字："

        receipt_prefixs = ["会计联", "货主联", "备用联"]
        content_body = ""
        for prefix in receipt_prefixs:
            content_body += float_right("[{prefix}]".format(prefix=prefix))
            content_body += float_middle(zoom_in(title, 2))
            content_body += line_break("")
            content_body += line_break(zoom_in(num, 2))
            if record_data["supplier_name"] or record_data["supplier_phone"]:
                content_body += line_break(zoom_in(supplier, 2))
            content_body += line_break(zoom_in(goods, 2))
            content_body += line_break(zoom_in(quantity, 2))
            content_body += line_break(zoom_in(weight, 2))
            content_body += line_break(operator)
            content_body += line_break(time)
            content_body += line_break(zoom_in(sign, 2))
            content_body += line_break("")
            content_body += float_middle(self.support_text)
            content_body += line_break("")
            if prefix != receipt_prefixs[-1]:
                content_body += line_break(cut())

        content_body = content_body.format(**record_data)

        return content_body

    def get_allocate_print_body(self,
                                record_data,
                                goods_list,
                                weight_unit_text="斤"):
        """库存调拨记录打印"""
        _paper_width = self._paper_width
        line_break = self.line_break
        float_middle = self.float_middle
        float_right = self.float_right
        color_invert = self.color_invert
        zoom_in = self.zoom_in
        cut = self.cut
        if record_data["apply_shop_id"] == record_data["delivery_shop_id"]:
            title = "发货单"
            s_title = "[发货清单]"
        else:
            title = "收货单"
            s_title = "[收货清单]"
        allocate_num = "调货单号：{num}"
        delivery_shop_name = "发货门店:{delivery_shop_name}"
        receive_shop_name = "收货门店:{receive_shop_name}"
        goods_count = "货品数:{goods_count}"
        total_quantity = ""
        if record_data["quantity"]:
            total_quantity += "%s件" % (record_data["quantity"])
        total_weight = ""
        if record_data["weight"]:
            total_weight += "%s%s" % (record_data["weight"], weight_unit_text)
        if total_quantity and total_weight:
            total_num = "总调货量:" + total_quantity + "+" + total_weight
        else:
            total_num = "总调货量:" + (total_weight or total_quantity)
        status_text = ["草稿", "审核中", "已通过", "已拒绝"]
        status = "审核状态:%s" % (status_text[record_data["status"]])
        print_info = "打印信息:{user_name}      {time_now}"

        content_body = ""
        content_body += float_middle(zoom_in(title, 2))
        content_body += line_break(allocate_num)
        content_body += line_break(delivery_shop_name)
        content_body += line_break(receive_shop_name)
        content_body += line_break(goods_count)
        content_body += line_break(total_num)
        if record_data["remark"]:
            remark = "备注:%s" % (record_data["remark"])
            content_body += line_break(remark)
        content_body += line_break(status)
        _accountant_title = color_invert(s_title + (_paper_width - 10) * " ")
        content_body += _accountant_title
        if not self._type == "80localprint":
            content_body += line_break("")
        # 打印商品信息
        for _goods in goods_list:
            if _goods["quantity"]:
                _goods_count = "%s件" % _goods["quantity"]
            else:
                _goods_count = "%s%s" % (_goods["weight"], weight_unit_text)
            _goods_name = _goods["detail_name"]
            content_body += self.fill_box_to_float_right(
                _goods_name, 48, _goods_count)
            _goods_remark = _goods["remark"]
            if _goods_remark:
                if not self._type == "80localprint":
                    content_body += line_break("")
                _goods_remark = "备注：%s" % _goods_remark
                content_body += line_break(_goods_remark)
        content_body += self.bottom_line_break()
        if self._type == "80localprint":
            content_body += float_middle(zoom_in(print_info, 1))
            content_body += float_middle(zoom_in(self.support_text, 1))
        else:
            content_body += float_middle(print_info)
            content_body += float_middle(self.support_text)
        content_body = content_body.format(**record_data)
        return content_body


# 小票模版
class ReceiptTemplateFunc():
    """小票模版"""

    @classmethod
    def get_receipt_key_list(cls):
        key_list = [
            "salesman_customer_receipt_temp",
            "salesman_accountant_receipt_temp",
            "salesman_goods_receipt_temp",
            "salesman_async_customer_receipt_temp",
            "salesman_async_accountant_receipt_temp",
            "salesman_async_goods_receipt_temp",
            "cashier_accountant_receipt_temp",
            "cashier_goods_receipt_temp",
            "multi_salesman_customer_receipt_temp",
            "multi_salesman_accountant_receipt_temp",
            "multi_salesman_goods_receipt_temp",
            "multi_salesman_async_customer_receipt_temp",
            "multi_salesman_async_accountant_receipt_temp",
            "multi_salesman_async_goods_receipt_temp",
            "multi_cashier_accountant_receipt_temp",
            "multi_cashier_goods_receipt_temp",
        ]
        return key_list

    @classmethod
    def get_receipt_temp_detail(cls, temp_key, shop_id):
        redis_key = "%s:%d" % (temp_key, shop_id)
        redis_temp = redis_config.get(redis_key)
        if redis_temp:
            redis_temp = redis_temp.decode("utf-8")
            redis_temp = json.loads(redis_temp)
        else:
            redis_temp = cls.get_default_receipt_temp(temp_key)
        return redis_temp

    @classmethod
    def get_default_receipt_temp(cls, temp_key):
        '''
        单品小票模版
            salesman_customer_receipt_temp:开票-客户联-单品小票模版
            salesman_accountant_receipt_temp:开票-会计联-单品小票模版
            salesman_goods_receipt_temp:开票-货物联-单品小票模版
            salesman_async_customer_receipt_temp:手机收款-客户联-单品小票模版
            salesman_async_accountant_receipt_temp:手机收款-会计联-单品小票模版
            salesman_async_goods_receipt_temp:手机收款-货物联-单品小票模版
            cashier_accountant_receipt_temp:收银-会计联-单品小票模版
            cashier_goods_receipt_temp:收银-货物联-单品小票模版

        多品小票模版
            multi_salesman_customer_receipt_temp:开票-客户联-多品小票模版
            multi_salesman_accountant_receipt_temp:开票-会计联-多品小票模版
            multi_salesman_goods_receipt_temp:开票-货物联-多品小票模版
            multi_salesman_async_customer_receipt_temp:手机收款-客户联-多品小票模版
            multi_salesman_async_accountant_receipt_temp:手机收款-会计联-多品小票模版
            multi_salesman_async_goods_receipt_temp:手机收款-货物联-多品小票模版
            multi_cashier_accountant_receipt_temp:收银-会计联-多品小票模版
            multi_cashier_goods_receipt_temp:收银-货物联-多品小票模版

        参数说明
            name       打印条目名称
            position   居中／居左／居右    center/left/right
            size       字号               1/2/3
            print      是否打印           0:不打印  1:打印
            newline    是否换行           0:不换行  1:换行
        '''
        if temp_key in [
                "salesman_customer_receipt_temp",
                "salesman_async_customer_receipt_temp"
        ]:
            default_temp = [{
                "name": "shop_name",
                "position": "center",
                "size": "2",
                "print": 1,
                "newline": 1
            },
                            {
                                "name": "barcode",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "goods_name",
                                "position": "center",
                                "size": "3",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "piece",
                                "position": "center",
                                "size": "3",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "sale_number",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "batch_num",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "weight",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "price",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "commission",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "deposit",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_goods_sale",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_commission",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_deposit",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_money",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "code",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "shop_customer",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "salesman_info",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "accountant_info",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "pay_info",
                                "position": "center",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "receipt_text",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_img",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "remark",
                                "position": "left",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            }]
        elif temp_key in [
                "salesman_accountant_receipt_temp",
                "salesman_async_accountant_receipt_temp"
        ]:
            default_temp = [{
                "name": "shop_name",
                "position": "center",
                "size": "2",
                "print": 1,
                "newline": 1
            },
                            {
                                "name": "goods_name",
                                "position": "center",
                                "size": "3",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "piece",
                                "position": "center",
                                "size": "3",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "sale_number",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "batch_num",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "weight",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "price",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_goods_sale",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_commission",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_deposit",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_money",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "code",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "barcode",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "deposit",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "commission",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "shop_customer",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "salesman_info",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "accountant_info",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "pay_info",
                                "position": "center",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "receipt_text",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_img",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "remark",
                                "position": "left",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            }]
        elif temp_key in [
                "salesman_goods_receipt_temp",
                "salesman_async_goods_receipt_temp"
        ]:
            default_temp = [{
                "name": "shop_name",
                "position": "center",
                "size": "2",
                "print": 1,
                "newline": 1
            },
                            {
                                "name": "code",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "goods_name",
                                "position": "center",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "piece",
                                "position": "center",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "weight",
                                "position": "center",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_money",
                                "position": "center",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "sale_number",
                                "position": "center",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "batch_num",
                                "position": "center",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "price",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "total_goods_sale",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "total_commission",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "total_deposit",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "barcode",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "deposit",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "commission",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "shop_customer",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "salesman_info",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "accountant_info",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "pay_info",
                                "position": "center",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "receipt_text",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_img",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "remark",
                                "position": "left",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            }]
        elif temp_key == "cashier_accountant_receipt_temp":
            default_temp = [{
                "name": "shop_name",
                "position": "center",
                "size": "2",
                "print": 1,
                "newline": 1
            },
                            {
                                "name": "goods_name",
                                "position": "center",
                                "size": "3",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "piece",
                                "position": "center",
                                "size": "3",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "sale_number",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "batch_num",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "weight",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "price",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_goods_sale",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_commission",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_deposit",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_money",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "code",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "barcode",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "deposit",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "commission",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "salesman_info",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "accountant_info",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "pay_info",
                                "position": "center",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "shop_customer",
                                "position": "left",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "signature_img",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_text",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_img",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "remark",
                                "position": "left",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            }]
        elif temp_key == "cashier_goods_receipt_temp":
            default_temp = [{
                "name": "shop_name",
                "position": "center",
                "size": "2",
                "print": 1,
                "newline": 1
            },
                            {
                                "name": "goods_name",
                                "position": "center",
                                "size": "3",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "piece",
                                "position": "center",
                                "size": "3",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "sale_number",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "batch_num",
                                "position": "left",
                                "size": "2",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "weight",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "price",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "commission",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "deposit",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_goods_sale",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_commission",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "total_deposit",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_money",
                                "position": "left",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "code",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "barcode",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "salesman_info",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "accountant_info",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "pay_info",
                                "position": "center",
                                "size": "2",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "shop_customer",
                                "position": "left",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "receipt_text",
                                "position": "center",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            },
                            {
                                "name": "receipt_img",
                                "position": "center",
                                "size": "1",
                                "print": 0,
                                "newline": 1
                            },
                            {
                                "name": "remark",
                                "position": "left",
                                "size": "1",
                                "print": 1,
                                "newline": 1
                            }]
        elif temp_key in [
                "multi_salesman_customer_receipt_temp",
                "multi_salesman_accountant_receipt_temp",
                "multi_salesman_goods_receipt_temp",
                "multi_salesman_async_customer_receipt_temp",
                "multi_salesman_async_accountant_receipt_temp",
                "multi_salesman_async_goods_receipt_temp",
        ]:
            default_temp = [
                {
                    "name": "shop_name",
                    "position": "center",
                    "size": "2",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "barcode",
                    "position": "center",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "code",
                    "position": "center",
                    "size": "3",
                    "print": 0,
                    "newline": 1
                },
                {
                    "name": "order_num",
                    "position": "left",
                    "size": "2",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "category",
                    "position": "left",
                    "size": "2",
                    "print": 0,
                    "newline": 0
                },
                {
                    "name": "total_commission_mul",
                    "position": "left",
                    "size": "2",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "total_money",
                    "position": "left",
                    "size": "2",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "shop_customer",
                    "position": "left",
                    "size": "2",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name":
                    "goods_list",
                    "style": [{
                        "name": "sale_number",
                        "position": "left",
                        "size": "1",
                        "print": 1,
                        "newline": 1
                    },
                              {
                                  "name": "batch_num",
                                  "position": "left",
                                  "size": "1",
                                  "print": 0,
                                  "newline": 1
                              },
                              {
                                  "name": "goods_name",
                                  "position": "left",
                                  "size": "2",
                                  "print": 1,
                                  "newline": 1
                              },
                              {
                                  "name": "piece",
                                  "position": "left",
                                  "size": "2",
                                  "print": 1,
                                  "newline": 0
                              },
                              {
                                  "name": "receipt_money",
                                  "position": "left",
                                  "size": "2",
                                  "print": 1,
                                  "newline": 1
                              },
                              {
                                  "name": "price",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 0
                              },
                              {
                                  "name": "total_goods_sale",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 1
                              },
                              {
                                  "name": "commission",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 0
                              },
                              {
                                  "name": "total_commission",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 1
                              },
                              {
                                  "name": "deposit",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 0
                              },
                              {
                                  "name": "total_deposit",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 1
                              },
                              {
                                  "name": "weight",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 1
                              }]
                },
                {
                    "name": "salesman_info",
                    "position": "center",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "accountant_info",
                    "position": "center",
                    "size": "1",
                    "print": 0,
                    "newline": 1
                },
                {
                    "name": "pay_info",
                    "position": "center",
                    "size": "1",
                    "print": 0,
                    "newline": 1
                },
                {
                    "name": "erase_text",
                    "position": "center",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "receipt_text",
                    "position": "center",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "remark",
                    "position": "left",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
            ]
        elif temp_key in [
                "multi_cashier_accountant_receipt_temp",
                "multi_cashier_goods_receipt_temp"
        ]:
            default_temp = [
                {
                    "name": "shop_name",
                    "position": "center",
                    "size": "2",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "barcode",
                    "position": "center",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "code",
                    "position": "center",
                    "size": "3",
                    "print": 0,
                    "newline": 1
                },
                {
                    "name": "order_num",
                    "position": "left",
                    "size": "2",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "category",
                    "position": "left",
                    "size": "2",
                    "print": 0,
                    "newline": 0
                },
                {
                    "name": "total_commission_mul",
                    "position": "left",
                    "size": "2",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "total_money",
                    "position": "left",
                    "size": "2",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "shop_customer",
                    "position": "left",
                    "size": "2",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name":
                    "goods_list",
                    "style": [{
                        "name": "sale_number",
                        "position": "left",
                        "size": "1",
                        "print": 1,
                        "newline": 1
                    },
                              {
                                  "name": "batch_num",
                                  "position": "left",
                                  "size": "1",
                                  "print": 0,
                                  "newline": 1
                              },
                              {
                                  "name": "goods_name",
                                  "position": "left",
                                  "size": "2",
                                  "print": 1,
                                  "newline": 1
                              },
                              {
                                  "name": "piece",
                                  "position": "left",
                                  "size": "2",
                                  "print": 1,
                                  "newline": 0
                              },
                              {
                                  "name": "receipt_money",
                                  "position": "left",
                                  "size": "2",
                                  "print": 1,
                                  "newline": 1
                              },
                              {
                                  "name": "price",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 0
                              },
                              {
                                  "name": "total_goods_sale",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 1
                              },
                              {
                                  "name": "commission",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 0
                              },
                              {
                                  "name": "total_commission",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 1
                              },
                              {
                                  "name": "deposit",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 0
                              },
                              {
                                  "name": "total_deposit",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 1
                              },
                              {
                                  "name": "weight",
                                  "position": "left",
                                  "size": "1",
                                  "print": 1,
                                  "newline": 1
                              }]
                },
                {
                    "name": "salesman_info",
                    "position": "center",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "accountant_info",
                    "position": "center",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "pay_info",
                    "position": "center",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "erase_text",
                    "position": "center",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "receipt_text",
                    "position": "center",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
                {
                    "name": "remark",
                    "position": "left",
                    "size": "1",
                    "print": 1,
                    "newline": 1
                },
            ]
        else:
            default_temp = []
        return default_temp


# 无线打印配置信息
class WirelessPrintInfoFunc():
    # 获取开票员绑定的打印机
    @classmethod
    def get_salersman_printer_info(cls, session, user_id, shop_id):
        printer_id = session.query(models.HireLink.printer_id).filter_by(
            account_id=user_id, shop_id=shop_id).scalar()
        printer_info = cls.get_printer_info(session, printer_id)
        return printer_info

    # 获取收银员绑定的打印机
    @classmethod
    def get_accountant_printer_info(cls, session, user_id, shop_id):
        printer_id = session.query(
            models.HireLink.accountant_printer_id).filter_by(
                account_id=user_id, shop_id=shop_id).scalar()
        printer_info = cls.get_printer_info(session, printer_id)
        return printer_info

    # 获取打印机详细信息
    @classmethod
    def get_printer_info(cls, session, printer_id):
        printer_info = {"id": 0, "remark": "", "brand": "", "num": ""}
        if printer_id:
            printer = session.query(
                models.Printer).filter_by(id=printer_id).first()
            printer_info["id"] = printer.id
            printer_info["remark"] = printer.remark
            printer_info[
                "brand"] = '易联云' if printer.receipt_type == 1 else '森果1型' if printer.receipt_type == 2 else '森果2型' if printer.receipt_type == 3 else '未知品牌'
            printer_info["num"] = printer.wireless_print_num
        return printer_info


# 账户
class AccountFunc():
    # 根据手机号获取账户
    @classmethod
    def get_account_by_phone(self, session, phone):
        Accountinfo = models.Accountinfo
        account = session.query(Accountinfo).filter_by(phone=phone).first()
        return account

    # 根据手机添加账户
    @classmethod
    def add_account_by_phone(self, session, phone):
        Accountinfo = models.Accountinfo
        account = Accountinfo(phone=phone, uuid=Accountinfo.make_uuid(session))
        session.add(account)
        session.flush()
        return account

    # 根据id列表获取账户基本信息
    @classmethod
    def get_account_through_id_list(cls, session, account_id_list):
        if account_id_list:
            Accountinfo = models.Accountinfo
            account = session.query(Accountinfo) \
                .filter(Accountinfo.id.in_(account_id_list)) \
                .all()
            account_dict = {x.id: x for x in account}
        else:
            account_dict = {}
        return account_dict

    # 根据ID列表获取用户实际姓名
    @classmethod
    def get_account_name_through_id_list(cls, session, account_id_list):
        if account_id_list:
            Accountinfo = models.Accountinfo
            account = session.query(Accountinfo.id, Accountinfo.realname, Accountinfo.nickname) \
                .filter(Accountinfo.id.in_(account_id_list)) \
                .all()
            account_dict = {x.id: x.realname or x.nickname for x in account}
        else:
            account_dict = {}
        return account_dict

    # 根据ID列表获取用户小头像, 如果用户绑定了微信就返回，否则为""；
    @classmethod
    def get_account_wx_head_imgurl_through_id_list(cls, session,
                                                   account_id_list):
        if account_id_list:
            Accountinfo = models.Accountinfo
            operator_list = session.query(Accountinfo) \
            .filter(Accountinfo.id.in_(account_id_list)).all()
            account_dict = {
                x.id: x.head_imgurl_small if x.wx_unionid else ""
                for x in operator_list
            }
        else:
            account_dict = {}
        return account_dict

    # 根据ID获取用户小头像, 如果用户绑定了微信就返回，否则为""；
    @classmethod
    def get_account_wx_head_imgurl_through_id(cls, session, account_id):
        Accountinfo = models.Accountinfo
        account = session\
            .query(Accountinfo) \
            .filter(Accountinfo.id == account_id)\
            .first()
        if account:
            account_img = account.head_imgurl_small if account.wx_unionid else ""
        else:
            account_img = ""
        return account_img

    # 根据id列表获取用户名及绑定的微信头像，如果没绑定微信，则返回""；
    @classmethod
    def get_account_name_wximg_through_id_list(cls, session, account_id_list):
        if account_id_list:
            Accountinfo = models.Accountinfo
            account = session.query(Accountinfo.id, Accountinfo.realname, Accountinfo.nickname,
                Accountinfo.headimgurl, Accountinfo.wx_unionid) \
                .filter(Accountinfo.id.in_(account_id_list)) \
                .all()
            name_dict = {x.id: x.realname or x.nickname for x in account}
            wximg_dict = {
                x.id: models.AddImgDomain.add_domain_headimgsmall(x.headimgurl)
                if x.wx_unionid else ""
                for x in account
            }
            name_wximg_list = [name_dict, wximg_dict]
        else:
            name_wximg_list = [{}, {}]
        return name_wximg_list

    # 根据ID列表获取用户是否是店铺的货主
    @classmethod
    def get_account_goods_owner_role_through_id_list(cls, session, shop_id,
                                                     account_id_list):
        result = {}
        if account_id_list:
            hirelinks = session.query(
                models.HireLink.active_goods_owner, models.Accountinfo).join(
                    models.Accountinfo, models.Accountinfo.id == models.
                    HireLink.account_id).filter(
                        models.Accountinfo.id.in_(account_id_list),
                        models.HireLink.shop_id == shop_id).all()
            suppliers = session.query(models.ShopSupplier) \
                .filter(models.ShopSupplier.account_id.in_(account_id_list)) \
                .all()
            map_suppliers = {s.account_id: s for s in suppliers}
            for active_goods_owner, account_info in hirelinks:
                supplier = map_suppliers.get(account_info.id)
                result[account_info.id] = dict(
                    active_goods_owner=1 if active_goods_owner == 1 else 0,
                    supplier_headimg=account_info.head_imgurl
                    if active_goods_owner == 1 else "",
                    supplier_id=supplier.id if supplier else 0)
        return result

    # 根据ID列表获取用户信息
    @classmethod
    def get_userinfo_through_id_list(cls, session, account_id_list):
        if not account_id_list:
            return {}

        Accountinfo = models.Accountinfo
        records = session.query(Accountinfo) \
            .filter(Accountinfo.id.in_(account_id_list)) \
            .all()
        datadict = {}
        for account in records:
            data = dict(
                id=account.id,
                passport_id=account.passport_id,
                name=account.realname or account.nickname,
                head_img=account.head_imgurl_small,
            )
            datadict[account.id] = data
        return datadict

    # 根据id获取账户
    @classmethod
    def get_account_through_id(cls, session, account_id):
        Accountinfo = models.Accountinfo
        account = session.query(Accountinfo).filter_by(id=account_id).first()
        return account

    # 根据passport_id获取账户
    @classmethod
    def get_account_through_passport_id(cls, session, passport_id):
        Accountinfo = models.Accountinfo
        account = session.query(Accountinfo).filter_by(
            passport_id=passport_id).one_or_none()
        return account

    # 根据uuid获取账户
    @classmethod
    def get_account_through_uuid(cls, session, account_uuid):
        Accountinfo = models.Accountinfo
        account = session.query(Accountinfo).filter_by(
            uuid=account_uuid).first()
        return account

    # 获取账户真实姓名
    @classmethod
    def get_account_realname(cls, session, account_id):
        Accountinfo = models.Accountinfo
        realname = session.query(
            Accountinfo.realname).filter_by(id=account_id).scalar() or ""
        return realname

    # 获取账户姓名
    @classmethod
    def get_account_name(cls, session, account_id):
        Accountinfo = models.Accountinfo
        account = session.query(
            Accountinfo.realname,
            Accountinfo.nickname).filter_by(id=account_id).first()
        if account:
            realname = account.realname or account.nickname
        else:
            realname = ""
        return realname

    # 获取账户手机号
    @classmethod
    def get_account_phone(cls, session, account_id):
        Accountinfo = models.Accountinfo
        phone = session.query(
            Accountinfo.phone).filter_by(id=account_id).scalar() or ""
        return phone

    # 通过手机号获取用户id
    @classmethod
    def get_account_id_through_phone(cls, session, phone):
        Accountinfo = models.Accountinfo
        _id = session.query(
            Accountinfo.id).filter_by(phone=phone).scalar() or 0
        return _id

    # 通过手机号获取用户id,name字典
    @classmethod
    def get_account_id_name_dict_through_phone(cls, session, phone,
                                               many=False):
        Accountinfo = models.Accountinfo
        id_name_dict = {}
        if many:
            result = session.query(Accountinfo.id, Accountinfo.realname, Accountinfo.nickname) \
                .filtery(Accountinfo.phone.in_(phone)).all()
            for item in result:
                id_name_dict[item.id] = item.realname or item.nickname or "-"
        else:
            result = session.query(Accountinfo.id, Accountinfo.realname, Accountinfo.nickname)\
                             .filter_by(phone=phone).first()
            if result:
                id_name_dict[result.
                             id] = result.realname or result.nickname or "-"
        return id_name_dict

    # 获取性别文字
    @classmethod
    def get_sex_text(cls, sex):
        sex_text_dict = {1: "男", 2: "女"}
        sex_text = sex_text_dict.get(sex, "未知")
        return sex_text

    @classmethod
    def parse_account_info(cls, account, get_wximgurl=False):
        """ 解析账户的基本信息

        :param account: obj, class `Accountinfo`

        :param get_wximgurl 需要返回微信头像时为true

        :rtype: dict
        """
        result = dict(
            # 接口兼容, 两个字段同一值
            id=account.id,
            account_id=account.id,
            nickname=account.nickname or "",
            realname=account.realname or "",
            imgurl=account.head_imgurl_small or "",
            supplier_headimg=account.head_imgurl or "",
            phone=account.phone or "",
        )
        if get_wximgurl:
            result[
                "wximgurl"] = account.head_imgurl_small if account.wx_unionid else ""
        return result

    @classmethod
    def list_account_infos(cls, session, account_id_list):
        """ 通过账户id列表获取账户列表对象 """
        result = session.query(models.Accountinfo) \
            .filter(models.Accountinfo.id.in_(account_id_list)).all() \
            if account_id_list else []
        return result

    @classmethod
    def get_account_name_phone_by_id(cls, session, _id):
        Accountinfo = models.Accountinfo
        result = session.query(Accountinfo.phone, Accountinfo.realname, Accountinfo.nickname)\
                    .filter_by(id=_id).first()
        name = phone = "-"
        if result:
            name = result.realname or result.nickname or "-"
            phone = result.phone
        return name, phone


# 退款订单
class RefundOrderFunc():
    @classmethod
    def get_refund_pay_type(cls, refund_pay_type):
        type_text_dict = {1: "现金", 3: "微信原路退还", 4: "支付宝原路退还", 9: "赊账退还"}
        refund_pay_type_text = type_text_dict.get(refund_pay_type, "")
        return refund_pay_type_text

    @classmethod
    def get_refund_record_time(cls, refund_order):
        time = str(refund_order.create_year) + '-' + str(
            refund_order.create_month).zfill(2) + '-' + str(
                refund_order.create_day).zfill(2) + ' ' + str(
                    refund_order.create_time)
        return time


# 付费功能
class FunctionPaidFunc():
    @classmethod
    def get_service_paid(cls, session, shop_id, service_type=0):
        PaidServiceOrder = models.PaidServiceOrder
        service_paid = session.query(PaidServiceOrder) \
            .filter_by(shop_id=shop_id, service_type=service_type, status=1) \
            .first()
        if service_paid:
            return True
        else:
            return False


# 供货商
class SupplierFunc():

    # 获取某个店铺的所有供应商
    @classmethod
    def get_shop_supplier_all(cls, session, shop_id):
        """ 获取某个店铺的所有供应商, id与name的映射

        :param shop_id: 店铺id

        :rtype: dict
        """
        shop_suppliers = session.query(models.ShopSupplier).filter_by(
            shop_id=shop_id, status=1).all()
        # 供应商id到其真实姓名的映射
        return {s.id: s.realname for s in shop_suppliers}

    # 获取供货商的商品数量
    @classmethod
    def get_supplier_goods_count(cls, session, shop_id, shop_supplier_id_list):
        if not shop_supplier_id_list:
            return {}
        Goods = models.Goods
        goods_query = session.query(Goods.shop_supplier_id,
                                    func.count(Goods.id)) \
            .filter_by(shop_id=shop_id) \
            .filter(Goods.active != 0,
                    Goods.shop_supplier_id.in_(shop_supplier_id_list)) \
            .group_by(Goods.shop_supplier_id) \
            .all()
        return {i[0]: i[1] for i in goods_query}

    # 获取供货商的商品ID列表
    @classmethod
    def get_supplier_goods_id_through_id(cls, session, shop_id,
                                         shop_supplier_id_list):
        Goods = models.Goods
        goods_query = session.query(Goods.id) \
            .filter(Goods.shop_id==shop_id,
                    Goods.shop_supplier_id.in_(shop_supplier_id_list)) \
            .all()
        return {x.id for x in goods_query}

    # 根据店铺及供货商姓名获取供货商
    @classmethod
    def get_shop_supplier_through_name(cls, session, shop_id, realname):
        ShopSupplier = models.ShopSupplier
        shop_supplier = session.query(ShopSupplier) \
            .filter_by(realname=realname, shop_id=shop_id, status=1) \
            .first()
        return shop_supplier

    # 根据店铺及供货商手机号获取供货商
    @classmethod
    def get_shop_supplier_through_phone(cls, session, shop_id, phone):
        if not phone:
            return None
        ShopSupplier = models.ShopSupplier
        shop_supplier = session.query(ShopSupplier) \
            .filter_by(phone=phone, shop_id=shop_id, status=1) \
            .first()
        return shop_supplier

    # 根据供货商id获取供货商
    @classmethod
    def get_shop_supplier_through_id(cls, session, shop_id, shop_supplier_id):
        ShopSupplier = models.ShopSupplier
        shop_supplier = session.query(ShopSupplier) \
            .filter_by(id=shop_supplier_id, shop_id=shop_id) \
            .first()
        return shop_supplier

    # 根据供货商id列表获取供货商
    @classmethod
    def get_shop_supplier_through_id_list(cls, session, shop_id,
                                          shop_supplier_id_list):
        ShopSupplier = models.ShopSupplier
        shop_suppliers = session.query(ShopSupplier) \
            .filter_by(shop_id=shop_id) \
            .filter(ShopSupplier.id.in_(shop_supplier_id_list)) \
            .all()
        return shop_suppliers

    # 获取供货商基本信息
    @classmethod
    def get_shop_supplier_base_info(cls, session, shop_id,
                                    shop_supplier_id_list):
        info_dict = {}
        shop_suppliers = cls.get_shop_supplier_through_id_list(
            session, shop_id, shop_supplier_id_list)
        account_id_list = [supplier.account_id for supplier in shop_suppliers]
        supplier_accounts = AccountFunc.list_account_infos(
            session, account_id_list)
        map_accounts = {account.id: account for account in supplier_accounts}
        for supplier in shop_suppliers:
            account = map_accounts.get(supplier.account_id)
            account_info = AccountFunc.parse_account_info(
                account, get_wximgurl=True) if account else {}
            info_dict[supplier.id] = {
                "supplier_id":
                supplier.id,
                "name":
                supplier.realname,
                "phone":
                supplier.phone or "",
                "headimgurl":
                account_info.get("supplier_headimg", ""),
                "wximgurl":
                account_info.get("wximgurl", ""),
                "active_goods_owner":
                1 if supplier.active_goods_owner == 1 else 0,
                "account_id":
                supplier.account_id
            }
        return info_dict

    # 获取供货商基本信息
    @classmethod
    def get_shop_supplier_names(cls, session, supplier_id_list):
        name_dict = {}
        if supplier_id_list:
            ShopSupplier = models.ShopSupplier
            shop_suppliers = session.query(ShopSupplier.id, ShopSupplier.realname) \
                .filter(ShopSupplier.id.in_(supplier_id_list)) \
                .all()
            name_dict = {x.id: x.realname for x in shop_suppliers}
        return name_dict

    # 获取供应商预支
    @classmethod
    def get_borrow_status_through_id_list(cls, session, id_list):
        status_dict = {}
        ShopSupplierBorrowing = models.ShopSupplierBorrowing
        borrow_list = session.query(ShopSupplierBorrowing.id,
                                    ShopSupplierBorrowing.status) \
            .filter(ShopSupplierBorrowing.id.in_(id_list)) \
            .all()
        status_dict = {x.id: x.status for x in borrow_list}
        return status_dict

    # 批量获取供货商信息
    @classmethod
    def get_supplier_info(cls, session, shop_id, data_sales_ret):
        try:
            supplier_id_list = [x.supperlier_id for x in data_sales_ret]
        except:
            supplier_id_list = [x.supplier_id for x in data_sales_ret]
        if supplier_id_list:
            supplier_list = cls.get_shop_supplier_through_id_list(
                session, shop_id, supplier_id_list)
            supplier_dict = {x.id: x.realname for x in supplier_list}
        else:
            supplier_dict = {}
        return supplier_dict

    # 根据商品名称查找供应商
    @classmethod
    def get_supplier_through_goods_name(cls,
                                        session,
                                        shop_id,
                                        name_list,
                                        filter_goods_list=None):
        """
        :param name_list 需要查找货品名列表
        :param filter_goods_list 过滤一些特定条件的货品ID
        """
        if isinstance(name_list, str):
            name_list = {name_list}

        if not filter_goods_list:
            filter_goods_list = []

        # 查找同名商品
        Goods = models.Goods
        goods_list = session \
            .query(Goods) \
            .filter_by(shop_id=shop_id, active=1) \
            .filter(
            Goods.name.in_(name_list),
            ~Goods.id.in_(filter_goods_list)
        ) \
            .all()

        supplier_id_list = {x.shop_supplier_id for x in goods_list}

        # 获取商品供应商信息
        shop_suppliers = cls.get_shop_supplier_through_id_list(
            session, shop_id, supplier_id_list)
        shop_supplier_dict = {x.id: x for x in shop_suppliers}

        data_list_dict = collections.defaultdict(list)
        for goods in goods_list:
            goods_name = goods.name

            data_dict = {}
            data_dict["goods_id"] = goods.id
            shop_supplier_id = goods.shop_supplier_id
            if shop_supplier_id:
                shop_supplier = shop_supplier_dict.get(shop_supplier_id)
                phone = shop_supplier.phone if shop_supplier and shop_supplier.phone else ""
                shop_supplier_name = shop_supplier.realname if shop_supplier and shop_supplier.realname else ""
            else:
                phone = ""
                shop_supplier_name = ""
            data_dict["supplier_id"] = shop_supplier_id
            data_dict["supplier_name"] = shop_supplier_name
            data_dict["supplier_phone"] = phone
            data_dict["goods_storage"] = check_float(goods.storage / 100)
            data_dict["goods_storage_num"] = check_float(
                goods.storage_num / 100)
            data_dict["production_place"] = goods.production_place
            data_dict["specification"] = goods.specification
            data_dict["brand"] = goods.brand
            data_dict["commodity_code"] = goods.commodity_code
            data_dict["supply_type"] = GoodsFunc.get_supply_type(
                goods.supply_type)
            data_list_dict[goods_name].append(data_dict)
        return data_list_dict

    # 根据商品ID查找供应商
    @classmethod
    def get_supplier_info_through_goods_id(cls,
                                           session,
                                           shop_id,
                                           goods_id,
                                           check_supply_type=False):
        # 查找供应商
        shop_supplier_id = session.query(models.Goods.shop_supplier_id) \
            .filter_by(shop_id=shop_id, id=goods_id)
        if check_supply_type:
            shop_supplier_id = shop_supplier_id.filter(
                models.Goods.supply_type > 0)
        shop_supplier_id = shop_supplier_id.scalar() or 0
        data_dict = {}
        if shop_supplier_id:
            # 获取供应商信息
            shop_supplier = cls.get_shop_supplier_through_id(
                session, shop_id, shop_supplier_id)
            supplier_account = AccountFunc.get_account_through_id(
                session, shop_supplier.account_id)
            data_dict = dict(
                supplier_id=shop_supplier.id,
                supplier_name=shop_supplier.realname,
                active_goods_owner=1
                if shop_supplier.active_goods_owner == 1 else 0,
                supplier_headimg=supplier_account.head_imgurl
                if supplier_account else "",
            )
        return data_dict

    # 根据商品ID查找供应商
    @classmethod
    def get_supplier_info_through_goods_id_list(cls,
                                                session,
                                                shop_id,
                                                goods_id_list,
                                                check_supply_type=False):
        # 查找供应商
        supplier_id_list = session.query(models.Goods.shop_supplier_id) \
            .filter_by(shop_id=shop_id)\
            .filter(models.Goods.id.in_(goods_id_list))
        if check_supply_type:
            supplier_id_list = supplier_id_list.filter(
                models.Goods.supply_type > 0)
        supplier_id_list = supplier_id_list.all() or []
        data_dict = {}
        if supplier_id_list:
            supplier_id_list = {x.shop_supplier_id for x in supplier_id_list}
            supplier_list = cls.get_shop_supplier_through_id_list(
                session, shop_id, supplier_id_list)
            data_dict = {x.id: x.realname for x in supplier_list}
        return data_dict

    @classmethod
    def check_supplier_unique_personal(cls,
                                       session,
                                       shop_id,
                                       username,
                                       phone,
                                       db_supplier=None):
        """ 添加和编辑个体户供货商时，检查姓名和手机号的唯一性
        2018-6-25将规则变更为: 供应商和手机号只要有一个不同即可
        """
        supplier = session.query(models.ShopSupplier).filter_by(
            shop_id=shop_id, realname=username, phone=phone, status=1).first()
        # 不能与其他个体户相同
        if supplier:
            return False, "已经存在供应商名称与手机号都相同的个体户供货商"
        # 不能与其他企业简称和全称相同
        c_suppliers = session.query(models.ShopSupplier).filter_by(
            shop_id=shop_id, status=1).filter(
                or_(models.ShopSupplier.company_short_name == username,
                    models.ShopSupplier.company_full_name == username)).all()
        if c_suppliers:
            if db_supplier and len(
                    c_suppliers) == 1 and c_suppliers[0].id == db_supplier.id:
                return True, ""
            return False, "已经存在名称相同的企业供应商"
        return True, ""

    @classmethod
    def check_supplier_unique_company(cls,
                                      session,
                                      shop_id,
                                      company_short_name,
                                      company_full_name,
                                      db_supplier=None):
        """ 添加和编辑企业供货商时, 检查是否存在同名供应商

        :param db_supplier: 编辑供应商的时候，需要判断查到的同名供应商是不是自己
        """
        s_suppliers = []
        c_suppliers = []
        if company_short_name:
            s_suppliers = session.query(models.ShopSupplier).filter_by(
                shop_id=shop_id, status=1).filter(
                    or_(
                        models.ShopSupplier.realname == company_short_name,
                        models.ShopSupplier.company_short_name ==
                        company_short_name, models.ShopSupplier.
                        company_full_name == company_short_name)).all()
        if company_full_name:
            c_suppliers = session.query(models.ShopSupplier).filter_by(
                shop_id=shop_id, status=1).filter(
                    or_(
                        models.ShopSupplier.realname == company_full_name,
                        models.ShopSupplier.company_short_name ==
                        company_full_name, models.ShopSupplier.
                        company_full_name == company_full_name)).all()
        if s_suppliers or c_suppliers:
            if db_supplier:
                supplier_id_set = set()
                for s in s_suppliers:
                    supplier_id_set.add(s.id)
                for c in c_suppliers:
                    supplier_id_set.add(c.id)
                # 自己不能算
                if len(supplier_id_set) == 1 and supplier_id_set == {
                        db_supplier.id
                }:
                    return True, ""
            return False, "检测到存在同名的企业或个体户，请确认"
        return True, ""

    # 通过一个用户的手机号/昵称/真实姓名来匹配供应商
    @classmethod
    def get_supplier_by_account_info(cls, session, shop_id, account_info):
        """ 通过一个用户的手机号/昵称/真实姓名来匹配店铺供应商, 没有返回None

        :param account_info: obj, `class models.Accountinfo`

        :return: `class models.ShopSupplier`
        :rtype: obj
        """
        # 以手机号作为供应商的唯一标识
        result = session.query(models.ShopSupplier).filter_by(
            shop_id=shop_id, status=1, account_id=account_info.id).first()
        return result

    # 获取供货商的商品数量
    @classmethod
    def get_supplier_goods_id_list(cls, session, shop_id, shop_supplier_id):
        """ 通过供应商ID取出供应商对应的所有货品ID

        :param shop_supplier_id：供应商ID

        :return: 商品ID列表
        :rtype: set
        """
        Goods = models.Goods
        goods_query = session.query(Goods.id) \
            .filter_by(shop_id=shop_id, shop_supplier_id=shop_supplier_id) \
            .all()
        return {i.id for i in goods_query}

    # 获取供货商的商品数量
    @classmethod
    def get_multi_supplier_goods_id_list(cls, session, shop_id,
                                         supplier_id_list):
        """ 通过供应商ID取出供应商对应的所有货品ID

        :param shop_supplier_id：供应商ID

        :return: 商品ID列表
        :rtype: set
        """
        Goods = models.Goods
        goods_query = session.query(Goods.id) \
            .filter_by(shop_id=shop_id) \
            .filter(Goods.shop_supplier_id.in_(supplier_id_list))\
            .all()
        return {i.id for i in goods_query}

    # 检查货品是否属于供应商
    @classmethod
    def check_supplier_goods(cls, session, shop_id, shop_supplier_id,
                             goods_id):
        goods = session.query(models.Goods.id) \
            .filter_by(id=goods_id, shop_id=shop_id, shop_supplier_id=shop_supplier_id) \
            .first()
        return goods

    # 获取货品费率
    @classmethod
    def get_multi_goods_rate(cls, session, shop_id, supplier_goods_id_list):
        ClearGoodsRate = models.ClearGoodsRate
        goods_rate_info = session.query(ClearGoodsRate) \
            .filter_by(shop_id=shop_id, status=1) \
            .filter(ClearGoodsRate.goods_id.in_(supplier_goods_id_list)) \
            .all()
        goods_rate_dict = {i.goods_id: i for i in goods_rate_info}
        return goods_rate_dict

    # 获取货品底价
    @classmethod
    def get_multi_goods_bottom_price(cls, session, supplier_goods_id_list):
        bottom_price = session.query(models.BottomPriceGoodsRate).filter(
            models.BottomPriceGoodsRate.goods_id.in_(
                supplier_goods_id_list)).all()
        result = {}
        for b in bottom_price:
            inner_dict = result.setdefault(b.goods_id, {})
            inner_dict.update({b.bottom_price_date: b.bottom_price})
        return result

    # 添加供应商
    @classmethod
    def handle_add_supplier(self,
                            session,
                            shop_id,
                            username,
                            phone,
                            edit_user_id,
                            source="salesman"):
        ShopSupplier = models.ShopSupplier
        shop_supplier = ShopSupplier(
            shop_id=shop_id, realname=username, phone=phone)
        session.add(shop_supplier)
        session.flush()

        # 添加来源
        if source == "boss":
            source_type = 2
        else:
            source_type = 3

        return shop_supplier

    @classmethod
    def parse_supplier_to_dict(cls, supplier):
        """ 将供应商对象解析成字典 """
        result = dict(
            supplier_id=supplier.id,
            supplier_phone=supplier.phone,
            # supplier_headimg=supplier.headimgurl,
            company_corporation_realname=supplier.company_corporation_realname,
            company_full_name=supplier.company_full_name,
            company_short_name=supplier.company_short_name,
            company_tax_num=supplier.company_tax_num,
            company_type=supplier.company_type,
            remarks=supplier.remarks,
            supplier_name=supplier.realname,
            contact_name=supplier.company_contact_name \
                if supplier.company_type == 1 else supplier.realname,
            active_goods_owner=supplier.active_goods_owner,
            account_id=supplier.account_id,
            username_remarks=supplier.username_remarks,
            permission_list=PermissionFunc.loads_permission_list(
                supplier.goods_owner_permission),
            status=supplier.status,
        )
        return result

    @classmethod
    def get_supplier_name_through_id_list(cls, session, supplier_id_list):
        """ 通过ID列表批量获取供应商姓名
        """
        ShopSupplier = models.ShopSupplier
        shop_suppliers = session.query(ShopSupplier.id,
                                       ShopSupplier.realname) \
            .filter(ShopSupplier.id.in_(supplier_id_list)) \
            .all()
        return {x.id: x.realname for x in shop_suppliers}


# 供货商详情-入库记录年/月汇总展示与导出：
class SupplierGoodsStockin():
    @classmethod
    def format_export_supplier_goods_stockin(cls, data_list, total_data_sum,
                                             weight_unit_text):
        data_content = []
        data_content.append([
            '序号', '类型', '货品', '入库量/件',
            '入库量/%s' % weight_unit_text, '采购均价', '总费用/元', '已付款', '赊账待付', '已结算',
            '应付款'
        ])
        i = 1
        for data in data_list:
            data_content.append([
                i,
                data["supply_type_text"],
                data["goods_name"],
                data["quantity"],
                data["weight"],
                (data["quantity_unit_price"] or "-") if data["goods_purchase_unit"] \
                    else (data["weight_unit_price"] or "-"),
                data["total_price"],
                data["paid_cent"] if data["supply_type"] not in [1, 2] else "-",
                data["credit_cent"] if data["supply_type"] not in [1, 2] else "-",
                data["payback_cent"] if data["supply_type"] not in [1, 2] else "-",
                data["should_pay_cent"] if data["supply_type"] not in [1, 2] else "-"
            ])
            i += 1
        data_content.append([
            '累计', '', '', total_data_sum["quantity_sum"],
            total_data_sum["weight_sum"], '',
            total_data_sum["total_price_sum"], total_data_sum["paid_cent_sum"],
            total_data_sum["credit_cent_sum"],
            total_data_sum["payback_cent_sum"],
            total_data_sum["should_pay_cent_sum"]
        ])
        return data_content


# 供货商结算
class SupplierSettlementFunc():

    # 累计已结款（货款+预支）
    @classmethod
    def get_clearing_data(cls, session, shop_id, goods_id):
        ShopSupplierClearing = models.ShopSupplierClearing
        clearing_count, clearing_money = session.query(func.count(ShopSupplierClearing.id),
                                                       func.sum(ShopSupplierClearing.borrow_cent +
                                                                ShopSupplierClearing.clearing_cent)) \
                                             .filter_by(shop_id=shop_id, goods_id=goods_id) \
                                             .first() or (0, 0)
        clearing_count = check_int(clearing_count) if clearing_count else 0
        clearing_money = check_float(
            clearing_money / 100) if clearing_money else 0
        return clearing_count, clearing_money

    # 未进行结算的预支
    @classmethod
    def get_unclearing_borrow_data(cls, session, shop_id, shop_supplier_id):
        redis_key = "supplier_borrow:%d" % shop_supplier_id
        if redis.get(redis_key):
            return 0, 0
        ShopSupplierBorrowing = models.ShopSupplierBorrowing
        borrow_count, borrow_money = session.query(func.count(ShopSupplierBorrowing.id),
                                                   func.sum(ShopSupplierBorrowing.borrow_cent)) \
                                         .filter_by(shop_id=shop_id,
                                                    shop_supplier_id=shop_supplier_id,
                                                    status=0) \
                                         .first() or (0, 0)
        borrow_count = int(borrow_count) if borrow_count else 0
        borrow_money = check_float(borrow_money / 100) if borrow_money else 0
        return borrow_count, borrow_money

    # 未结算的货款(未结算流水)
    @classmethod
    def get_unclearing_money(cls, session, shop_id, goods_id_list):
        """ 未结算流水 + 采购入库赊账待付未结算

        :rtype: float
        """
        proxy_goods_id_set, self_sell_goods_id_set = cls._divide_goods_supply_type(
            session, goods_id_list)
        # 代卖
        GoodsSalesRecord = models.GoodsSalesRecord
        unclearing_money_proxy = session.query(
            func.sum(func.IF(GoodsSalesRecord.record_type == 0,
                             GoodsSalesRecord.sales_money,
                             -GoodsSalesRecord.sales_money))) \
                                     .filter_by(shop_id=shop_id, clearing_record_id=0) \
                                     .filter(GoodsSalesRecord.status.in_([3, 5]),
                                             GoodsSalesRecord.goods_id.in_(proxy_goods_id_set)) \
                                     .with_hint(GoodsSalesRecord, "FORCE INDEX(ix_shop_goods_status_clearing)") \
                                     .scalar() or 0 if proxy_goods_id_set else 0
        # 自营
        StockInDocsDetail = models.StockInDocsDetail
        unclearing_money_self_sell = session.query(
            func.sum(StockInDocsDetail.total_price)).filter_by(
            clearing_record_id=0, pay_type=5, status=1).filter(
            StockInDocsDetail.goods_id.in_(self_sell_goods_id_set)).scalar() or 0 \
            if self_sell_goods_id_set else 0
        unclearing_money = check_float(
            (unclearing_money_proxy + unclearing_money_self_sell) / 100)
        return unclearing_money

    @classmethod
    def get_unclearing_money_dict(cls,
                                  session,
                                  shop_id,
                                  agents_goods_id_list,
                                  return_type="",
                                  clearing_record_ids=None,
                                  end_date_time=None):
        """ 获取货品的未结算总金额和未结算的件数,
            return_type目前参数值为supplier和goods，分别用于返回供应列表对应未结货款以及商品列表对应未结算货款

        :param clearing_record_ids: list, 结算记录id列表，用来计算某一天的未结算（查处这天之前的所有计算记录）
        :param end_date_time: 结束日期，必须和clearing_record_ids一起传
        """
        GoodsSalesRecord = models.GoodsSalesRecord
        proxy_goods_id_set, self_sell_goods_id_set = cls._divide_goods_supply_type(
            session, agents_goods_id_list)

        # 代卖的货品未结货款算未结算的销售记录
        proxy_filter_params = [
            GoodsSalesRecord.shop_id == shop_id,
            GoodsSalesRecord.status.in_([3, 5]),
            GoodsSalesRecord.goods_id.in_(proxy_goods_id_set)
        ]
        # 获取当前所有未结
        if clearing_record_ids is None:
            proxy_filter_params.append(
                GoodsSalesRecord.clearing_record_id == 0)
        elif clearing_record_ids:
            proxy_filter_params.append(
                not_(
                    GoodsSalesRecord.clearing_record_id.in_(
                        clearing_record_ids)))
            proxy_filter_params.append(
                GoodsSalesRecord.create_time < end_date_time)
        goods_unclearing_info_proxy = session \
            .query(GoodsSalesRecord.goods_id,
                   func.sum(func.IF(GoodsSalesRecord.record_type == 0,
                                    GoodsSalesRecord.sales_money,
                                    -GoodsSalesRecord.sales_money)),
                   func.sum(func.IF(GoodsSalesRecord.record_type == 0,
                                    GoodsSalesRecord.commission_mul,
                                    -GoodsSalesRecord.commission_mul))) \
            .filter(*proxy_filter_params) \
            .with_hint(GoodsSalesRecord,"FORCE INDEX(ix_shop_goods_status_clearing)") \
            .group_by(GoodsSalesRecord.goods_id) \
            .all() if proxy_goods_id_set else []
        goods_unclearing_dict = {
            g[0]: {
                "money": g[1],
                "commission_mul": g[2]
            }
            for g in goods_unclearing_info_proxy
        }

        # 自营货品的未结货款算采购入库赊账待付的总金额
        StockInDocsDetail = models.StockInDocsDetail
        StockInDocs = models.StockInDocs
        self_filter_params = [
            StockInDocs.status == 1, StockInDocsDetail.pay_type == 5,
            StockInDocsDetail.status == 1,
            StockInDocsDetail.goods_id.in_(self_sell_goods_id_set)
        ]
        if clearing_record_ids is None:
            self_filter_params.append(
                StockInDocsDetail.clearing_record_id == 0)
        elif clearing_record_ids:
            self_filter_params.append(
                not_(
                    StockInDocsDetail.clearing_record_id.in_(
                        clearing_record_ids)))
            self_filter_params.append(StockInDocs.operate_date < end_date_time)
        goods_unclearing_info_self_sell = session\
            .query(
                StockInDocsDetail.goods_id,
                func.sum(StockInDocsDetail.total_price).label("total_price"),
                func.sum(StockInDocsDetail.quantity).label("quantity")) \
            .join(StockInDocs, StockInDocsDetail.doc_id == StockInDocs.id) \
            .filter(*self_filter_params) \
            .group_by(StockInDocsDetail.goods_id) \
            .all() if self_sell_goods_id_set else []
        goods_unpay_dict = {
            g[0]: {
                "money": g.total_price,
                "commission_mul": g.quantity
            }
            for g in goods_unclearing_info_self_sell
        }
        goods_unclearing_dict.update(goods_unpay_dict)

        data_dict = {}
        if return_type == "supplier":
            # 处理每个供应商的未结货款，返回每个供应商的未结货款需要用商品反查供货商ID
            Goods = models.Goods
            goods_list = session.query(Goods.id, Goods.shop_supplier_id) \
                .filter_by(shop_id=shop_id) \
                .filter(Goods.id.in_(agents_goods_id_list)) \
                .all() if agents_goods_id_list else []
            for goods_data in goods_list:
                goods_id = goods_data.id
                shop_supplier_id = goods_data.shop_supplier_id
                unclearing_info = goods_unclearing_dict.get(goods_id, {})
                unclearing_money = int(unclearing_info.get("money", 0))
                if shop_supplier_id in data_dict:
                    data_dict[shop_supplier_id] += unclearing_money
                else:
                    data_dict[shop_supplier_id] = unclearing_money
        else:
            for goods_id in agents_goods_id_list:
                unclearing_info = goods_unclearing_dict.get(goods_id, {})
                unclearing_money = int(unclearing_info.get("money", 0))
                unclearing_commission_mul = check_float(
                    unclearing_info.get("commission_mul", 0) / 100)
                data_dict[goods_id] = {
                    "money": unclearing_money,
                    "commission_mul": unclearing_commission_mul
                }
        return data_dict

    @classmethod
    def _divide_goods_supply_type(cls, session, goods_id_list):
        """ 将商品列表分拆成自营和代卖

        :rtype: tuple
        """
        proxy_goods = session.query(models.Goods) \
            .filter(models.Goods.id.in_(goods_id_list),
                    models.Goods.supply_type.in_([1, 2])) \
            .all()
        # 代卖货品id
        proxy_goods_id_set = {g.id for g in proxy_goods}
        all_goods_id_set = set(goods_id_list)
        self_sell_goods_id_set = all_goods_id_set - proxy_goods_id_set

        return proxy_goods_id_set, self_sell_goods_id_set

    @classmethod
    def get_unpayback_money_dict(cls, session, shop_id, agents_goods_id_list):
        """
            根据商品ID列表获取未还款商品金额(供货商没有还给老板的店)
        """
        GoodsSalesRecord = models.GoodsSalesRecord
        unpayback_data = session.query(GoodsSalesRecord.goods_id,
                                       func.sum(func.IF(GoodsSalesRecord.record_type == 0,
                                                        GoodsSalesRecord.unpayback_money,
                                                        -GoodsSalesRecord.unpayback_money))) \
            .filter_by(shop_id=shop_id, pay_type=9) \
            .filter(GoodsSalesRecord.status.in_([3, 5]),
                    GoodsSalesRecord.goods_id.in_(agents_goods_id_list),
                    GoodsSalesRecord.unpayback_money > 0) \
            .group_by(GoodsSalesRecord.goods_id) \
            .all()
        unpayback_dict = {x[0]: x[1] for x in unpayback_data}
        return unpayback_dict

    @classmethod
    def is_goods_settlement_finished(cls, goods, unclearing_dict):
        """ 商品是否结算完成

        :param goods: `class Goods`
        :param unclearing_dict: 商品未结算信息

        :rtype: bool
        """
        unclearing_info = unclearing_dict.get(goods.id, {})
        # 销售未结算金额
        unclearing = check_float(unclearing_info.get("money", 0) / 100)
        is_finished = bool(unclearing <= 0)
        # 没有供货商无法结算
        if goods.shop_supplier_id == 0:
            is_finished = 1
        return is_finished

    @classmethod
    def parse_borrow_to_dict(cls, borrow, operator_names):
        """ 将供货商预支对象解析成字典

        :param borrow: obj, `class ShopSupplierBorrowing`
        :param operator_names: dict, 操作人的姓名字典

        :rtype: dict
        """
        operater_id = borrow.operater_id
        result = dict(
            id=borrow.id,
            time=TimeFunc.time_to_str(borrow.create_date),
            borrow_type=borrow.borrow_type,
            borrow_type_text=borrow.borrow_type_text,
            money=check_float(borrow.borrow_cent / 100),
            borrow_pay_type_text=borrow.borrow_pay_type_text,
            operater_id=operater_id,
            operator_name=operator_names.get(operater_id, ""),
            remark=borrow.reason,
        )
        return result

    @classmethod
    def get_clear_detail(cls, session, clearing, shop_name=None):
        """ 获取结算详情 """
        shop_id = clearing.shop_id
        if not shop_name:
            shop = models.Shop.get_by_id(session, shop_id).shop_name
        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)
        fee_text = ConfigFunc.get_fee_text(session, shop_id)
        # 获取供应商
        supplier = SupplierFunc.get_shop_supplier_through_id(
            session, shop_id, clearing.shop_supplier_id)
        # 获取货品列表
        goods_list = cls.get_clearing_goods(session, clearing.id)
        # 获取费率
        clearing_goods = session.query(
            models.ShopSupplierClearingGoods.goods_rate_id).filter_by(
                clearing_id=clearing.id).first()
        goods_rate = GoodsRateFunc.get_goods_rate_through_id(
            session, shop_id, clearing_goods.goods_rate_id)

        data_dict = {}
        data_dict["shop_name"] = shop_name
        data_dict["goods_list"] = goods_list
        data_dict["supplier_id"] = clearing.shop_supplier_id
        data_dict["supplier_name"] = supplier.realname
        data_dict["supplier_phone"] = supplier.phone or ""
        data_dict["clearing_money"] = check_float(clearing.clearing_cent / 100)
        # data_dict["up_clearing_money"] = NumFunc.upcase_number(data_dict["clearing_money"])
        data_dict["clear_commission_mul"] = check_float(
            clearing.clear_commission_mul / 100)
        data_dict["clear_sales_num"] = check_float(
            clearing.get_clear_sales_num(weight_unit_text=weight_unit_text) /
            100)
        data_dict["clear_sales_money"] = check_float(
            clearing.clear_sales_cent / 100)
        data_dict["borrow_money"] = check_float(
            (clearing.borrow_cent + clearing.other_value_cent) / 100)
        data_dict["procedure_money"] = check_float(
            clearing.procedure_cent / 100)
        data_dict["remark"] = clearing.remark
        data_dict["time"] = TimeFunc.time_to_str(clearing.create_date)
        data_dict["operater_id"] = clearing.operater_id
        data_dict["operater_name"] = AccountFunc.get_account_name(
            session, data_dict["operater_id"])
        data_dict["pay_type"] = clearing.pay_type
        data_dict["total_payout"] = check_float(data_dict["borrow_money"] +
                                                data_dict["procedure_money"])
        data_dict["rate_type_text"] = GoodsRateFunc.get_rate_type_text(
            goods_rate, fee_text=fee_text, weight_unit_text=weight_unit_text) \
            if goods_rate else "-"
        data_dict["weight_unit_text"] = weight_unit_text
        return data_dict

    @classmethod
    def get_clearing_goods(cls, session, clearing_id):
        """ 获取结款记录对应的货品 """
        ShopSupplierClearingGoods = models.ShopSupplierClearingGoods

        clearing_goods_id_list = session \
            .query(ShopSupplierClearingGoods.goods_id) \
            .filter_by(clearing_id=clearing_id) \
            .all()
        clearing_goods_id_list = {x.goods_id for x in clearing_goods_id_list}

        goods_name_dict = GoodsFunc.get_goods_name_through_id_list(
            session, clearing_goods_id_list)

        goods_list = []
        for goods_id in clearing_goods_id_list:
            goods_name = goods_name_dict.get(goods_id, "")
            goods_list.append({"goods_id": goods_id, "goods_name": goods_name})
        return goods_list

    @classmethod
    def divide_borrows_under_clearing(cls, session, clearing):
        """ 获取一条结算下的各项预支类型的总和 """
        data_dict = {}
        # 支出信息
        ShopSupplierBorrowing = models.ShopSupplierBorrowing
        borrow_money = session \
            .query(func.sum(
                ShopSupplierBorrowing.borrow_cent),
                ShopSupplierBorrowing.borrow_type)\
            .filter_by(clearing_record_id=clearing.id)\
            .group_by(ShopSupplierBorrowing.borrow_type)\
            .all()
        type_map = {
            1: "supplier_borrow_money",
            2: "unload_money",
            3: "delivery_money",
            4: "park_money",
            5: "other_money",
            6: "business_money",
            7: "freeze_momey",
            8: "tricycle_money",
            9: "load_money",
            10: "overweight_money",
        }

        # 初始化各项值
        for key in type_map.values():
            data_dict[key] = 0

        # 兼容旧数据
        data_dict["other_money"] += clearing.other_value_cent

        # 计算各项支出总和
        for borrow_cent, borrow_type in borrow_money:
            key_name = type_map.get(borrow_type)
            data_dict[key_name] += borrow_cent

        # 处理精度
        for key in type_map.values():
            data_dict[key] = check_float(data_dict[key] / 100)
        return data_dict

    @classmethod
    def list_clearing_sum_each_day(cls, session, sales_record_id_list,
                                   borrow_id_list, data_type):
        """ 获取结算的（货款金额/赊账金额/还款金额/预支金额）每日汇总数据

        :param sale_record_list: 销售流水id列表
        :param borrow_record_list: 预支流水id列表
        """
        GoodsSalesRecord = models.GoodsSalesRecord
        group_by_column = models.GoodsSalesRecord.bill_date \
            if data_type == 0 else GoodsSalesRecord.account_period_date
        sales_query_base = session.query(
            group_by_column,
            *GoodsSalesRecord.list_query_param_summary(["sales_money"]))
        params_filter = [GoodsSalesRecord.id.in_(sales_record_id_list)]

        # 销售日分组
        sales_each_day = sales_query_base \
            .filter(*params_filter).group_by(group_by_column) \
            .all() if sales_record_id_list else []

        # 赊账日分组
        params_filter.append(GoodsSalesRecord.pay_type == 9)
        debt_each_day = sales_query_base \
            .filter(*params_filter).group_by(group_by_column) \
            .all() if sales_record_id_list else []

        # 还款
        payback_records = session \
            .query(
                models.GoodsSalesRecordPaybackRecord.id,
                models.GoodsSalesRecordPaybackRecord.create_time,
                func.IF(
                    models.GoodsSalesRecordPaybackRecord.payback_cent >= models.GoodsSalesRecord.sales_money,
                    models.GoodsSalesRecord.sales_money,
                    models.GoodsSalesRecordPaybackRecord.payback_cent).label("money")) \
            .join(
                models.GoodsSalesRecord,
                models.GoodsSalesRecord.id == models.GoodsSalesRecordPaybackRecord.goods_sale_record_id) \
            .filter(
                models.GoodsSalesRecordPaybackRecord.goods_sale_record_id.in_(sales_record_id_list),
                models.GoodsSalesRecordPaybackRecord.status == 1) \
            .all() if sales_record_id_list else []

        # 预支日分组
        group_by_column = func.date(models.ShopSupplierBorrowing.create_date) \
            if data_type == 0 else models.ShopSupplierBorrowing.account_period_date
        borrow_each_day = session \
            .query(
                group_by_column,
                func.sum(models.ShopSupplierBorrowing.borrow_cent).label("borrow_cent")) \
            .filter(models.ShopSupplierBorrowing.id.in_(borrow_id_list)) \
            .all() if borrow_id_list else []

        sales_dict = cls._parse_sales(sales_each_day, source="sales")
        debt_dict = cls._parse_sales(debt_each_day, source="debt")
        payback_dict = cls._parse_payback(session, payback_records, data_type)
        borrow_dict = cls._parse_borrow(borrow_each_day)

        result = cls._merge(sales_dict, debt_dict, payback_dict, borrow_dict)
        return result

    @classmethod
    def _parse_sales(cls, sales_each_day, source="sales"):
        """ 流水日分组解析

        :param source: sales销售, debt:赊账
        """
        result = {}
        for sale in sales_each_day:
            result[sale[0]] = {source: check_float(sale.sales_money / 100)}
        return result

    @classmethod
    def _parse_payback(cls, session, payback_records, data_type):
        """ 还款解析 """
        result = {}
        if data_type == 1:
            # 扎帐日数据从pb_order取账期日
            payback_record_ids = {p.id for p in payback_records}
            payback_order = session \
                .query(
                    models.GoodsSalesRecordPaybackRecord.create_time,
                    models.PbOrder.account_period_date) \
                .join(
                    models.PbOrder,
                    models.GoodsSalesRecordPaybackRecord.pb_order_id == models.PbOrder.id) \
                .filter(models.GoodsSalesRecordPaybackRecord.id.in_(payback_record_ids)) \
                .all()
            map_payback_date = {k: v for k, v in payback_order}

        for payback in payback_records:
            if data_type == 1:
                payback_date = map_payback_date[payback.create_time]
            else:
                payback_date = payback.create_time.date()
            result.setdefault(payback_date, {"payback": 0})
            result[payback_date]["payback"] += check_float(payback.money / 100)
        return result

    @classmethod
    def _parse_borrow(cls, borrow_each_day):
        """ 预支解析 """
        result = {}
        for borrow in borrow_each_day:
            result[borrow[0]] = {
                "borrow": check_float(borrow.borrow_cent / 100)
            }
        return result

    @classmethod
    def _merge(cls, sales_dict, debt_dict, payback_dict, borrow_dict):
        """ 同样结构字典合并 """
        daily_dict = {}
        for target in [sales_dict, debt_dict, payback_dict, borrow_dict]:
            for key, val in target.items():
                daily_dict.setdefault(
                    key, dict(sales=0, debt=0, payback=0, borrow=0))
                daily_dict[key].update(val)
        result_tmp = []
        for key, val in daily_dict.items():
            val.update({"date": key.strftime("%Y-%m-%d")})
            result_tmp.append(val)
        result = sorted(result_tmp, key=lambda r: r["date"], reverse=True)
        return result


# 费率信息
class GoodsRateFunc():
    @classmethod
    def get_rate_type_text(cls, goods_rate, fee_text="", weight_unit_text="斤"):
        '''
            费率转换为文字显示
            费率为多选，结果可能有多个
        '''
        rate_list = []
        if goods_rate.piece_active == 1:
            rate_type_text = "每件%s元" % (check_float(
                goods_rate.piece_cent / 100))
            rate_list.append(rate_type_text)
        if goods_rate.weight_active == 1:
            rate_type_text = "每%s%s元" % (
                weight_unit_text,
                check_float(
                    goods_rate.get_weight_cent(
                        weight_unit_text=weight_unit_text) / 100))
            rate_list.append(rate_type_text)
        if goods_rate.amount_active == 1:
            rate_type_text = "按总金额的%s%%" % (check_float(
                goods_rate.amount_cent / 100))
            rate_list.append(rate_type_text)
        if goods_rate.commission_active == 1:
            rate_type_text = "不再扣费"
            rate_list.append(rate_type_text)
        if goods_rate.bottom_price_active:
            rate_type_text = "按每日底价结算"
            rate_list.append(rate_type_text)
        rate_text = "+".join(rate_list)
        return rate_text

    @classmethod
    def get_goods_rate_through_id(cls, session, shop_id, rate_id):
        """ 根据费率ID获取商品费率 """
        goods_rate = session.query(models.ClearGoodsRate) \
                            .filter_by(id=rate_id, shop_id=shop_id) \
                            .first()
        return goods_rate


# 店铺客户
class ShopCustomerFunc():
    @classmethod
    def debtor_text(cls, debtor_type):
        text_dict = {1: "个体", 2: "超市", 3: "连锁"}
        return text_dict.get(debtor_type, "")

    @classmethod
    def get_shop_customer_through_phone(cls, session, shop_id, phone):
        """
            根据手机号获取客户
        """
        ShopCustomer = models.ShopCustomer
        shop_customer = session.query(models.ShopCustomer) \
            .filter_by(shop_id=shop_id, phone=phone) \
            .filter(ShopCustomer.status != -1) \
            .first()
        return shop_customer

    @classmethod
    def get_shop_customer_through_number(cls, session, shop_id, number):
        """
            根据客户编号获取客户
        """
        ShopCustomer = models.ShopCustomer
        shop_customer = session.query(models.ShopCustomer) \
            .filter_by(shop_id=shop_id, number=number) \
            .filter(ShopCustomer.status != -1) \
            .first()
        return shop_customer

    @classmethod
    def get_shop_customer_through_id_number(cls, session, shop_id, id_number):
        """
            根据客户身份证号获取客户
        """
        ShopCustomer = models.ShopCustomer
        shop_customer = session\
            .query(models.ShopCustomer) \
            .filter_by(shop_id = shop_id,id_number=id_number) \
            .filter(ShopCustomer.status!=-1) \
            .first()
        return shop_customer

    @classmethod
    def get_shop_customer_through_id(cls, session, shop_id, customer_id):
        """
            根据客户ID获取客户
        """
        ShopCustomer = models.ShopCustomer
        shop_customer = session.query(ShopCustomer) \
            .filter_by(id=customer_id, shop_id=shop_id) \
            .filter(ShopCustomer.status != -1) \
            .first()
        return shop_customer

    @classmethod
    def get_shop_customer_phone_through_id(cls, session, shop_id, customer_id):
        """
            根据客户ID获取客户手机号
        """
        ShopCustomer = models.ShopCustomer
        shop_customer_phone = session.query(ShopCustomer.phone) \
            .filter_by(id=customer_id, shop_id=shop_id) \
            .filter(ShopCustomer.status != -1) \
            .first()
        if shop_customer_phone:
            phone = shop_customer_phone.phone
        else:
            phone = ""
        return phone

    @classmethod
    def get_shop_customer_through_name(cls, session, shop_id, name):
        """
            根据客户姓名获取客户
        """
        ShopCustomer = models.ShopCustomer
        shop_customer = session.query(ShopCustomer) \
            .filter_by(name=name, shop_id=shop_id) \
            .filter(ShopCustomer.status != -1) \
            .first()
        return shop_customer

    @classmethod
    def get_shop_customer_name_through_id(cls, session, customer_id):
        """
            根据客户ID获取客户姓名
        """
        ShopCustomer = models.ShopCustomer
        customer_name = session \
            .query(ShopCustomer.name) \
            .filter_by(id=customer_id) \
            .filter(ShopCustomer.status != -1) \
            .scalar() or ""
        return customer_name

    @classmethod
    def update_purchase_info(cls,
                             session,
                             customer_id,
                             shop_id,
                             purchase_money=0,
                             purchase_id=0):
        """
            更新用户采购金额/采购次数
        """

        if purchase_money < 0:
            delta_purchase_times = 0
        else:
            delta_purchase_times = 1

        shop_customer = cls.get_shop_customer_through_id(
            session, shop_id, customer_id)
        if shop_customer:
            shop_customer.purchase_times += delta_purchase_times
            shop_customer.purchase_cent += purchase_money

        # 如果是企业客户采购员更新信息的话，需要更新采购员的采购数据
        if purchase_id:
            customer_purchase = models.CustomerPurchase.get_by_id(
                session, purchase_id)
            if customer_purchase:
                customer_purchase.purchase_times += 1
                customer_purchase.purchase_cent += purchase_money
        session.flush()

    @classmethod
    def get_shop_customer_name_through_id_list(cls, session, customer_id_list):
        """
            批量获取客户姓名
            customer_id_list: 用户ID组成的数组
        """
        name_dict = {}
        if customer_id_list:
            ShopCustomer = models.ShopCustomer
            shop_customer = session.query(ShopCustomer.id, ShopCustomer.name) \
                .filter(ShopCustomer.id.in_(customer_id_list)) \
                .all()
            name_dict = {x.id: x.name for x in shop_customer}
        return name_dict

    @classmethod
    def get_shop_customer_through_id_list(cls, session, customer_id_list):
        """ 批量获取客户

        :param customer_id_list: 用户ID组成的数组

        :rtype: dict
        """
        result = {}
        if customer_id_list:
            ShopCustomer = models.ShopCustomer
            shop_customer = session.query(ShopCustomer)\
                .filter(ShopCustomer.id.in_(customer_id_list))\
                .all()
            result = {c.id: c for c in shop_customer}
        return result

    @classmethod
    def purchase_point(cls, session, shop_id, shop_customer, order,
                       operator_id):
        """
            采购操作增加积分入口
        """
        rule = session.query(models.PointRule) \
            .filter(models.PointRule.shop_id == shop_id, models.PointRule.active == 1) \
            .first()
        if not rule:
            return

        # 1. 采购额积分增长
        if rule.purchases:
            point_delta = order.fact_total_price // 100 * rule.purchases_point
            cls.update_point(
                session,
                shop_id,
                shop_customer,
                point_delta,
                operator_id,
                operate_type=1,
                rule_id=rule.id)

        if rule.weekly_purchase or rule.monthly_purchase:
            Order = models.Order
            ShopCustomerPointHistory = models.ShopCustomerPointHistory
            # 排除退款订单
            query = session.query(Order.create_time) \
                .filter(Order.shop_id == shop_id, Order.debtor_id == shop_customer.id, Order.status == 2,
                        Order.record_type == 0)

            # 1. 周采购天数积分增长
            if rule.weekly_purchase:
                today = datetime.date.today()
                # 本周第一天
                first_day_this_week = today + datetime.timedelta(
                    days=-today.weekday())
                first_day_this_week = datetime.datetime.fromordinal(
                    first_day_this_week.toordinal())

                # 如果本周加过就不再加
                point_history = session.query(ShopCustomerPointHistory) \
                    .filter(ShopCustomerPointHistory.operate_type == 3) \
                    .filter(ShopCustomerPointHistory.create_time > first_day_this_week) \
                    .filter(ShopCustomerPointHistory.shop_id == shop_id,
                            ShopCustomerPointHistory.customer_id == shop_customer.id) \
                    .first()
                if not point_history:
                    # 本周所有订单的日期
                    order_dates_this_week = query.filter(
                        Order.create_time > first_day_this_week).all()
                    # 本周产生了订单的天数
                    days_purchased_this_week = len(
                        {d[0].date(): ""
                         for d in order_dates_this_week})
                    # 采购天数达标则增加积分
                    if days_purchased_this_week >= rule.weekly_purchase_floor:
                        cls.update_point(
                            session,
                            shop_id,
                            shop_customer,
                            rule.weekly_purchase_point,
                            operator_id,
                            operate_type=3,
                            rule_id=rule.id)

            # 1. 月采购积分增长
            if rule.monthly_purchase:
                today = datetime.date.today()
                # 本月第一天
                first_day_this_month = today + datetime.timedelta(
                    days=-today.day + 1)
                first_day_this_month = datetime.datetime.fromordinal(
                    first_day_this_month.toordinal())

                # 如果本月加过就不再加
                point_history = session.query(ShopCustomerPointHistory) \
                    .filter(ShopCustomerPointHistory.operate_type == 4) \
                    .filter(ShopCustomerPointHistory.create_time > first_day_this_month) \
                    .filter(ShopCustomerPointHistory.shop_id == shop_id,
                            ShopCustomerPointHistory.customer_id == shop_customer.id) \
                    .first()
                if not point_history:
                    # 本月所有订单的日期
                    order_dates_this_month = query.filter(
                        Order.create_time > first_day_this_month).all()
                    # 本月产生了订单的天数
                    days_purchased_this_month = len(
                        {d[0].date(): ""
                         for d in order_dates_this_month})
                    # 采购天数达标则增加积分
                    if days_purchased_this_month >= rule.monthly_purchase_floor:
                        cls.update_point(
                            session,
                            shop_id,
                            shop_customer,
                            rule.monthly_purchase_point,
                            operator_id,
                            operate_type=4,
                            rule_id=rule.id)

    @classmethod
    def update_point(cls,
                     session,
                     shop_id,
                     shop_customer,
                     delta,
                     operator_id,
                     operate_type=0,
                     rule_id=0):
        """
            :param delta: 积分变更量
            :param operate_type: 参照 models.ShopCustomerPointHistory.operate_type
        """
        shop_customer.point += delta
        # 记录变更历史
        new_history = models.ShopCustomerPointHistory(
            customer_id=shop_customer.id,
            shop_id=shop_id,
            delta=delta,
            points=shop_customer.point,
            operator_id=operator_id,
            operate_type=operate_type,
            rule_id=rule_id)
        session.add(new_history)
        session.flush()

    @classmethod
    def get_shop_customer_history_debt_rest(cls, session, customer_id):
        """
            根据客户ID获取客户姓名
        """
        ShopCustomer = models.ShopCustomer
        history_debt_rest = session.query(ShopCustomer.history_debt_rest) \
                                .filter_by(id=customer_id) \
                                .scalar() or 0
        return history_debt_rest

    # 格式化客户信息
    @classmethod
    def format_customer_info(cls, session, customer_list, type="list"):
        CustomerBlackList = models.CustomerBlackList
        CustomerPurchase = models.CustomerPurchase
        DebtHistory = models.DebtHistory
        # 获取客户id
        black_customer_ids = []
        customer_ids = []
        for i in customer_list:
            customer_info = i if isinstance(i, models.ShopCustomer) else i[0]
            customer_ids.append(customer_info.id)
            if customer_info.status == -2:
                black_customer_ids.append(customer_info.id)

        # 搜集客户的采购员数量
        all_purchase_number = session\
            .query(CustomerPurchase.shop_customer_id,
                   func.count(CustomerPurchase.id))\
            .filter(CustomerPurchase.status!=-1,
                    CustomerPurchase.shop_customer_id.in_(customer_ids))\
            .group_by(CustomerPurchase.shop_customer_id)\
            .all()
        purchase_number_dict = {x[0]: x[1] for x in all_purchase_number}

        # 查询客户的拉黑信息
        black_list = session\
            .query(CustomerBlackList)\
            .filter(CustomerBlackList.customer_id.in_(black_customer_ids),
                    CustomerBlackList.status==0)\
            .all()
        black_list_dict = {x.customer_id: x for x in black_list}

        # 客户是否有历史欠款
        debt_historys = session\
            .query(DebtHistory.debtor_id)\
            .filter(DebtHistory.debt_type==3,
                    DebtHistory.debtor_id.in_(customer_ids))\
            .all()
        debt_history_ids = [x.debtor_id for x in debt_historys]
        data_list = []
        for i in customer_list:
            customer_info = i if isinstance(i, models.ShopCustomer) else i[0]
            purchase_info = None if isinstance(i,
                                               models.ShopCustomer) else i[1]
            # 查询担保人和创建人名字
            creator_id = customer_info.creator_id
            sponsor_id = customer_info.sponsor_id
            creator_name = AccountFunc.get_account_name(
                session, creator_id) if creator_id else ""
            sponsor_name = AccountFunc.get_account_name(
                session, sponsor_id) if sponsor_id else ""
            black_info = black_list_dict.get(customer_info.id, None)
            defriend_time = ""
            defriend_reason = ""
            defriend_operator_id = 0
            defriend_operator_name = ""
            if black_info:
                defriend_time = TimeFunc.time_to_str(black_info.edit_time,
                                                     "date")
                defriend_reason = black_info.reason
                defriend_operator_id = black_info.edit_user_id
                defriend_operator_name = AccountFunc.get_account_name(
                    session,
                    defriend_operator_id) if defriend_operator_id else ""
            customer_group_orm = CustomerGroupORM(session)
            group_id = customer_info.group_id
            group_name, = customer_group_orm.get_group_name_through_id(
                group_id)

            data = dict(
                customer_id=customer_info.id,
                phone=customer_info.phone or "",
                name=customer_info.name,
                company=customer_info.company,
                number=customer_info.number or "",
                debt_rest=check_float(customer_info.debt_rest / 100),
                customer_type=customer_info.debtor_type,
                customer_type_text=customer_info.debtor_type_text,
                sex=customer_info.sex,
                sex_text=customer_info.sex_text,
                purchase_times=customer_info.purchase_times,
                purchase_cent=check_float(customer_info.purchase_cent / 100),
                source=customer_info.source,
                balance=check_float(customer_info.balance / 100),
                purchase_id=purchase_info.id if purchase_info else 0,
                purchase_name=purchase_info.name if purchase_info else "",
                purchase_phone=purchase_info.phone if purchase_info else "",
                history_debt_rest=check_float(
                    customer_info.history_debt_rest / 100),
                point=customer_info.point,
                source_text=customer_info.source_text,
                short_name=customer_info.short_name,
                industry_type=customer_info.industry_type,
                industry_type_text=customer_info.industry_type_text,
                remark=customer_info.remark,
                creator_id=creator_id,
                creator_nickname=creator_name,
                create_time=TimeFunc.time_to_str(customer_info.create_time),
                credit_limit=check_float(customer_info.credit_limit / 100),
                sponsor_id=sponsor_id,
                sponsor_name=sponsor_name,
                birthday=TimeFunc.time_to_str(customer_info.birthday, "date"),
                business_licence=customer_info.business_licence,
                credit_code=customer_info.credit_code,
                customer_label=customer_info.customer_label,
                customer_label_text=customer_info.customer_label_text,
                id_number=customer_info.id_number,
                address=customer_info.address,
                proprietor_id_card_copy=customer_info.proprietor_id_card_copy,
                authorised_letter=customer_info.authorised_letter,
                purchase_id_card_copy=customer_info.purchase_id_card_copy,
                legal_representative=customer_info.legal_representative,
                legal_representative_id_card_copy=customer_info.
                legal_representative_id_card_copy,
                fingerprint_1=customer_info.fingerprint_1,
                fingerprint_2=customer_info.fingerprint_2,
                id_card_front=customer_info.id_card_front,
                status=customer_info.status,
                purchase_number=purchase_number_dict.get(customer_info.id, 0),
                defriend_time=defriend_time,
                defriend_reason=defriend_reason,
                defriend_operator_id=defriend_operator_id,
                defriend_operator_name=defriend_operator_name,
                has_debt=1 if customer_info.id in debt_history_ids else 0,
                group_id=group_id,
                group_name=group_name,
            )
            data_list.append(data)
        if type == "dict":
            return data_list[0]
        return data_list

    # 格式化客户基本信息
    @classmethod
    def format_customer_info_basic(cls, customer_list):
        result = []
        for customer in customer_list:
            data = dict(
                customer_id=customer.id,
                name=customer.name,
                industry_type_text=customer.industry_type_text,
                remark=customer.remark,
                industry_type=customer.industry_type,
                short_name=customer.short_name,
                status=customer.status,
            )
            result.append(data)
        return result

    # 获取赊账客户信息
    @classmethod
    def get_customer_info_through_id_list(self, session, shop_id,
                                          customer_id_list):
        ShopCustomer = models.ShopCustomer
        debtor_list = session \
            .query(ShopCustomer) \
            .filter_by(shop_id=shop_id) \
            .filter(ShopCustomer.id.in_(customer_id_list)) \
            .all()
        customer_info_dict = {
            x.id: {
                "name": x.name,
                "phone": x.phone or "",
                "number": x.number or "",
                "company": x.company,
                "accountant_id": x.creator_id,
                "debtor_type": x.debtor_type,
                "status": x.status
            }
            for x in debtor_list
        }
        return customer_info_dict

    #获取债务清单对应订单信息
    @classmethod
    def get_debtor_order_info(self, session, shop_id, history):
        """ 获取债务清单对应订单信息

        :return: 支付详情的文字 + 能否被撤销
        :rtype: tuple
        """
        debt_type = history.debt_type
        fk_id = history.fk_id
        remark = history.remark
        # 条码支付无法撤销
        can_be_reset = 1
        if debt_type == 0:
            Order = models.Order
            debt_order = session.query(Order) \
                                .filter_by(id=fk_id) \
                                .filter_by(shop_id=shop_id) \
                                .first()
            order_info = debt_order.num
        elif debt_type == 1:
            PbOrder = models.PbOrder
            debt_order = session.query(PbOrder) \
                                .filter_by(id=fk_id) \
                                .filter_by(shop_id=shop_id) \
                                .first()
            pay_type = debt_order.pay_type
            if pay_type in [3, 4]:
                can_be_reset = 0
            if pay_type == 10:
                combine_pay_dict = OrderBaseFunc.multi_combine_pay_type_text(
                    session, [fk_id], source_type="pb_order")
                pb_order_money = session.query(
                    models.PbOrderMoney).filter_by(id=fk_id).first()
                if pb_order_money and (pb_order_money.barcode_wx_cent > 0
                                       or pb_order_money.barcode_ali_cent > 0):
                    can_be_reset = 0
                order_info = OrderBaseFunc.order_pay_type_text(
                    fk_id, pay_type, combine_pay_dict)
            else:
                order_info = OrderBaseFunc.pay_type_text(pay_type) + "还款"
        elif debt_type == 2:
            order_info = "赊账退还"
        elif debt_type == 3:
            order_info = "欠款导入"
            if remark:
                order_info += "（%s）" % remark
        return order_info, can_be_reset

    @classmethod
    def get_customers_under_group(cls, session, group_id, page=-1):
        """ 获取分组下所有客户

        :param page: -1: 获取所有客户, >0: 获取特定分组
        """
        query_base = session \
            .query(models.ShopCustomer) \
            .filter_by(group_id=group_id) \
            .filter(models.ShopCustomer.status.in_([0, -2]))
        if page < 0:
            shop_customers = query_base.all()
            return shop_customers

        page_size = 20
        shop_customers = query_base.offset(
            page * page_size).limit(page_size).all()
        nomore = 0
        if len(shop_customers) < page_size:
            nomore = 1
        return shop_customers, nomore

    @classmethod
    def get_customer_by_id_list(cls, session, customer_id_list):
        ShopCustomer = models.ShopCustomer
        customers = session \
            .query(ShopCustomer) \
            .filter(ShopCustomer.id.in_(customer_id_list),
                    ShopCustomer.status!=-1) \
            .all()
        return customers

    @classmethod
    def get_purchase_by_id(cls, session, purchase_id):
        purchase = session \
            .query(models.CustomerPurchase)\
            .filter_by(id=purchase_id)\
            .first()
        return purchase


# 域名缩短
class UrlShorten():
    code_map = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
                'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
                'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                'Y', 'Z')

    @classmethod
    def get_md5(self, longurl):
        longurl = longurl.encode('utf8') if isinstance(longurl,
                                                       str) else longurl
        m = hashlib.md5()
        m.update(longurl)
        return m.hexdigest()

    @classmethod
    def get_hex(self, key):
        hkeys = []
        hex = self.get_md5(key)
        for i in range(0, 1):
            n = int(hex[i * 8:(i + 1) * 8], 16)
            v = []
            e = 0
            for j in range(0, 8):
                x = 0x0000003D & n
                e |= ((0x00000002 & n) >> 1) << j
                v.insert(0, self.code_map[x])
                n = n >> 6
            e |= n << 5
            v.insert(0, self.code_map[e & 0x0000003D])
            hkeys.append("".join(v))
        return hkeys[0]

    @classmethod
    def get_short_url(self, long_url):
        url = self.session.query(
            models.ShortUrl).filter_by(long_url=long_url).first()
        if url:
            short_url = url.short_url
            self.session.commit()
            return short_url
        else:
            hkeys = []
            hex = self.get_md5(long_url)
            for i in range(0, 1):
                n = int(hex[i * 8:(i + 1) * 8], 16)
                v = []
                e = 0
                for j in range(0, 8):
                    x = 0x0000003D & n
                    e |= ((0x00000002 & n) >> 1) << j
                    v.insert(0, self.code_map[x])
                    n = n >> 6
                e |= n << 5
                v.insert(0, self.code_map[e & 0x0000003D])
                hkeys.append("".join(v))
            url = models.ShortUrl(short_url=hkeys[0], long_url=long_url)
            self.session.add(url)
            self.session.commit()
            return hkeys[0]

    @classmethod
    def get_long_url(self, short_url):
        url = self.session.query(
            models.ShortUrl).filter_by(short_url=short_url).first()
        if not url:
            self.session.close()
            return False
        long_url = url.long_url
        self.session.commit()
        return long_url


# 数据导出
class DataExportFunc():

    # 数据导出权限检查
    def check_export_permission(permission_type):
        def decorator(func):
            def wrapper(*args, **kw):
                self = args[0]
                session = self.session
                shop_id = self.current_shop.id
                user_id = self.current_user.id
                config = ShopFunc.get_config(session, shop_id)
                # 数据导出权限检查
                if "dataexport" not in self.current_user.permission_list:
                    return self.send_error(403, error_msg="您没有数据导出权限")
                DataExportLog = models.DataExportLog
                log = DataExportLog(
                    shop_id=shop_id,
                    export_user_id=user_id,
                    export_content=permission_type)
                session.add(log)
                session.commit()
                return func(*args, **kw)

            return wrapper

        return decorator

    # 导出数据类型
    @classmethod
    def get_data_type_text(cls, data_type):
        data_type_text_dict = {0: "自然日", 1: "扎账日"}
        data_type_text = data_type_text_dict.get(data_type, "")
        return data_type_text

    # 有时间区间的数据导出，时间的处理
    @classmethod
    def check_input_date(cls, from_date, to_date):
        """ 有时间区间的数据导出，时间的处理
        具体的处理方式为:
        传了时间区间(开始和结束都传), 就导出时间区间内的数据
        其他情况，都导出七天的数据(开始和结束都传, 最近七天; 只传开始, 开始日期到七天后; 只传结束, 结束日期到七天前)

        :param from_date: 开始日期
        :param to_date: 结束日期

        :rtype: tuple
        """
        try:
            if from_date:
                from_date = datetime.datetime.strptime(from_date,
                                                       '%Y-%m-%d').date()
            if to_date:
                to_date = datetime.datetime.strptime(to_date, '%Y-%m-%d')
                # to_date 加上一天的偏移量，即支持当天到当天的形式
                to_date = (to_date + datetime.timedelta(days=1)).date()
        except BaseException:
            raise ValueError("日期传入参数格式错误，应为`yyyy-mm-dd`")
        if from_date and to_date and from_date > to_date:
            raise ValueError("参数错误，`from_date`应该小于`to_date`")

        if not from_date and not to_date:
            from_date = datetime.date.today() - datetime.timedelta(days=7)
            to_date = datetime.date.today() + datetime.timedelta(days=1)
        elif from_date and not to_date:
            to_date = from_date + datetime.timedelta(days=8)
        elif not from_date and to_date:
            from_date = to_date - datetime.timedelta(days=8)

        return from_date, to_date


# 用户权限数据处理
class PermissionFunc():
    @classmethod
    def permission_text_list(cls, role, permission_list):
        if role == "boss":
            permission_dict = {
                "1": "historydata",
                "2": "addeditstaff",
                "3": "supplierclearing",
                "4": "editreceipt",
                "5": "dataexport",
                "6": "addgoods",
                "7": "editgoods",
                "8": "editsupplier",
                "9": "accountingperiod",
                "10": "update_point",
                "11": "history_debt",
                "12": "deletegoods",
                "13": "viewsalaryrecord",
                "14": "defriendcustomer"
            }
        elif role == "accountant":
            permission_dict = {
                "1": "cashier_data",
                "2": "debtdetailexport",
                "3": "cancelinoutrecord",
                "4": "editpaytype",
                "5": "manual_order_paid",
                "6": "defriendcustomer",
                "7": "cancelpayback",
                "8": "orderprint"
            }
        elif role == "salesman":
            permission_dict = {
                "1": "addgoods",
                "2": "editgoods",
                "3": "setprint",
                "4": "addcustomer"
            }
        elif role == "goods_owner":
            permission_dict = {
                "1": "sell_goods_others",
                "2": "edit_goods_others"
            }
        else:
            permission_dict = {}
        permission_text_list = []
        for permission_code in permission_list:
            permission_text = permission_dict.get(permission_code, "")
            if permission_text:
                permission_text_list.append(permission_text)
        return permission_text_list

    @classmethod
    def loads_permission_list(cls, permission_list):
        """ 将str类型的权限解压成list

        :param permission_list: 字符串格式的权限

        :rtype: list
        """
        permission_list = permission_list.split(",") if permission_list else []
        return permission_list

    @classmethod
    def dumps_permission_list(cls, permission_list):
        """ 将list类型的权限压缩成str便于存储

        :param permission_list: 列表格式的权限

        :rtype: str
        """
        permission_list = DataFormatFunc.format_int_to_str_inlist(
            permission_list)
        permission_list.sort()
        permission_list = ",".join(permission_list)
        if not permission_list:
            permission_list = ""
        return permission_list


# 各类记录打印日志
class RecordPrintFunc():
    @classmethod
    def record_print_log(cl,
                         session,
                         shop_id,
                         fk_id,
                         user_id,
                         print_result,
                         error_txt,
                         printer_num,
                         print_type=0,
                         source_type=0):
        RecordPrintLog = models.RecordPrintLog
        print_log = RecordPrintLog(
            shop_id=shop_id,
            fk_id=fk_id,
            user_id=user_id,
            print_result=print_result,
            error_txt=error_txt,
            printer_num=printer_num,
            print_type=print_type,
            source_type=source_type)
        session.add(print_log)


# 扎账
class AccountingPeriodFunc():

    # #是否有指定日期的扎账记录
    # @classmethod
    # def get_accounting_time(cls,session,shop_id,choose_date=""):
    #     if not choose_date:
    #         choose_date = datetime.datetime.now().date()
    #     AccountingPeriodRecord = models.AccountingPeriodRecord
    #     accounting_time = session.query(AccountingPeriodRecord.create_date)\
    #                             .filter_by(shop_id=shop_id,record_date=choose_date)\
    #                             .scalar()
    #     return accounting_time

    # 获取扎账人/扎账具体时间
    @classmethod
    def get_accounting_info(cls, session, shop_id, date):
        AccountingPeriodRecord = models.AccountingPeriodRecord
        accounting_record = session.query(AccountingPeriodRecord) \
            .filter_by(shop_id=shop_id, record_date=date) \
            .first()
        accounting_info = {}
        if accounting_record:
            accounting_info["username"] = AccountFunc.get_account_realname(
                session,
                accounting_record.user_id) if accounting_record.user_id else ""
            accounting_info["time"] = TimeFunc.time_to_str(
                accounting_record.create_date, "time")
            accounting_info[
                "accounting_model"] = 1 if accounting_record.user_id else 2
        return accounting_info

    # 是否有指定日期的扎账记录并检查是否需要自动扎账
    @classmethod
    def get_accounting_time(cls, session, shop_id, choose_date=""):
        now_date = datetime.datetime.now()
        if not choose_date:
            choose_date = now_date.date()

        # 查询扎账时间
        AccountingPeriodRecord = models.AccountingPeriodRecord
        accounting_time = session.query(AccountingPeriodRecord.create_date) \
                              .filter_by(shop_id=shop_id, record_date=choose_date) \
                              .scalar() or None
        return accounting_time

    # 获取自动扎账时间
    @classmethod
    def get_auto_accounting_time(cls, session, shop_id):
        AutoAccountantTimeRecord = models.AutoAccountantTimeRecord
        auto_time = session.query(AutoAccountantTimeRecord.auto_time) \
            .filter_by(shop_id=shop_id, status=1) \
            .scalar()
        if auto_time:
            return TimeFunc.time_to_str(auto_time, "hour")
        else:
            return ""

    # 开启自动扎账定时任务
    @classmethod
    def open_auto_accounting_task(cls, session, shop_id):
        auto_time_record = session.query(models.AutoAccountantTimeRecord) \
            .filter_by(shop_id=shop_id, status=1) \
            .first()
        if auto_time_record:
            time_record_id = auto_time_record.id
            auto_time = auto_time_record.auto_time
            from handlers.celery_auto_accounting import auto_accounting

            # 计算任务执行时间，暂时使用countdown来处理
            time_now = datetime.datetime.now()
            time_auto = datetime.datetime(time_now.year, time_now.month,
                                          time_now.day, auto_time.hour,
                                          auto_time.minute, auto_time.second)
            if auto_time < time_now.time():
                time_auto = time_auto + datetime.timedelta(days=1)
            time_now_timestamp = int(time_now.timestamp())
            time_auto_timestamp = int(time_auto.timestamp())
            countdown = time_auto_timestamp - time_now_timestamp

            # 开启新的celery任务
            auto_accounting.apply_async(
                args=[shop_id, time_record_id], countdown=countdown)


# 员工信息
class StaffFunc():

    # 获取员工备注
    @classmethod
    def get_hirelink_name_through_account_id(cls, session, shop_id,
                                             account_id):
        HireLink = models.HireLink
        name = session.query(HireLink.name_remark).filter_by(
            shop_id=shop_id, id=account_id).scalar() or ""
        return name

    # 获取员工备注
    @classmethod
    def get_hirelink_name_through_account_id_list(cls, session, shop_id,
                                                  account_id_list):
        HireLink = models.HireLink
        name_list = session.query(HireLink.account_id, HireLink.name_remark) \
            .filter_by(shop_id=shop_id) \
            .filter(HireLink.account_id.in_(account_id_list)) \
            .all()
        name_dict = {x.account_id: x.name_remark for x in name_list}
        return name_dict

    @classmethod
    def is_salesman_goods_owner(cls, session, shop_id, staff_id):
        """ 检查一个开票员是否是货主(现在维护的是货主作为一种特殊的开票员)

        :param staff_id: 员工id
        :param shop_id: 店铺id

        :return: True, 是货主, False, 不是货主，仅仅是开票员
        :rtype: bool
        """
        hirelink = session.query(models.HireLink) \
            .filter_by(shop_id=shop_id, account_id=staff_id).first()
        if not hirelink or hirelink.active_salesman != 1:
            raise ValueError("非有效开票员")
        if hirelink.active_goods_owner == 1:
            return 1
        return 0

    @classmethod
    def assign_default_printers(cls, session, hire_link):
        """ 将店铺设置的默认打印机分配给员工, session不在方法内部commit, 仅完成操作

        :param hire_link: 员工雇佣对象
        :param defualt_printers: list, 默认打印机列表

        :rtype: None
        """
        default_printers = PrinterFunc.list_default_printers(
            session, hire_link.shop_id)
        for printer in default_printers:
            if printer.shop_id != hire_link.shop_id:
                raise ValueError("打印机店铺与员工店铺不相同")
            if printer.is_salesman_default and hire_link.active_salesman == 1 or \
                    printer.is_accountant_default and hire_link.active_accountant == 1 or \
                    printer.is_admin_default and hire_link.active_admin == 1:
                PrinterFunc.gen_printer_assign_link(session, printer.id,
                                                    hire_link.account_id)
        return None

    @classmethod
    def get_hirelink(cls, session, type="list", **params):
        HireLink = models.HireLink
        hirelink_list = session.query(HireLink).filter_by(**params).all()
        if type == "list":
            return hirelink_list
        else:
            return {
                hirelink.account_id: hirelink
                for hirelink in hirelink_list
            }

    @classmethod
    def get_shop_super_admin(cls, session, shop_id):
        shop = session.query(models.Shop).filter_by(id=shop_id).first()
        super_admin = session.query(models.HireLink).filter_by(
            shop_id=shop_id, account_id=shop.boss_id).first()
        return super_admin


# 货品操作记录
class GoodsOperationRecordFunc:
    @classmethod
    def get_operation_detail(cls, session, records, weight_unit_text="斤"):
        """获取货品操作记录详情，根据操作类型调用不同方法查询不同表，返回操作详情关键数据"""
        data = collections.defaultdict(set)
        for i in records:
            data[i.operate_type].add(i)

        result = {}
        result.update(
            cls.get_goods_history_data(
                session,
                data[1] | data[2] | data[3] | data[4] | data[17] | data[18]))
        result.update(
            cls.get_goods_stockin_data(
                session, data[5] | data[12],
                weight_unit_text=weight_unit_text))
        result.update(cls.get_goods_name_edit_data(session, data[6]))
        result.update(
            cls.get_goods_group_change_data(session, data[7] | data[8]))
        result.update(cls.get_goods_group_edit_data(session, data[9]))
        result.update(
            cls.get_goods_breakage_data(
                session, data[10], weight_unit_text=weight_unit_text))
        result.update(
            cls.get_goods_cost_price_edit_data(
                session, data[11], weight_unit_text=weight_unit_text))
        result.update(
            cls.get_goods_allocate_data(
                session,
                data[14] | data[15] | data[16],
                weight_unit_text=weight_unit_text))
        result.update(
            cls.get_goods_inventory_data(
                session, data[13], weight_unit_text=weight_unit_text))
        return result

    @classmethod
    def get_goods_allocate_data(cls, session, records, weight_unit_text="斤"):
        """查询StorageAllocateGoods表,获取商品ID、商品名和入库量"""
        StorageAllocateGoods = models.StorageAllocateGoods
        StorageAllocate = models.StorageAllocate
        fk_ids = {i.fk_id for i in records}
        goods_allocate = session.query(StorageAllocateGoods.id,
                                       StorageAllocateGoods.goods_id,
                                       StorageAllocateGoods.receive_goods_id,
                                       StorageAllocateGoods.quantity,
                                       StorageAllocateGoods.weight,
                                       StorageAllocate.delivery_shop_id) \
            .join(StorageAllocate, StorageAllocateGoods.record_id == StorageAllocate.id) \
            .filter(StorageAllocateGoods.id.in_(fk_ids)) \
            .all()
        goods_allocate_dict = {
            i.id: (i.quantity, i.weight)
            for i in goods_allocate
        }
        shop_dict = {i.id: i.delivery_shop_id for i in goods_allocate}
        goods_id_dict = {i.id: i.goods_id for i in goods_allocate}
        receive_goods_id_dict = {
            i.id: i.receive_goods_id
            for i in goods_allocate
        }
        all_goods_ids_list = list(goods_id_dict.values()) + list(
            receive_goods_id_dict.values())
        goods_name_dict = GoodsFunc.get_goods_name_through_id_list(
            session, all_goods_ids_list)

        data = {}
        for i in records:
            if i.shop_id == shop_dict.get(i.fk_id):
                goods_id = goods_id_dict.get(i.fk_id)
            else:
                goods_id = receive_goods_id_dict.get(i.fk_id)
            goods_name = goods_name_dict.get(goods_id)
            goods_allocate = goods_allocate_dict.get(i.fk_id)
            data[i.id] = dict(
                goods_id=goods_id,
                goods_name=goods_name,
                quantity=check_float(goods_allocate[0] / 100),
                weight=check_float(
                    UnitFunc.convert_weight_unit(
                        goods_allocate[1],
                        weight_unit_text=weight_unit_text,
                        source="weight") / 100),
                weight_unit=weight_unit_text)
        return data

    @classmethod
    def get_goods_history_data(cls, session, records):
        """查询GoodsHistory表，获取商品ID和商品名"""
        GoodsHistory = models.GoodsHistory
        fk_ids = {i.fk_id for i in records}
        goods_history = session.query(GoodsHistory.id, GoodsHistory.goods_id) \
            .filter(GoodsHistory.id.in_(fk_ids)) \
            .all()
        goods_id_dict = {i.id: i.goods_id for i in goods_history}
        goods_name_dict = GoodsFunc.get_goods_name_through_id_list(
            session, set(goods_id_dict.values()))

        data = {}
        for i in records:
            goods_id = goods_id_dict.get(i.fk_id)
            goods_name = goods_name_dict.get(goods_id)
            data[i.id] = dict(goods_id=goods_id, goods_name=goods_name)
        return data

    @classmethod
    def get_goods_stockin_data(cls, session, records, weight_unit_text="斤"):
        """查询GoodsStockinRecord表，获取商品ID、商品名和入库量"""
        GoodsStockinRecord = models.GoodsStockinRecord
        fk_ids = {i.fk_id for i in records}
        goods_stockin = session.query(GoodsStockinRecord.id,
                                      GoodsStockinRecord.goods_id,
                                      GoodsStockinRecord.quantity,
                                      GoodsStockinRecord.weight) \
            .filter(GoodsStockinRecord.id.in_(fk_ids)) \
            .all()
        goods_stockin_dict = {
            i.id: (i.quantity, i.weight)
            for i in goods_stockin
        }
        goods_id_dict = {i.id: i.goods_id for i in goods_stockin}
        goods_name_dict = GoodsFunc.get_goods_name_through_id_list(
            session, set(goods_id_dict.values()))

        data = {}
        for i in records:
            goods_id = goods_id_dict.get(i.fk_id)
            goods_name = goods_name_dict.get(goods_id)
            goods_stockin = goods_stockin_dict.get(i.fk_id)
            data[i.id] = dict(
                goods_id=goods_id,
                goods_name=goods_name,
                quantity=check_float(goods_stockin[0] / 100),
                weight=check_float(
                    UnitFunc.convert_weight_unit(
                        goods_stockin[1],
                        weight_unit_text=weight_unit_text,
                        source="weight") / 100),
                weight_unit=weight_unit_text)
        return data

    @classmethod
    def get_goods_name_edit_data(cls, session, records):
        """查询GoodsNameEditHistory表，获取商品ID、改前名，改后名"""
        GoodsNameEditHistory = models.GoodsNameEditHistory
        fk_ids = {i.fk_id for i in records}
        goods_name_edit = session.query(GoodsNameEditHistory) \
            .filter(GoodsNameEditHistory.id.in_(fk_ids)) \
            .all()
        goods_name_edit_dict = {i.id: i for i in goods_name_edit}

        data = {}
        for i in records:
            t = goods_name_edit_dict.get(i.fk_id)
            if t:
                data[i.id] = dict(
                    goods_id=t.goods_id,
                    origin_name=t.origin_name,
                    now_name=t.now_name)
            else:
                data[i.id] = dict(
                    goods_id=None, origin_name=None, now_name=None)
        return data

    @classmethod
    def get_goods_group_change_data(cls, session, records):
        """查询GoodsGroupChangeRecord表，获取商品分组ID和分组名"""
        GoodsGroupChangeRecord = models.GoodsGroupChangeRecord
        fk_ids = {i.fk_id for i in records}
        group_change_record = session.query(GoodsGroupChangeRecord.id,
                                            GoodsGroupChangeRecord.group_id) \
            .filter(GoodsGroupChangeRecord.id.in_(fk_ids)) \
            .all()
        group_id_dict = {i.id: i.group_id for i in group_change_record}

        group_name_dict = GoodsGroupFunc.get_group_name_dict(
            session, set(group_id_dict.values()))

        data = {}
        for i in records:
            group_id = group_id_dict.get(i.fk_id)
            group_name = group_name_dict.get(group_id)
            data[i.id] = dict(group_name=group_name)
        return data

    @classmethod
    def get_goods_group_edit_data(cls, session, records):
        """查询GoodsGroupEditHistory表，获取商品ID、改前名，改后名"""
        GoodsGroupEditHistory = models.GoodsGroupEditHistory
        fk_ids = {i.fk_id for i in records}
        goods_group_edit = session.query(GoodsGroupEditHistory) \
            .filter(GoodsGroupEditHistory.id.in_(fk_ids)) \
            .all()
        goods_group_edit_dict = {i.id: i for i in goods_group_edit}

        origin_group_ids = {i.origin_group_id for i in goods_group_edit}
        now_group_ids = {i.now_group_id for i in goods_group_edit}
        group_ids = set(list(origin_group_ids) + list(now_group_ids))
        group_name_dict = GoodsGroupFunc.get_group_name_dict(
            session, group_ids)

        goods_ids = {i.goods_id for i in goods_group_edit}
        goods_name_dict = GoodsFunc.get_goods_name_through_id_list(
            session, goods_ids)

        data = {}
        for i in records:
            t = goods_group_edit_dict.get(i.fk_id)
            if t:
                data[i.id] = dict(
                    goods_id=t.goods_id,
                    goods_name=goods_name_dict.get(t.goods_id, ""),
                    origin_group_name=group_name_dict.get(
                        t.origin_group_id, ""),
                    now_group_name=group_name_dict.get(t.now_group_id, ""))
            else:
                data[i.id] = dict(
                    goods_id=None,
                    goods_name=None,
                    origin_group_name=None,
                    now_group_name=None)
        return data

    @classmethod
    def get_goods_breakage_data(cls, session, records, weight_unit_text="斤"):
        """ 查询GoodsBreakageRecord表, 获取商品ID, 商品名和报损量 """
        GoodsBreakageRecord = models.GoodsBreakageRecord
        fk_ids = {i.fk_id for i in records}
        goods_breakage = session.query(GoodsBreakageRecord.id,
                                       GoodsBreakageRecord.goods_id,
                                       GoodsBreakageRecord.quantity,
                                       GoodsBreakageRecord.weight) \
            .filter(GoodsBreakageRecord.id.in_(fk_ids)) \
            .all()
        goods_breakage_dict = {
            i.id: (i.quantity, i.weight)
            for i in goods_breakage
        }
        goods_id_dict = {i.id: i.goods_id for i in goods_breakage}
        goods_name_dict = GoodsFunc.get_goods_name_through_id_list(
            session, set(goods_id_dict.values()))

        data = {}
        for i in records:
            goods_id = goods_id_dict.get(i.fk_id)
            goods_name = goods_name_dict.get(goods_id)
            goods_breakage = goods_breakage_dict.get(i.fk_id)
            data[i.id] = dict(
                goods_id=goods_id,
                goods_name=goods_name,
                quantity=check_float(goods_breakage[0] / 100),
                weight=check_float(
                    UnitFunc.convert_weight_unit(
                        goods_breakage[1],
                        weight_unit_text=weight_unit_text,
                        source="weight") / 100),
                weight_unit=weight_unit_text)
        return data

    @classmethod
    def get_goods_cost_price_edit_data(cls,
                                       session,
                                       records,
                                       weight_unit_text="斤"):
        """ 查询GoodsCostPriceEditHistory, 获取商品id, 商品名, 变更前后的成本价 """
        fk_ids = {i.fk_id for i in records}
        historys = session.query(models.GoodsCostPriceEditHistory).filter(
            models.GoodsCostPriceEditHistory.id.in_(fk_ids)).all()

        history_dict = {h.id: h for h in historys}
        goods_id_dict = {h.id: h.goods_id for h in historys}
        goods_name_dict = GoodsFunc.get_goods_name_through_id_list(
            session, set(goods_id_dict.values()))

        data = {}
        for r in records:
            goods_id = goods_id_dict.get(r.fk_id)
            goods_name = goods_name_dict.get(goods_id)
            history = history_dict.get(r.fk_id)
            data[r.id] = dict(
                goods_id=goods_id,
                goods_name=goods_name,
                origin_cost_price_quantity=check_float(
                    history.origin_cost_price_quantity / 100),
                origin_cost_price_weight=check_float(
                    UnitFunc.convert_weight_unit(
                        history.origin_cost_price_weight,
                        weight_unit_text=weight_unit_text,
                        source="price") / 100),
                new_cost_price_quantity=check_float(
                    history.new_cost_price_quantity / 100),
                new_cost_price_weight=check_float(
                    UnitFunc.convert_weight_unit(
                        history.new_cost_price_weight,
                        weight_unit_text=weight_unit_text,
                        source="price") / 100),
                weight_unit=weight_unit_text,
            )
        return data

    @classmethod
    def get_goods_inventory_data(cls, session, records, weight_unit_text="斤"):
        """ 查询GoodsInventoryRecord表, 获取商品ID, 商品名和盘点量 """
        GoodsInventoryRecord = models.GoodsInventoryRecord
        fk_ids = {i.fk_id for i in records}
        goods_inventory = session.query(GoodsInventoryRecord.id,
                                        GoodsInventoryRecord.goods_id,
                                        GoodsInventoryRecord.quantity,
                                        GoodsInventoryRecord.weight) \
            .filter(GoodsInventoryRecord.id.in_(fk_ids)) \
            .all()
        goods_inventory_dict = {
            i.id: (i.quantity, i.weight)
            for i in goods_inventory
        }
        goods_id_dict = {i.id: i.goods_id for i in goods_inventory}
        goods_name_dict = GoodsFunc.get_goods_name_through_id_list(
            session, set(goods_id_dict.values()))

        data = {}
        for i in records:
            goods_id = goods_id_dict.get(i.fk_id)
            goods_name = goods_name_dict.get(goods_id)
            goods_inventory = goods_inventory_dict.get(i.fk_id)
            data[i.id] = dict(
                goods_id=goods_id,
                goods_name=goods_name,
                quantity=check_float(goods_inventory[0] / 100),
                weight=check_float(
                    UnitFunc.convert_weight_unit(
                        goods_inventory[1],
                        weight_unit_text=weight_unit_text,
                        source="weight") / 100),
                weight_unit=weight_unit_text)
        return data


class AuthFunc:
    """用于简单接口鉴权"""

    @classmethod
    def _calc_token(cls, timestr):
        """token计算方式"""
        s = AUTH_API_SECRETKEY + timestr
        return hashlib.md5(s.encode("utf-8")).hexdigest()

    @classmethod
    def gen_token(cls):
        """生成token"""
        timestr = datetime.datetime.now().strftime("%Y%m%d%H")
        return cls._calc_token(timestr)

    @classmethod
    def verify_token(cls, token):
        """验证token"""
        # 上一小时的token在这一小时的前5分钟内仍然有效
        token_expire_delay = 5
        now = datetime.datetime.now()
        tokens = {now}
        if now.minute <= token_expire_delay:
            tokens.add(now - datetime.timedelta(hours=1))
        tokens = map(lambda x: x.strftime("%Y%m%d%H"), tokens)
        tokens = map(lambda x: cls._calc_token(x), tokens)
        return token in tokens

    @classmethod
    def get_passportinfo(cls, passport_id):
        """根据passport表id获取passport信息"""
        url = urllib.parse.urljoin(AUTH_HOST_NAME, "/passport/get")
        headers = {"Authorization": cls.gen_token()}
        resp = requests.post(url, json=dict(id=passport_id), headers=headers)
        if resp.status_code != 200:
            raise Exception(resp.text)
        return resp.json()["data"]

    @classmethod
    def update_passportinfo(cls, passport_id, type_, value):
        """更新passport表信息"""
        assert type_ in ("phone", "wx_unionid")
        url = urllib.parse.urljoin(AUTH_HOST_NAME, "/passport/update")
        data = dict(
            id=passport_id,
            type=type_,
            value=value,
        )
        headers = {"Authorization": cls.gen_token()}
        resp = requests.post(url, json=data, headers=headers)
        if resp.status_code != 200:
            raise Exception(resp.text)
        resp = resp.json()
        if not resp["success"]:
            return False, resp["error_text"]
        return True, ""

    @classmethod
    def verify_passportinfo(cls, type_, value, force_create=False):
        """验证passport信息"""
        assert type_ in ("phone", "wx_unionid")
        url = urllib.parse.urljoin(AUTH_HOST_NAME, "/passport/verify")
        data = dict(
            source="pifa",
            type=type_,
            value=value,
            force_create=bool(force_create),
        )
        headers = {"Authorization": cls.gen_token()}
        resp = requests.post(url, json=data, headers=headers)
        if resp.status_code != 200:
            raise Exception(resp.text)
        resp = resp.json()
        return resp

    @classmethod
    def merge_passport(cls, session, passport_id, phone):
        url = urllib.parse.urljoin(AUTH_HOST_NAME, "/passport/merge")
        data = dict(
            passport_id=passport_id,
            phone=phone,
            source="pifa",
        )
        headers = {"Authorization": cls.gen_token()}
        resp = requests.post(url, json=data, headers=headers)
        if resp.status_code != 200:
            raise Exception(resp.text)
        resp = resp.json()
        if not resp["success"]:
            errmsg = resp["error_text"]
            if errmsg == "NOT EXIST":
                return False, "请先使用微信登录"
            elif errmsg == "USE UPDATE":
                return False, "USE UPDATE"
            elif errmsg == "SAME PASSPORT":
                return False, "您已绑定该手机号"
            elif errmsg == "CANT MERGE 3":
                return False, "该手机号已绑定其他微信"
            elif errmsg == "CANT MERGE 4":
                return False, "该手机号已被冻结，请更换手机号"
            elif errmsg == "CANT MERGE 5":
                return False, "请使用微信扫码登录"
            elif errmsg == "CANT MERGE 6":
                return False, "您已绑定手机号，请前往个人中心进行修改"
            else:
                return False, "绑定失败，请联系森果客服 400-027-0135"

        source_passport_id = resp["source_passport_id"]
        target_passport_id = resp["target_passport_id"]

        merge_key = "passport_merge:{}|{}:pf".format(source_passport_id,
                                                     target_passport_id)
        if auth_redis.exists(merge_key):
            if auth_redis.get(merge_key) == 2:
                return True, ""
            auth_redis.set(merge_key, 2)
            cls.update_single_accountinfo(
                source_passport_id,
                cls.get_passportinfo(source_passport_id),
                session,
            )
            cls.update_single_accountinfo(
                target_passport_id,
                cls.get_passportinfo(target_passport_id),
                session,
            )
            auth_redis.delete(merge_key)
        return True, ""

    @classmethod
    def update_single_accountinfo(cls,
                                  passport_id,
                                  data,
                                  session,
                                  force_create=False):
        """更新当前系统的accountinfo表的单条记录"""
        Accountinfo = models.Accountinfo

        user = session.query(Accountinfo) \
            .filter_by(passport_id=passport_id) \
            .first()
        if not user:
            if force_create:
                user = Accountinfo(
                    passport_id=passport_id,
                    uuid=Accountinfo.make_uuid(session))
                session.add(user)
                session.flush()
            else:
                return None
        else:
            user = session.query(Accountinfo) \
                .filter_by(id=user.id) \
                .with_lockmode("update") \
                .first()

        for k, v in data.items():
            if hasattr(user, k) and k in ("phone", "email", "wx_unionid",
                                          "qq_account"):
                setattr(user, k, v)
        session.commit()
        return user

    @classmethod
    def update_accountinfo(cls, session):
        """根据redis更新accountinfo"""
        cls.update_merge_accountinfo(session)

        keystr = "passport_update:{}:pf"
        keys = auth_redis.keys(keystr.format("*"))
        keys = {int(i.decode().split(":")[1]) for i in keys}

        for i in keys:
            this_key = keystr.format(i)
            status = int(auth_redis.get(this_key))
            # 值为2表示正在处理中，跳过
            if status == 2:
                continue
            # 开始处理时设置值为2
            auth_redis.set(this_key, 2)
            data = cls.get_passportinfo(i)
            cls.update_single_accountinfo(i, data, session)
            # 处理完成后删除
            auth_redis.delete(this_key)

    @classmethod
    def update_merge_accountinfo(cls, session):
        keystr = "passport_merge:{}|{}:pf"
        keys = auth_redis.keys(keystr.format("*", "*"))
        keys = {i.decode().split(":")[1] for i in keys}

        for i in keys:
            source, target = map(int, i.split("|"))
            this_key = keystr.format(source, target)
            status = int(auth_redis.get(this_key) or "2")
            if status == 2:
                continue
            auth_redis.set(this_key, 2)
            cls.update_single_accountinfo(
                source,
                cls.get_passportinfo(source),
                session,
            )
            cls.update_single_accountinfo(
                target,
                cls.get_passportinfo(target),
                session,
            )
            auth_redis.delete(this_key)

    @classmethod
    def login_by_wx(cls, session, wx_userinfo):
        """微信登录"""
        wx_unionid = wx_userinfo["unionid"]
        resp = cls.verify_passportinfo(
            "wx_unionid", wx_unionid, force_create=True)
        # 错误处理
        if not resp["success"]:
            errmsg = resp["error_text"]
            if errmsg == "LOCKED":
                return False, "该账户已被冻结，请联系森果客服 400-027-0135"
            else:
                return False, "无法登录，请联系森果客服 400-027-0135"

        data = resp["data"]
        user = session.query(
            models.Accountinfo).filter_by(passport_id=data["id"]).first()
        if not user:
            passport_id = data.pop("id")
            user = cls.update_single_accountinfo(
                passport_id, data, session, force_create=True)
        return True, user

    @classmethod
    def login_by_phone_password(cls, session, phone, password):
        """手机号+密码 登录"""
        resp = cls.verify_passportinfo("phone", phone)
        # 错误处理
        if not resp["success"]:
            errmsg = resp["error_text"]
            if errmsg == "LOCKED":
                return False, "该账户已被冻结，请联系森果客服 400-027-0135"
            else:
                return False, "用户名或密码错误"
        data = resp["data"]
        user = session.query(
            models.Accountinfo).filter_by(passport_id=data["id"]).first()
        if not user:
            passport_id = data.pop("id")
            user = cls.update_single_accountinfo(
                passport_id, data, session, force_create=True)
        if user.password != password or user.password is None:
            return False, "用户名或密码错误"
        return True, user

    @classmethod
    def login_by_phone_code(cls, session, phone):
        """手机号登录"""
        resp = cls.verify_passportinfo("phone", phone)
        # 错误处理
        if not resp["success"]:
            errmsg = resp["error_text"]
            if errmsg == "LOCKED":
                return False, "该账户已被冻结，请联系森果客服 400-027-0135"
            elif errmsg == "NOT EXIST":
                return False, "您还不是系统用户"
            else:
                return False, "登录失败，请联系森果客服 400-027-0135"
        data = resp["data"]
        user = session.query(
            models.Accountinfo).filter_by(passport_id=data["id"]).first()
        if not user:
            passport_id = data.pop("id")
            user = cls.update_single_accountinfo(
                passport_id, data, session, force_create=True)
        return True, user

    @classmethod
    def register_with_phone(cls, session, phone):
        """使用手机号注册"""
        resp = cls.verify_passportinfo("phone", phone)
        if resp.get("error_text") != "NOT EXIST":
            return False, "您已注册，如果您已是员工，请直接登录；如还不是员工请先使用手机号添加成为员工"
        resp = cls.verify_passportinfo("phone", phone, force_create=True)
        data = resp["data"]
        passport_id = data.pop("id")
        user = cls.update_single_accountinfo(
            passport_id, data, session, force_create=True)
        return True, user

    @classmethod
    def update_through_wx(cls, session, wx_userinfo, account, action="update"):
        """通过微信更新用户资料"""
        headimgurl = wx_userinfo.get("headimgurl", None)
        if headimgurl:
            headimgurl = headimgurl.replace("http://", "https://")
        if action == "bind":
            account.wx_unionid = wx_userinfo.get("unionid", None)
        if action in ["bind", "update_all"]:
            now_openid = account.wx_openid or ""
            if not now_openid.startswith("oA") \
                    and wx_userinfo.get("openid", "").startswith("oA"):
                account.wx_openid = wx_userinfo.get("openid", None)
        account.wx_country = wx_userinfo.get("country", None)
        account.wx_province = wx_userinfo.get("province", None)
        account.wx_city = wx_userinfo.get("city", None)
        account.headimgurl = headimgurl
        account.nickname = wx_userinfo.get("nickname", None)
        account.sex = wx_userinfo.get("sex", 0)
        session.add(account)
        session.commit()


# 串货调整
class SalesRecordAjustFunc():
    @classmethod
    def get_ajust_new_goods_info_through_id_list(cls, session,
                                                 origin_record_id_list):
        """ 获取串货调整新货品ID
        """
        GoodsSalesRecordAjust = models.GoodsSalesRecordAjust
        ajust_record_list = session.query(GoodsSalesRecordAjust.shop_id,
                                          GoodsSalesRecordAjust.origin_record_id,
                                          GoodsSalesRecordAjust.new_goods_id) \
            .filter(GoodsSalesRecordAjust.origin_record_id.in_(origin_record_id_list)) \
            .all()
        ajust_new_goods_dict = {}
        if ajust_record_list:
            new_goods_id_list = {x.new_goods_id for x in ajust_record_list}
            shop_id = ajust_record_list[0].shop_id
            goods_info_dict = GoodsFunc.get_goods_info_list_through_id_list(
                session, shop_id, new_goods_id_list)

            for shop_id, origin_record_id, new_goods_id in ajust_record_list:
                new_goods_info = goods_info_dict.get(new_goods_id, {})
                ajust_new_goods_dict[origin_record_id] = new_goods_info
        return ajust_new_goods_dict

    @classmethod
    def get_ajust_info(cls, session, record):
        """ 获取串货调整信息
        """
        could_ajust = False
        ajust_info = {}

        if record.bill_date + datetime.timedelta(
                days=7) > datetime.datetime.now().date() and record.status in (
                    3, 5):
            could_ajust = True

        # # 退款小票不可调整
        # if record.record_type == 1:
        #     could_ajust = False

        # 进行过退款的不可调整
        if record.refund_cent:
            could_ajust = False

        # 已进行供货商结算不可调整
        if record.clearing_record_id:
            could_ajust = False

        # 是否有调整记录
        ajust_record = cls.get_ajust_record(session, record.id)

        if ajust_record:
            could_ajust = False
            shop_id = ajust_record.shop_id
            origin_goods_id = ajust_record.origin_goods_id
            new_goods_id = ajust_record.new_goods_id
            origin_record_num = ajust_record.origin_record_num
            refund_record_num = ajust_record.refund_record_num
            new_record_num = ajust_record.new_record_num

            # 判断调整类型
            # ajust_type 1:原始单据 2:冲红单据 3:新单据
            now_record_num = record.num
            if now_record_num == origin_record_num:
                ajust_type = 1
            elif now_record_num == refund_record_num:
                ajust_type = 2
            else:
                ajust_type = 3

            ajust_info["ajust_origin_goods_id"] = origin_goods_id  # 原始货品ID
            ajust_info[
                "ajust_origin_goods_name"] = GoodsFunc.get_goods_name_with_supplier(
                    session, shop_id, origin_goods_id)  # 原始货品名
            ajust_info["ajust_new_goods_id"] = new_goods_id  # 新货品ID
            ajust_info[
                "ajust_new_goods_name"] = GoodsFunc.get_goods_name_with_supplier(
                    session, shop_id, new_goods_id)  # 新货品名
            ajust_info["ajust_origin_num"] = origin_record_num  # 原始单据号
            ajust_info["ajust_refund_num"] = refund_record_num  # 冲红单据号
            ajust_info["ajust_new_num"] = new_record_num  # 新单据号
            ajust_info["ajust_operator_id"] = ajust_record.operator_id  # 调整人ID
            ajust_info[
                "ajust_operator_name"] = AccountFunc.get_account_realname(
                    session, ajust_record.operator_id)  # 调整人姓名
            ajust_info["ajust_operator_time"] = TimeFunc.time_to_str(
                ajust_record.create_time)  # 调整时间
            ajust_info["ajust_type"] = ajust_type  # 调整类型
            ajust_info[
                "ajust_origin_id"] = ajust_record.origin_record_id  # 原始单号ID
            ajust_info[
                "ajust_refund_id"] = ajust_record.refund_record_id  # 冲红单号ID
            ajust_info["ajust_new_id"] = ajust_record.new_record_id  # 新单据ID

        return could_ajust, ajust_info

    @classmethod
    def get_ajust_record(cls, session, record_id):
        """ 获取单张小票串货调整记录
        """
        GoodsSalesRecordAjust = models.GoodsSalesRecordAjust
        ajust_record = session.query(GoodsSalesRecordAjust) \
            .filter(or_(GoodsSalesRecordAjust.origin_record_id == record_id,
                        GoodsSalesRecordAjust.refund_record_id == record_id,
                        GoodsSalesRecordAjust.new_record_id == record_id)) \
            .first()
        return ajust_record

    @classmethod
    def get_multi_ajust_info(cls, session, record_id_list):
        """ 批量判断小票是否进行串货调整
        """
        GoodsSalesRecordAjust = models.GoodsSalesRecordAjust
        ajust_record_id_list = session.query(GoodsSalesRecordAjust.origin_record_id) \
            .filter(GoodsSalesRecordAjust.origin_record_id.in_(record_id_list)) \
            .all()
        ajust_record_id_list = {
            x.origin_record_id
            for x in ajust_record_id_list
        }
        return ajust_record_id_list

    @classmethod
    def get_multi_ajust_all(cls, session, record_id_list):
        """ 批量判断小票是否进行串货调整(或为串货调整后生成的新数据)
        """
        GoodsSalesRecordAjust = models.GoodsSalesRecordAjust
        origin_record_id_list = session.query(GoodsSalesRecordAjust.origin_record_id) \
            .filter(GoodsSalesRecordAjust.origin_record_id.in_(record_id_list))

        refund_record_id_list = session.query(GoodsSalesRecordAjust.refund_record_id) \
            .filter(GoodsSalesRecordAjust.refund_record_id.in_(record_id_list))

        new_record_id_list = session.query(GoodsSalesRecordAjust.new_record_id) \
            .filter(GoodsSalesRecordAjust.new_record_id.in_(record_id_list))

        record_id_list = origin_record_id_list.union(
            refund_record_id_list).union(new_record_id_list)
        ajust_record_id_list = {x[0] for x in record_id_list}

        return ajust_record_id_list


# 采购入库
class StockInFunc():
    @classmethod
    def get_doc_detail_unit_pub(cls,
                                session,
                                doc_detail,
                                doc,
                                goods_id_list,
                                filter_detail_id_list=[]):
        """获取最新的入库/报损记录的货品单位
        """
        detail_ids = session \
            .query(func.max(doc_detail.id)) \
            .join(doc, doc_detail.doc_id == doc.id) \
            .filter(doc_detail.goods_id.in_(goods_id_list),
                    not_(doc_detail.id.in_(filter_detail_id_list)),
                    doc.status == 1) \
            .group_by(doc_detail.goods_id) \
            .all() if goods_id_list else []
        detail_id_list = [d[0] for d in detail_ids]
        details = session \
            .query(doc_detail.goods_id,
                   doc_detail.unit) \
            .filter(doc_detail.id.in_(detail_id_list)) \
            .all() if detail_id_list else []
        map_goods_unit = {detail.goods_id: detail.unit for detail in details}
        return map_goods_unit

    @classmethod
    def get_flow(cls, session, shop_id, page, **kwargs):
        """ 获取入库流水

        :param shop_id: 当前店铺的id
        :param page: 页数
        :param kwargs: 查询过程中一些需要的筛选条件, 包括如下选择:
        :param supply_type: 货品供应类型
        :param from_date: 入库时间开始时间
        :param to_date: 入库时间结束日期
        :param receive_date: 到货日期
        :param goods_id: 货品id,
        :param is_unclearing: 是否全部是未结算的
        :param pay_type: 入库单的付款方式列表, 0:没有付款方式(代卖) 1:现金 2:微信 3:支付宝 4:银行卡 5:赊账待付
        :param batch_id_list: 批次号id列表，因为可能存在重名情况
        :param detail_id_list: 入库详情的id列表
        :param page_size: 调用端定义的页面大小
        :param asc: 是否正序
        :param only_branch: 是否仅展示分货

        :return: 总页数和入库数据列表, 选择性获取汇总数据
        :rtype: tuple
        """
        page_size = kwargs.get("page_size", 20)
        supply_type = kwargs.get("supply_type")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        receive_date = kwargs.get("receive_date")
        goods_id = kwargs.get("goods_id")
        pay_type = kwargs.get("pay_type", [])
        batch_id_list = kwargs.get("batch_id_list")
        detail_id_list = kwargs.get("detail_id_list")
        asc = kwargs.get("asc", False)
        shop_supplier_id = kwargs.get("shop_supplier_id", 0)
        is_unclearing = kwargs.get("is_unclearing", False)
        only_branch = kwargs.get("only_branch", False)
        goods_id_list = kwargs.get("goods_id_list", [])
        group_id_list = kwargs.get("group_id_list", [])
        # 供货商入库汇总
        is_credit = kwargs.get("is_credit")  # 是否赊账待付
        is_clearing = kwargs.get("is_clearing")  # 是否已结算
        should_pay = kwargs.get("should_pay")  # 应付款
        order_condition = kwargs.get(
            "order_condition", 0)  # 排序条件 0: 按流水 ID 1: 按入库重量 2: 按入库件数 3: 按费用

        from_date, to_date = TimeFunc.transfer_input_range_to_date(
            from_date, to_date)
        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)

        # 分组id到name的映射
        map_groups_id_name = GoodsGroupFunc.get_groups_under_shop(
            session, shop_id)
        flow_base = session.query(
            models.StockInDocsDetail,
            # 入库单内的信息
            models.StockInDocs.operate_date.label("operate_date"),
            models.StockInDocs.num.label("num"),
            models.StockInDocs.status.label("status"),
            models.StockInDocs.hand_num.label("hand_num"),
            models.StockInDocs.receive_date.label("receive_date"),
            models.StockInDocs.operator_id.label("operator_id"),
            models.StockInDocs.id.label("doc_id"),
            # 商品表内的信息
            models.Goods.group_id.label("group_id"),
            models.Goods.supply_type.label("supply_type"),
            models.Goods.shop_supplier_id.label("shop_supplier_id"))
        count_base = session.query(func.count(models.StockInDocsDetail.id))

        param_join_first = [
            models.StockInDocs,
            models.StockInDocs.id == models.StockInDocsDetail.doc_id
        ]
        param_join_second = [
            models.Goods, models.Goods.id == models.StockInDocsDetail.goods_id
        ]
        params_filter = [
            models.StockInDocs.shop_id == shop_id,
            models.StockInDocs.status.in_([-1, 1, 2]),
            models.StockInDocsDetail.status == 1
        ]

        # 过滤
        if supply_type is not None:
            if supply_type not in [0, 1, 2, 3]:
                raise ValueError("供应类型选择错误，只能为自营、代卖、货主、联营")
            params_filter.append(models.Goods.supply_type == supply_type)
        if from_date:
            params_filter.append(models.StockInDocs.receive_date >= from_date)
        if to_date:
            params_filter.append(models.StockInDocs.receive_date < to_date)
        if receive_date:
            params_filter.append(
                models.StockInDocs.receive_date == receive_date)
        if goods_id:
            params_filter.append(models.StockInDocsDetail.goods_id == goods_id)
        if is_unclearing:
            # 未结算不展示草稿和撤销的内容
            params_filter.append(
                models.StockInDocsDetail.clearing_record_id == 0)
            params_filter.append(models.StockInDocs.status == 1)
        if pay_type:
            params_filter.append(
                models.StockInDocsDetail.pay_type.in_(pay_type))
        if batch_id_list:
            params_filter.append(
                models.StockInDocsDetail.goods_batch_id.in_(batch_id_list))
        if detail_id_list:
            params_filter.append(
                models.StockInDocsDetail.id.in_(detail_id_list))
        if shop_supplier_id:
            params_filter.append(
                models.Goods.shop_supplier_id == shop_supplier_id)
        if group_id_list:
            params_filter.append(models.Goods.group_id.in_(group_id_list))
        if is_credit:
            params_filter.append(models.StockInDocsDetail.pay_type == 5)
        if is_clearing:
            params_filter.append(models.StockInDocsDetail.pay_type == 5)
            params_filter.append(
                models.StockInDocsDetail.clearing_record_id != 0)
        if should_pay:
            params_filter.append(models.StockInDocsDetail.pay_type == 5)
            params_filter.append(
                models.StockInDocsDetail.clearing_record_id == 0)
            params_filter.append(models.StockInDocs.status == 1)
        params_filter_goods = params_filter[:]
        # 货品 ID 列表过滤和主货品过滤放在备选货品过滤之后以免将需要的备选货品筛除
        if goods_id_list:
            # 筛选主货品时需要把分货品添加进来
            params_filter.append(
                or_(
                    models.StockInDocsDetail.goods_id.in_(goods_id_list),
                    models.Goods.master_goods_id.in_(goods_id_list)))
        if only_branch:
            params_filter.append(models.Goods.is_master == 0),
        if order_condition == 0:
            params_order_by = models.StockInDocs.id
        elif order_condition == 1:
            params_order_by = models.StockInDocsDetail.weight
        elif order_condition == 2:
            params_order_by = models.StockInDocsDetail.quantity
        elif order_condition == 3:
            params_order_by = models.StockInDocsDetail.total_price
        else:
            raise ValueError("排序条件无效")
        # 排序
        if asc:
            flow_base = flow_base.order_by(params_order_by.asc())
        else:
            flow_base = flow_base.order_by(params_order_by.desc())

        flow_base = flow_base.join(*param_join_first).join(
            *param_join_second).filter(*params_filter)

        # 提供获取全部数据的方法
        if page == -1:
            flows = flow_base.all()
            page_sum = 1
        else:
            flows = flow_base.offset(page_size * page).limit(page_size).all()
            count = count_base.join(*param_join_first).join(
                *param_join_second).filter(*params_filter).scalar()
            page_sum = math.ceil(count / page_size)

        data_list = StockinHistoryFunc.batch_format_stockin_history_info(
            session,
            flows,
            weight_unit_text,
            map_groups_id_name,
            return_type="list")

        # 统计累计值
        sum_stockin = session.query(
            func.count(models.StockInDocsDetail.id).label("count"),
            func.sum(models.StockInDocsDetail.quantity).label("quantity"),
            func.sum(models.StockInDocsDetail.weight).label("weight"),
            func.sum(models.StockInDocsDetail.total_price).label(
                "total_price")).join(*param_join_first).join(
                    *param_join_second).filter(*params_filter).first()
        sum_dict = dict(
            count=check_int(sum_stockin.count) if sum_stockin.count else 0,
            quantity=check_float(sum_stockin.quantity / 100)
            if sum_stockin.quantity else 0,
            weight=check_float(
                UnitFunc.convert_weight_unit(
                    sum_stockin.weight,
                    weight_unit_text=weight_unit_text,
                    source="weight") / 100) if sum_stockin.weight else 0,
            total_price=check_float(sum_stockin.total_price / 100)
            if sum_stockin.total_price else 0)
        if is_unclearing:
            # 结算的时候附加赊账未还的信息
            sum_stockin_unpay = session.query(
                func.sum(models.StockInDocsDetail.total_price).label(
                    "total_price")).filter(*params_filter).filter_by(
                        pay_type=5).join(*param_join_first).join(
                            *param_join_second).first()
            sum_dict["total_price_unpay"] = check_float(
                sum_stockin_unpay.total_price /
                100) if sum_stockin_unpay.total_price else 0

            ids_stockin = session.query(
                models.StockInDocsDetail.id).filter(*params_filter).join(
                    *param_join_first).join(*param_join_second).all()
            sum_dict["stockin_record_id_list"] = [s.id for s in ids_stockin]

        # 筛选用货品信息
        total_goods_list = session.query(models.StockInDocsDetail.goods_id, models.Goods) \
            .join(*param_join_first) \
            .join(*param_join_second) \
            .filter(*params_filter_goods)
        total_goods_list = GoodsMasterBranchFunc.apply_filter(
            total_goods_list, shop_id)
        total_goods_list = total_goods_list.all()
        total_goods_list = [{
            "id": goods.id,
            "name": GoodsFunc.get_goods_detail_name(goods)
        } for _, goods in total_goods_list]

        return page_sum, data_list, sum_dict, total_goods_list

    @classmethod
    def get_statistic_each_goods(cls, cls_doc_detail, cls_doc, shop_id,
                                 session, page, **kwargs):
        """ 获取入库单的采购汇总 """
        page_size = 20
        supply_type = kwargs.get("supply_type")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        supplier_id = kwargs.get("supplier_id")
        goods_id_list = kwargs.get("goods_id_list", [])
        group_id_list = kwargs.get("group_id_list", [])
        order_condition = kwargs.get(
            "order_condition", 0)  # 排序条件 0: 按货品 ID 1: 按入库重量 2: 按入库件数 3: 按费用
        asc = kwargs.get("asc", False)  # 排序顺序 False: 降序 True: 升序

        try:
            from_date, to_date = TimeFunc.transfer_input_range_to_date(
                from_date, to_date)
        except ValueError as why:
            raise ValueError(str(why))

        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)

        # 分组id到name的映射
        map_groups_id_name = GoodsGroupFunc.get_groups_under_shop(
            session, shop_id)
        label_quantity = func.sum(cls_doc_detail.quantity).label("quantity")
        label_weight = func.sum(cls_doc_detail.weight).label("weight")
        label_total_price = func.sum(
            cls_doc_detail.total_price).label("total_price")
        label_cost_price_quantity = func.sum(
            cls_doc_detail.cost_price_quantity).label("cost_price_quantity")
        label_cost_price_weight = func.sum(
            cls_doc_detail.cost_price_weight).label("cost_price_weight")
        query_list = [
            cls_doc_detail.goods_id,
            label_quantity,
            label_weight,
            label_total_price,
            label_cost_price_quantity,
            label_cost_price_weight,
            # 商品表内的信息
            models.Goods.supply_type,
            models.Goods.group_id,
            cls_doc_detail.unit,
            models.Goods.is_master,
            models.Goods.master_goods_id
        ]
        if cls_doc_detail == models.StockInDocsDetail:
            query_list.append(
                func.sum(
                    case(
                        [(cls_doc_detail.pay_type == 5,
                          cls_doc_detail.total_price)])).label("credit_money"))
        statistic_base = session.query(*query_list)
        count_base = session.query(func.count(cls_doc_detail.goods_id))

        param_join_first = [cls_doc, cls_doc.id == cls_doc_detail.doc_id]
        param_join_second = [
            models.Goods, models.Goods.id == cls_doc_detail.goods_id
        ]
        params_group_by = [
            cls_doc_detail.goods_id, models.Goods.supply_type,
            models.Goods.group_id
        ]
        params_filter = [
            cls_doc.shop_id == shop_id,
            cls_doc.status.in_([-1, 1, 2]), cls_doc_detail.status == 1
        ]
        if supply_type is not None:
            if supply_type not in [0, 1, 2, 3]:
                raise ValueError("供应类型选择错误，只能为自营、代卖、货主、联营")
            params_filter.append(models.Goods.supply_type == supply_type)
        if from_date:
            params_filter.append(cls_doc.receive_date >= from_date)
        if to_date:
            params_filter.append(cls_doc.receive_date <= to_date)
        # 过滤供应商
        if supplier_id:
            params_filter.append(models.Goods.shop_supplier_id == supplier_id)
        if group_id_list:
            params_filter.append(models.Goods.group_id.in_(group_id_list))
        params_filter_goods = params_filter[:]
        if goods_id_list:
            params_filter.append(cls_doc_detail.goods_id.in_(goods_id_list))
        if order_condition == 0:
            params_order_by = cls_doc_detail.goods_id
        elif order_condition == 1:
            params_order_by = label_weight
        elif order_condition == 2:
            params_order_by = label_quantity
        elif order_condition == 3:
            params_order_by = label_total_price
        else:
            raise ValueError("排序条件无效")
        if asc:
            params_order_by = params_order_by.asc()
        else:
            params_order_by = params_order_by.desc()

        # 查询合并货品记录条数
        statistic_base = statistic_base.join(*param_join_first) \
            .join(*param_join_second) \
            .filter(*params_filter) \
            .group_by(*params_group_by)
        master_statistic_base = GoodsMasterBranchFunc.apply_filter(
            statistic_base, shop_id)
        if page == -1:
            statistic = master_statistic_base.all()
            page_sum = 1
        else:
            statistic = master_statistic_base.order_by(params_order_by) \
                .offset(page_size * page) \
                .limit(page_size) \
                .all()
            # 总条目数
            db_count = count_base.join(*param_join_first) \
                .join(*param_join_second) \
                .filter(*params_filter) \
                .group_by(*params_group_by)
            db_count = GoodsMasterBranchFunc.apply_filter(db_count, shop_id)
            db_count = db_count.all()
            count = len(db_count)
            page_sum = math.ceil(count / page_size)

        # 查询主货品对应的分货品汇总记录
        master_goods_ids = [
            item.goods_id for item in statistic if item.is_master == 1
        ]
        branch_statistics = statistic_base \
            .filter(models.Goods.master_goods_id.in_(master_goods_ids)) \
            .all()
        branch_statistics_dict = defaultdict(list)
        for branch_statistics_item in branch_statistics:
            branch_statistics_dict[branch_statistics_item.
                                   master_goods_id].append(
                                       branch_statistics_item)

        # 计算累计数据
        total_master_statistic = master_statistic_base.all()
        total_data_sum = defaultdict(int)
        for master_statistic in total_master_statistic:
            total_data_sum["total_weight"] += UnitFunc.convert_weight_unit(
                master_statistic.weight,
                source="weight",
                weight_unit_text=weight_unit_text)
            total_data_sum["total_quantity"] += master_statistic.quantity
            total_data_sum["total_price"] += master_statistic.total_price
        total_data_sum["total_weight"] = check_float(
            total_data_sum["total_weight"] / 100)
        total_data_sum["total_quantity"] = check_float(
            total_data_sum["total_quantity"] / 100)
        total_data_sum["total_price"] = check_float(
            total_data_sum["total_price"] / 100)

        # 筛选用货品信息
        total_goods_list = session.query(cls_doc_detail.goods_id, models.Goods.name) \
            .join(*param_join_first) \
            .join(*param_join_second) \
            .filter(*params_filter_goods)
        total_goods_list = GoodsMasterBranchFunc.apply_filter(
            total_goods_list, shop_id)
        total_goods_list = total_goods_list.all()
        total_goods_list = [{
            "id": item.goods_id,
            "name": item.name
        } for item in total_goods_list]

        goods_id_list = [s.goods_id for s in statistic + branch_statistics]
        g_dict = GoodsFunc.get_goods_in_shop(
            session,
            shop_id,
            weight_unit_text,
            goods_id_list,
            merge_self_goods=False)

        # 获取商品最新入库的入库单位
        map_detail_unit = StockInFunc.get_doc_detail_unit_pub(
            session, cls_doc_detail, cls_doc, goods_id_list)

        def prepare_data(doc_detail, goods, detail_unit, type):
            data_dict = dict(
                goods_id=doc_detail.goods_id,
                goods_group_name=map_groups_id_name[doc_detail.group_id],
                goods_name=goods["detail_name"],
                supplier_name=goods["supplier_name"],
                supplier_id=goods["supplier_id"],
                quantity=check_float(doc_detail.quantity / 100)
                if doc_detail.quantity else "-",
                weight=check_float(
                    UnitFunc.convert_weight_unit(
                        doc_detail.weight,
                        source="weight",
                        weight_unit_text=weight_unit_text) / 100)
                if doc.weight else "-",
                total_price=check_float(doc_detail.total_price / 100),
                goods_supply_type=doc_detail.supply_type,
                storage_unit=goods["storage_unit"],
                storage_unit_text="件"
                if goods["storage_unit"] == 1 else weight_unit_text,
                purchase_unit=goods["purchase_unit"],
                purchase_unit_text="件"
                if goods["purchase_unit"] == 1 else weight_unit_text,
                detail_unit=detail_unit,
            )
            # 入库单的价格用总价计算
            if type == models.StockInDocsDetail:
                data_dict["quantity_unit_price"] = check_float(
                    doc_detail.total_price /
                    doc_detail.quantity) if doc_detail.quantity else ""
                try:
                    data_dict["weight_unit_price"] = check_float(
                        doc_detail.total_price / doc_detail.get_weight(
                            weight_unit_text=weight_unit_text)
                    ) if doc_detail.weight else ""
                except AttributeError:
                    data_dict["weight_unit_price"] = check_float(UnitFunc.convert_weight_unit(
                        doc_detail.total_price / doc_detail.weight,
                        source="price",
                        weight_unit_text=weight_unit_text)) \
                        if doc_detail.weight else ""
            else:
                data_dict["cost_price_quantity"] = check_float(
                    doc_detail.cost_price_quantity / 100)
                try:
                    data_dict["cost_price_weight"] = check_float(
                        doc_detail.get_cost_price_weight(
                            weight_unit_text=weight_unit_text) / 100)
                except AttributeError:
                    data_dict["cost_price_weight"] = check_float(
                        UnitFunc.convert_weight_unit(
                            doc_detail.cost_price_weight,
                            source="price",
                            weight_unit_text=weight_unit_text) / 100)
            return data_dict

        data_list = []
        for doc in statistic:
            master_goods = g_dict[doc.goods_id]

            data = prepare_data(doc, master_goods, doc.unit, cls_doc_detail)
            data["branch_goods_info"] = []
            if doc.is_master:
                branch_docs = branch_statistics_dict.get(doc.goods_id, [])
                for branch_doc in branch_docs:
                    detail_unit = map_detail_unit.get(branch_doc.goods_id,
                                                      branch_doc.unit)
                    branch_goods = g_dict[branch_doc.goods_id]
                    branch_data = prepare_data(branch_doc, branch_goods,
                                               detail_unit, cls_doc_detail)
                    branch_data["parent_goods_id"] = doc.goods_id
                    data["branch_goods_info"].append(branch_data)

            data_list.append(data)
        return data_list, page_sum, total_data_sum, total_goods_list


# 店铺设置
class ConfigFunc():
    @classmethod
    def get_statistic_gross(cls, session, shop_id):
        statistic_gross = session \
                              .query(models.Config.disable_statistic_gross) \
                              .filter_by(id=shop_id) \
                              .scalar() or 0
        return statistic_gross

    @classmethod
    def get_modify_paytype_days(cls, session, shop_id):
        modify_paytype_days = session \
                                  .query(models.Config.modify_paytype_days) \
                                  .filter_by(id=shop_id) \
                                  .scalar() or 0
        return modify_paytype_days

    @classmethod
    def get_fee_text(cls, session, shop_id):
        fee_text_type = session \
                            .query(models.Config.fee_text_type) \
                            .filter_by(id=shop_id) \
                            .scalar() or 0

        fee_text_dict = {0: "行费", 1: "箱费"}
        return fee_text_dict.get(fee_text_type, "")

    @classmethod
    def get_weight_unit_text(cls, session, shop_id):
        display_kg_instead = session \
                                 .query(models.Config.display_kg_instead) \
                                 .filter_by(id=shop_id) \
                                 .scalar() or 0

        weight_unit_dict = {0: "斤", 1: "kg"}
        return weight_unit_dict.get(display_kg_instead, "")

    @classmethod
    def get_config(cls, session, shop_id):
        config = session.query(models.Config).filter_by(id=shop_id).first()
        return config


# 单位换算
class UnitFunc():
    """ 单位换算公用方法, 主要适用于数据库查询不能直接使用ORM的setter和getter的情况 """

    @classmethod
    def convert_weight_unit(cls, val, source="weight", weight_unit_text="斤"):
        """ 重量单位换算

        :param val: 数据库里直接取到的值
        :param source: weight, 重量换算, 除以2; price, 价格换算, 乘以2.
        """
        result = val
        if weight_unit_text == "kg":
            if source == "weight":
                result /= 2
            elif source == "price":
                result *= 2
        return result

    @classmethod
    def fix_weight_unit(cls, val, weight_unit_text="斤"):
        """ 从前端直接取的数值重量单位换算
            重量换算成斤，乘以2
        :param val: 前端直接取到的值
        """
        result = val
        if weight_unit_text == "kg":
            result *= 2
        return result


# 支付方式修改
class PaytypeModifyFunc():
    @classmethod
    def get_paytype_text(cls, action, modify_record):
        """ 解析支付方式文字

        :param action
            origin 原始支付方式
            new 新支付方式
        :param modify_record 修改记录
        """
        key_name_dict = OrderBaseFunc.pay_type_column_dict
        modify_data_dict = modify_record.all_props()
        pay_type_key = "{0}_pay_type".format(action)
        pay_type = modify_data_dict[pay_type_key]

        if pay_type == 10:
            combine_pay_text_list = []
            for key in key_name_dict:
                key_name = "{0}_{1}".format(action, key)
                combine_pay_type = key_name_dict[key]
                combine_money = modify_data_dict[key_name]
                if combine_money:
                    combine_money = check_float(combine_money / 100)
                    combine_pay_type_text = OrderBaseFunc.pay_type_dict[
                        combine_pay_type]
                    combine_pay_type_text = "{0}:{1}元".format(
                        combine_pay_type_text, combine_money)
                    combine_pay_text_list.append(combine_pay_type_text)
            pay_type_text = "+".join(combine_pay_text_list)
        else:
            pay_type_text = OrderBaseFunc.pay_type_dict[pay_type]

        return pay_type_text

    @classmethod
    def get_origin_paytype_text(cls, session, order_id, source_type=1):
        OrderPaytypeModifyRecord = models.OrderPaytypeModifyRecord
        modify_record = session \
            .query(OrderPaytypeModifyRecord) \
            .filter_by(order_id=order_id, modify_type=source_type) \
            .first()
        if not modify_record:
            return ""
        origin_pay_type_text = cls.get_paytype_text("origin", modify_record)
        return origin_pay_type_text


# 银行信息
class LcBankFunc():
    @classmethod
    def get_bank_name_dict(cls, session, bank_nos):
        banks = session \
            .query(models.LcParentBank) \
            .filter(models.LcParentBank.parent_bank_no.in_(bank_nos)) \
            .all()
        bank_name_dict = {
            bank.parent_bank_no: bank.parent_bank_name
            for bank in banks
        }
        return bank_name_dict

    @classmethod
    def get_bank_name(cls, session, bank_no):
        bank_name = session \
                        .query(models.LcParentBank.parent_bank_name) \
                        .filter_by(parent_bank_no=bank_no) \
                        .scalar() or ""
        return bank_name

    @classmethod
    def get_bank_info_by_code(cls, session, bank_no):
        """ 通过支行编码来获取相关银行的信息

        :param bank_num: 支行编码

        :rtype: dict
        """
        LcBank = models.LcBank
        LcParentBank = models.LcParentBank
        LcAreaCode = models.LcAreaCode
        # 获取开户行信息
        bank_info = session.query(LcBank.bank_no,
                                  LcBank.bank_name,
                                  LcParentBank.parent_bank_no,
                                  LcParentBank.parent_bank_name,
                                  LcAreaCode.province_code,
                                  LcAreaCode.province_text,
                                  LcAreaCode.city_code,
                                  LcAreaCode.city_text) \
            .join(LcParentBank,
                  LcParentBank.parent_bank_no == LcBank.parent_bank_no) \
            .join(LcAreaCode, LcAreaCode.city_code == LcBank.city_code) \
            .filter(LcBank.bank_no == bank_no) \
            .first()
        result = dict(
            bank=(bank_info.bank_no, bank_info.bank_name),
            parent_bank=(bank_info.parent_bank_no, bank_info.parent_bank_name),
            province=(bank_info.province_code, bank_info.province_text),
            city=(bank_info.city_code, bank_info.city_text),
        )
        return result


# 开票员的货
class SalesmanFunc():
    @classmethod
    def _list_goods_id_created_by(cls, session, shop_id, account_id):
        """ 获取开票员自己添加的货品列表

        :param account_id: 开票员id

        :rtype: list
        """
        goods = session.query(models.Goods.id) \
            .filter_by(shop_id=shop_id, operater_id=account_id, active=1) \
            .all()
        result = [g.id for g in goods]
        return result

    @classmethod
    def list_goods_id_editable(cls, session, shop_id, account_id,
                               permission_text_list):
        """ 获取开票员可以编辑的货品id

        :rtype: list
        """
        if "editgoods" in permission_text_list:
            result = ShopFunc.list_goods_under_shop(session, shop_id)
        else:
            result = cls._list_goods_id_created_by(session, shop_id,
                                                   account_id)
        return result


# 货主的货
class GoodsOwnerFunc():
    @classmethod
    def list_goods_id_owned(cls, session, shop_id, account_id):
        """ 获取货主拥有的货(货主对货有完全的控制权)

        :rtype: list
        """
        supplier = cls.get_supplier(session, shop_id, account_id)
        goods = session.query(models.Goods.id) \
            .filter_by(shop_id=shop_id, shop_supplier_id=supplier.id, active=1) \
            .all()
        result = [g.id for g in goods]
        return result

    @classmethod
    def list_goods_id_editable(cls, session, shop_id, account_id):
        """ 获取货主可以编辑的货

        :rtype: list
        """
        permission_text_list = cls.permission_text_list(
            session, shop_id, account_id)
        if "edit_goods_others" in permission_text_list:
            result = ShopFunc.list_goods_under_shop(session, shop_id)
        else:
            result = cls.list_goods_id_owned(session, shop_id, account_id)
        return result

    @classmethod
    def get_supplier(cls, session, shop_id, account_id):
        """ 获取货主的账户对应的供货商id

        :rtype: obj, class `ShopSupplier`
        """
        result = session.query(models.ShopSupplier) \
            .filter_by(shop_id=shop_id, account_id=account_id, status=1) \
            .first()
        return result

    @classmethod
    def permission_text_list(cls, session, shop_id, account_id):
        """ 解析货主权限列表成文字

        :rtype: list
        """
        supplier = cls.get_supplier(session, shop_id, account_id)
        result = []
        if supplier:
            result = PermissionFunc.permission_text_list(
                "goods_owner",
                PermissionFunc.loads_permission_list(
                    supplier.goods_owner_permission))
        return result


class PrinterFunc():
    @classmethod
    def list_default_printers(cls, session, shop_id):
        """ 获取默认打印机列表

        :rtype: list of `sa.Printer`
        """
        result = session.query(models.Printer) \
            .filter(or_(
            models.Printer.is_salesman_default == 1,
            models.Printer.is_accountant_default == 1,
            models.Printer.is_admin_default == 1)) \
            .filter_by(status=1, shop_id=shop_id) \
            .all()
        return result

    @classmethod
    def gen_printer_assign_link(cls, session, printer_id, account_id):
        """ 生成员工打印机分配的记录, 并将分配状态设置为正常

        :param printer_id: 打印机id
        :param account_id: 账户id

        :rtype: None
        """
        _, assign_link = models.PrinterAssignLink.get_or_create_instance(
            session, printer_id=printer_id, account_id=account_id)
        assign_link.status = 1
        return None


class AppDownloadFunc():
    @staticmethod
    def get_redis_app_update_info(keys_name, app_type="Android", rtype=str):
        """ 从redis中获取APP更新的信息

        :param rtype: str, 获取字符串格式的url; dict, 获取url和更新时间的字典

        :rtype: str or dict
        """
        redis_update_info = redis.get(keys_name)
        if redis_update_info:
            redis_update_info = json.loads(redis_update_info.decode("utf-8"))
            url = redis_update_info[app_type].get("url", "")
            update_time = redis_update_info[app_type].get("verDate", "")
        else:
            url = ""
            update_time = ""
        if rtype == dict:
            return dict(url=url, time=update_time)
        return url


class OrderFinishFunc():
    @classmethod
    def update_goods_storage(cls, goods, quantity, weight):
        """ 更新商品的库存

        :return: 商品更新前的库存信息
        :rtype: tuple
        """
        origin_quantity = goods.storage
        origin_weight = goods.storage_num
        goods.storage -= quantity
        goods.storage_num -= weight
        return origin_quantity, origin_weight

    @classmethod
    def gen_sales_record_by_origin(cls,
                                   session,
                                   sale_record,
                                   record_type,
                                   goods,
                                   batch_id,
                                   order=None,
                                   num=None,
                                   rate=1):
        """ 通过原始小票记录生成新的小票记录 """
        now = datetime.datetime.now()
        t_year, t_month, t_day, t_week = TimeFunc.get_date_split(now)
        # 检查是否扎帐
        account_period_date = now.date()
        if AccountingPeriodFunc.get_accounting_time(session,
                                                    sale_record.shop_id):
            account_period_date = account_period_date + datetime.timedelta(1)

        record = models.GoodsSalesRecord(
            shop_id=sale_record.shop_id,
            goods_id=goods.id,
            salesman_id=sale_record.salesman_id,
            accountant_id=sale_record.accountant_id)
        if not num:
            num = GoodsSaleFunc.gen_sale_record_num(
                session,
                sale_record.shop_id,
                base_time=sale_record.bill_date if not order else None,
            )

        record.record_type = record_type
        record.storage = goods.storage
        record.storage_num = goods.storage_num
        # 没有order是分货记录, 永远跟着主记录走
        record.bill_year = sale_record.bill_year if not order else t_year
        record.bill_month = sale_record.bill_month if not order else t_month
        record.bill_day = sale_record.bill_day if not order else t_day
        record.bill_week = sale_record.bill_week if not order else t_week
        record.bill_date = sale_record.bill_date if not order else now.date()
        record.bill_time = sale_record.bill_time if not order else now.strftime(
            '%H:%M:%S')
        record.create_time = now
        record.num = num
        record.fact_price = sale_record.fact_price
        record.sales_num = sale_record.sales_num * rate
        record.sales_money = sale_record.sales_money * rate
        record.commission = sale_record.commission * rate
        record.commission_mul = sale_record.commission_mul * rate
        record.commission_money = sale_record.commission_money * rate
        record.deposit_avg = sale_record.deposit_avg
        record.deposit_money = sale_record.deposit_money * rate
        record.receipt_money = sale_record.receipt_money * rate
        record.status = 3
        record.shop_customer_id = sale_record.shop_customer_id
        record.account_period_date = sale_record.account_period_date if not order else account_period_date
        record.cost_price = goods.cost_price
        record.pay_type = order.pay_type if order else 0
        record.order_id = order.id if order else 0
        record.order_time = now
        record.goods_batch_id = batch_id
        record.set_gross_money(goods)

        session.add(record)
        session.flush()

        return record

    @classmethod
    def gen_sales_record_by_frontend(cls,
                                     session,
                                     operater_id,
                                     goods,
                                     batch_id,
                                     status,
                                     fact_price,
                                     sales_num,
                                     commission,
                                     commission_mul,
                                     commission_sum,
                                     deposit_avg,
                                     deposit_sum,
                                     receipt_money,
                                     shop_customer_id=0,
                                     remark="",
                                     wireless_print_num="",
                                     printer_remark="",
                                     tare_weight=0,
                                     is_refund=False,
                                     purchase_id=0,
                                     num=None,
                                     bill_time=None,
                                     accountant_id=0,
                                     order_id=None,
                                     pay_type=None,
                                     image_remark=""):
        """ 通过前端传递的参数生成小票 """
        shop_id = goods.shop_id
        user_id = operater_id
        now = datetime.datetime.now()
        weight_unit_text = ConfigFunc.get_weight_unit_text(session, shop_id)

        shop_supplier = session.query(
            models.ShopSupplier).filter_by(id=goods.shop_supplier_id).first()
        if bill_time:
            t_year, t_month, t_day, t_week = TimeFunc.get_date_split(bill_time)
        else:
            t_year, t_month, t_day, t_week = TimeFunc.get_date_split(now)

        sales_record = models.GoodsSalesRecord(
            salesman_id=user_id, shop_id=shop_id, status=0, goods_id=goods.id)
        if not num:
            num = GoodsSaleFunc.gen_sale_record_num(
                session, shop_id, base_time=bill_time)
        sales_record.num = num

        sales_record.set_fact_price(
            round(fact_price * 100),
            goods.unit,
            weight_unit_text=weight_unit_text)
        sales_record.set_sales_num(
            sales_num, goods.unit, weight_unit_text=weight_unit_text)
        sales_record.commission = round(commission * 100)
        sales_record.commission_mul = round(commission_mul * 100)
        sales_record.commission_money = round(commission_sum * 100)
        sales_record.deposit_avg = round(deposit_avg * 100)
        sales_record.deposit_money = round(deposit_sum * 100)
        sales_record.receipt_money = round(receipt_money * 100)
        sales_record.sales_money = \
            sales_record.receipt_money - \
            sales_record.commission_money - \
            sales_record.deposit_money
        sales_record.printer_num = wireless_print_num
        sales_record.printer_remark = printer_remark
        sales_record.set_tare_weight(
            tare_weight, weight_unit_text=weight_unit_text)
        sales_record.bill_year = t_year
        sales_record.bill_month = t_month
        sales_record.bill_day = t_day
        sales_record.bill_week = t_week
        sales_record.bill_date = bill_time.date() if bill_time else now.date()
        sales_record.bill_time = bill_time.time(
        ) if bill_time else now.strftime('%H:%M:%S')
        sales_record.status = status
        sales_record.cost_price = goods.cost_price
        sales_record.shop_customer_id = shop_customer_id
        sales_record.shop_supplier_name = shop_supplier.realname if shop_supplier else ""
        sales_record.remark = remark
        sales_record.purchase_id = purchase_id
        sales_record.goods_batch_id = batch_id
        sales_record.accountant_id = accountant_id
        if is_refund:
            sales_record.record_type = 1
        if order_id:
            sales_record.order_id = order_id
        if pay_type:
            sales_record.pay_type = pay_type
        sales_record.image_remark = image_remark
        sales_record.set_gross_money(goods)
        session.add(sales_record)
        session.flush()

        return sales_record

    @classmethod
    def gen_storage_record(cls, session, goods, origin_storage,
                           origin_storage_num, sale_record, operator_id,
                           record_type):
        """ 生成库存变更记录 """
        storage_record = models.GoodsStorageRecord(
            shop_id=goods.shop_id,
            goods_id=goods.id,
            type=record_type,
            fk_id=sale_record.id,
            operater_id=operator_id,
            origin_storage=origin_storage,
            now_storage=goods.storage,
            origin_storage_num=origin_storage_num,
            now_storage_num=goods.storage_num)
        session.add(storage_record)


class CustomerSignature():
    """客户签名类"""

    @classmethod
    def create_sign_filekey(cls, fileid, source):
        """
        param:fileid 文件ID
        param:source 签名来源，用作文件前缀
            ordersign 赊账
            borrowsign 借支
        """
        from libs.senguo_encrypt import SimpleEncrypt
        from settings import SIGNATURE_IMGS_QINIU_PATH

        # 加密文件名
        encrypt_fileid = SimpleEncrypt.encrypt(fileid)
        file_key = "{path}/{source}/{filename}.png".format(
            path=SIGNATURE_IMGS_QINIU_PATH,
            source=source,
            filename=encrypt_fileid)

        return file_key

    @classmethod
    def get_signature_image(cls, fileid, source="order"):
        """ 从七牛获取签名图片并转为base64 """
        import base64

        import requests

        from libs.senguo_encrypt import SimpleEncrypt
        from settings import SHOP_IMG_HOST, SIGNATURE_IMGS_QINIU_PATH

        # 加密文件名
        encrypt_fileid = SimpleEncrypt.encrypt(fileid)
        file_url = "{domain}/{path}/{source}/{filename}.png".format(
            domain=SHOP_IMG_HOST,
            path=SIGNATURE_IMGS_QINIU_PATH,
            source=source,
            filename=encrypt_fileid)
        ret = requests.get(file_url)

        signature_image = ""
        if ret.status_code == 200:
            signature_image = str(
                base64.b64encode(ret.content).decode("utf-8"))

        return signature_image


class CommonToolsFunc():
    """通用工具函数类"""

    @staticmethod
    def transform_json_list_to_python_list(list_str):
        """将json格式的列表转为Python列表"""
        try:
            list_str = list_str.replace("'", '"')
            desk_list = json.loads(list_str)
            # 避免"123"之类的直接被转为int，结果不是列表
            if isinstance(desk_list, list):
                temp_list = list()
                # 避免出现["1", "2", "3"]
                for item in desk_list:
                    if isinstance(item, str) and item.isdigit():
                        temp_list.append(int(item))
                    else:
                        temp_list.append(item)
                desk_list = temp_list
                return desk_list
            else:
                return list()
        except Exception as e:
            return list()

    @staticmethod
    def transform_csv_to_list(csv_str):
        """将1，2，3这样的字符串转为整型数字列表"""
        try:
            str_list = str.split(csv_str, ',')
            desk_list = list()
            for item in str_list:
                desk_list.append(int(item))
            return desk_list
        except Exception as e:
            return list()

    @staticmethod
    def transform_str_to_list(list_or_csv_str):
        """将[1,2,3]或1,2,3这样的字符串转为list"""
        if list_or_csv_str == "[]" or list_or_csv_str == "":
            return list()
        if list_or_csv_str.startswith("["):
            python_list = CommonToolsFunc.transform_json_list_to_python_list(
                list_or_csv_str)
        else:
            python_list = CommonToolsFunc.transform_csv_to_list(
                list_or_csv_str)
        return python_list

    @staticmethod
    def transform_xml_to_array(xml):
        """将xml转为array"""
        array_data = {}
        root = ET.fromstring(xml)
        for child in root:
            value = child.text
            array_data[child.tag] = value
        return array_data

    @staticmethod
    def trans_xml_to_dict(data_xml):
        """XML转Dict的函数"""
        soup = BeautifulSoup(data_xml, features='xml')
        xml = soup.find('xml')  # 解析XML
        if not xml:
            return {}
        data_dict = dict([(item.name, item.text) for item in xml.find_all()])
        return data_dict

    @staticmethod
    def check_phone(phone_str):
        phone_re = r"1[3456789]\d{9}"
        return True if re.match(phone_re, phone_str) else False

    @staticmethod
    def model_to_dict(model, items):
        """
        将模型类转为字典
        :param model: model
        :param items: iterator
        :return: dict
        """
        info = {}
        for item in items:
            info[item] = getattr(model, item, "")
        return info


class ShopRegisterFunc():
    """注册店铺时用到的公共方法"""

    @staticmethod
    def get_applying_or_failed_shop_by_user_id(session, user_id):
        """
        获取用户最新一次店铺的申请状态
        :param session:
        :param user_id: 申请人id
        :return:
        """
        ShopApply = models.ShopApply
        exist_apply_shop = session.query(ShopApply).filter(ShopApply.account_id == user_id)\
            .order_by(ShopApply.id.desc()).first()  # type: ShopApply
        return exist_apply_shop

    @staticmethod
    def get_deposit_paid_record_by_id(session, record_id):
        """通过id获取paid_record"""
        DepositPaidRecord = models.DepositPaidRecord
        record = session.query(DepositPaidRecord).filter_by(
            id=record_id).first()
        return record

    @staticmethod
    def get_shop_apply_pay_status_by_user_id(session, user_id):
        """通过用户id获取店铺押金支付状态"""
        ShopApply = models.ShopApply
        shop_apply = session.query(ShopApply).filter(
            ShopApply.account_id == user_id,
            ShopApply.status.in_([0, 1])).first()
        if not shop_apply:
            return None
        if shop_apply.deposit_paid == shop_apply.amount_payable and shop_apply.status == 1:
            return True
        else:
            return False

    @staticmethod
    def get_shop_apply_by_user_id(session, user_id):
        """通过用户id获取支付前的shop_apply"""
        ShopApply = models.ShopApply
        shop_apply = session.query(ShopApply).filter(
            ShopApply.account_id == user_id, ShopApply.status == 0).first()
        return shop_apply

    @staticmethod
    def get_shop_apply_by_user_id_and_apply_id(session, user_id, apply_id):
        """通过用户id获取支付前的shop_apply"""
        ShopApply = models.ShopApply
        shop_apply = session.query(ShopApply)\
            .filter(ShopApply.account_id == user_id, ShopApply.status == 0, ShopApply.id == apply_id).first()
        return shop_apply

    @staticmethod
    def get_recent_rejected_shop_apply_by_user_id(session, user_id):
        """通过用户id获取最新的拒绝记录"""
        ShopApply = models.ShopApply
        shop_apply = session.query(ShopApply).filter(ShopApply.account_id == user_id, ShopApply.status == 3)\
            .order_by(ShopApply.id.desc()).first()
        return shop_apply

    @staticmethod
    def is_current_user_applying(session, user_id):
        """通过用户id查询用户是否正在申请店铺"""
        ShopApply = models.ShopApply
        shop_apply = session.query(ShopApply.id)\
            .filter(ShopApply.account_id == user_id, ShopApply.status.in_([0, 1])).limit(1).first()
        return shop_apply


class SuperAdminFunc():
    """总后台模型类相关的函数集"""

    @staticmethod
    def get_super_admin_by_id(session, _id, status=None):
        """默认返回未删除的super_admin"""
        SuperAdmin = models.SuperAdmin
        if not status:
            super_admin = session.query(SuperAdmin).filter(
                SuperAdmin.id == _id, SuperAdmin.is_delete == 0).first()
        else:
            super_admin = session.query(SuperAdmin).filter(
                SuperAdmin.id == _id).first()
        return super_admin

    @staticmethod
    def get_apply_shop_by_passed_shop_id(session, _id):
        """通过正式店铺的id查找申请中的店铺"""
        shop_apply = session.query(
            models.ShopApply).filter_by(shop_id=_id).first()
        return shop_apply

    @staticmethod
    def get_shop_is_trial_close_dict_by_id_list(session, id_list):
        """获取店铺id列表对应的店铺的试用状态"""
        Shop = models.Shop
        shop_trail = session.query(Shop.id, Shop.is_trial, Shop.status).filter(
            Shop.id.in_(id_list)).all()
        id_trial_dict = dict()
        id_status_dict = dict()
        for result in shop_trail:
            id_trial_dict[result[0]] = result[1]
            id_status_dict[result[0]] = result[2]
        return id_trial_dict, id_status_dict


class FileToolFunc():
    """文件相关函数工具"""

    @staticmethod
    def get_file_status_redis_key(shop_id, user_id, timestamp):
        """从redis中获取文件状态的Key"""
        redis_key = "status:{}:{}:{}".format(shop_id, user_id, timestamp)
        return redis_key

    @staticmethod
    def get_file_name_redis_key(shop_id, user_id, timestamp):
        """从redis中获取文件名的Key"""
        redis_key = "name:{}:{}:{}".format(shop_id, user_id, timestamp)
        return redis_key

    @staticmethod
    def get_file_name(redis_key):
        """获取文件基本文件名，不带路径"""
        file_name = redis_export.get(redis_key)
        if not file_name:
            return ""
        else:
            return file_name.decode("utf8")

    @staticmethod
    def get_file_path_name(file_name, file_path=EXPORT_PATH):
        """获取文件带路径文件名"""
        # TODO 升级为多台服务器后，实现服务器间文件共享，例如NFS服务。
        path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), file_path))
        if not os.path.exists(path):
            os.makedirs(path)
        file = os.path.join(path, file_name)
        return file

    @staticmethod
    def get_file_status(redis_key):
        """获取文件状态，0为生成中，1为成功，2为异常"""
        file_status = redis_export.get(redis_key)
        if not file_status:
            return ""
        else:
            return file_status.decode("utf8")

    @staticmethod
    def set_file_status(redis_key, status):
        """获取文件状态，0为生成中，1为成功，2为异常"""
        status_key = redis_key.replace("name", "status")
        redis_export.setex(status_key, 360, status)

    @staticmethod
    def write_csv_file(file_name,
                       content,
                       shop_id,
                       user_id,
                       timestamp,
                       file_path=EXPORT_PATH):
        """本地保存csv文件"""
        file_name += "|{}:{}:{}".format(shop_id, user_id, timestamp)
        file = FileToolFunc.get_file_path_name(file_name, file_path=file_path)
        name_key = FileToolFunc.get_file_name_redis_key(
            shop_id, user_id, timestamp)
        status_key = FileToolFunc.get_file_status_redis_key(
            shop_id, user_id, timestamp)
        try:
            with open(file, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(content)
                redis_export.setex(name_key, 360, file_name)
                redis_export.setex(status_key, 360, 1)
        except Exception as e:
            redis_export.setex(status_key, 360, 2)
            return False, str(e)
        return True, "ok"

    @staticmethod
    def read_csv_file(redis_key, file_path=EXPORT_PATH):
        """读取csv文件"""
        file_name = FileToolFunc.get_file_name(redis_key)
        return_file_name = FileToolFunc.get_return_file_name(file_name)
        path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), file_path))
        file = os.path.join(path, file_name)
        content = []
        try:
            with open(file, 'r', newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.reader(csvfile)
                for item in reader:
                    content.append(item)
        except Exception as e:
            return return_file_name, [["导出错误， 请重试"]]
        try:
            # 每次读取后删除文件
            FileToolFunc.del_local_file(file)
            status_key = redis_key.replace("name", "status")
            # 删除redis
            redis_export.delete(redis_key)
            redis_export.delete(status_key)
        except Exception as e:
            pass
        return return_file_name, content

    @staticmethod
    def del_local_file(file):
        """删除本地文件"""
        try:
            if os.path.exists(file):
                os.remove(file)
        except Exception as e:
            return False
        return True

    @staticmethod
    def get_csv_file_status(redis_key):
        """获取文件状态，0为生成中，1为成功，2为异常"""
        status_key = redis_key.replace("name", "status")
        status = FileToolFunc.get_file_status(status_key)
        return status

    @staticmethod
    def get_return_file_name(full_file_name):
        """通过redis中储存的文件名获取返回给用户的文件名"""
        if full_file_name and "|" in full_file_name:
            return_file_name = full_file_name.split('|')[0]
        else:
            return_file_name = "导出错误，请重试"
        return return_file_name

    @staticmethod
    def set_shop_export_status(shop_id, user_id, exporting_key, seconds=5):
        """设置店铺导出状态，默认十秒"""
        redis_export.setex("exporting:%d-%d" % (shop_id, user_id), seconds,
                           exporting_key)

    @staticmethod
    def get_shop_exporting_item(shop_id, user_id):
        """获取店铺用户是否正在导出"""
        return redis_export.get("exporting:%d-%d" % (shop_id, user_id))

    @staticmethod
    def get_export_msg(redis_key):
        """获取导出时的异常消息"""
        msg_key = redis_key.replace("name", "msg")
        msg = redis_export.get(msg_key)
        if not msg:
            return "导出错误，请重试"
        else:
            return msg.decode("utf8")

    @staticmethod
    def set_export_msg(redis_key, msg):
        """设置导出时的异常消息"""
        msg_key = redis_key.replace("name", "msg")
        redis_export.setex(msg_key, 300, msg)

