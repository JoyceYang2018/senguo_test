/home/yang/.local/bin/yapf /home/yang/working/wholesale/pf.senguo.cc/pfsource/handlers/salersman.py
import datetime
import os, base64
import math
import json
import copy

import tornado.web
from sqlalchemy import func, or_, and_, not_

import dal.models_statistic as models_statistic
import dal.models_account_periods_statistic as models_account_periods_statistic
import dal.models as models
from dal.db_configs import redis
from handlers.base.pub_web import SalesmanBaseHandler
from handlers.base.pub_func import (
    GoodsFunc,
    TimeFunc,
    GoodsSaleFunc,
    WirelessPrintFunc,
    OrderBaseFunc,
    StatisticFunc,
    NumFunc,
    WirelessPrintInfoFunc,
    ShopFunc,
    ShopCustomerFunc,
    AccountingPeriodFunc,
    SupplierFunc,
    Emoji,
    AccountFunc,
    GoodsBatchFunc,
    SalesRecordAjustFunc,
    GraphFunc,
    MultiOrderFunc,
    ConfigFunc,
    UnitFunc,
    GoodsOwnerFunc,
    SalesmanFunc,
    StaffFunc,
    GoodsMasterBranchFunc,
    OrderFinishFunc,
    DataFormatFunc,
    CustomerSignature,
)
from handlers.celery_asyn_work import asyn_log
from handlers.base.pub_log import log_msg
from handlers.boss_salersman import (
    GoodsManageBase,
    SupplierManageBase,
    PrintSetBase,
    GoodsDocsBase,
)
from libs.utils import Qiniu
from libs.senguo_encrypt import QRCodeCrypto
import handlers.base.pub_statistic as pub_statistic
import handlers.base.pub_statistic_account_periods as pub_statistic_account_periods
from settings import ROOT_HOST_NAME
from handlers.boss_accountant import ShopCustomerBase
from dal.customer.model_group import CustomerGroupORM
from dal.customer.func_group import CustomerGroupFunc
from service.goods.batch import GoodsBatchService

check_float = NumFunc.check_float
check_int = NumFunc.check_int


# 店铺设置数据
class ShopConfigData(SalesmanBaseHandler):
    def get(self):
        slave_session = self.slave_session
        shop_id = self.current_shop.id

        config = ShopFunc.get_config(slave_session, shop_id)
        receipt_config = ShopFunc.get_receipt_config(slave_session, shop_id)

        enable_deposit = config.enable_deposit  # 是否启用押金
        enable_commission = config.enable_commission  # 是否启用行费/箱费
        stockin_receipt = receipt_config.stockin_receipt  # 是否开启到货单打印
        distinguish_weight = config.distinguish_weight  # 是否显示区分毛重净重相关设置
        salesman_edit_commission = config.salesman_edit_commission  # 行费可在开票助手修改
        salesman_edit_deposit = config.salesman_edit_deposit  # 押金可在开票助手修改
        deposit_model = config.deposit_model  # 押金模式

        return self.send_success(
            enable_deposit=enable_deposit,
            enable_commission=enable_commission,
            goods_export=1,
            data_export=1,
            stockin_receipt=stockin_receipt,
            distinguish_weight=distinguish_weight,
            salesman_edit_commission=salesman_edit_commission,
            salesman_edit_deposit=salesman_edit_deposit,
            deposit_model=deposit_model,
        )


# 检查更新
class CheckUpdate(tornado.web.RequestHandler):
    def post(self):
        return self.write(redis.get("billapp_update_info") or "")


# 商品入库-页面
class GoodsInStock(SalesmanBaseHandler):
    def get(self, goods_id):
        slave_session = self.slave_session
        shop_id = self.current_shop.id
        goods = GoodsFunc.get_goods(slave_session, shop_id, goods_id)
        if not goods:
            return self.redirect(self.reverse_url("salesHome") + "?tab=2")
        goods_info = GoodsFunc.get_goods_info_with_supplier(
            slave_session, shop_id, goods)
        # 获取最近一次的销售类型
        pay_type = GoodsFunc.get_latest_pay_type(slave_session, goods)
        goods_info["stockin_unit"] = goods.purchase_unit
        goods_info["pay_type"] = pay_type
        return self.render(
            "kaipiao/goods-in-stock.html", goods_info=goods_info)


# 商品入库-接口
class GoodsStockIn(SalesmanBaseHandler, GoodsDocsBase):
    def get(self):
        pass

    @property
    def cls_doc(self):
        """ 单据记录表 """
        return models.StockInDocs

    @property
    def cls_doc_detail(self):
        """ 单据内容（商品详情）表 """
        return models.StockInDocsDetail

    @property
    def doc_name_cn(self):
        """ 单据中文名，用于生成提示信息 """
        return "入库单"

    @property
    def source_record(self):
        """ 生成订单号的参数 """
        return "stockin_order"

    @property
    def cls_record(self):
        """ 记录表 """
        return models.GoodsStockinRecord

    @property
    def storage_type(self):
        """ 库存变更记录类型，index0: 正向记录, index1: 反向记录 """
        return (1, 4)

    def _check_total_price(self, goods_dict, db_goods_dict):
        """ 入库单总价用户输入 """
        if goods_dict.get("total_price") is None:
            raise ValueError("入库数据传入不完整")
        if goods_dict["total_price"] > 9999999:
            raise ValueError("请不要填写过大数据")
        return check_int(goods_dict["total_price"] * 100)

    def _update_goods_cost_price(self, goods, doc_detail):
        """ 入库单加权移动平均法算
        更新单条商品的成本价, 包括按件的成本价和按斤的成本价

        :param goods: 待更新的商品数据
        :param doc_detail: 入库单明细

        :rtype: tuple
        """

        def cal_cost_price(in_stock, add_to_stock):
            """ 通过仓库内的商品数据和新入库的商品数据来计算成本价

            :param in_stock: dict, 仓库内商品数据
            :param add_to_stock: dict, 新入库数据

            :rtype: int
            """
            if in_stock["storage"] < 0:
                in_stock["storage"] = 0
            cost_total = in_stock["storage"] / 100 * in_stock["price"] / 100
            this_total = add_to_stock["price"] / 100
            if cost_total:
                try:
                    cost_price = (cost_total + this_total) / (
                        (in_stock["storage"] / 100) +
                        (add_to_stock["storage"] / 100))
                except BaseException:
                    cost_price = 0
            else:
                try:
                    cost_price = (add_to_stock["price"]) / (
                        add_to_stock["storage"])
                except ZeroDivisionError:
                    cost_price = 0
            return check_int(cost_price * 100)

        goods.cost_price_quantity = cal_cost_price(
            {
                "storage": goods.storage,
                "price": goods.cost_price_quantity
            },
            {
                "storage": doc_detail.quantity,
                "price": doc_detail.total_price
            },
        )
        goods.cost_price_weight = cal_cost_price(
            {
                "storage": goods.storage_num,
                "price": goods.cost_price_weight
            },
            {
                "storage": doc_detail.weight,
                "price": doc_detail.total_price
            },
        )
        return goods.cost_price_quantity, goods.cost_price_weight

    def _update_goods_storage(self, goods, doc_detail):
        """ 入库单算加法 """
        goods.storage += doc_detail.quantity
        goods.storage_num += doc_detail.weight

    def _set_price_info(self, goods_dict, detail):
        """ 入库单用总价来求 """
        goods_dict["quantity_unit_price"] = (check_float(
            detail.total_price / detail.quantity) if detail.quantity else "")
        try:
            goods_dict["weight_unit_price"] = (check_float(
                detail.total_price /
                detail.get_weight(weight_unit_text=self.weight_unit_text))
                                               if detail.weight else "")
        except AttributeError:
            goods_dict["weight_unit_price"] = (check_float(
                UnitFunc.convert_weight_unit(
                    detail.total_price / detail.weight,
                    source="price",
                    weight_unit_text=self.weight_unit_text,
                )) if detail.weight else "")
        return goods_dict

    def _handle_batch(self, goods, doc_detail, is_reset=False, **kwargs):
        if doc_detail.quantity > 0 or doc_detail.weight > 0:
            batch = doc_detail.goods_batch
            if not is_reset:
                goods_dict = kwargs["goods_dict"]
                # 此处去掉了: 新入库的商品需要吃掉默认批次被消耗的库存
                if not batch:
                    batch = GoodsBatchService.produce_purchase(
                        self.session,
                        goods,
                        doc_detail,
                        goods_dict.get("supplier_id", 0),
                        goods_dict.get("batch_num", ""),
                    )
                    if batch:
                        doc_detail.goods_batch_id = batch.id
                else:
                    if batch and not goods.is_master:
                        GoodsBatchService.edit(self.session, batch, doc_detail,
                                               goods_dict["batch_num"])
            else:
                if batch:
                    # 此处去掉了: 如果库存已经被消耗, 从当前批次中扣减
                    batch.batch_status = 0
        else:
            GoodsBatchService.consume(
                self.session,
                goods,
                stockin_record=doc_detail,
                is_reset=is_reset)


# 主页
class Home(GoodsManageBase, SalesmanBaseHandler):
    @SalesmanBaseHandler.check_arguments("tab?:int")
    def get(self):
        session = self.session
        shop_id = self.current_shop.id
        current_user = self.current_user
        current_user_id = current_user.id

        hirelink = (session.query(models.HireLink).filter_by(
            shop_id=shop_id, account_id=current_user_id).first())
        if not hirelink or not hirelink.active_salesman:
            return self.write("您不是当前店铺的有效开票员")

        active_goods_owner = StaffFunc.is_salesman_goods_owner(
            session, shop_id, current_user_id)
        permission_text_list = GoodsOwnerFunc.permission_text_list(
            session, shop_id, current_user_id)
        if active_goods_owner and "sell_goods_others" not in permission_text_list:
            supplier = SupplierFunc.get_supplier_by_account_info(
                session, shop_id, current_user)
            if supplier:
                first_goods = (session.query(models.Goods.id).filter_by(
                    shop_id=shop_id, shop_supplier_id=supplier.id).filter(
                        models.Goods.master_goods_id == 0).first() or None)
            if not supplier or not first_goods:
                first_goods_id = 0
            else:
                first_goods_id = first_goods.id
        else:
            first_goods_id = (self.get_last_receipt_goods_id(
                session, current_user_id, shop_id) or self.get_first_goods_id(
                    session, current_user_id, shop_id) or 0)

        if active_goods_owner:
            goods_id_owned = GoodsOwnerFunc.list_goods_id_owned(
                session, shop_id, current_user_id)
            goods_id_editable = GoodsOwnerFunc.list_goods_id_editable(
                session, shop_id, current_user_id)
        else:
            goods_id_owned = []
            goods_id_editable = SalesmanFunc.list_goods_id_editable(
                session, shop_id, current_user_id,
                self.current_user.permission_list)

        tab = self.args.get("tab")
        if tab is None:
            if first_goods_id:
                return self.redirect(
                    self.reverse_url("goodsSale", first_goods_id) + "?first=1")
            tab = 2

        user_info = self.get_current_user_info()
        shop_info = {}
        shop_info["shop_name"] = self.current_shop.shop_name
        shop_info["shop_logo"] = self.current_shop.shop_img_with_domain

        config = ShopFunc.get_config(session, shop_id)
        config_info = {}
        config_info["img_show"] = config.salesman_goods_img
        config_info["multi_sale_goods"] = config.multi_sale_goods
        config_info["boss_app_pro"] = config.boss_app_pro
        config_info["accounting_periods"] = config.accounting_periods

        groups_info = self.get_group_list()
        priority_group = GoodsManageBase.get_priority_group(
            session, shop_id, current_user_id)
        priority_group_info = {}
        if priority_group:
            priority_group_info["id"] = priority_group.id
            priority_group_info["name"] = priority_group.name
        else:
            priority_group_info["id"] = groups_info[0]["id"]
            priority_group_info["name"] = groups_info[0]["name"]

        # 转字符串
        goods_id_owned = ",".join([str(i) for i in goods_id_owned])
        goods_id_editable = ",".join([str(i) for i in goods_id_editable])

        return self.render(
            "kaipiao/salesman.html",
            user_info=user_info,
            tab=tab,
            shop_info=shop_info,
            groups_info=groups_info,
            config_info=config_info,
            first_goods_id=first_goods_id,
            active_goods_owner=active_goods_owner,
            goods_id_owned=goods_id_owned,
            goods_id_editable=goods_id_editable,
            priority_group_info=priority_group_info,
        )

    def get_first_goods_id(self, session, current_user_id, shop_id):
        filter_list = self.filter_goods_id(session, current_user_id, shop_id)
        Goods = models.Goods
        first_goods_id = (session.query(Goods.id).filter_by(
            shop_id=shop_id,
            active=1).filter(~Goods.id.in_(filter_list)).first())
        if first_goods_id:
            first_goods_id = first_goods_id[0]
        else:
            first_goods_id = 0
        return first_goods_id

    def get_last_receipt_goods_id(self, session, current_user_id, shop_id):
        filter_list = self.filter_goods_id(session, current_user_id, shop_id)
        # 查询上一次开票的商品
        GoodsSalesRecord = models.GoodsSalesRecord
        last_receit_goods_id = (session.query(
            GoodsSalesRecord.goods_id).filter_by(
                salesman_id=current_user_id, shop_id=shop_id).filter(
                    ~GoodsSalesRecord.goods_id.in_(filter_list)).order_by(
                        GoodsSalesRecord.id.desc()).first())
        if last_receit_goods_id:
            last_receit_goods_id = last_receit_goods_id[0]
        else:
            last_receit_goods_id = 0
        return last_receit_goods_id

    def filter_goods_id(self, session, current_user_id, shop_id):
        """过滤隐藏商品及不可见商品"""
        # 查询不可售卖商品
        goods_limit = GoodsFunc.get_salesman_goods_limit_list(
            session, shop_id, current_user_id)
        # 查询隐藏商品
        goods_top = self.get_goods_top(session, shop_id, current_user_id)
        # 查询分货
        map_goods_branch = GoodsMasterBranchFunc.list_map_branch_to_master(
            session, shop_id)
        if goods_top:
            goods_limit += json.loads(goods_top.invisible_list)
        if map_goods_branch:
            goods_limit += map_goods_branch.keys()
        filter_list = list(set(goods_limit))
        return filter_list

    @SalesmanBaseHandler.check_arguments("action:str")
    def post(self):
        action = self.args["action"]
        if action == "shop_change":
            return self.shop_change()
        elif action == "get_valid_shops":
            shoplist = self.get_valid_shops("salesman")
            return self.send_success(shoplist=shoplist)
        elif action == "get_flow":
            return self.get_flow()
        elif action == "nullify_ticket":
            return self.nullify_ticket()
        elif action == "reopen_ticket":
            return self.reopen_ticket()
        elif action == "sales_record":
            return self.get_sales_record()
        elif action == "sales_record_finished":
            return self.get_sales_record_finished()
        elif action == "get_multi_order_detail":
            return self.get_multi_order_detail()
        elif action == "get_record_detail":
            return self.get_record_detail()
        elif action == "get_goods_info":
            return self.get_goods_info()
        elif action == "get_sales_each_goods":
            return self.get_sales_each_goods()
        elif action == "get_sales_each_day":
            return self.get_sales_each_day()
        elif action == "get_sales_each_week":
            return self.get_sales_each_week()
        elif action == "get_sales_each_month":
            return self.get_sales_each_month()
        elif action == "signature":
            return self.get_order_signature_image()
        elif action == "set_priority_group":
            return self.set_priority_group()
        return self.send_fail("action有误")

    @SalesmanBaseHandler.check_arguments("shop_id:int")
    def shop_change(self):
        shop_id = self.args["shop_id"]
        try:
            to_log_dict = tornado.escape.json_decode(self.request.body)
            log_date = datetime.datetime.now().strftime("%Y%m%d")
            if to_log_dict.get("log", ""):
                operate_log_str = to_log_dict.pop("log")
                asyn_log("operatelog/%d/%s" % (int(shop_id), log_date),
                         operate_log_str)
            to_log_dict["receipt_time"] = datetime.datetime.now().strftime(
                "%H:%M:%S")
            asyn_log("frontparams/%d/%s" % (int(shop_id), log_date),
                     to_log_dict)
        except:
            import traceback, sys

            server_error_messsage = traceback.format_exception(*sys.exc_info())
            server_error_messsage = ",".join(server_error_messsage)
            log_msg(
                "asyn_log_error",
                str(datetime.datetime.now()) + ": " + server_error_messsage,
            )

        current_user = self.current_user
        session = self.session
        hirelink = (session.query(models.HireLink).filter_by(
            account_id=current_user.id, shop_id=shop_id,
            active_salesman=1).first())
        if not hirelink:
            return self.send_fail("您不是选定店铺的有效销售经理")
        else:
            self.set_secure_cookie("pfshop_id", str(shop_id))
            return self.send_success()

    # 小票流水，按订单展示
    @SalesmanBaseHandler.check_arguments(
        "from_date?:str",
        "to_date?:str",
        "choose_date?:str",
        "page?:int",
        "show_type?:int",
    )
    def get_flow(self):
        """ 获取开票流水, 按订单展示 """
        page = self.args.get("page", 0)
        try:
            from_date = datetime.datetime.strptime(
                self.args.get("from_date"), "%Y-%m-%d").date()
            to_date = datetime.datetime.strptime(
                self.args.get("to_date"), "%Y-%m-%d").date()
        except BaseException:
            return self.send_fail("日期传入格式错误，应为`YYYY-mm-dd`")

        # 查询小票流水
        records, page_sum, _ = GoodsSaleFunc.query_sales_record_by_billing_record(
            start_date=from_date,
            end_date=to_date,
            session=self.slave_session,
            shop_id=self.current_shop.id,
            salesman_id_list=[self.current_user.id],
            page=page,
            accountant_id_list=[],
            pay_type_list=[],
            show_type=self.args.get("show_type", 0),
            only_refund=False,
            only_paid=False,
            all_page=True,
        )
        data_list = GoodsSaleFunc.list_merged_records(
            self.slave_session, self.current_shop.id, records, sub_multi=False)
        nomore = 1 if page >= page_sum else 0
        return self.send_success(data_list=data_list, nomore=nomore)

    # 小票作废
    @SalesmanBaseHandler.check_arguments("order_id?:int", "record_id?:int")
    def nullify_ticket(self):
        """ 小票作废(作废以后不能结算)

        :param order_id: 订单id
        :param record_id: 流水id
        """
        order_id = self.args.get("order_id")
        record_id = self.args.get("record_id")
        if not (bool(order_id) ^ bool(record_id)):
            return self.send_fail("参数错误, 必须且仅能传一个参数")
        if order_id:
            is_success, msg = GoodsSaleFunc.nullify_order(
                self.session, order_id, self.current_user.id,
                self.current_shop.id)
        else:
            is_success, msg = GoodsSaleFunc.nullify_sales_record(
                self.session, [record_id], self.current_user.id,
                self.current_shop.id)
        if not is_success:
            return self.send_fail(msg)
        self.session.commit()
        return self.send_success()

    # 重新开票
    @SalesmanBaseHandler.check_arguments("order_id?:int", "record_id?:int")
    def reopen_ticket(self):
        """ 获取重新开票的必要信息, 暂时仅返回商品和售出信息 """
        order_id = self.args.get("order_id")
        record_id = self.args.get("record_id")
        if not (bool(order_id) ^ bool(record_id)):
            return self.send_fail("参数错误, 必须且仅能传一个参数")
        if order_id:
            return self.reopen_order(order_id)
        else:
            return self.reopen_record(record_id)

    def reopen_order(self, order_id):
        """ 获取多品重新开票的信息 """
        try:
            order = self._check_input_order_id(order_id)
        except ValueError as why:
            return self.send_fail(str(why))
        records = self._query_records_under_order(order)
        config = ShopFunc.get_config(self.slave_session, self.current_shop.id)
        data_list = [
            GoodsSaleFunc.get_sale_record_basic_info(self.slave_session, r,
                                                     config.display_kg_instead)
            for r in records
        ]
        return self.send_success(
            data_list=data_list,
            goods_info=self._get_goods_info_under_records(records))

    def reopen_record(self, record_id):
        """ 获取单品重新开票信息 """
        try:
            record = self._check_input_record_id(record_id)
        except ValueError as why:
            return self.send_fail(str(why))
        config = ShopFunc.get_config(self.slave_session, self.current_shop.id)
        return self.send_success(
            data=GoodsSaleFunc.get_sale_record_basic_info(
                self.slave_session, record, config.display_kg_instead))

    # 已完成记录
    @SalesmanBaseHandler.check_arguments("choose_date:str", "goods_id:int",
                                         "page?:int")
    def get_sales_record_finished(self):
        """ 获取某一商品的已完成（已结算或退款）记录 """
        try:
            choose_date = datetime.datetime.strptime(self.args["choose_date"],
                                                     "%Y-%m-%d").date()
        except BaseException:
            return self.send_fail("日期传入格式错误，应为`YYYY-mm-dd`")
        goods_id = self.args["goods_id"]
        records, nomore = self._query_sales_record(
            choose_date, goods_id, page=self.args.get("page", 0))
        _, goods_unit = GoodsFunc.get_goods_name_unit_through_id(
            self.slave_session, goods_id)

        data_list = []
        for r in records:
            data = dict(
                id=r.id,
                bill_time=TimeFunc.time_to_str(r.bill_time, "time"),
                num=r.num[-4:],
                fact_price=NumFunc.check_float(
                    r.get_fact_price(
                        goods_unit, weight_unit_text=self.weight_unit_text) /
                    100),
                sales_weigh=NumFunc.check_float(
                    r.get_sales_num(
                        goods_unit, weight_unit_text=self.weight_unit_text)),
                commission_mul=NumFunc.check_float(r.commission_mul / 100),
                receipt_money=NumFunc.check_float(r.receipt_money / 100),
                is_after_accounting=getattr(r, "is_after_accounting", 0),
            )
            data["receipt_money"] = (data["receipt_money"]
                                     if r.record_type == 0 else
                                     -data["receipt_money"])
            if goods_unit == 1:
                data["sales_weigh"] = "-"
            data_list.append(data)
        return self.send_success(data_list=data_list, nomore=nomore)

    def _get_goods_info_under_records(self, records):
        """ 获取开票流水下所有商品的数据, 格式与前端缓存一致
        XXX 缓存与这的押金与行费开关已经全部由前端处理了

        :rtype: dict
        """
        slave_session = self.slave_session
        shop_id = self.current_shop.id
        Goods = models.Goods
        GoodsGroup = models.GoodsGroup
        if not records:
            return {}
        goods_id_list = [r.goods_id for r in records]
        goods_all = (slave_session.query(Goods).filter_by(
            shop_id=shop_id).filter(Goods.id.in_(goods_id_list)).all())
        # 查询供应商信息
        shop_supplier_id_list = [x.shop_supplier_id for x in goods_all]
        supplier_dict = SupplierFunc.get_shop_supplier_base_info(
            slave_session, shop_id, shop_supplier_id_list)

        # 查询分组信息
        group_ids = {i.group_id for i in goods_all}
        group_infos = (slave_session.query(
            GoodsGroup.id, GoodsGroup.name).filter(
                GoodsGroup.id.in_(group_ids)).all())
        group_infos = {i.id: i.name for i in group_infos}

        # 根据商品名称返回同名商品的供应商信息
        name_list = {x.name for x in goods_all}
        supplier_list_dict = SupplierFunc.get_supplier_through_goods_name(
            slave_session, shop_id, name_list)

        # 构造返回数据
        result = {}
        for goods in goods_all:
            goods_id = goods.id
            shop_supplier_id = goods.shop_supplier_id
            goods_info = GoodsFunc.get_goods_info(
                goods, multi=True, weight_unit_text=self.weight_unit_text)
            supplier_info = supplier_dict.get(shop_supplier_id)

            goods_info["supplier_id"] = shop_supplier_id
            goods_info["supplier_name"] = ""
            goods_info["supplier_phone"] = ""
            goods_info["top"] = False
            goods_info["hide"] = 0
            goods_info["group_id"] = goods.group_id
            goods_info["group_name"] = group_infos.get(goods.group_id, "")
            goods_info["supplier_list"] = supplier_list_dict.get(
                goods.name, [])

            if supplier_info:
                goods_info["supplier_name"] = supplier_info["name"]
                goods_info["supplier_phone"] = supplier_info["phone"]

            result[goods_id] = goods_info
        return result

    @SalesmanBaseHandler.check_arguments("show_type?:int")
    def _query_sales_record(self, choose_date, goods_id=None, page=None):
        """ 查询小票流水

        :param choose_date: date对象
        :param goods_id: 货品id, 传了货品id就意味着获取的是已完成的记录

        :return: 当日所有小票流水
        :rtype: list of SqlAlchemy `result`
        """
        page_size = 20
        data_type = self.args.get("show_type", 0)
        query_base = self.session.query(models.GoodsSalesRecord).filter_by(
            shop_id=self.current_shop.id, salesman_id=self.current_user.id)
        # 扎账日显示
        if data_type == 1:
            if choose_date == datetime.date.today():
                if not goods_id:
                    query_base1 = query_base.filter(
                        models.GoodsSalesRecord.account_period_date ==
                        choose_date,
                        models.GoodsSalesRecord.status.in_([3, 5]),
                    )
                    query_base2 = query_base.filter(
                        models.GoodsSalesRecord.bill_date == choose_date,
                        models.GoodsSalesRecord.status.in_([3, 5]),
                    )
                else:
                    query_base1 = query_base.filter(
                        models.GoodsSalesRecord.account_period_date ==
                        choose_date,
                        models.GoodsSalesRecord.status.in_([3, 5]),
                        models.GoodsSalesRecord.goods_id == goods_id,
                    )
                    query_base2 = query_base.filter(
                        models.GoodsSalesRecord.bill_date == choose_date,
                        models.GoodsSalesRecord.status.in_([3, 5]),
                        models.GoodsSalesRecord.goods_id == goods_id,
                    )
                query_base = query_base1.union(query_base2)
            else:
                query_base = query_base.filter_by(
                    account_period_date=choose_date)
        # 自然日
        else:
            query_base = query_base.filter(
                models.GoodsSalesRecord.bill_date == choose_date)

        if not goods_id:
            query_base = query_base.filter(
                models.GoodsSalesRecord.status.in_([-1, 1, 2, 3, 4, 5]))
        else:
            query_base = query_base.filter(
                models.GoodsSalesRecord.status.in_([3, 5]),
                models.GoodsSalesRecord.goods_id == goods_id,
            )
        query_base = query_base.order_by(models.GoodsSalesRecord.id.desc())
        if page is not None:
            query_base = query_base.offset(page * page_size).limit(page_size)

        result = query_base.all()

        # 根据轧账日来进行展示的时候只有今天的数据需要判断
        if choose_date == datetime.date.today() and data_type == 1:
            accounting_time = AccountingPeriodFunc.get_accounting_time(
                self.slave_session, self.current_shop.id)
            if accounting_time:
                accounting_time = accounting_time.time()
                [
                    setattr(
                        r,
                        "is_after_accounting",
                        1 if r.account_period_date > choose_date or
                        (r.bill_time > accounting_time
                         and r.bill_date == datetime.date.today()) else 0,
                    ) for r in result
                ]

        nomore = 1 if len(result) < page_size else 0
        return result, nomore

    def _query_records_under_order(self, order):
        """ 获取一个多品订单下的所有小票流水

        :rtype: list
        """
        result = (self.session.query(models.GoodsSalesRecord).filter_by(
            shop_id=self.current_shop.id,
            salesman_id=self.current_user.id,
            bill_date=order.create_time.date(),
            multi_order_id=order.id,
        ).all())
        return result

    def _check_input_record_id(self, record_id, is_nullify=False):
        """ 检查传入的小票记录id
        若有效, 返回这条记录

        :param record_id: 记录id
        :param is_nullify: 查找是否是为了作废

        :rtype: obj, `class GoodsSalesRecord`
        """
        record = (self.session.query(models.GoodsSalesRecord).filter_by(
            id=record_id, shop_id=self.current_shop.id).first())
        if not record:
            raise ValueError("小票流水不存在")
        if is_nullify and record.status != 1:
            raise ValueError("小票流水已支付或已取消, 无法作废")
        return record

    def _check_input_order_id(self, order_id, is_nullify=False):
        """ 检查传入的小票记录id
        若有效, 返回这条记录

        :param order_id: 订单id
        :param is_nullify: 查找是否是为了作废

        :rtype: obj, `class Order`
        """
        order = (self.session.query(models.Order).filter_by(
            id=order_id, shop_id=self.current_shop.id).first())
        if not order:
            raise ValueError("订单不存在")
        if is_nullify and order.status != 0:
            raise ValueError("订单已付款或已取消，无法作废")
        return order

    @SalesmanBaseHandler.check_arguments("id:int")
    def get_multi_order_detail(self):
        """ 获取订单详情, 仅针对多品

        :param id: 订单id
        """
        order = models.Order.get_by_id(self.slave_session, self.args["id"])
        records = self._query_records_under_order(order)
        goods_list, order_total_dict = GoodsSaleFunc.get_multi_order_info(
            self.slave_session, records, order)
        return self.send_success(
            goods_list=goods_list, order_total_dict=order_total_dict)

    # 某张小票的详情
    @SalesmanBaseHandler.check_arguments("id:int")
    def get_record_detail(self):
        """ 获取小票记录的详情

        :param id: 记录id
        """
        slave_session = self.slave_session
        record = models.GoodsSalesRecord.get_by_id(slave_session,
                                                   self.args["id"])
        if not record:
            return self.send_fail("开票记录不存在")

        data = GoodsSaleFunc.get_sale_record_info(
            slave_session, record, weight_unit_text=self.weight_unit_text)
        if record.record_type == 1:
            data["commission_mul"] = -data["commission_mul"]
            data["sales_num"] = -data["sales_num"]
            data["goods_sumup"] = -data["goods_sumup"]
            data["commission_sumup"] = -data["commission_sumup"]
            data["deposit_total"] = -data["deposit_total"]
            data["receipt_money"] = -data["receipt_money"]

        # 增加抹零相关信息
        if record.order_id:
            order = OrderBaseFunc.get_order_throuth_id(slave_session,
                                                       record.order_id)
            data["order_num"] = order.num
            data["erase_money"] = check_float(order.erase_money / 100)
            data["order_record_count"] = (slave_session.query(
                models.GoodsSalesRecord.id).filter(
                    models.GoodsSalesRecord.order_id == record.order_id).
                                          count())
        else:
            data["order_num"] = ""
            data["erase_money"] = 0
            data["order_record_count"] = 0

        # 增加串货调整信息
        could_ajust, ajust_info = SalesRecordAjustFunc.get_ajust_info(
            slave_session, record)
        data["ajust_info"] = ajust_info

        return self.send_success(data=data)

    # 获取单个商品的信息
    @SalesmanBaseHandler.check_arguments("id:int")
    def get_goods_info(self):
        """ 获取单个商品的信息 """
        goods = (self.slave_session.query(models.Goods).filter_by(
            shop_id=self.current_shop.id, id=self.args["id"]).filter(
                models.Goods.active.in_([0, 1, 2])).first())
        if not goods:
            return self.send_fail("商品不存在或已被删除")
        return self.send_success(
            data=GoodsFunc.get_goods_info_with_supplier(
                self.slave_session, self.current_shop.id, goods))

    # 按天统计
    @SalesmanBaseHandler.check_arguments("from_date?:str", "to_date?:str",
                                         "page?:int", "show_type?:int",
                                         "goods_id?:int")
    def get_sales_each_day(self):
        """ 日统计 """
        from_date = self.args.get("from_date")
        to_date = self.args.get("to_date")
        data_type = self.args.get("show_type", 0)
        page = self.args.get("page", 0)
        goods_id = check_int(self.args.get("goods_id", 0))
        try:
            from_date, to_date = self._check_input_date(from_date, to_date)
        except ValueError as why:
            return self.send_fail(str(why))

        data_list = self._list_sales_statistic(
            from_date,
            to_date,
            page=page,
            data_type=data_type,
            goods_id=goods_id,
            statistic_type=1,
        )

        sum_dict = self._get_sales_sum(
            from_date, to_date, data_type=data_type, goods_id=goods_id)
        graph_list = self._get_graph_data_list(data_list)

        # 除第一页数据之外，其他时间点不更新图表
        if page > 0:
            return self.send_success(data_list=data_list, sum_dict=sum_dict)
        return self.send_success(
            data_list=data_list, graph_list=graph_list, sum_dict=sum_dict)

    # 按周统计
    @SalesmanBaseHandler.check_arguments("page?:int", "goods_id?:int",
                                         "show_type?:int")
    def get_sales_each_week(self):
        """ 周统计 """
        data_type = self.args.get("show_type", 0)
        page = self.args.get("page", 0)
        goods_id = check_int(self.args.get("goods_id", 0))

        data_list = self._list_sales_statistic(
            None,
            None,
            page=page,
            data_type=data_type,
            goods_id=goods_id,
            statistic_type=2,
        )
        sum_dict = self._get_sales_sum(
            None, None, data_type=data_type, goods_id=goods_id)
        graph_list = self._get_graph_data_list(data_list)

        # 除第一页数据之外，其他时间点不更新图表
        if page > 0:
            return self.send_success(data_list=data_list, sum_dict=sum_dict)
        return self.send_success(
            data_list=data_list, graph_list=graph_list, sum_dict=sum_dict)

    # 按月统计
    @SalesmanBaseHandler.check_arguments("page?:int", "goods_id?:int",
                                         "show_type?:int")
    def get_sales_each_month(self):
        """ 月统计 """
        data_type = self.args.get("show_type", 0)
        page = self.args.get("page", 0)
        goods_id = check_int(self.args.get("goods_id", 0))

        data_list = self._list_sales_statistic(
            None,
            None,
            page=page,
            data_type=data_type,
            goods_id=goods_id,
            statistic_type=3,
        )
        sum_dict = self._get_sales_sum(
            None, None, data_type=data_type, goods_id=goods_id)
        graph_list = self._get_graph_data_list(data_list)

        # 除第一页数据之外，其他时间点不更新图表
        if page > 0:
            return self.send_success(data_list=data_list, sum_dict=sum_dict)
        return self.send_success(
            data_list=data_list, graph_list=graph_list, sum_dict=sum_dict)

    # 按商品统计
    @SalesmanBaseHandler.check_arguments("from_date?:str", "to_date?:str",
                                         "show_type?:int", "page?:int")
    def get_sales_each_goods(self):
        """ 获取各货品在某段时间的销售情况 """
        from_date = self.args.get("from_date")
        to_date = self.args.get("to_date")
        data_type = self.args.get("show_type", 0)
        page = self.args.get("page", 0)
        try:
            from_date, to_date = self._check_input_date(from_date, to_date)
        except ValueError as why:
            return self.send_fail(str(why))

        data_list = self._list_sales_each_goods(
            from_date, to_date, page, data_type=data_type)
        goods_id_list = [data.get("goods_id") for data in data_list]
        goods = (self.session.query(models.Goods).filter(
            models.Goods.id.in_(goods_id_list)).order_by(
                models.Goods.id).all() if goods_id_list else [])
        goods_dict = {g.id: g for g in goods}
        for data in data_list:
            goods_id = data["goods_id"]
            goods_name = GoodsFunc.get_goods_name_with_attr(
                goods_dict.get(goods_id))
            data.setdefault("goods_name", goods_name)

        sum_dict = self._get_sales_sum(from_date, to_date, data_type=data_type)
        return self.send_success(data_list=data_list, sum_dict=sum_dict)

    def _check_input_date(self, from_date, to_date):
        return TimeFunc.transfer_input_range_to_date(from_date, to_date)

    def _get_graph_data_list(self, data_list):
        """ 根据统计数据获取绘制折线图的数据, 为美观暂控制最多12条

        :param data_list: 统计数据

        :rtype: list
        """
        return GraphFunc.convert(data_list, "sales_money")

    def _list_sales_statistic(self,
                              from_date,
                              to_date,
                              page=0,
                              data_type=0,
                              goods_id=None,
                              statistic_type=1):
        """ 开票员销售走势统计(日统计/周统计/月统计/年统计)

        :param from_date: 开始日期
        :param to_date: 结束日期
        :param data_type: 数据展示类型
        :param goods_id: 商品id, 传入表示单个开票员单个商品的销售走势统计
        :param statistic_type: 统计类型 1: 日统计 2: 周统计 3: 月统计 4: 年统计
        :param page: 页码
        """
        self._insert_session(data_type)
        page_size = 20
        if data_type == 1:
            sTable = (models_account_periods_statistic.
                      AccountPeriodsStatisticSalesGoodsSalesman)
        else:
            sTable = models_statistic.StatisticSalesGoodsSalesman

        columns = StatisticFunc.get_query_param(sTable)
        group_by_param = StatisticFunc.get_group_by_param(
            sTable, statistic_type)
        columns += group_by_param

        query_base = self.statistic_session.query(
            # 串货调整会生成负向记录, 影响最终展示的笔数, 求绝对值
            func.sum(sTable.sales_pay_count +
                     2 * sTable.refund_count).label("count"),
            *columns).filter_by(
                shop_id=self.current_shop.id,
                salesman_id=self.current_user.id,
                statistic_type=StatisticFunc.get_filter_type(statistic_type),
            )
        if from_date:
            query_base = query_base.filter(sTable.statistic_date >= from_date)
        if to_date:
            query_base = query_base.filter(sTable.statistic_date < to_date)
        if goods_id:
            query_base = query_base.filter(sTable.goods_id == goods_id)

        master_goods_id_set = GoodsMasterBranchFunc.list_goods_id_master(
            self.slave_session, self.current_shop.id)
        query_base = query_base.filter(
            sTable.goods_id.in_(master_goods_id_set))

        goods = None
        if goods_id:
            goods = models.Goods.get_by_id(self.session, goods_id)

        query_base = query_base.having(
            and_(
                func.sum(sTable.total_goods_cent) != 0,
                func.sum(sTable.commission_mul) != 0,
                func.sum(sTable.sales_pay_count + 2 * sTable.refund_count) !=
                0,
            ))

        basic_order_by_param = StatisticFunc.get_basic_order_by_param(
            sTable, statistic_type)
        query_result = (query_base.group_by(*group_by_param).order_by(
            *basic_order_by_param).offset(
                page_size * page).limit(page_size).all())

        today = datetime.datetime.today()
        result = []
        for q in query_result:
            date_range = TimeFunc.list_date_range(
                statistic_type,
                date=getattr(q, "statistic_date", None),
                year=getattr(q, "year", None),
                week=getattr(q, "week", None),
                month=getattr(q, "month", None),
            )
            data = {
                "date":
                StatisticFunc.format_date(q, statistic_type),
                "sales_count":
                check_int(q.sales_count),
                "date_range":
                date_range,
                "is_after_accounting":
                StatisticFunc.is_after_accounting(q, statistic_type, today),
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
                        q.sales_weigh / 100,
                        source="weight",
                        weight_unit_text=self.weight_unit_text,
                    )),
                "sales_money":
                check_float(q.total_goods_cent / 100),
                "total_commission":
                check_float(q.total_commission / 100),
                "deposit_cent":
                check_float(q.deposit_cent / 100),
                "gross_cent":
                check_float(q.gross_cent / 100),
            }
            if goods and goods.unit == 1:
                data["sales_weigh"] = "-"
            result.append(data)
        return result

    def _list_sales_each_goods(self, from_date, to_date, page, data_type=0):
        """ 货品统计, 获取某段时间内各商品的各卖了多少, 每条商品一条数据 """
        self._insert_session(data_type)
        data_list, _ = GoodsSaleFunc.list_sales_each_goods_salesman(
            self.slave_session,
            self.statistic_session,
            self.current_shop.id,
            self.current_user.id,
            from_date,
            to_date,
            page,
            data_type=data_type,
        )
        return data_list

    def _get_sales_sum(self, from_date, to_date, data_type=0, goods_id=None):
        """ 销售记录求和, 获取一段时间内的销售情况汇总 """
        if data_type == 1:
            sTable = (models_account_periods_statistic.
                      AccountPeriodsStatisticSalesGoodsSalesman)
        else:
            sTable = models_statistic.StatisticSalesGoodsSalesman

        master_goods_id_set = GoodsMasterBranchFunc.list_goods_id_master(
            self.slave_session, self.current_shop.id)
        table_query_param = StatisticFunc.get_query_param(sTable)
        query_base = (
            self.statistic_session.query(
                # 串货调整会生成负向记录, 影响最终展示的笔数, 求绝对值
                func.sum(sTable.sales_pay_count +
                         2 * sTable.refund_count).label("count"),
                *table_query_param).filter_by(
                    shop_id=self.current_shop.id,
                    statistic_type=1,
                    salesman_id=self.current_user.id,
                ).filter(sTable.goods_id.in_(master_goods_id_set)).having(
                    and_(
                        func.sum(sTable.total_goods_cent) != 0,
                        func.sum(sTable.commission_mul) != 0,
                        func.sum(sTable.sales_pay_count +
                                 2 * sTable.refund_count) != 0,
                    )))
        if from_date:
            query_base = query_base.filter(sTable.statistic_date >= from_date)
        if to_date:
            query_base = query_base.filter(sTable.statistic_date < to_date)
        if goods_id:
            query_base = query_base.filter(sTable.goods_id == goods_id)
        query_result = query_base.first() if master_goods_id_set else None

        if query_result:
            result = {
                "sum_sales_count":
                check_int(query_result.sales_count),
                # 历史遗留问题, sales_pay_count的名字被占用了
                "sum_sales_pay_count":
                check_int(query_result.count),
                "sum_refund_count":
                check_int(query_result.refund_count),
                "sum_commission_mul":
                check_float(query_result.commission_mul / 100),
                "sum_sales_weigh":
                check_float(
                    UnitFunc.convert_weight_unit(
                        query_result.sales_weigh,
                        source="weight",
                        weight_unit_text=self.weight_unit_text,
                    ) / 100),
                "sum_sales_money":
                check_float(query_result.total_goods_cent / 100),
                "sum_total_commission":
                check_float(query_result.total_commission / 100),
                "sum_deposit_cent":
                check_float(query_result.deposit_cent / 100),
                "sum_gross_cent":
                check_float(query_result.gross_cent / 100),
            }
        else:
            result = {
                "sum_sales_count": 0,
                "sum_sales_pay_count": 0,
                "sum_refund_count": 0,
                "sum_commission_mul": 0,
                "sum_sales_weigh": 0,
                "sum_sales_money": 0,
                "sum_total_commission": 0,
                "sum_deposit_cent": 0,
                "sum_gross_cent": 0,
            }
        return result

    # 后续如果统计需要进行扩展，还可以继续对这个方法进行重构
    def _insert_session(self, data_type):
        """ 计算单个开票员今日的统计数据，并将结果插入sa.session，但是不更新数据库 """

        # 搬运自pub_statistic.py func: goods_sale_record

        today = datetime.date.today()
        GoodsSalesRecord = models.GoodsSalesRecord
        accounting_time_today = AccountingPeriodFunc.get_accounting_time(
            self.slave_session, self.current_shop.id)
        query_base = (self.slave_session.query(GoodsSalesRecord).filter_by(
            shop_id=self.current_shop.id,
            salesman_id=self.current_user.id).filter(
                GoodsSalesRecord.status.in_([1, 2, 3, 4, 5])))
        # 已经轧帐需要算两天的时间
        if data_type == 1 and accounting_time_today:
            query_base = query_base.filter_by(
                account_period_date=today + datetime.timedelta(1))
        elif data_type == 1:
            query_base = query_base.filter_by(account_period_date=today)
        else:
            query_base = query_base.filter_by(bill_date=today)
        all_goods_sales_records = query_base.all()
        salesman_id_goods_id_list = {
            "%s-%s" % (x.salesman_id, x.goods_id)
            for x in all_goods_sales_records
        }

        goods_info_dict = {
            "goods_unit": 0,
            "supperlier_id": 0,
            "goods_name": ""
        }
        sales_kpi_base_dict = {
            "commission_mul": 0,
            "sales_weigh": 0,
            "total_commission": 0,
            "deposit_cent": 0,
            "total_goods_cent": 0,
        }
        salesman_kpi_dict = {
            "sales_count": 0,
            "sales_pay_count": 0,
            "refund_count": 0,
            "avg_reciept_cent": 0,
        }
        goods_kpi_dict = {"avg_price": 0, "gross_cent": 0, "gross_percent": 0}

        from collections import ChainMap

        base_goods_salesman_dict = dict(
            ChainMap(goods_info_dict, salesman_kpi_dict, sales_kpi_base_dict,
                     goods_kpi_dict))

        goods_id_list = {x.goods_id for x in all_goods_sales_records}
        # 获取小票对应商品信息
        goods_infos = {}
        Goods = models.Goods
        if goods_id_list:
            goods_list = (self.slave_session.query(
                Goods.id,
                Goods.unit,
                Goods.shop_supplier_id,
                Goods.name,
                Goods.supply_type,
            ).filter(Goods.id.in_(goods_id_list)).all())
            goods_infos = {x.id: x for x in goods_list}

        goods_salesman_dicts = {}
        # 开票员商品
        for _salesman_id_goods_id in salesman_id_goods_id_list:
            goods_id = int(_salesman_id_goods_id.split("-")[1])
            goods = goods_infos[goods_id]
            goods_salesman_dicts_value = copy.deepcopy(
                base_goods_salesman_dict)
            goods_salesman_dicts_value["goods_unit"] = goods.unit
            goods_salesman_dicts_value[
                "supperlier_id"] = goods.shop_supplier_id
            goods_salesman_dicts_value["goods_name"] = goods.name
            goods_salesman_dicts[
                _salesman_id_goods_id] = goods_salesman_dicts_value

        for all_record in all_goods_sales_records:
            status = all_record.status
            record_type = all_record.record_type
            goods_id = all_record.goods_id
            salesman_id = all_record.salesman_id
            accountant_id = all_record.accountant_id
            temp_receipt_money = all_record.receipt_money
            commission_mul = all_record.commission_mul
            sales_weigh = all_record.sales_num
            total_commission = all_record.commission_money
            deposit_cent = all_record.deposit_money
            total_goods_cent = all_record.sales_money
            gross_cent = all_record.gross_money

            goods_id = all_record.goods_id
            salesman_id = all_record.salesman_id
            salesman_id_goods_id = "%s-%s" % (salesman_id, goods_id)
            goods_salesman_dicts[salesman_id_goods_id]["sales_count"] += 1

            # 开票员商品
            salesman_id_goods_id = "%s-%s" % (salesman_id, goods_id)
            goods_salesman_dicts_value = goods_salesman_dicts[
                salesman_id_goods_id]

            # 已支付小票
            if status in [3, 5] and record_type == 0:
                goods_salesman_dicts_value["sales_pay_count"] += 1
                goods_salesman_dicts_value["gross_cent"] += gross_cent

                for key in sales_kpi_base_dict:
                    if key == "sales_weigh":
                        goods_salesman_dicts_value[key] += round(
                            eval(key) * 100)
                    else:
                        goods_salesman_dicts_value[key] += eval(key)

            if record_type == 1 and status == 3:
                goods_salesman_dicts_value["refund_count"] += 1
                goods_salesman_dicts_value["sales_pay_count"] -= 1
                goods_salesman_dicts_value["gross_cent"] -= gross_cent

                for key in sales_kpi_base_dict:
                    if key == "sales_weigh":
                        goods_salesman_dicts_value[key] -= round(
                            eval(key) * 100)
                    else:
                        goods_salesman_dicts_value[key] -= eval(key)

        chooseday = (datetime.datetime.now() if not accounting_time_today else
                     datetime.datetime.now() + datetime.timedelta(1))
        choose_year = chooseday.year
        choose_month = chooseday.month
        choose_day = chooseday.day
        choose_date = chooseday.date()
        choose_month_first_date = (datetime.datetime(choose_year, choose_month,
                                                     1)).strftime("%Y-%m-%d")
        choose_week = int(chooseday.strftime("%W"))
        choose_time = (
            choose_year,
            choose_month,
            choose_day,
            choose_date,
            choose_month_first_date,
            choose_week,
        )
        if data_type == 0:
            updata_sales_goods_salesman = pub_statistic.updata_sales_goods_salesman
        else:
            updata_sales_goods_salesman = (
                pub_statistic_account_periods.updata_sales_goods_salesman)

        updata_sales_goods_salesman(
            self.statistic_session,
            self.current_shop.id,
            choose_time,
            new_timestamp=0,
            goods_salesman_dicts=goods_salesman_dicts,
            should_commit=False,
        )

    @SalesmanBaseHandler.check_arguments("order_id:int")
    def get_order_signature_image(self, ):
        """ 获取订单签名图片 """
        shop_id = self.current_shop.id
        order_id = self.args["order_id"]
        order = (self.slave_session.query(models.Order).filter_by(
            id=order_id,
            shop_id=shop_id,
            record_type=0,
            tally_order_id=0,
            pay_type=9,
            status=2,
        ).first())
        if order and order.customer_signature:
            signature_image = CustomerSignature.get_signature_image(order_id)
        else:
            signature_image = ""
        return self.send_success(signature_image=signature_image)

    @SalesmanBaseHandler.check_arguments("priority_group_id:int")
    def set_priority_group(self):
        priority_group_id = self.args["priority_group_id"]
        shop_id = self.current_shop.id
        group_exists = (self.session.query(models.GoodsGroup).filter_by(
            id=priority_group_id, shop_id=shop_id).first())
        if not group_exists:
            return self.send_fail("参数错误，设置默认分组失败，请联系森果客服")
        _, goods_top = models.GoodsTop.get_or_create_instance(
            self.session, salesman_id=self.current_user.id, shop_id=shop_id)
        goods_top.priority_group = priority_group_id
        self.session.commit()
        return self.send_success()


# 货品管理
class Goods(SalesmanBaseHandler, GoodsManageBase):
    @SalesmanBaseHandler.check_arguments("goods_id?:int")
    def get(self):
        goods_id = self.args.get("goods_id", 0)
        session = self.session
        qiniuToken = Qiniu.get_qiniu_token()
        shop_id = self.current_shop.id
        if goods_id:
            goods = GoodsFunc.get_goods(session, shop_id, goods_id)
            if not goods:
                return self.redirect(self.reverse_url("salesHome") + "?tab=2")
            goods_info = GoodsFunc.get_goods_info_with_supplier(
                session, shop_id, goods)
        else:
            goods_info = {}

        try:
            real_active_goods_owner = StaffFunc.is_salesman_goods_owner(
                session, shop_id, self.current_user.id)
        except ValueError as why:
            return self.send_fail(str(why))
        supplier = GoodsOwnerFunc.get_supplier(session, shop_id,
                                               self.current_user.id)
        if real_active_goods_owner and not supplier:
            return self.send_error(404, error_msg="未能找到货主您对应的供应商信息，请联系店铺管理员")

        # 可以修改别人的货品信息的时候可以选到所有的货品
        permission_text_list = GoodsOwnerFunc.permission_text_list(
            session, shop_id, self.current_user.id)
        # 货主只能添加供货商为自己的货品/货主修改其他人的货品的时候可以选到所有供货商
        # active_goods_owner主要用来控制供货商为自己且不能修改
        active_goods_owner = 0
        if real_active_goods_owner and (
                "edit_goods_others" not in permission_text_list
                or not goods_id):
            active_goods_owner = 1

        groups_info = self.get_group_list()
        config = ShopFunc.get_config(session, shop_id)
        enable_deposit = config.enable_deposit
        receipt_config = ShopFunc.get_receipt_config(session, shop_id)
        stockin_receipt = receipt_config.stockin_receipt

        printer_info = WirelessPrintInfoFunc.get_salersman_printer_info(
            session, self.current_user.id, shop_id)

        return self.render(
            "kaipiao/goods_add.html",
            token=qiniuToken,
            goods_id=goods_id,
            goods_info=goods_info,
            groups_info=groups_info,
            enable_deposit=enable_deposit,
            stockin_receipt=stockin_receipt,
            printer_info=printer_info,
            active_goods_owner=active_goods_owner,
            supplier=supplier,
        )


# 商品管理接口
class GoodsManage(GoodsManageBase, SalesmanBaseHandler):
    @SalesmanBaseHandler.check_arguments("action:str")
    def post(self):
        action = self.args["action"]
        session = self.session
        current_user_id = self.current_user.id
        shop_id = self.current_shop.id
        if action == "set":
            return self.set_goods(shop_id, source="sales")
        elif action == "goods_list":
            goods_active_count, goods_list = self.get_goods_list(
                shop_id, source="sales")
            return self.send_success(
                goods_active_count=goods_active_count, goods_list=goods_list)
        elif action == "edit_active":
            return self.edit_active(shop_id)
        elif action == "top":
            return self.top_goods(shop_id)
        elif action == "hide":
            return self.hide_goods(shop_id)
        elif action == "batch_add_goods":
            return self.batch_add_goods(shop_id, source="sales")
        elif action == "search_goods_name":
            return self.search_goods_name(shop_id)
        elif action == "get_group_info":
            return self.get_group_name_list(shop_id, current_user_id)
        elif action == "get_goods_storage":
            return self.get_goods_storage(shop_id)
        elif action == "get_same_name_goods":
            return self.get_same_name_goods(shop_id)
        else:
            return self.send_fail(404)

    @SalesmanBaseHandler.check_arguments("active:int", "group?:int")
    def get_goods_list(self, shop_id, source="sales"):
        """获取商品列表，用于开票员端，boss后台已经单独出来，需要把boss后台的都去掉 MRCHI 2018.01.19

        参数：
        active 按商品状态筛选，1上架，2下架
        group 按商品分组筛选，0全部分组，int>0 按分组id筛选

        返回：
        goods_active_count 上架商品数量
        goods_list 商品列表
        """
        current_user_id = self.current_user.id
        slave_session = self.slave_session
        Goods = models.Goods
        GoodsGroup = models.GoodsGroup
        active = self.args["active"]
        group_id = self.args.get("group", 0)

        # 获得上架商品数量和下架商品数量
        master_goods_id_set = GoodsMasterBranchFunc.list_goods_id_master(
            slave_session, shop_id)
        goods_active_count = slave_session.query(func.count(
            Goods.id)).filter_by(
                shop_id=shop_id, active=1)
        if master_goods_id_set:
            goods_active_count = goods_active_count.filter(
                models.Goods.id.in_(master_goods_id_set))
        # 查询不可售卖商品
        goods_limit = GoodsFunc.get_salesman_goods_limit_list(
            slave_session, shop_id, current_user_id)
        if goods_limit:
            goods_active_count = goods_active_count.filter(
                ~Goods.id.in_(goods_limit))

        goods_active_count = goods_active_count.scalar() or 0

        # 查询隐藏商品和置顶商品
        # invisible_list 隐藏商品id的list
        # top_list 置顶商品id的list
        # top_goods 置顶商品Goods对象的list

        invisible_list = []
        top_list = []
        top_goods = []

        goods_top = self.get_goods_top(slave_session, shop_id, current_user_id)
        if goods_top:
            invisible_list = json.loads(goods_top.invisible_list)

        if source == "sales" and active == 1 and group_id:
            if goods_top and goods_top.top_list:
                top_list = json.loads(goods_top.top_list)
                top_goods = (slave_session.query(Goods).filter(
                    Goods.id.in_(top_list)).filter_by(
                        shop_id=shop_id, active=active, group_id=group_id).
                             filter(~Goods.id.in_(goods_limit)).all())

        # 查询可售卖、非置顶的商品列表
        # goods_all 返回的所有商品Goods对象集合
        # 基础查询，包括了按商品状态的筛选
        goods_query = GoodsMasterBranchFunc.get_goods_base_query(
            slave_session, shop_id)
        goods_all = (goods_query.filter_by(
            shop_id=shop_id, active=active).filter(~Goods.id.in_(top_list)).
                     filter(~Goods.id.in_(goods_limit)))
        # 按商品分组筛选
        if group_id:
            goods_all = goods_all.filter_by(group_id=group_id)
        # 把置顶商品放在前面就不用再排序了
        goods_all = top_goods + goods_all.order_by(Goods.id).all()

        try:
            active_goods_owner = StaffFunc.is_salesman_goods_owner(
                slave_session, shop_id, current_user_id)
        except ValueError as why:
            return self.send_fail(str(why))
        permission_text_list = GoodsOwnerFunc.permission_text_list(
            slave_session, shop_id, current_user_id)
        if active_goods_owner and "sell_goods_others" not in permission_text_list:
            supplier = SupplierFunc.get_supplier_by_account_info(
                slave_session, shop_id, self.current_user)
            if supplier:
                goods_all = slave_session.query(Goods).filter_by(
                    shop_id=shop_id,
                    active=active,
                    shop_supplier_id=supplier.id)
                if group_id:
                    goods_all = goods_all.filter_by(group_id=group_id)
                goods_all = goods_all.all()
            else:
                goods_all = []

        # 查询供应商信息
        shop_supplier_id_list = [x.shop_supplier_id for x in goods_all]
        supplier_dict = SupplierFunc.get_shop_supplier_base_info(
            slave_session, shop_id, shop_supplier_id_list)

        # 查询分组信息
        group_ids = {i.group_id for i in goods_all}
        group_infos = (slave_session.query(
            GoodsGroup.id, GoodsGroup.name).filter(
                GoodsGroup.id.in_(group_ids)).all())
        group_infos = {i.id: i.name for i in group_infos}

        # 过滤不在开票范围和在隐藏列表里的商品ID
        map_branch_to_master = GoodsMasterBranchFunc.list_map_branch_to_master(
            slave_session, shop_id)
        branch_list = [m for m in map_branch_to_master.keys()]
        filter_goods_list = set(goods_limit + invisible_list + branch_list)
        # 根据商品名称返回同名商品的供应商信息
        name_list = {x.name for x in goods_all}
        supplier_list_dict = SupplierFunc.get_supplier_through_goods_name(
            slave_session,
            shop_id,
            name_list,
            filter_goods_list=filter_goods_list)

        # 构造返回数据
        goods_list = []
        supplier_id_set = set()
        for goods in goods_all:
            goods_id = goods.id
            shop_supplier_id = goods.shop_supplier_id
            goods_info = GoodsFunc.get_goods_info(
                goods,
                source=source,
                multi=True,
                weight_unit_text=self.weight_unit_text)
            supplier_info = supplier_dict.get(shop_supplier_id)

            goods_info["supplier_id"] = shop_supplier_id
            goods_info["supplier_name"] = ""
            goods_info["supplier_phone"] = ""
            goods_info["top"] = False
            goods_info["hide"] = 0
            goods_info["group_id"] = goods.group_id
            goods_info["group_name"] = group_infos.get(goods.group_id, "")
            goods_info["supplier_list"] = supplier_list_dict.get(
                goods.name, [])

            if supplier_info:
                goods_info["supplier_name"] = supplier_info["name"]
                goods_info["supplier_phone"] = supplier_info["phone"]
            if goods_id in top_list:
                goods_info["top"] = True
            if goods_id in invisible_list:
                goods_info["hide"] = 1
            goods_list.append(goods_info)
        return goods_active_count, goods_list

    # 商品置顶
    @SalesmanBaseHandler.check_arguments("goods_id:int", "active:int")
    def top_goods(self, shop_id):
        goods_id = self.args["goods_id"]
        active = self.args["active"]
        session = self.session
        slave_session = self.slave_session
        goods = GoodsFunc.get_goods(slave_session, shop_id, goods_id)
        if not goods or not goods.active:
            return self.send_fail("该商品不存在")

        salesman_id = self.current_user.id
        goods_top = self.get_goods_top(session, shop_id, salesman_id)

        if active == 1:
            if goods_top:
                invisible_list = json.loads(goods_top.invisible_list)
                if goods_id in invisible_list:
                    return self.send_fail("请先取消商品隐藏")
            if not goods_top:
                GoodsTop = models.GoodsTop
                goods_top = GoodsTop(shop_id=shop_id, salesman_id=salesman_id)
                session.add(goods_top)
                session.flush()
            top_list = json.loads(goods_top.top_list)
            if goods_id in top_list:
                return self.send_fail("该商品已置顶")

            # 如果有商品在老板后台被下架或删除也要从开票员的置顶列表中去掉
            pop_list = self.check_top_list(session, shop_id, top_list)
            top_list = list(set(top_list) ^ set(pop_list))

            # 根据toplist找到对应分组的置顶数量
            goods_group_id = goods.group_id
            group_top_count = (slave_session.query(func.count(1)).filter(
                models.Goods.id.in_(top_list),
                models.Goods.group_id == goods_group_id,
            ).scalar() or 0)

            # 龙腾果品/绿林果品商品置顶上限调整为30个
            if shop_id in [116, 126]:
                if group_top_count >= 30:
                    return self.send_fail("每个分组最多置顶30个商品")
            else:
                if group_top_count >= 8:
                    return self.send_fail("每个分组最多置顶8个商品")
            top_list.append(goods_id)
        else:
            if not goods_top:
                return self.send_fail("不可取消置顶")
            top_list = json.loads(goods_top.top_list)
            if goods_id not in top_list:
                return self.send_fail("该商品不在置顶列表中")
            pop_list = [goods_id]
            top_list = list(set(top_list) ^ set(pop_list))

        top_list_to_str = json.dumps(top_list)
        if len(top_list_to_str) > 900:
            return self.send_fail("请减少置顶数量")

        goods_top.top_list = top_list_to_str
        session.commit()
        return self.send_success()

    # 商品隐藏
    @SalesmanBaseHandler.check_arguments("goods_id:int", "active:int")
    def hide_goods(self, shop_id):
        goods_id = self.args["goods_id"]
        active = self.args["active"]
        session = self.session
        goods = GoodsFunc.get_goods(session, shop_id, goods_id)
        if not goods or not goods.active:
            return self.send_fail("该商品不存在")

        salesman_id = self.current_user.id
        goods_top = self.get_goods_top(session, shop_id, salesman_id)

        if active == 1:
            if not goods_top:
                return self.send_fail("不可取消隐藏")
            invisible_list = json.loads(goods_top.invisible_list)
            if goods_id not in invisible_list:
                return self.send_fail("该商品不在隐藏列表中")
            pop_list = [goods_id]
            invisible_list = list(set(invisible_list) ^ set(pop_list))
        else:
            goods_top = self.get_goods_top(session, shop_id, salesman_id)
            if goods_top:
                top_list = json.loads(goods_top.top_list)
                if goods_id in top_list:
                    return self.send_fail("请先取消商品置顶")
            if not goods_top:
                GoodsTop = models.GoodsTop
                goods_top = GoodsTop(shop_id=shop_id, salesman_id=salesman_id)
                session.add(goods_top)
                session.flush()
            invisible_list = json.loads(goods_top.invisible_list)
            if goods_id in invisible_list:
                return self.send_fail("该商品已隐藏")

            invisible_list.append(goods_id)

        invisible_list_to_str = json.dumps(invisible_list)
        if len(invisible_list_to_str) > 1800:
            return self.send_fail("请减少隐藏数量")
        goods_top.invisible_list = invisible_list_to_str
        session.commit()
        return self.send_success()

    # 商品库存
    @SalesmanBaseHandler.check_arguments("goods_id:int")
    def get_goods_storage(self, shop_id):
        goods_id = self.args["goods_id"]
        session = self.session
        goods = GoodsFunc.get_goods(session, shop_id, goods_id)
        if not goods or not goods.active:
            return self.send_fail("该商品不存在")
        storage_text = GoodsFunc.get_storage_text(
            goods, weight_unit_text=self.weight_unit_text)
        return self.send_success(
            storage_text=storage_text,
            storage_unit=goods.storage_unit,
            storage_num=check_float(goods.storage_num / 100),
            storage=check_float(goods.storage / 100),
        )

    @SalesmanBaseHandler.check_arguments("name:str")
    def get_same_name_goods(self, shop_id):
        name = self.args["name"]
        slave_session = self.slave_session
        current_user_id = self.current_user.id

        # 去掉不在开票员开票范围内的货品
        filter_goods_list = GoodsFunc.get_salesman_goods_limit_list(
            slave_session, shop_id, current_user_id)

        # 去掉不显示在主页的货品
        invisible_list = []
        goods_top = self.get_goods_top(slave_session, shop_id, current_user_id)
        if goods_top:
            invisible_list = json.loads(goods_top.invisible_list)

        # 去掉分货品
        map_branch_to_master = GoodsMasterBranchFunc.list_map_branch_to_master(
            slave_session, shop_id)
        branch_list = [m for m in map_branch_to_master.keys()]

        filter_goods_list = set(filter_goods_list + invisible_list +
                                branch_list)

        data_dict = SupplierFunc.get_supplier_through_goods_name(
            slave_session, shop_id, name, filter_goods_list=filter_goods_list)

        data_list = data_dict.get(name, [])
        return self.send_success(data_list=data_list)


# 单品开票
class GoodsSale(SalesmanBaseHandler):
    @SalesmanBaseHandler.check_arguments("from?:str")
    def get(self, goods_id):
        # 来源，前端判断用
        fromsrc = self.args.get("from", "")
        slave_session = self.slave_session
        shop_id = self.current_shop.id

        # 控制开票助手的货主显示
        real_active_goods_owner = StaffFunc.is_salesman_goods_owner(
            slave_session, shop_id, self.current_user.id)
        permission_text_list = GoodsOwnerFunc.permission_text_list(
            slave_session, shop_id, self.current_user.id)
        # 是货主且不能卖他人的货才是真货主, 控制货物相关操作
        active_goods_owner = (
            1 if real_active_goods_owner
            and "sell_goods_others" not in permission_text_list else 0)

        if active_goods_owner:
            supplier = SupplierFunc.get_supplier_by_account_info(
                self.slave_session, self.current_shop.id, self.current_user)
            if supplier:
                goods = GoodsFunc.get_supplier_goods(self.slave_session,
                                                     self.current_shop.id,
                                                     goods_id, supplier.id)
            else:
                goods = None
        else:
            goods = GoodsFunc.get_goods(slave_session, shop_id, goods_id)
        if not goods:
            return self.redirect(self.reverse_url("salesHome") + "?tab=2")
        if goods.master_goods_id:
            return self.redirect(
                self.reverse_url("goodsSale", goods.master_goods_id))
        current_user = self.current_user
        user_id = current_user.id
        goods_info = GoodsFunc.get_goods_info_with_supplier(
            slave_session, shop_id, goods, source="goodssale")
        printer_info = WirelessPrintInfoFunc.get_salersman_printer_info(
            slave_session, user_id, shop_id)
        shop_name = self.current_shop.shop_name
        user_info = self.get_current_user_info()

        config_info = self.get_config_info(slave_session, shop_id)
        *_, lc_access_token = ShopFunc.get_lcpay_account(
            slave_session, shop_id)

        qiniuToken = Qiniu.get_qiniu_token()

        return self.render(
            "kaipiao/kaipiao.html",
            token=qiniuToken,
            goods_info=goods_info,
            printer_info=printer_info,
            config_info=config_info,
            shop_id=shop_id,
            shop_name=shop_name,
            user_info=user_info,
            fromsrc=fromsrc,
            lc_access_token=lc_access_token,
            active_goods_owner=active_goods_owner,
            real_active_goods_owner=real_active_goods_owner,
        )

    def get_config_info(self, session, shop_id):
        config = ShopFunc.get_config(session, shop_id)
        config_info = {}
        config_info["kaipiao_sure"] = config.kaipiao_sure
        config_info["multi_weigh"] = config.multi_weigh
        config_info["multi_sale_goods"] = config.multi_sale_goods
        config_info["img_show"] = config.salesman_goods_img
        config_info["enable_deposit"] = config.enable_deposit
        config_info["force_choose_supplier"] = config.force_choose_supplier
        config_info["precision"] = config.precision
        config_info["precision_type"] = config.precision_type
        config_info["weight_float"] = config.weight_float
        config_info["amount_float"] = config.amount_float
        config_info["kaipiao_done_sure"] = config.kaipiao_done_sure
        config_info["boss_app_pro"] = config.boss_app_pro
        config_info["display_kg_instead"] = config.display_kg_instead
        config_info["distinguish_weight"] = config.distinguish_weight
        config_info["storage_notice"] = config.storage_notice
        config_info["deposit_model"] = config.deposit_model
        config_info["salesman_storage_shown"] = config.salesman_storage_shown
        return config_info

    @SalesmanBaseHandler.check_arguments(
        "fact_price:float",
        "sales_num:float",
        "commission:float",
        "commission_mul:float",
        "receipt_money:float",
        "deposit:float",
        "sales_num_total?:str",
        "commission_mul_total?:str",
        "local_print?:int",
        "gross_weight?:float",
        "tare_weight?:float",
        "precision:int",
        "precision_type:int",
        "customer_id?:int",
        "low_price_force?:int",
        "low_storage_force?:int",
        "remark?:str",
        "is_refund?:int",
        "is_settlement?:int",
        "kg_upgrade?:int",
        "purchase_id?:int",
        "set_change_total?:int",
        "image_remark?:list",
    )
    def post(self, goods_id):
        goods_id = int(goods_id)
        session = self.session
        shop_id = self.current_shop.id
        user_id = self.current_user.id
        config = ShopFunc.get_config(session, shop_id)
        goods = (session.query(models.Goods).filter_by(
            id=goods_id, shop_id=shop_id).first())
        if not goods:
            return self.send_fail("当前店铺下没有这个商品,请返回主页重新选择")
        elif goods.active != 1:
            return self.send_fail("商品已下架,请返回主页进行编辑")
        goods_limit = GoodsFunc.get_salesman_goods_limit_list(
            session, shop_id, user_id)
        if goods_id in goods_limit:
            return self.send_fail("您没有操作该商品的权限")

        if self.is_boss_app() and config.boss_app_pro:
            return self.send_fail("老板助手收款功能已迁移到开票助手中，请在开票助手中进行操作")

        # 检查开票员是否有收款的权限
        is_settlement = self.args.get("is_settlement", 0)  # 是否为收款 0:否 1:是
        if is_settlement:
            if not config.boss_app_pro:
                return self.send_fail("收款功能尚未开启")
            if "accountant" not in self.current_user.role_list:
                return self.send_fail("您没有收款的权限")

        # 来自老板助手的不打印开票单，不做打印机检查
        local_print = self.args.get("local_print", 0)  # 是否为本地打印 0:否 1:是
        if not is_settlement:
            if local_print == 1:
                wireless_print_num = "本机打印"
                printer_remark = "本机打印"
            elif local_print == 2:
                wireless_print_num = "蓝牙打印"
                printer_remark = "蓝牙打印"
            else:
                wireless_print_type, wireless_print_num, wireless_print_key, printer_remark = WirelessPrintFunc.get_hirelink_printer(
                    session, shop_id, user_id, scene=1)
                wireless_print_type = int(wireless_print_type)
                if wireless_print_type not in (2, 3):
                    return self.send_fail("当前打印机无效，请点击切换打印机")
        else:
            if local_print == 1:
                wireless_print_num = "本机打印"
                printer_remark = "本机打印"
            elif local_print == 2:
                wireless_print_num = "蓝牙打印"
                printer_remark = "蓝牙打印"
            else:
                wireless_print_num = ""
                printer_remark = "收款模式除POS和蓝牙开票不打印"

        fact_price = self.args["fact_price"]  # 单价
        sales_num = self.args["sales_num"]  # 重量(净重)
        commission = self.args["commission"]  # 行费（／件）
        commission_mul = self.args["commission_mul"]  # 件数
        deposit_avg = self.args["deposit"]  # 押金（／件）
        receipt_money = self.args["receipt_money"]  # 小票金额
        gross_weight = self.args.get("gross_weight", 0)  # 毛重
        tare_weight = self.args.get("tare_weight", 0)  # 皮重
        customer_id = self.args.get("customer_id", 0)  # 客户ID，如果没有客户赋为0
        low_price_force = self.args.get("low_price_force", 0)  # 售价低于成本价强制开票
        low_storage_force = self.args.get("low_storage_force", 0)  # 库存不足强制开票
        remark = self.args.get("remark", "")  # 备注
        image_remark = self.args.get("image_remark", [])  # 图片备注
        is_refund = self.args.get("is_refund", 0)  # 是否为退款小票 0:否 1:是
        purchase_id = self.args.get("purchase_id", 0)  # 企业采购员信息
        set_change_total = self.args.get("set_change_total", 0)  # 是否开启小计修改

        # 店铺设置
        config = ShopFunc.get_config(session, shop_id)
        deposit_model = config.deposit_model

        sales_num = check_float(sales_num)
        price_sum = check_float(fact_price * sales_num)
        commission_sum = check_float(commission * commission_mul)

        # 押金按单
        origin_deposit_avg = deposit_avg
        if deposit_model == 1:
            deposit_sum = deposit_avg
            deposit_avg = (check_float(deposit_sum / commission_mul)
                           if commission_mul else 0)
        else:
            deposit_sum = deposit_avg * commission_mul

        low_price_need_record = False
        low_storage_need_record = False

        fee_text = self.fee_text

        if not config.enable_deposit and deposit_avg:
            return self.send_fail("店铺押金功能已关闭，请重新开票", error_code=205)
        if not config.enable_commission and commission:
            return self.send_fail("店铺行费功能已关闭，请重新开票", error_code=205)

        if (config.enable_commission and not goods.edit_commission
                and goods.commission != check_float(commission * 100)):
            return self.send_fail("该商品{0}不允许修改，请编辑商品{0}".format(fee_text))

        if (config.enable_deposit and not goods.edit_deposit
                and goods.deposit != origin_deposit_avg):
            return self.send_fail("该商品押金不允许修改，请编辑商品押金")

        if not is_refund:
            if config.low_price_notice and fact_price < goods.cost_price / 100:
                if low_price_force:
                    low_price_need_record = True
                else:
                    return self.send_fail(
                        "售价低于成本价，请谨慎开票。</br>点击确定继续完成开票。",
                        error_code=206,
                        error_key="price",
                    )

            if config.storage_notice == 1:
                if (goods.storage_unit == 0
                        and goods.storage_num - round(sales_num * 100) < 0 or
                    (goods.storage_unit == 1
                     and goods.storage - round(commission_mul * 100) < 0)):
                    if low_storage_force:
                        low_storage_need_record = True
                    else:
                        return self.send_fail(
                            "商品库存不足，请谨慎开票。</br>点击确定继续完成开票。",
                            error_code=206,
                            error_key="storage",
                        )
            elif config.storage_notice == 2:
                if (goods.storage_unit == 0
                        and goods.storage_num - round(sales_num * 100) < 0 or
                    (goods.storage_unit == 1
                     and goods.storage - round(commission_mul * 100) < 0)):
                    return self.send_fail(
                        "商品剩余库存不足<br>不可开票",
                        error_code=206,
                        error_key="storage_fail")

            if sales_num < 0 or commission_mul < 0:
                return self.send_fail("商品重量或件数不允许为负数，请编辑商品信息")

            if gross_weight < 0:
                return self.send_fail("商品毛重小于皮重")

        if goods.unit == 1 and sales_num != commission_mul:
            return self.send_fail("该商品单位可能发生变化，请刷新商品数据")

        if not config.weight_float and sales_num != int(
                sales_num) and goods.unit == 0:
            return self.send_fail("该商品重量不允许输入小数，请编辑商品重量")

        if not config.amount_float and commission_mul != int(commission_mul):
            return self.send_fail("该商品件数不允许输入小数，请编辑商品件数")

        # 根据精度设置规则判断前后端计算是否一致，当小票金额发生变化时才检验精度设置
        calc_cent = NumFunc.handle_precision(
            (price_sum + commission_sum + deposit_sum),
            config.precision,
            config.precision_type,
        )
        receipt_cent = int(round(receipt_money * 100))
        if abs(calc_cent - receipt_cent) > 1:
            precision = self.args["precision"]
            if precision != config.precision:
                return self.send_fail("店铺小票精度设置方式已发生改变，请重新开票", error_code=205)

            precision_type = self.args["precision_type"]
            if precision_type != config.precision_type:
                return self.send_fail("店铺小票数据取用规则已发生改变，请重新开票", error_code=205)

        # 前后端金额校验差值容差值。开启了小计修改的校验0.01*重量，未开启的再做严格校验
        if set_change_total:
            tolerance = sales_num
            # 小计修改以后，根据进一法圆整的时候可能与服务器计算有差异, 将误差调整到1
            if tolerance < 100:
                tolerance = 100
        else:
            tolerance = 10
        if abs(calc_cent - receipt_cent) > tolerance:
            return self.send_fail("小票金额与服务器计算金额不一致")

        # 如果店铺开启了去皮功能需要校验一下tare_weight
        if config.distinguish_weight and tare_weight:
            caculate_gross_weight = check_float(sales_num + tare_weight)
            if caculate_gross_weight != gross_weight:
                return self.send_fail("皮重与服务器计算不一致，请编辑商品皮重")
        else:
            if tare_weight:
                return self.send_fail("区分毛重净重功能已关闭，请重新开票", error_code=205)

        if receipt_money > 999999:
            return self.send_fail("单张小票金额不可过大")

        # 检查一下客户ID的真实性
        customer_id = 0 if customer_id < 0 else customer_id
        if customer_id:
            customer = ShopCustomerFunc.get_shop_customer_through_id(
                session, shop_id, customer_id)
            if not customer:
                return self.send_fail("客户信息不存在，请重新输入")
            if customer.status == -2:
                return self.send_fail("被拉黑的客户不能开票")

        # 控制一下同一商品开小票的并发问题
        redis_key = "sale_record_goods_check:%d:%d" % (user_id, goods_id)
        if not redis.setnx(redis_key, 1):
            return self.send_fail("同一商品正在开票中，请稍后重试")
        redis.expire(redis_key, 2 * 60)

        # 处理备注长度
        remark = Emoji.filter_emoji(remark).strip()[:50]

        # 处理图片备注格式
        image_remark = ",".join(image_remark)

        current_batch = GoodsBatchFunc.get_current_batch(
            self.slave_session, goods_id)
        batch_id = current_batch.id if current_batch else 0

        # 3.生成新的票号
        try:
            valid_sale_record = OrderFinishFunc.gen_sales_record_by_frontend(
                self.session,
                self.current_user.id,
                goods,
                batch_id,
                1,
                fact_price,
                sales_num,
                commission,
                commission_mul,
                commission_sum,
                deposit_avg,
                deposit_sum,
                receipt_money,
                shop_customer_id=customer_id,
                remark=remark,
                wireless_print_num=wireless_print_num,
                printer_remark=printer_remark,
                tare_weight=tare_weight,
                is_refund=is_refund,
                purchase_id=purchase_id,
                image_remark=image_remark,
            )
        except BaseException as e:
            redis.delete(redis_key)
            raise e

        # 关联货品和开票流水表
        billing_sales_record = models.BillingSalesRecord(
            salesman_id=user_id,
            shop_id=shop_id,
            type_id=0,
            record_id=valid_sale_record.id,
            bill_time=datetime.datetime.now().strftime("%H:%M:%S"),
        )
        session.add(billing_sales_record)
        session.flush()

        # 保存强制提交记录
        force_record_add_list = []
        # 低售价强制提交记录
        if low_price_need_record:
            force_record_add_list.append(
                models.GoodsSalesRecordForceRecord(
                    goods_sale_record_num=valid_sale_record.num,
                    force_type=1,
                    salesman_id=user_id,
                ))

        # 低库存强制提交记录
        if low_storage_need_record:
            force_record_add_list.append(
                models.GoodsSalesRecordForceRecord(
                    goods_sale_record_num=valid_sale_record.num,
                    force_type=2,
                    salesman_id=user_id,
                ))
        if force_record_add_list:
            session.add_all(force_record_add_list)

        # 2.无线打印小票
        # 来自老板助手的不打印开票单
        if not is_settlement:
            if local_print == 1:
                wpp = WirelessPrintFunc("smposprint", shop=self.current_shop)
            elif local_print == 2:
                wpp = WirelessPrintFunc(3, shop=self.current_shop)
            else:
                wpp = WirelessPrintFunc(
                    wireless_print_type, shop=self.current_shop)
            sales_num_total = self.args.get("sales_num_total", "")
            commission_mul_total = self.args.get("commission_mul_total", "")
            sale_record_info = GoodsSaleFunc.get_sale_record_info(
                session,
                valid_sale_record,
                weight_unit_text=self.weight_unit_text)
            receipt_config = ShopFunc.get_receipt_config(session, shop_id)
            accounting_time = AccountingPeriodFunc.get_accounting_time(
                session, shop_id)
            content_body = wpp.get_sale_record_print_body(
                sale_record_info,
                receipt_config,
                sales_num_total,
                commission_mul_total,
                shop_id=shop_id,
                accounting_time=accounting_time,
                source="salesman",
                fee_text=self.fee_text,
            )

            print_body = ""
            if content_body:
                if local_print == 1:
                    print_body = content_body
                elif local_print == 2:
                    print_body = wpp.sgbluetooth(content_body)
                else:
                    if_print_success, error_txt = wpp.send_print_request(
                        content_body, wireless_print_num)
                    if not if_print_success:
                        return self.send_fail(error_txt)
        else:
            print_body = ""

        session.commit()
        redis.delete(redis_key)

        return self.send_success(
            print_body=print_body,
            valid_record_id=valid_sale_record.id,
            valid_record_num=valid_sale_record.num[-4:],
        )


# 一单多品开票
class GoodsSaleMulti(SalesmanBaseHandler):
    def get(self):
        shop_id = self.current_shop.id
        salesman_id = self.current_user.id
        session = self.session
        printer_info = WirelessPrintInfoFunc.get_salersman_printer_info(
            session, salesman_id, shop_id)
        return self.render("kaipiao/goods_mul.html", printer_info=printer_info)

    @SalesmanBaseHandler.check_arguments("action:str", "kg_upgrade?:int")
    def post(self):
        action = self.args["action"]
        shop_id = self.current_shop.id
        salesman_id = self.current_user.id
        if action == "save_temp_order":
            return self.save_order_sales_goods(shop_id, salesman_id)
        else:
            return self.send_fail(404)

    @SalesmanBaseHandler.check_arguments(
        "data:dict",
        "local_print?:int",
        "customer_id?:int",
        "low_price_force?:int",
        "low_storage_force?:int",
        "remark?:str",
        "is_settlement?:int",
        "is_refund?:int",
        "purchase_id?:int",
        "set_change_total?:int",
        "image_remark?:list",
        "need_check?:bool",
    )
    def save_order_sales_goods(self, shop_id, salesman_id):
        need_check = self.args.get("need_check", False)

        session = self.session
        config = ShopFunc.get_config(session, shop_id)
        if not config.multi_sale_goods:
            return self.send_fail("一单多品功能尚未开启")

        # 检查开票员是否有收款的权限
        is_settlement = self.args.get("is_settlement", 0)  # 是否为收款 0:否 1:是
        if is_settlement:
            if not config.boss_app_pro:
                return self.send_fail("收款功能尚未开启")
            if "accountant" not in self.current_user.role_list:
                return self.send_fail("您没有收款的权限")

        # 检查打印机
        local_print = self.args.get("local_print", 0)
        if not is_settlement:
            if local_print == 1:
                wireless_print_num = "本机打印"
                printer_remark = "本机打印"
            elif local_print == 2:
                wireless_print_num = "蓝牙打印"
                printer_remark = "蓝牙打印"
            else:
                wireless_print_type, wireless_print_num, wireless_print_key, printer_remark = WirelessPrintFunc.get_hirelink_printer(
                    session, shop_id, salesman_id, scene=1)
                wireless_print_type = int(wireless_print_type)
                if wireless_print_type not in (2, 3):
                    return self.send_fail("当前打印机无效，请点击切换打印机")
        else:
            if local_print == 1:
                wireless_print_num = "本机打印"
                printer_remark = "本机打印"
            elif local_print == 2:
                wireless_print_num = "蓝牙打印"
                printer_remark = "蓝牙打印"
            else:
                wireless_print_num = ""
                printer_remark = "收款模式除POS和蓝牙开票不打印"

        # 检查客户录入信息
        customer_id = self.args.get("customer_id", 0)
        purchase_id = self.args.get("purchase_id", 0)
        if customer_id:
            customer = ShopCustomerFunc.get_shop_customer_through_id(
                session, shop_id, customer_id)
            if not customer:
                return self.send_fail("客户信息不存在，请重新输入")
            if customer.status == -2:
                return self.send_fail("被拉黑的客户不能开票")
            # 为了兼容开票助手旧的接口，在客户添加了采购员之后，必须要求刷新页面
            have_purchase = (session.query(models.CustomerPurchase).filter_by(
                shop_customer_id=customer_id).first())
            if purchase_id:
                purchase = (session.query(models.CustomerPurchase).filter_by(
                    id=purchase_id, shop_customer_id=customer.id).first())
                if not purchase:
                    return self.send_fail("客户采购员信息不存在，请重新输入")

        # 是否为退票单
        is_refund = self.args.get("is_refund", 0)

        # 是否开启小计修改
        set_change_total = self.args.get("set_change_total", 0)

        # 检查商品信息
        goods_dict = self.args["data"]
        GoodsSalesRecord = models.GoodsSalesRecord
        goods_limit = GoodsFunc.get_salesman_goods_limit_list(
            session, shop_id, salesman_id)
        total_price = 0
        sale_record_list = []
        branch_sale_record_list = []
        now = datetime.datetime.now()

        # 强制提交
        GoodsSalesRecordForceRecord = models.GoodsSalesRecordForceRecord
        low_price_force = check_int(self.args.get("low_price_force",
                                                  0))  # 开票售价预警
        low_storage_force = check_int(self.args.get("low_storage_force",
                                                    0))  # 开票库存预警
        low_price_list = []
        low_storage_list = []
        force_record_add_list = []

        # 用于处理前端传来的商品顺序
        sorted_keys = sorted(goods_dict.keys())

        # 押金模式
        deposit_model = config.deposit_model

        fee_text = self.fee_text
        # 获取商品最新入库的入库单位
        goods_id_list = []
        for key in sorted_keys:
            goods_data = goods_dict[key]
            if not isinstance(goods_data, dict):
                continue
            try:
                goods_id = int(goods_data["goods_id"])
            except:
                return self.send_fail("商品ID不存在")
            goods_id_list.append(goods_id)

        collect_goods_dict = {}
        collect_multi_info = {}
        for key in sorted_keys:
            goods_data = goods_dict[key]
            if not isinstance(goods_data, dict):
                continue
            try:
                goods_id = int(goods_data["goods_id"])
            except:
                return self.send_fail("商品ID不存在")
            goods = (session.query(models.Goods).filter_by(
                id=goods_id, shop_id=shop_id).first())
            if not goods:
                return self.send_fail("商品不存在")
            goods_name = goods.name
            if goods.active != 1:
                return self.send_fail("商品%s已下架,请编辑商品信息" % goods_name)
            if goods_id in goods_limit:
                return self.send_fail("您没有售卖商品%s的权限，请删除该商品" % goods_name)

            # 收集货品库存
            if not collect_goods_dict.get(goods_id):
                collect_goods_dict[goods_id] = {
                    "storage": goods.storage,
                    "storage_num": goods.storage_num,
                }

            goods_storage_data = collect_goods_dict[goods_id]

            shop_supplier = (session.query(models.ShopSupplier).filter_by(
                id=goods.shop_supplier_id).first())

            fact_price = check_float(goods_data["fact_price"])  # 单价
            sales_num = check_float(goods_data["sales_num"])  # 重量
            commission = check_float(goods_data["commission"])  # 行费
            commission_mul = check_float(goods_data["commission_mul"])  # 件数
            deposit_avg = check_float(goods_data["deposit"])  # 押金（／件）
            receipt_money = check_float(goods_data["receipt_money"])  # 小票金额
            precision = check_int(goods_data.get("precision", 3))  # 小票精度设置方式
            precision_type = check_int(goods_data.get("precision_type",
                                                      1))  # 小票数据取用规则
            gross_weight = check_float(goods_data.get("gross_weight", 0))  # 毛重
            tare_weight = check_float(goods_data.get("tare_weight", 0))  # 皮重
            sales_num_total = goods_data.get("sales_num_total", "")  # 多次称重（重量）
            commission_mul_total = goods_data.get("commission_mul_total",
                                                  "")  # 多次称重（件数）

            price_sum = check_float(fact_price * sales_num)  # 货品小计算
            commission_sum = check_float(commission * commission_mul)  # 行费小计

            # 押金按单
            origin_deposit_avg = deposit_avg
            if deposit_model == 1:
                deposit_sum = deposit_avg
                deposit_avg = (check_float(deposit_sum / commission_mul)
                               if commission_mul else 0)
            else:
                deposit_sum = check_float(deposit_avg * commission_mul)

            # 非退单小票才检查押金是否开启、重量件数不允许为负数的情况
            if not is_refund:
                if not config.enable_deposit and deposit_avg:
                    return self.send_fail("店铺押金功能已关闭，请编辑货品信息")
                if not config.enable_commission and commission:
                    return self.send_fail("店铺行费功能已关闭，请重新开票", error_code=205)

                if (config.enable_commission and not goods.edit_commission
                        and goods.commission != check_float(commission * 100)):
                    return self.send_fail(
                        "「%s」%s不允许修改，请删除该商品" % (goods_name, fee_text))

                if (config.enable_deposit and not goods.edit_deposit
                        and goods.deposit != origin_deposit_avg):
                    return self.send_fail("「%s」押金不允许修改，请删除该商品" % goods_name)

                if sales_num < 0 or commission_mul < 0:
                    return self.send_fail(
                        "「%s」重量或件数不允许为负数，请编辑货品信息" % goods_name)

                # 如果店铺开启了去皮功能需要校验一下tare_weight
                if config.distinguish_weight and tare_weight:
                    caculate_gross_weight = check_float(sales_num +
                                                        tare_weight)
                    if caculate_gross_weight != gross_weight:
                        return self.send_fail(
                            "「%s」皮重与服务器计算不一致，请编辑货品皮重" % goods_name)
                else:
                    if tare_weight:
                        return self.send_fail(
                            "区分毛重净重功能已关闭，请重新编辑「%s」" % goods_name)

            if goods.unit == 1 and sales_num != commission_mul:
                return self.send_fail("「%s」单位可能发生变化，请刷新数据" % goods_name)

            if (not config.weight_float and sales_num != int(sales_num)
                    and goods.unit == 0):
                return self.send_fail("「%s」重量不允许输入小数，请编辑商品重量" % goods_name)

            if not config.amount_float and commission_mul != int(
                    commission_mul):
                return self.send_fail("「%s」件数不允许输入小数，请编辑商品件数" % goods_name)

            if gross_weight < 0:
                return self.send_fail("「%s」毛重小于皮重，请编辑货品信息" % goods_name)

            low_price_need_record = False
            if config.low_price_notice and fact_price < goods.cost_price / 100:
                if low_price_force:
                    low_price_need_record = True
                else:
                    low_price_list.append("「%s」" % goods_name)

            low_storage_need_record = False
            if config.storage_notice == 1:
                if (goods.storage_unit == 0
                        and goods_storage_data["storage_num"] - round(
                            sales_num * 100) < 0) or (
                                goods.storage_unit == 1
                                and goods_storage_data["storage"] - round(
                                    commission_mul * 100) < 0):
                    if low_storage_force:
                        low_storage_need_record = True
                    else:
                        low_storage_list.append("「%s」" % goods_name)
            elif config.storage_notice == 2:
                if (goods.storage_unit == 0
                        and goods_storage_data["storage_num"] - round(
                            sales_num * 100) < 0) or (
                                goods.storage_unit == 1
                                and goods_storage_data["storage"] - round(
                                    commission_mul * 100) < 0):
                    low_storage_list.append("「%s」" % goods_name)

            if receipt_money > 999999:
                return self.send_fail("「%s」小票金额不可过大" % goods_name)

            # 根据精度设置规则判断前后端计算是否一致，当小票金额发生变化时才检验精度设置
            calc_cent = NumFunc.handle_precision(
                (price_sum + commission_sum + deposit_sum),
                config.precision,
                config.precision_type,
            )
            receipt_cent = int(round(receipt_money * 100))
            if abs(calc_cent - receipt_cent) > 1:
                if precision != config.precision:
                    return self.send_fail(
                        "店铺小票精度设置方式已发生改变，请编辑「%s」信息" % goods_name)

                if precision_type != config.precision_type:
                    return self.send_fail(
                        "店铺小票数据取用规则已发生改变，请编辑「%s」信息" % goods_name)

            # 前后端金额校验差值容差值。开启了小计修改的校验0.01*重量，未开启的再做严格校验
            if set_change_total:
                tolerance = sales_num
                if tolerance < 100:
                    tolerance = 100
            else:
                tolerance = 10
            if abs(calc_cent - receipt_cent) > tolerance:
                return self.send_fail("「%s」小票金额与服务器计算金额不一致" % goods_name)

            goods_batch_id = 0
            current_batch = GoodsBatchFunc.get_current_batch(
                self.slave_session, goods_id)
            if current_batch:
                goods_batch_id = current_batch.id

            valid_sale_record = OrderFinishFunc.gen_sales_record_by_frontend(
                self.session,
                self.current_user.id,
                goods,
                goods_batch_id,
                1,
                fact_price,
                sales_num,
                commission,
                commission_mul,
                commission_sum,
                deposit_avg,
                deposit_sum,
                receipt_money,
                wireless_print_num=wireless_print_num,
                printer_remark=printer_remark,
                tare_weight=tare_weight,
                is_refund=is_refund,
                purchase_id=purchase_id,
            )
            sale_record_list.append(valid_sale_record)

            total_price += valid_sale_record.receipt_money

            # 低售价强制提交记录
            if low_price_need_record:
                force_record_add_list.append(
                    GoodsSalesRecordForceRecord(
                        goods_sale_record_num=valid_sale_record.num,
                        force_type=1,
                        salesman_id=salesman_id,
                    ))

            # 低库存强制提交记录
            if low_storage_need_record:
                force_record_add_list.append(
                    GoodsSalesRecordForceRecord(
                        goods_sale_record_num=valid_sale_record.num,
                        force_type=2,
                        salesman_id=salesman_id,
                    ))

            goods_storage_data["storage_num"] -= round(sales_num * 100)
            goods_storage_data["storage"] -= round(commission_mul * 100)
            collect_goods_dict[goods_id] = goods_storage_data

            collect_multi_info[valid_sale_record.num] = {
                "sales_num_total": sales_num_total,
                "commission_mul_total": commission_mul_total,
            }

        if not is_refund:
            # 售价低于成本价预警
            if low_price_list:
                low_price_name_str = "、".join(low_price_list)
                return self.send_fail(
                    "「%s」售价低于成本价，请谨慎开票。</br>点击确定继续完成开票。" % low_price_name_str,
                    error_code=206,
                    error_key="price",
                )

            # 库存过低预警
            if low_storage_list:
                low_storage_name_str = "、".join(low_storage_list)
                if config.storage_notice == 1:
                    return self.send_fail(
                        "「%s」库存不足，请谨慎开票。</br>点击确定继续完成开票。" %
                        low_storage_name_str,
                        error_code=206,
                        error_key="storage",
                    )
                elif config.storage_notice == 2:
                    return self.send_fail(
                        "「%s」库存已发生变动，剩余库存不足<br>不可开票。" % low_storage_name_str,
                        error_code=206,
                        error_key="storage_fail",
                    )

        # 弃用临时单号，使用正式单号生成一单多品
        order = OrderBaseFunc.create_db_order(
            session, shop_id, 0, 0, "", if_pborder=False, multi=True)
        order.multi_goods = 1
        order.debtor_id = customer_id
        if is_refund:
            order.record_type = 1

        # 处理备注
        remark = self.args.get("remark", "")
        remark = Emoji.filter_emoji(remark).strip()[:50]
        order.remark = remark

        # 处理图片备注
        image_remark = self.args.get("image_remark", [])
        image_remark = ",".join(image_remark)
        order.image_remark = image_remark

        # 将订单号放入流水小票中
        for valid_sale_record in sale_record_list:
            valid_sale_record.multi_order_id = order.id
            valid_sale_record.shop_customer_id = customer_id
            valid_sale_record.purchase_id = purchase_id

        # 将多品订单号放入开票流水表中记录
        # order为一单多品订单，order_id表示id号，order_num表示多品订单流水号
        billing_sales_record = models.BillingSalesRecord(
            salesman_id=salesman_id,
            shop_id=shop_id,
            type_id=1,
            record_id=order.id,
            bill_time=now,
        )

        session.add(billing_sales_record)
        session.add_all(sale_record_list)
        session.flush()

        # 保存强制提交记录
        if force_record_add_list:
            session.add_all(force_record_add_list)

        # 保存订单总价
        order.total_price = total_price
        order.fact_total_price = total_price

        sale_record_info_list, temp_order_info = GoodsSaleFunc.get_temp_sale_order_info(
            session, sale_record_list, order, source="salesman")

        if not need_check:
            # 2.无线打印小票
            if not is_settlement:
                if local_print == 1:
                    wpp = WirelessPrintFunc(
                        "smposprint", shop=self.current_shop)
                elif local_print == 2:
                    wpp = WirelessPrintFunc(3, shop=self.current_shop)
                else:
                    wpp = WirelessPrintFunc(
                        wireless_print_type, shop=self.current_shop)
                receipt_config = ShopFunc.get_receipt_config(session, shop_id)
                accounting_time = AccountingPeriodFunc.get_accounting_time(
                    session, shop_id)
                content_body, _ = wpp.get_multi_sale_record_print_body(
                    sale_record_info_list,
                    temp_order_info,
                    receipt_config,
                    accounting_time=accounting_time,
                    source="salesman",
                    fee_text=self.fee_text,
                    collect_multi_info=collect_multi_info,
                )

                print_body = ""

                if content_body:
                    if local_print == 1:
                        print_body = content_body
                    elif local_print == 2:
                        print_body = wpp.sgbluetooth(content_body)
                    else:
                        if_print_success, error_txt = wpp.send_print_request(
                            content_body, wireless_print_num)
                        if not if_print_success:
                            return self.send_fail(error_txt)
            else:
                print_body = ""

            session.commit()
        else:
            # 清除临时生成的流水号缓存，避免跳流水号 TODO 临时方案，应该从逻辑上处理预检查时无效流水号生成的问题
            order_num_key = "multi_order_num:{}".format(shop_id)
            redis.delete(order_num_key)
            bill_time_list = {record.bill_date for record in sale_record_list}
            for bill_time in bill_time_list:
                record_num_key = "sale_record_num:{}:{}".format(
                    shop_id, bill_time.strftime("%y%m%d"))
                redis.delete(record_num_key)
            return self.send_success(check_passed=True)

        goods_type_count = temp_order_info.get("total_goods_type_count", 0)

        # 收款模式回传流水ID组成的数组
        sale_record_ids = []
        if is_settlement:
            sale_record_ids = [x.id for x in sale_record_list]

        return self.send_success(
            print_body=print_body,
            multi_order_num=order.num,
            goods_type_count=goods_type_count,
            sale_record_ids=sale_record_ids,
            check_passed=False,
        )


# 无线打印机设置
class PrinterSet(SalesmanBaseHandler, PrintSetBase):
    @SalesmanBaseHandler.check_arguments("action:str")
    def post(self):
        action = self.args["action"]

        if (action not in ["get_printer_list", "change_printer"]
                and "setprint" not in self.current_user.permission_list):
            return self.send_fail("您没有设置打印机权限")

        if action == "get_printer_list":
            return self.get_printer_list(scene=1)
        elif action == "assign":
            return self.assign()
        elif action == "commit_assign":
            return self.commit_assign()
        elif action == "try_print":
            return self.try_print()
        elif action == "remark":
            return self.remark()
        elif action == "del_printer":
            return self.del_printer()
        elif action == "add_printer":
            return self.add_printer()
        elif action == "change_printer":
            return self.change_printer(scene=1)
        else:
            return self.send_fail("action错误")


# 取货
class Pick(SalesmanBaseHandler):
    """
        取货目前分两种场景根据单号首个字符来进行区分
        1.单品流水号
        2.一单多品订单号(已弃用)
        3.订单号（暂使用shop_id+6位长度判断）

    """

    def get(self, record_num):
        session = self.session
        shop_id = self.current_shop.id
        check_notice = ""
        record_info_list = []
        if_order = OrderBaseFunc.check_if_order_through_num(
            record_num, shop_id=shop_id)
        # 一单多品
        if if_order:
            check_notice, record_info_list = self.get_order_sale_record(
                session, record_num, shop_id=shop_id)
        else:
            # 单品
            sale_record = self.get_sale_record(session, record_num)
            check_notice = self.record_check_notice(sale_record)
            record_info = {}
            if not check_notice:
                record_info = self.get_record_info(sale_record)
                record_info_list = [record_info]
                check_notice = "已付款，请取货%s，%d件。" % (
                    record_info["goods_name"],
                    record_info["commission_mul"],
                )
            record_num = record_num[-4:]
        from libs.utils import Baidu

        baiduyuyin_token = Baidu.get_baidu_token()
        return self.render(
            "kaipiao/scanorder.html",
            check_notice=check_notice,
            record_info_list=record_info_list,
            record_num=record_num,
            baiduyuyin_token=baiduyuyin_token,
        )

    @SalesmanBaseHandler.check_arguments("action:str")
    def post(self, record_num):
        action = self.args["action"]
        if action == "pick":
            return self.pick_goods(record_num)
        elif action == "refundcheck":
            return self.check_refund(record_num)
        else:
            return self.send_error(404)

    def pick_goods(self, record_num):
        return self.handle_record(record_num, "pick")

    def check_refund(self, record_num):
        return self.handle_record(record_num, "check_refund")

    def handle_record(self, record_num, action):
        """
            pick  取货
            check_refund 退款退货确认
        """
        session = self.session
        salesman_id = self.current_user.id
        shop_id = self.current_shop.id
        time_now = datetime.datetime.now()
        if_order = OrderBaseFunc.check_if_order_through_num(
            record_num, shop_id=shop_id)
        # 一单毒品
        if if_order:
            order = OrderBaseFunc.get_order_through_num(
                session, record_num, shop_id=shop_id)
            sale_record_list = self.get_sale_record_through_order(order.id)
            for sale_record in sale_record_list:
                check_notice = self.record_check_notice(sale_record)
                if check_notice:
                    return self.send_fail(check_notice)
                self.handler_sale_record(
                    sale_record, salesman_id, time_now, action=action)
        else:
            # 单品
            sale_record = self.get_sale_record(session, record_num)
            check_notice = self.record_check_notice(sale_record)
            if check_notice:
                return self.send_fail(check_notice)
            self.handler_sale_record(
                sale_record, salesman_id, time_now, action=action)
        session.commit()
        return self.send_success()

    # 取货/退款退货后流水状态的变更
    def handler_sale_record(self,
                            sale_record,
                            salesman_id,
                            time_now,
                            action=""):
        if action == "pick":
            sale_record.status = 5
            sale_record.pick_salesman_id = salesman_id
            sale_record.pick_time = time_now
        else:
            sale_record.could_refund = 1
            sale_record.refund_check_salesman_id = salesman_id
            sale_record.refund_check_time = time_now

    def get_record_info(self, sale_record):
        session = self.session
        shop_id = self.current_shop.id
        goods_name = self.get_goods_name(session, sale_record.goods_id,
                                         shop_id)
        commission_mul = NumFunc.check_float(sale_record.commission_mul / 100)
        record_info = dict(
            record_num=sale_record.num[-4:],
            goods_name=goods_name,
            commission_mul=commission_mul,
        )
        return record_info

    def get_goods_name(self, session, goods_id, shop_id):
        Goods = models.Goods
        goods_name = (session.query(Goods.name).filter_by(
            id=goods_id, shop_id=shop_id).scalar() or "")
        return goods_name

    def record_check_notice(self, sale_record):
        shop_id = self.current_shop.id
        now_date = datetime.datetime.now()
        now_date = datetime.datetime(now_date.year, now_date.month,
                                     now_date.day)
        if not sale_record:
            return "非有效单据"
        if sale_record.shop_id != shop_id:
            shop_name = self.get_shop_name()
            return "非%s的单据" % shop_name
        if now_date > sale_record.create_time:
            return "单据已过期"
        if sale_record.status == 1:
            return "该单未付款，请付款后取货"
        if sale_record.status != 3:
            return "非有效单据"
        if sale_record.could_refund:
            return "该单据已退货"
        return ""

    def get_shop_name(self):
        shop_name = self.current_shop.shop_name
        return shop_name

    def get_sale_record(self, session, record_num):
        GoodsSalesRecord = models.GoodsSalesRecord
        sale_record = (session.query(GoodsSalesRecord).filter_by(
            num=record_num, record_type=0).first())
        return sale_record

    ##############一单多品#############
    def get_order_sale_record(self, session, record_num, shop_id=0):
        """
           record_num为订单号
        """
        record_info_list = []
        order = OrderBaseFunc.get_order_through_num(
            session, record_num, shop_id=shop_id)
        check_notice = self.order_check_notice(order)
        if not check_notice:
            order_id = order.id
            total_commission_mul = 0
            sale_record_list = self.get_sale_record_through_order(order_id)
            for sale_record in sale_record_list:
                total_commission_mul += sale_record.commission_mul
                record_info = self.get_record_info(sale_record)
                record_info_list.append(record_info)
            total_commission_mul = NumFunc.check_float(
                total_commission_mul / 100)
            check_notice = "已付款，请取货%d件。" % total_commission_mul
        return check_notice, record_info_list

    # 获取一单多品对应的商品
    def get_sale_record_through_order(self, multi_order_id):
        """
            一单多品要使用multi_order_id来获取对应的货品
        """
        session = self.session
        shop_id = self.current_shop.id
        sale_record_list = (session.query(models.GoodsSalesRecord).filter_by(
            shop_id=shop_id,
            multi_order_id=multi_order_id,
            status=3,
            could_refund=0,
            record_type=0,
        ).all())
        return sale_record_list

    def order_check_notice(self, temp_order):
        check_notice = ""
        shop_id = self.current_shop.id
        now_date = datetime.datetime.now()
        now_date = datetime.datetime(now_date.year, now_date.month,
                                     now_date.day)
        if not temp_order:
            return "没有对应的单据"
        if temp_order.shop_id != shop_id:
            shop_name = self.get_shop_name()
            return "非%s的单据" % shop_name
        if not temp_order.tally_order_id:
            return "该单据非一单多品结算订单"
        if now_date > temp_order.create_time:
            return "单据已过期"
        return ""


# 供货商管理
class SupplierManage(SalesmanBaseHandler, SupplierManageBase):
    @SalesmanBaseHandler.check_arguments("action:str")
    def post(self):
        action = self.args["action"]
        shop_id = self.current_shop.id
        if action == "add":
            return self.add_supplier(shop_id, source="salesman")
        elif action == "search_by_phone":
            return self.search_supplier_by_phone()
        elif action == "send_sms":
            return self.send_sms()
        else:
            return self.send_error(404)


# 客户搜索和扫码添加客户
class SearchCustomer(SalesmanBaseHandler):
    @SalesmanBaseHandler.check_arguments("action:str")
    def post(self):
        action = self.args["action"]
        if action == "search_customer":
            return self.search_customer()
        elif action == "scan_qrcode":
            return self.scan_qrcode()
        else:
            return self.send_error(404)

    @SalesmanBaseHandler.check_arguments("data:str")
    def search_customer(self):
        data = self.args["data"]
        session = self.session
        shop_id = self.current_shop.id
        ShopCustomer = models.ShopCustomer
        CustomerPurchase = models.CustomerPurchase
        limit_count = 30
        customer_data_list = (session.query(ShopCustomer).filter_by(
            shop_id=shop_id).filter(
                or_(
                    ShopCustomer.phone.like("%{}%".format(data)),
                    ShopCustomer.name.contains(data),
                    ShopCustomer.short_name.contains(data),
                    ShopCustomer.company.contains(data),
                    ShopCustomer.name_acronym.contains(data),
                    ShopCustomer.number == data,
                ),
                ShopCustomer.status != -1,
            ).limit(limit_count).all())
        count_record = len(customer_data_list)
        customer_ids = [x.id for x in customer_data_list]
        customer_with_purchase = (session.query(
            ShopCustomer, CustomerPurchase).join(
                CustomerPurchase,
                ShopCustomer.id == CustomerPurchase.shop_customer_id).filter(
                    or_(
                        CustomerPurchase.name.contains(data),
                        CustomerPurchase.phone.like("%{}%".format(data)),
                    ),
                    not_(CustomerPurchase.shop_customer_id.in_(customer_ids)),
                    ShopCustomer.shop_id == shop_id,
                    ShopCustomer.status != -1,
                    CustomerPurchase.status != -1,
                ).limit(limit_count - count_record).all()
                                  if count_record < limit_count else [])
        customer_data_list += customer_with_purchase
        data_list = ShopCustomerFunc.format_customer_info(
            session, customer_data_list)
        return self.send_success(data_list=data_list)

    @SalesmanBaseHandler.check_arguments("data:str")
    def scan_qrcode(self):
        """通过扫描采购助手二维码，添加客户"""
        data = self.args["data"].strip()
        shop_id = self.current_shop.id
        session = self.session
        ShopCustomer = models.ShopCustomer
        # 解密二维码字符串
        data = QRCodeCrypto.decrypt(data)
        if not data:
            return self.send_fail("无效的二维码")
        passport_id, name, phone, sex, birthday = json.loads(data)
        # birthday 时间戳解析成datetime
        if birthday:
            birthday = datetime.datetime.fromtimestamp(int(birthday)).date()
        else:
            birthday = None
        # 参数检查
        if not passport_id:
            return self.send_fail("无效的二维码内容")
        if not phone:
            return self.send_fail("该用户尚未绑定手机号，无法添加")

        # 根据passport_id查询
        exist_id_customer = (session.query(ShopCustomer).filter_by(
            shop_id=shop_id, source=1, passport_id=passport_id).one_or_none())
        # 根据phone查询
        exist_phone_customer = (session.query(ShopCustomer).filter_by(
            shop_id=shop_id, phone=phone, source=1).one_or_none())

        edit_content = ""

        # 如果客户已存在，更新资料
        if exist_id_customer:
            # 如果新手机号绑定了其他账户，抛出异常
            if exist_phone_customer and exist_phone_customer.id != exist_id_customer.id:
                raise ValueError("扫码录入客户时，手机号{}重复，扫码ID为{}，已存在客户ID为{}".format(
                    phone, passport_id, exist_phone_customer.passport_id))
            # 否则，更新资料，记录日志
            if exist_id_customer.name != name:
                edit_content += "姓名更改({}->{});".format(exist_id_customer.name,
                                                       name)
                exist_id_customer.name = name
            if exist_id_customer.phone != phone:
                edit_content += "手机号更改({}->{});".format(
                    exist_id_customer.phone, phone)
                exist_id_customer.phone = phone
            if exist_id_customer.sex != sex:
                text_dict = {0: "未知", 1: "男", 2: "女"}
                edit_content += "性别({}->{});".format(
                    text_dict.get(exist_id_customer.sex, "未知"),
                    text_dict.get(sex, "未知"))
                exist_id_customer.sex = sex
            if exist_id_customer.birthday != birthday:
                edit_content += "生日({}->{});".format(
                    exist_id_customer.birthday, birthday)
                exist_id_customer.birthday = birthday
            if edit_content:
                edit_content += "编辑来源：开票助手扫码"
            customer = exist_id_customer
        # 如果客户不存在
        else:
            # 如果手机号已经绑定账户，抛出异常
            if exist_phone_customer:
                raise ValueError("扫码录入客户时，手机号{}重复，扫码ID为{}，已存在客户ID为{}".format(
                    phone, passport_id, exist_phone_customer.passport_id))
            # 否则，添加新客户
            customer_group_orm = CustomerGroupORM(session)
            group_id = customer_group_orm.get_default_group_id(shop_id)
            customer = ShopCustomer(
                shop_id=shop_id,
                name=name,
                phone=phone,
                passport_id=passport_id,
                source=1,
                creator_id=self.current_user.id,
                industry_type=1,
                group_id=group_id,
            )
            edit_content += "添加来源：开票助手扫码"
            session.add(customer)
            session.flush()
        if edit_content:
            shop_customer_log = models.ShopCustomerLog(
                shop_id=shop_id,
                customer_id=customer.id,
                edit_user_id=self.current_user.id,
                edit_content=edit_content,
            )
            session.add(shop_customer_log)
        session.commit()
        return self.send_success(data=self.format_info(customer))

    def format_info(self, customer):
        data_dict = ShopCustomerFunc.format_customer_info(
            self.session, [customer], type="dict")
        return data_dict


# 开票员近30天常用小票备注
class CommonRemark(SalesmanBaseHandler):
    @SalesmanBaseHandler.check_arguments("multi:bool")
    def get(self):
        """取最近30天的备注

        multi True:多品备注 False:单品备注
        """
        slave_session = self.slave_session
        GoodsSalesRecord = models.GoodsSalesRecord
        shop_id = self.current_shop.id
        salesman_id = self.current_user.id
        multi = self.args.get("multi", False)

        now_date = datetime.datetime.now().date()
        thirty_days_before_date = now_date - datetime.timedelta(days=30)

        record_status = (1, 3, 5)

        if multi:
            # 查找最近30天的一单多品订单
            multi_order_id_list = (slave_session.query(
                GoodsSalesRecord.multi_order_id).filter_by(
                    salesman_id=salesman_id, shop_id=shop_id).filter(
                        GoodsSalesRecord.bill_date >= thirty_days_before_date,
                        GoodsSalesRecord.status.in_(record_status),
                        GoodsSalesRecord.multi_order_id > 0,
                    ).all())
            multi_order_id_list = (x.multi_order_id
                                   for x in multi_order_id_list)

            # 取出符合条件的一单多品订单备注
            Order = models.Order
            remark_list = (slave_session.query(
                Order.remark).filter_by(shop_id=shop_id).filter(
                    Order.id.in_(multi_order_id_list),
                    Order.remark != "").group_by(Order.remark).order_by(
                        func.count(1).desc()).limit(3).all())
        else:
            # 取出最近30天单品小票上的备注
            remark_list = (slave_session.query(
                GoodsSalesRecord.remark).filter_by(
                    salesman_id=salesman_id, shop_id=shop_id).filter(
                        GoodsSalesRecord.bill_date >= thirty_days_before_date,
                        GoodsSalesRecord.status.in_(record_status),
                        GoodsSalesRecord.remark != "",
                    ).group_by(GoodsSalesRecord.remark).order_by(
                        func.count(1).desc()).limit(3).all())
        remark_list = [x.remark for x in remark_list]
        return self.send_success(remark_list=remark_list)


# 盘点
class Inventory(SalesmanBaseHandler, GoodsManageBase):
    def get(self):
        return self.render("kaipiao/goods-check.html")

    @SalesmanBaseHandler.check_arguments("action:str")
    def post(self):
        action = self.args["action"]
        if action == "add":
            return self.add_doc()
        elif action == "get_goods_in_audit":
            return self.get_goods_in_audit()
        elif action == "get_goods_in_inventory":
            return self.get_goods_in_inventory()
        else:
            return self.send_fail(404)

    # 新增盘点单
    @SalesmanBaseHandler.check_arguments("goods_list:list", "remark?:str")
    def add_doc(self):
        """ 新增一条盘点单 """
        input_goods_list = self.args["goods_list"]
        try:
            goods_list = self._check_input_goods(input_goods_list)
        except ValueError as why:
            return self.send_fail(str(why))

        # 生成盘点单
        doc = models.InventoryDocs(
            shop_id=self.current_shop.id,
            num=OrderBaseFunc.gen_order_num(self.session, self.current_shop.id,
                                            "inventory_order"),
            creator_id=self.current_user.id,
            remark=self.args.get("remark", ""),
        )
        self.session.add(doc)
        self.session.flush()

        # 生成盘点单详情, 先将盘点库存更新成和现在库存一样的
        map_db_goods = self._get_map_db_goods(goods_list)
        total_first_quantity = 0
        total_first_weight = 0
        total_first_sys_quantity = 0
        total_first_sys_weight = 0
        goods_kind_count = 0
        for goods_dict in goods_list:
            goods_id = goods_dict.get("goods_id")
            if goods_dict.get("storage_unit") == 0:
                first_quantity = 0
                first_weight = goods_dict.get("amount")
                first_sys_quantity = 0
                first_sys_weight = map_db_goods.get(goods_id).storage_num
            elif goods_dict.get("storage_unit") == 1:
                first_quantity = goods_dict.get("amount")
                first_weight = 0
                first_sys_quantity = map_db_goods.get(goods_id).storage
                first_sys_weight = 0
            doc_detail = models.InventoryDocsDetail(
                doc_id=doc.id,
                goods_id=goods_id,
                first_quantity=first_quantity,
                second_quantity=first_quantity,
                first_sys_quantity=first_sys_quantity,
                first_sys_weight=first_sys_weight,
            )
            doc_detail.set_first_weight(
                first_weight, weight_unit_text=self.weight_unit_text)
            doc_detail.second_weight = doc_detail.first_weight
            self.session.add(doc_detail)
            total_first_quantity += first_quantity
            total_first_weight += doc_detail.first_weight
            total_first_sys_quantity += first_sys_quantity
            total_first_sys_weight += first_sys_weight
            goods_kind_count += 1

        # 更新盘点单的汇总信息
        doc.total_first_quantity = total_first_quantity
        doc.total_first_weight = total_first_weight
        doc.total_second_quantity = total_first_quantity
        doc.total_second_weight = total_first_weight
        doc.total_first_sys_quantity = total_first_sys_quantity
        doc.total_first_sys_weight = total_first_sys_weight
        doc.goods_kind_count = goods_kind_count
        self.session.commit()

        return self.send_success()

    # 获取所有正在盘点审核的货品
    def get_goods_in_audit(self):
        # 只需要查询一天内的入库单, 一天以后就作废了
        a_day_before = datetime.datetime.now() - datetime.timedelta(1)
        docs_detail = (self.session.query(
            models.InventoryDocsDetail.goods_id).join(
                models.InventoryDocs,
                models.InventoryDocs.id == models.InventoryDocsDetail.doc_id,
            ).filter(
                models.InventoryDocs.create_date > a_day_before,
                models.InventoryDocs.shop_id == self.current_shop.id,
                models.InventoryDocsDetail.status == 1,
            ).all())
        data_list = list(set(d.goods_id for d in docs_detail))
        return self.send_success(data_list=data_list)

    # 获取所有正在盘点审核/已审核的货品
    def get_goods_in_inventory(self):
        # 只需要查询一天内的入库单, 一天以后就作废了
        a_day_before = datetime.datetime.now() - datetime.timedelta(1)
        docs_detail = (self.session.query(
            models.InventoryDocsDetail.goods_id).join(
                models.InventoryDocs,
                models.InventoryDocs.id == models.InventoryDocsDetail.doc_id,
            ).filter(
                models.InventoryDocs.create_date > a_day_before,
                models.InventoryDocs.shop_id == self.current_shop.id,
                models.InventoryDocsDetail.status.in_([1, 2]),
            ).all())
        data_list = list(set(d.goods_id for d in docs_detail))
        return self.send_success(data_list=data_list)

    def _check_input_goods(self, goods_list):
        """ 传入参数校验, 传入的盘点库存量在方法内部会被更新*100

        :rtype: list
        """
        if not goods_list:
            raise ValueError("至少添加一条数据")
        # 商品的id到商品的映射
        result = []
        for goods_dict in goods_list:
            if not isinstance(goods_dict, dict):
                raise ValueError("包含不合法商品数据")
            for key in ["goods_id", "amount", "storage_unit"]:
                if goods_dict.get(key) is None:
                    raise ValueError("入库数据传入不完整")
            goods_dict["goods_id"] = check_int(goods_dict["goods_id"])
            goods_dict["amount"] = check_int(goods_dict["amount"] * 100)
            if goods_dict["amount"] > 9999999:
                raise ValueError("请不要填写过大数据")
            goods_dict["storage_unit"] = check_int(goods_dict["storage_unit"])
            if goods_dict["storage_unit"] not in [0, 1]:
                raise ValueError("传入商品库存单位不正确")
            result.append(goods_dict)
        map_db_goods = self._get_map_db_goods(goods_list)
        # 校验传入库存单位
        for goods_dict in goods_list:
            if (goods_dict["storage_unit"] != map_db_goods.get(
                    goods_dict["goods_id"]).storage_unit):
                raise ValueError("库存单位已被修改，请刷新商品数据后重试")
        return result

    def _get_map_db_goods(self, goods_list):
        """ 通过传入的商品列表获取数据库中的货品id到货品的映射

        :param goods_list: 传入商品列表

        :rtype: dict
        """
        goods_id_list = [g.get("goods_id") for g in goods_list]
        return GoodsFunc.get_map_id_goods(self.slave_session, goods_id_list)


# 添加客户
class CustomerManage(SalesmanBaseHandler, ShopCustomerBase):
    def get(self):
        qiniuToken = Qiniu.get_qiniu_token()
        return self.render(
            "kaipiao/customer-add.html",
            qiniuToken=qiniuToken,
            current_user=self.current_user,
        )

    @SalesmanBaseHandler.check_arguments("action:str")
    def post(self):
        action = self.args["action"]
        shop_id = self.current_shop.id
        if action == "customer_group_list":
            return self.customer_group_list()
        elif action == "add_edit_customer":
            # 检查开票员是否有手工录入客户权限(超管除外)
            if self.current_user.id != self.current_shop.boss_id:
                if not self.check_add_customer_through_salersman():
                    return self.send_fail("该开票员没有添加客户权限")
            return self.add_edit_customer(shop_id, source="salesman")
        elif action == "add_edit_purchase":
            return self.add_edit_purchase()
        elif action == "get_customer_info":
            return self.get_customer_info_post(shop_id)

    @SalesmanBaseHandler.check_arguments("customer_id:int")
    def get_customer_info_post(self, shop_id):
        customer_id = self.args["customer_id"]
        customer = models.ShopCustomer.get_by_id(self.session, customer_id)
        customer_info = ShopCustomerFunc.format_customer_info(
            self.session, [customer], type="dict")
        return self.send_success(customer_info=customer_info)

    def check_add_customer_through_salersman(self):
        hire_link = (self.session.query(
            models.HireLink.salesman_permission).filter_by(
                account_id=self.current_user.id,
                shop_id=self.current_shop.id).first())
        salesman_permission = DataFormatFunc.split_str(hire_link[0])
        if "4" not in salesman_permission:
            return False
        return True

    def customer_group_list(self):
        # 客户分组
        customer_group_orm = CustomerGroupORM(self.slave_session)
        groups = customer_group_orm.get_all_group(self.current_shop.id)
        customers_count = customer_group_orm.get_group_customers_count(
            self.current_shop.id)
        group_list = CustomerGroupFunc.format_group_list(
            groups, customers_count)
        return self.send_success(group_list=group_list)

