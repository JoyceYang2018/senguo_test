def single_sale_record_print_log(cls,
                                 session,
                                 shop_id,
                                 sale_record_list,
                                 user_id,
                                 print_result,
                                 error_txt,
                                 printer_num,
                                 source=""):
    pass

bank_info = session\
    .query(
        LcBank.bank_no, LcBank.bank_name,
        LcParentBank.parent_bank_no, LcParentBank.parent_bank_name) \
    .join(LcParentBank,LcParentBank.parent_bank_no == LcBank.parent_bank_no) \
    .join(LcAreaCode, LcAreaCode.city_code == LcBank.city_code) \
    .filter(LcBank.bank_no == bank_no) \
    .first()


if (receipt_config.cashier_accountant_receipt
    and receipt_config.merge_print_accountant == 1)\
    or (receipt_config.cashier_goods_receipt
        and receipt_config.merge_print_goods == 1):



def single_sale_record_print_log(
        cls,session,shop_id,sale_record_list,user_id,
        print_result,error_txt,printer_num,source=""):
    pass

print_log = TempOrderPrintLog(
    shop_id=shop_id, order_id=order_id, user_id=user_id,
    print_result=print_result, error_txt=error_txt,
    source_type=source_type, printer_num=printer_num)


if (receipt_config.cashier_accountant_receipt and
    receipt_config.merge_print_accountant == 1)\
    or (receipt_config.cashier_goods_receipt and
        receipt_config.merge_print_goods == 1):
