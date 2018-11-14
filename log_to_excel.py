# coding:utf-8
import os
import xlwt
import json
import codecs

codecs.register(lambda name: codecs.lookup('utf8') if name == 'utf8mb4' else None)

file_path = os.path.abspath("/home/yang/桌面/Log/")
for root, dirnames, filenames in os.walk(file_path):
    to_file_dict = {}
    for filename in filenames:
        file_array = filename.split('_')
        len_array = len(file_array)
        str_date = file_array[len_array - 1][:8]
        shop_id = file_array[1]
        machine_MAC = file_array[2]
        print(machine_MAC)
        file_array.pop()
        file_array.append("")
        file_pre = '_'.join(file_array)
        to_file = '%s/%s%s.txt' % (file_path, file_pre, str_date)
        if machine_MAC not in to_file_dict:
            to_file_dict[machine_MAC]=[]
        to_file_dict[machine_MAC].append(to_file)
        for i in range(24):
            from_file = '%s/%s%s%s.txt' % (file_path, file_pre, str_date, str(i).zfill(2))
            if os.path.exists(from_file):
                if os.path.exists(to_file):
                    os.system('cat %s >> %s' % (from_file, to_file))
                else:
                    os.system('cat %s > %s' % (from_file, to_file))
                os.system('rm %s' % from_file)
    def insert_excel(ws,inserted_row,inserted_list):
        for i in range(len(inserted_list)):
            ws.write(inserted_row,i,inserted_list[i])

    for MAC in to_file_dict:
        x=xlwt.Workbook()
        to_file_dict[MAC].sort()
        for to_file in to_file_dict[MAC]:
            file_array = to_file.split('_')
            str_date = file_array[-1][:8]
            ws_invalid = x.add_sheet(str_date)
            j=0
            tr_head = ['请求小时', '请求发起时间', '本地打印时间', '请求返回']
            insert_excel(ws_invalid, j, tr_head)
            with open(to_file, 'rt') as f:
                for line in f:
                    if line.startswith(u'\ufeff'):
                        line = line.encode('utf8')[3:].decode('utf8')
                    info = json.loads(line)
                    for k in info:
                        v = info[k]
                        j+=1
                        tr_body = [v['send_time'][:2],v['send_time'], v['local_time'], str(v['res'])]
                        insert_excel(ws_invalid, j, tr_body)
        x.save(file_path + '/%s-%s.xls' % (str(shop_id),MAC))