import re

# count=0
# for i in range(0,10000):
#     if '6' in str(i) or '8' in str(i) or '9' in str(i):
#         count+=1
# print(count)


# filter_list = []
# filter_pattern1 = '\d*[6,8,9]\d*[6,8,9]+\d*'
# filter_pattern2 = '\d*(?P<k1>\d)(?P=k1){2,}\d*'
# filter_pattern3 = '\d*(?P<k1>\d)(?P=k1){1}\d*'
# filter_list4 = [123,234,345,456,567,678,789,987,876,765,654,543,432,321,1234,2345,4567,5678,6789,9876,8765,7654,6543,5432,4321]
# for i in range(100,10000):
#     if re.match(filter_pattern1,str(i)) or re.match(filter_pattern2,str(i)) or re.match(filter_pattern3,str(i)) or i in filter_list4:
#         filter_list.append(i)
# print(filter_list)
# print(len(filter_list))
# print(len([i for i in range(100,10000)]))

# id=144
# filter_pattern1 = '\d*[6,8,9]\d*[6,8,9]+\d*'
# filter_pattern2 = '\d*(?P<k1>\d)(?P=k1){2,}\d*'
# filter_pattern3 = '(\d*(?P<k1>\d)(?P=k1){1}\d*)'
# filter_list4 = [123,234,345,456,567,678,789,987,876,765,654,543,432,321,1234,2345,4567,5678,6789,9876,8765,7654,6543,5432,4321]
#
# while True:
#     if not (re.match(filter_pattern1,str(id)) or re.match(filter_pattern2,str(id)) or re.match(filter_pattern3,str(id)) or id in filter_list4):
#         break
#     id+=1
# print(id)

# b=2
#
# a = 1 if b==4 else 2 if b==3 else 3 if b==2 else 4
#
# print(a)