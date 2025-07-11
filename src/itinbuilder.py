import datetime
from collections import deque

file = open("./data/cassim0401","r")
dpt_dict = dict()
#flt_dict = dict()
MAX_STOP = 2

for line in file:
    if line.startswith("3"):
        fltid = line[5:9].replace(" ","")
        # 解析起飞和到达日期
        start_date = datetime.datetime.strptime(line[14:21], "%d%b%y")
        end_date = datetime.datetime.strptime(line[21:28], "%d%b%y")
        frequency = list(line[28:35].replace(" ",""))
        frequency = list(map(int, frequency))
        dpt_station = line[36:39]
        arr_station = line[54:57]
        std_local = int(line[43:47])  # 出发地本地时间（hhmm）
        sta_local = int(line[61:65])  # 到达地本地时间（hhmm）
        dpt_utc_offset = int(line[47:52])  # 出发地时区偏移（分钟）
        arr_utc_offset = int(line[65:70])  # 到达地时区偏移（分钟）

        # 拼接start_date和std_local，得到本地起飞时间
        std_hour = std_local // 100
        std_minute = std_local % 100
        start_local_dt = start_date.replace(hour=std_hour, minute=std_minute)
        end_local_dt = end_date.replace(hour=std_hour, minute=std_minute)
        # 拼接end_date和sta_local，得到本地到达时间
        sta_hour = sta_local // 100
        sta_minute = sta_local % 100
        

        # 计算总飞行时间（分钟）
        flight_minutes = (sta_hour * 60 + sta_minute)-(arr_utc_offset//100*60+arr_utc_offset%100) - ((std_hour * 60 + std_minute)-(dpt_utc_offset//100*60+dpt_utc_offset%100))
        if flight_minutes < 0:
            # 到达时间已跨天
            flight_minutes += 24 * 60  # 补一天

        # 转为UTC时间
        start_utc_dt = start_local_dt - datetime.timedelta(minutes=dpt_utc_offset)
        end_utc_dt = end_local_dt - datetime.timedelta(minutes=dpt_utc_offset)

        # frequency原始为[1,2,3,4,5,6,7]，代表周一到周日
        # 需要将frequency调整为UTC起飞的周几
        # 例如：原frequency为2（周二），本地起飞为周二，UTC起飞为周一，则frequency应为1（周一）

        # 构建frequency_utc
        frequency_utc = []
        for f in frequency:
            # f: 1=周一, ..., 7=周日
            # 计算本地起飞weekday
            local_wd = (f - 1) % 7  # 0=周一
            # 计算对应UTC weekday
            # local_wd + (std_utc_dt - std_local_dt).days
            delta_days = (start_utc_dt - start_local_dt).days
            utc_wd = (local_wd + delta_days) % 7
            frequency_utc.append(utc_wd + 1)  # 1=周一

        dpt_dict.setdefault(dpt_station, []).append(
            (start_utc_dt, end_utc_dt, frequency_utc, fltid, arr_station, flight_minutes)
        )
file.close()

#request = input("Enter your request: ")
request = "PEKFRA01MAY25"
dpt = request[:3]
arr = request[3:6]
date = request[6:]
date = datetime.datetime.strptime(date, "%d%b%y")
stack = deque()
paths = []
stack.append((dpt, date, 0, [dpt]))  # (current station, current date, current stop count, path)
while stack:
    current_dpt, current_date, stop_count, path = stack.pop()
    
    # 如果到达目的地，记录路径
    if current_dpt == arr:
        paths.append(path)
        continue
    
    # 如果超过最大中转次数，跳过
    if stop_count >= MAX_STOP:
        continue
    
    # 获取当前出发站的航班信息
    flights = dpt_dict.get(current_dpt, [])
    
    for flight in flights:
        start_utc_dt, end_utc_dt, frequency_utc, fltid, next_station, flight_minutes = flight

        # 检查current_date是否在start_utc_dt与end_utc_dt之间
        if start_utc_dt.date() <= current_date.date() <= end_utc_dt.date():
            # 检查当前weekday是否在frequency_utc
            if current_date.weekday() + 1 in frequency_utc:  # +1 因为frequency_utc是1-7
                # 计算该天的起飞时间
                dep_time = datetime.datetime.combine(current_date.date(), start_utc_dt.time())
                # 衔接时间限制：最小1小时，最大12小时
                if len(path) == 1:
                    # 第一段，直接允许
                    min_connect = datetime.timedelta(hours=0)
                else:
                    min_connect = datetime.timedelta(hours=1)
                max_connect = datetime.timedelta(hours=12)
                last_arrival_time = current_date
                connect_time = dep_time - last_arrival_time
                if min_connect <= connect_time <= max_connect:
                    # 计算落地时间
                    arr_time = dep_time + datetime.timedelta(minutes=flight_minutes)
                    # 在path中补充更多信息：出发站, 到达站, 航班号, 起飞时间, 到达时间
                    new_leg = {
                        "from": current_dpt,
                        "to": next_station,
                        "flight": fltid,
                        "dep_time": dep_time.strftime("%Y-%m-%d %H:%M"),
                        "arr_time": arr_time.strftime("%Y-%m-%d %H:%M"),
                        "flight_minutes": flight_minutes
                    }
                    stack.append((next_station, arr_time, stop_count + 1, path + [new_leg]))

# 输出所有找到的路径
if paths:
    print(f"Found {len(paths)} paths from {dpt} to {arr}:")
    for idx, path in enumerate(paths, start=1):
        print(f"\nPath {idx}:")
        for i, leg in enumerate(path[1:], start=1):  # path[0]是起点字符串
            print(
                f"  Leg {i}: {leg['from']} -> {leg['to']} | "
                f"Flight: {leg['flight']} | "
                f"Dep: {leg['dep_time']} | Arr: {leg['arr_time']} | "
                f"Duration: {leg['flight_minutes']} min"
            )
else:
    print(f"No paths found from {dpt} to {arr}.")
# 输出请求的航班信息