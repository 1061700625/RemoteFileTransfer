# coding:utf-8
import re
import sqlite3
import requests
import time
import paho.mqtt.client as mqtt
from bs4 import BeautifulSoup
import os
import threading
import ctypes
import inspect
import sys
from goto import with_goto

MQTTHOST = "ncurobot.club"
MQTTPORT = 1883
USERNAME = "SXF"
PASSWORD = "1061700625"
HEARTBEAT = 60

class DB:
    def __init__(self):
        self.id = 0
        self.Start(DBPATH)
        self.CreatTable()
        self.id = len(self.SelectALL())
        self.Close()

    def Start(self, database):
        self.conn = sqlite3.connect(database)
        self.cursor = self.conn.cursor()

    def CreatTable(self):
        try:
            self.cursor.execute('''CREATE TABLE ZhiWang(
            ID INT PRIMARY KEY NOT NULL,
            IP VARCHAR(30) NOT NULL ,
            CNT INT NOT NULL,
            FILENAME VARCHAR(100) NULL DEFAULT ' ',
            LOC varchar(100) NULL DEFAULT ' '
            );''')
            return 1
        except Exception as e:
            # print(e)
            return 0

    def Insert(self, IP):
        try:
            self.id += 1
            self.cursor.execute('''INSERT INTO ZhiWang (ID, IP, CNT, FILENAME, LOC) VALUES (?, ?,5,' ', ' ')''', (self.id, IP))
            self.conn.commit()
            return 1
        except Exception as e:
            print("!!! Insert:", str(e))
            return 0

    def UpdateLoc(self, IP, Loc):
        try:
            self.cursor.execute('''UPDATE ZhiWang set LOC=(?) WHERE IP=(?)''', (Loc, IP))
            self.conn.commit()
            return 1
        except Exception as e:
            print(e)
            return 0

    def UpdateFilename(self, Filename, IP):
        try:
            temp = self.cursor.execute('''SELECT FILENAME from ZhiWang WHERE IP=(?)''', (IP,)).fetchone()[0]
            temp = temp + ';' + Filename
            self.cursor.execute('''UPDATE ZhiWang set FILENAME = (?) WHERE IP=(?);''', (temp, IP))
            self.conn.commit()
            return 1
        except Exception as e:
            print('!!! UpdateFilename: ' + str(e))
            return 0

    def Select(self, IP):
        try:
            self.cursor.execute("SELECT * from ZhiWang WHERE IP=(?)", (IP,))
        except Exception as e:
            print("!!! Select:", str(e))
            pass
        return self.cursor.fetchall()

    def UpdateDown(self, IP):
        cnt = self.cursor.execute("SELECT * from ZhiWang WHERE IP=(?);", (IP,)).fetchone()[2]
        self.cursor.execute("UPDATE ZhiWang set CNT = (?) WHERE IP=(?);", (cnt - 1, IP))
        self.conn.commit()

    def UpdateUp(self, IP):
        cnt = self.cursor.execute("SELECT * from ZhiWang WHERE IP=(?);", (IP,)).fetchone()[2]
        self.cursor.execute("UPDATE ZhiWang set CNT = (?) WHERE IP=(?);", (cnt + 1, IP))
        self.conn.commit()

    def UpdateAdd(self, IP):
        cnt = self.cursor.execute("SELECT * from ZhiWang WHERE IP=(?);", (IP,)).fetchone()[2]
        self.cursor.execute("UPDATE ZhiWang set CNT = (?) WHERE IP=(?);", (5, IP))
        self.conn.commit()

    def Check(self, IP, cnt):
        self.cursor.execute('''SELECT * from ZhiWang WHERE IP=(?)''', (IP,))
        if self.cursor.fetchone()[2] >= cnt:
            return 1
        else:
            return 0

    def Close(self):
        self.cursor.close()
        self.conn.close()

    def SelectALL(self):
        sql = "SELECT * from ZhiWang;"
        self.cursor.execute(sql)
        return self.cursor.fetchall()

HEAD = b'#W'
DATAEND1 = b'@#$%'
DATAEND2 = b'%$#@'
REC_FLAG = 0
C_CONNECT = 0
C_ALLOW_URL = 0
class MQTT:
    def __init__(self):
        self.firstenter = 1
        self.name = '1.txt'
        self.filesize = 0
        self.p_last = 0
        self.first_frame = 1
        self.topic_s = '/serverListen/'+PRISEQ
        self.topic_c = '/clientListen/'+PRISEQ
        self.TOPICBUFF = []
        self.URLBUFF = []
        self.topicNow = ''
        self.urlNow = ''
        if S_OR_C == 'S':
            self.db = DB()
        self.time_pre = 0
        self.time_now = 0
        self.THREADBUFF = []
        self.dwnTime_pre = 0
        self.dwnTime_now = 0

    def GetCAJ(self, url):
        print(">> 请求文件")
        try:
            sess = requests.session()
            requests.adapters.DEFAULT_RETRIES = 5
            sess.keep_alive = False
            html = sess.get(url, timeout=30)
            soup = BeautifulSoup(html.text, 'lxml')
            suffix = '.pdf'
            href = ''
            if soup.find(id='pdfDown'):
                suffix = '.pdf'
                href = soup.find(id='pdfDown')
            elif soup.find(id='cajDown'):
                suffix = '.caj'
                href = soup.find(id='cajDown')
            elif soup.find(class_='icon-dlGreen'):
                suffix = '.caj'
                href = soup.find(class_='icon-dlGreen')
            else:
                href = ''
            try:
                href = href['href']
            except Exception as e:
                print("!!! GetCAJ_1:", str(e))
                print('url: ', url)
                print('href: ', href)
                client.publish(self.topicNow, ">> MSG: 下载失败, 这篇文章提供'pdf/caj/整本'下载吗？", 1)
                return None

            time.sleep(1)
            href = r'https://kns.cnki.net' + href
            name = soup.find(class_='wxTitle').find(class_='title').string + suffix
            print(">> 文件名称:", name)
            print(">> 文件链接:", href)
            files = sess.get(href, timeout=30).content
            # if 'alert' in files.decode('utf-8'):
            #     client.publish(self.topicNow, ">> MSG: \r\n%s" % files.decode('utf-8'), 1)
            #     print(files.decode('utf-8'))
            #     return None
            with open(name, 'wb+') as f:
                f.write(files)
            print(">> 下载完成")
            return name
        except Exception as e:
            print("!!! GetCAJ_2:", str(e))
            return None

    def process_data(self, contents, mode=0):  # mode=0:数据帧；mode=1:结束帧
        if mode == 0:
            return HEAD + contents + DATAEND1
        if mode == 1:
            return HEAD + contents + DATAEND2

    def send_data(self, strings, topic):
        # try:
        #     client.publish(topic=self.topic_c, payload=strings.encode('utf8'), qos=0)
        # except:
        client.publish(topic=topic, payload=strings, qos=1)

    # 打开文件
    def open_file(self, file_path):
        try:
            self.file = open(file_path, 'rb')
            self.first_frame = 1
        except Exception as e:
            self.file.close()
            print(e)

    # 读取指定字节文件
    def read_file(self, bytes_cnt, mode=0):
        try:
            read_content = ''
            if mode == 0:  # 读一次
                read_content = self.file.read()
            elif mode == 1:  # 读指定字节
                read_content = self.file.read(bytes_cnt)
            return read_content, self.file.tell()
        except Exception as e:
            self.file.close()
            print(e)

    # 发送文件
    def send_file(self, path, topic):
        global REC_FLAG, C_ALLOW_URL, C_NOT_END
        self.open_file(path)
        time_start = time.time()
        total_size = os.path.getsize(path)
        while True:
            if self.first_frame == 1:
                self.first_frame = 0
                file_name = path.split('\\')[-1]+';'+str(total_size)
                print('>> 文件名:', file_name)
                send_filedata = self.process_data(file_name.encode(), 0)
                # print(send_filedata)
                self.send_data(send_filedata, topic)
                # time.sleep(0.05)

            read_content, p = self.read_file(bytes_cnt=3072, mode=1)
            print('>> 当前指针:{}/{}'.format(p, total_size))
            # if p - self.p_last < 4096:
            if p >= total_size:
                send_filedata = self.process_data(read_content, 1)
                self.send_data(send_filedata, topic)
                self.file.close()
                self.first_frame = 0
                # print(send_filedata)
                print('>> File End!')
                print(">> 耗时(s)：%.2f" % (time.time()-time_start))
                self.p_last = 0
                C_NOT_END = 0
                C_ALLOW_URL = 0
                self.THREADBUFF.pop()
                os.remove(path)
                break
            else:
                self.p_last = p
                send_filedata = self.process_data(read_content, 0)
                self.send_data(send_filedata, topic)
                while REC_FLAG == 0:
                    pass
                REC_FLAG = 0
            # time.sleep(0.05)

    # MQTT连接回调
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(">> 连接云服务器成功")
        if S_OR_C == 'S':
            print(">> 订阅主题 -> %s" % self.topic_s+'#')
            client.subscribe(self.topic_s+'#')
        elif S_OR_C == 'C':
            print(">> 你的IP -> %s" % self.topic_c.split('/')[-1])
            client.subscribe(self.topic_c)

    # 各状态初始化
    def paras_Init(self):
        global REC_FLAG, C_ALLOW_URL, C_NOT_END
        self.stop_thread(self.THREADBUFF.pop())
        self.p_last = 0
        C_NOT_END = 0
        C_ALLOW_URL = 0
        REC_FLAG = 0
        self.file.close()
        self.first_frame = 0

    # MQTT接收回调
    def on_message(self, client, userdata, msg):
        global C_NOT_END, REC_FLAG, C_CONNECT, C_ALLOW_URL, REMIND_MSG
        if S_OR_C == 'C':  # 当前客户端
            MQTT_Rx_Buff = msg.payload
            end_flag = 0
            if MQTT_Rx_Buff.startswith(b'>> CONNECTED'):
                print(">> 连接[服务端]成功！")
                C_CONNECT = 1
                return
            elif MQTT_Rx_Buff.startswith(b'>> MSG'):
                print(MQTT_Rx_Buff.decode('utf-8'))
                return

            if MQTT_Rx_Buff.endswith(DATAEND2):
                end_flag = 1
            MQTT_Rx_Buff = MQTT_Rx_Buff[2:-4]
            if self.firstenter == 1:
                self.firstenter = 0
                print('>> 第一帧')
                self.name = MQTT_Rx_Buff.decode('utf-8').split(';')[0]
                self.filesize = MQTT_Rx_Buff.decode('utf-8').split(';')[1]
                if os.path.isfile(self.name):
                    os.remove(self.name)
                client.publish(self.topic_s, 'P', 1)
            else:
                with open(self.name, 'ab+') as fp:
                    fp.write(MQTT_Rx_Buff)
                    print('>> 数据帧[%s/%s]' % (fp.tell(), self.filesize))
                    client.publish(self.topic_s, 'P', 1)
            if end_flag == 1:
                print(">> 完成，重启软件排队")
                self.firstenter = 1
                end_flag = 0

        elif S_OR_C == 'S':  # 当前服务端
            MQTT_Rx_Buff = str(msg.payload, encoding='utf-8')
            IP = msg.topic.split('/')[-1]
            temptopic = '/clientListen/' + PRISEQ + msg.topic.split('/')[-1]
            if MQTT_Rx_Buff.startswith(">> DEAD"):
                print(">> [%s] 客户端下线: %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), temptopic))
                if self.topicNow.split('/')[-1] == IP and C_ALLOW_URL == 1:  # 正在下载的客户端中途下线
                    print(">> 正在下载的客户端中途下线")
                    self.paras_Init()
                    return
                self.URLBUFF.pop(self.TOPICBUFF.index(temptopic))
                self.TOPICBUFF.remove(temptopic)
                return
            elif ('/clientListen/'+PRISEQ) in MQTT_Rx_Buff:
                print(">> [%s] 客户端上线: %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), temptopic))
                client.publish(temptopic, REMIND_MSG, qos=1)

                self.db.Start(DBPATH)
                if not self.db.Select(IP):  # 不在表内
                    print(">> 新增入表 - ", IP)
                    self.db.Insert(IP)
                self.db.Close()

                time.sleep(1)
                client.publish(temptopic, '>> CONNECTED', 1)
                return

            elif MQTT_Rx_Buff.startswith('http'):
                self.db.Start(DBPATH)
                print(">> 收到下载请求 -", IP)
                if not self.db.Select(IP):  # 不在表内
                    print(">> 新增入表 -", IP)
                    self.db.Insert(IP)
                if self.db.Check(IP, 0) == 0:  # 次数用完
                    client.publish(temptopic, '>> MSG:你当前次数已用完，免费增加请与我联系', 1)
                    print(">> 次数用完 -", IP)
                    return
                remaincnt = self.db.Select(IP)[0][2]
                print(">> 次数充足 - %s [%s]" % (remaincnt, IP))
                try:
                    self.TOPICBUFF.append(temptopic)
                    client.publish(temptopic, '>> MSG:你当前队伍[%s/%s]，请等待，会自动开始。可用次数[%s]' % (self.TOPICBUFF.index(temptopic)+1, len(self.TOPICBUFF), remaincnt), 1)
                    self.URLBUFF.append(MQTT_Rx_Buff)
                    self.db.UpdateDown(IP)
                    self.db.Close()
                except Exception as e:
                    print(e)
                    self.db.Close()
                return

            if MQTT_Rx_Buff == 'P':  # 传完一帧等待客户端回应
                self.dwnTime_now = time.time()
                REC_FLAG = 1
                return

    def CreatSendTask(self):
        global C_ALLOW_URL, C_NOT_END
        while True:
            if C_ALLOW_URL == 0:  # 允许发一次文件
                if len(self.TOPICBUFF) > 0 and len(self.URLBUFF) > 0:
                    self.topicNow = self.TOPICBUFF.pop(0)
                    self.urlNow = self.URLBUFF.pop(0)
                    name = self.GetCAJ(self.urlNow)
                    if name:
                        self.db.Start(DBPATH)
                        self.db.UpdateFilename(name, self.topicNow.split('/')[-1])
                        self.db.Close()
                        print(">> 开始发送")
                        client.publish(topic=self.topicNow, payload=">> MSG: 获取文章成功", qos=1)
                        try:
                            send_task = threading.Thread(target=self.send_file, args=(name, self.topicNow))
                            send_task.setDaemon(True)
                            send_task.start()
                            self.THREADBUFF.append(send_task)
                            self.dwnTime_now = time.time()
                            C_ALLOW_URL = 1
                        except Exception as e:
                            print(e)
                        # send_task.join()
                    else:
                        print(">> 下载失败")
                        self.db.Start(DBPATH)
                        self.db.UpdateUp(self.topicNow.split('/')[-1])
                        self.db.Close()
                        client.publish(topic=self.topicNow, payload=">> MSG:下载失败,次数不减; 检查URL是否正确/知网页面是否支持pdf文档，重启软件排队。若还是不行，那就是不支持下载了", qos=1)
            else:
                if int(time.time() - self.dwnTime_now) > 120:
                    print(">> 传输超时")
                    client.publish(topic=self.topicNow, payload=">> MSG:网速太慢，传送超时，已终止连接", qos=1)
                    self.paras_Init()
            time.sleep(2)

################################强制关闭线程##################################################
    def _async_raise(self, tid, exctype):
        """raises the exception, performs cleanup if needed"""
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def stop_thread(self, thread):
        self._async_raise(thread.ident, SystemExit)
###############################################################################################

    def mqtt(self):
        if S_OR_C == 'C':
            self.topic_c += CLIENTID
            self.topic_s += CLIENTID
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.username_pw_set(USERNAME, PASSWORD)
        client.will_set(self.topic_s, '>> DEAD:' + self.topic_c, 1)
        client.connect(MQTTHOST, MQTTPORT, HEARTBEAT)

        if S_OR_C == 'C':
            client.publish(topic=self.topic_s, payload=self.topic_c, qos=0)
        client.loop_start()  # 线程
        # client.loop_forever()  # 阻塞

    def sendURL(self, URL):
        print(">> 上报URL")
        client.publish(topic=self.topic_s, payload=URL, qos=0)

# 获取外网IP
def GetOuterIP():
    ip = requests.get('https://api.ipify.org/?format=json').json()['ip']
    return str(ip)



REMIND_MSG = ">> MSG:" + "\r\n\r\n** [服务端] 调试中，请勿使用 **\r\n\r\n"  # 提示信息
# REMIND_MSG = ">> MSG:" + "\r\n\r\n** 有新版可用(V1.0)，旧版不显示版本号，请更新 **\r\n\r\n"
DBPATH = r'ZW.db'                   # 数据库位置
CLIENTID = GetOuterIP() or "SXF"    # MQTT 订阅ID
S_OR_C = 'S'                        # 客户端/服务端
PRISEQ = '001'                      # 序列号
# if S_OR_C == 'S':
#     CLIENTID = "SXF"
#     DBPATH = r'C:\Users\SXF\Desktop\dist\ZW.db'
#     if not os.path.isfile(DBPATH):
#         DBPATH = r'ZW.db'
client = mqtt.Client(CLIENTID)      # MQTT实例
C_NOT_END = 1                       # 客户端传输文件未完成
VERSION = '1.0'                     # 版本号
def main():
    global S_OR_C, PRISEQ
    S_OR_C = input("服务端输入S,客户端输入C: ").strip().upper()
    PRISEQ = input("输入“序列号”，随便输，只要客户端与服务端对应就能联通了: ").strip() + '/'
    if S_OR_C == 'C':
        print('*' * 60)
        print(">> 当前为【客户端】设备，版本号:", VERSION)
        print(">> 仅供学习使用，禁止不良牟利")
        print(">> 启动后约等待10s, 30s超时")
        print(">> 5次次数使用完，可留言你的IP增加")
        print(">> 若右键无法粘贴,在标题栏右击，选属性；勾选“快速编辑模式”")
        print(">> github: https://github.com/1061700625/RemoteFileTransfer")
        print('*' * 60)
        print('\r\n')
        mqtt = MQTT()
        mqtt.mqtt()
        print(">> 等待[服务端]响应")
        start_time = time.time()
        while True:
            if C_CONNECT:
                while True:
                    URL = input('>> 复制后右击输入URL后排队:').strip()
                    if re.match('http', URL):
                        break
                    else:
                        print(">> URL错误，应为http(s)形式")
                mqtt.sendURL(URL)
                while 1:
                    time.sleep(1)
            time.sleep(1)
            end_time = time.time()
            if int(end_time - start_time) > 30:
                print(">> [服务端]连接超时, 任意键退出")
                input()
                os._exit(0)

    elif S_OR_C == 'S':
        print(">> 当前为【服务端】设备")
        mqtt = MQTT()
        mqtt.mqtt()
        CreatSendTask_thread = threading.Thread(target=mqtt.CreatSendTask)
        CreatSendTask_thread.setDaemon(True)
        CreatSendTask_thread.start()

        while 1:
            time.sleep(5)
            pass


if __name__ == '__main__':
    '''
    1. 服务端运行在校园网环境，客户端为用户使用。双方建立连接后，客户端上报知网文章URL，服务端获取文章后，即可下发到客户端。
    2. 搜索下载顺序为：pdf下载 -> caj下载 -> 整本下载
    3. 引入排队下载机制，当有多个用户同时请求下载时，将按照先进先出的原则依次运行。
    4. 同时使用sqlite3数据库对用户进行管理。
    5. 增加设置120s传输超时
    6. 程序运行后可选择服务端或是客户端，也就是说，只要稍加改动源码，就可以将程序DIY为两台PC之间的远程文件传输。如：
        a. 办公室电脑运行服务端， 家里电脑运行客户端，即可远程利用公司内网下载权限文件至处于外网环境下的电脑。
        b. 服务器器运行服务端，自己电脑运行客户端，即可将服务器上文件下载下来。
    7. 下载完成说明：
        出现“完成，重启软件排队”
    8. 右键无法粘贴的:
        在标题那里右击，选属性；然后把“快速编辑模式”打钩，这样就能右键粘贴输入了
    9. github：https://github.com/1061700625/RemoteFileTransfer
    10. 程序中用到的MQTT服务器，大家可以继续用我的，只不过买的服务器还有一个月就要过期了。程序中用到的MQTT服务器，大家可以继续用我的，只不过买的服务器还有一个月就要过期了。
    11. 为了防止大家各自的程序混乱，所以在打开软件后会要求你输入一个“序列号”，随便输，只要客户端与服务端对应就能联通了。
    12. 如果大家没有校园网，可以跟我联系，我适当运行一下服务端，同时我的序列号就设置为‘SXF’了。
'''
    main()

