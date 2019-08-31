# coding:utf-8

import sqlite3

class DB:
    def __init__(self):
        self.id = 0
        self.Start()
        self.CreatTable()
        self.id = len(self.SelectALL())
        # print(self.id)

    def Start(self):
        self.conn = sqlite3.connect(r'C:\Users\SXF\Desktop\dist\ZW.db')
        self.cursor = self.conn.cursor()

    def CreatTable(self):
        try:
            self.cursor.execute('''CREATE TABLE ZhiWang(
            ID INT PRIMARY KEY NOT NULL,
            IP VARCHAR(30) NOT NULL ,
            CNT INT NOT NULL);''')
            return 1
        except Exception as e:
            print(e)
            return 0

    def Insert(self, IP):
        try:
            self.id += 1
            self.cursor.execute('''INSERT INTO ZhiWang (ID, IP, CNT) VALUES (?, ?,5)''', (self.id, IP))
            self.conn.commit()
            return 1
        except Exception as e:
            print(e)
            return 0

    def Select(self, IP):
        self.cursor.execute('''SELECT * from ZhiWang WHERE IP=(?)''', (IP,))
        return self.cursor.fetchall()

    def UpdateDown(self, IP):
        cnt = self.cursor.execute("SELECT * from ZhiWang WHERE IP=(?);", (IP,)).fetchone()[2]
        self.cursor.execute("UPDATE ZhiWang set CNT = (?) WHERE IP=(?);", (cnt - 1, IP))
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

    def UpdateAdd(self, IP, count):
        cnt = self.cursor.execute("SELECT * from ZhiWang WHERE IP=(?);", (IP,)).fetchone()[2]
        self.cursor.execute("UPDATE ZhiWang set CNT = (?) WHERE IP=(?);", (count, IP))
        self.conn.commit()

    def AlterAdd(self):
        self.cursor.execute('''ALTER TABLE ZhiWang ADD LOC VARCHAR(100) NULL DEFAULT '';''')
        self.conn.commit()


    def UpdateFilename(self, Filename, IP):
        try:
            temp = self.cursor.execute('''SELECT FILENAME from ZhiWang WHERE IP=(?)''', (IP,)).fetchone()[0]
            temp = temp + ';' + Filename
            self.cursor.execute('''UPDATE ZhiWang set FILENAME = (?) WHERE IP=(?);''', (Filename, IP))
            self.conn.commit()
            return 1
        except Exception as e:
            print(e)
            return 0

def AddIPsCNT(IP, CNT):  # 为某个IP增加次数
    db = DB()
    db.UpdateAdd(IP, CNT)
    res = db.Select(IP)
    db.Close()
    print(res)

def SearchAll():
    db = DB()
    res = db.SelectALL()
    db.Close()
    print(res)

def Insert(ID, IP, CNT):
    pass



AddIPsCNT('112.244.79.95', 10)
# SearchAll()
