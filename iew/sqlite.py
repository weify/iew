'''
Created on 2012-4-28

@author: Wei
'''
import sqlite3

class DBManager(object):
    '''
    classdocs
    '''
    def __init__(self, database):
        '''
        Constructor
        '''
        self.database = database
    def get_connect(self):
        return sqlite3.connect(self.database)
    def close_connect(self, con):
        con.close()
    def generate_table(self, sql):
        self.update(None, sql)
    def generate_index(self, indexname, tablename, fieldnames):
        self.update(None, "CREAT INDEX IF NOT EXISTS {0} ON {1} ({2})".format(indexname, tablename, ','.join(fieldnames)))
    def is_table_exists(self, tablename):
        ite = self.query_one("select count(*) from sqlite_master where type='table' and name='" + tablename + "'")
        if ite != None and ite[0] == 1:
            return True
        return False
    def insert(self, con, sql):
        self.update(con, sql)
    def remove(self, con, sql):
        self.update(con, sql)
    def query_list(self, con, sql):
        list = []
        try:
            commit = con == None
            if commit:
                con = self.get_connect()
            cur = con.cursor()
            cur.execute(sql)
            res = cur.fetchall()
            for ite in res:
                list.append(ite)
            return list
        finally:
            if commit:
                con.close()
        return list
    def query_one(self, con, sql):
        list = self.query_list(con, sql)
        if list != None:
            return list[0]
        return None
    def update(self, con, sql):
        try:
            commit = con == None;
            if commit:
                con = self.get_connect()
            cur = con.cursor()
            cur.execute(sql)
        finally:
            if commit:
                con.commit()
                con.close()