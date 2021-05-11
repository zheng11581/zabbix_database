#!/opt/python/3.5.2/bin/python3
# coding: utf-8
# vim: tabstop=2 noexpandtab
"""
    Author: Danilo F. Chilene
	Email:	bicofino at gmail dot com
"""

import argparse
import pymysql
import inspect
import json
import re
import time

version = 0.2


class Checks(object):
    # qps is float type
    def check_qps(self):
      sql = "show global status where variable_name in ('Queries', 'uptime')"
      self.cur.execute(sql)
      res1 = self.cur.fetchall()
      ret1 = [int(i.get("Value")) for i in res1]
      time.sleep(10)
      self.cur.execute(sql)
      res2 = self.cur.fetchall()
      ret2 = [int(i.get("Value")) for i in res2]
      ret = list(zip(ret1, ret2))
      qps = (ret[0][1] - ret[0][0]) / (ret[1][1] - ret[1][0])
      print(qps)

    # tps is float type
    def check_tps(self):
      sql = "show global status where variable_name in ('com_insert' , 'com_delete' , 'com_update', 'uptime')"
      self.cur.execute(sql)
      res1 = self.cur.fetchall()
      ret1 = [int(i.get("Value")) for i in res1]
      time.sleep(10)
      self.cur.execute(sql)
      res2 = self.cur.fetchall()
      ret2 = [int(i.get("Value")) for i in res2]
      ret = list(zip(ret1, ret2))
      tps = ((ret[0][1] + ret[1][1] + ret[2][1]) - (ret[0][0] + ret[1][0] + ret[2][0])) / (ret[3][1] - ret[3][0])
      print(tps)

    # running_threads is int type
    def check_threads_running(self):
      sql = "show global status like 'Threads_running'"
      self.cur.execute(sql)
      res = self.cur.fetchall()
      if not res:
        print(None)
      else:
        running_threads = int(res[0]["Value"])
        print(running_threads)

    # connected_threads is int type
    def _check_threads_connected(self):
      sql = "show global status like 'Threads_connected'"
      self.cur.execute(sql)
      res = self.cur.fetchall()
      if not res:
        print(None)
      else:
        connected_threads = int(res[0]["Value"])
        print(connected_threads)

    # max_connections is int type
    def _check_max_connections(self):
      sql = "show variables like 'max_connections'"
      self.cur.execute(sql)
      res = self.cur.fetchall()
      if not res:
        print(None)
      else:
        max_connections = int(res[0]["Value"])
        print(max_connections)

    # percent is float type
    def check_connection_percent(self):
      sql1 = "show global status like 'Threads_connected'"
      self.cur.execute(sql1)
      res1 = self.cur.fetchall()
      if not res1:
        connected_threads = 0
      else:
        connected_threads = int(res1[0]["Value"])

      sql2 = "show variables like 'max_connections'"
      self.cur.execute(sql2)
      res2 = self.cur.fetchall()
      max_connections = int(res2[0]["Value"])
      percent = connected_threads/max_connections*100
      print(percent)

    # buffer_hit_rate is float type
    def check_buffer_hit_rate(self):
      sql1 = "show global status like 'innodb_buffer_pool_read_requests'"
      self.cur.execute(sql1)
      res1 = self.cur.fetchall()

      sql2 = "show global status like 'innodb_buffer_pool_reads'"
      self.cur.execute(sql2)
      res2 = self.cur.fetchall()

      rate = (int(res1[0]["Value"])-int(res2[0]["Value"]))/int(res1[0]["Value"])*100
      print(rate)

    # avalaible is string type
    def check_avalaible(self):
      sql = "select @@version as version"
      self.cur.execute(sql)
      res = self.cur.fetchall()
      print(res[0]["version"])

    def check_blocking(self, vth):
      sql = "SELECT waiting_pid AS '被阻塞线程', " \
            "waiting_query AS '被阻塞SQL', " \
            "blocking_pid AS '阻塞线程', " \
            "blocking_query AS '阻塞SQL', " \
            "wait_age AS '阻塞时间', " \
            "sql_kill_blocking_query AS '建议操作' " \
            "FROM sys.innodb_lock_waits " \
            "WHERE (unix_timestamp( ) - unix_timestamp(wait_started)) > %s" % (vth)
      self.cur.execute(sql)
      res = self.cur.fetchall()
      if not res:
        print(None)
      else:
        print(res)

    def check_slow_query(self, time):
      sql = "select * from information_schema.processlist where time > %s and command <> 'Sleep'" % (time)
      self.cur.execute(sql)
      res = self.cur.fetchall()
      if not res:
        print(None)
      else:
        print(res)

    def check_slave_delay(self):
      sql = "show slave status"
      self.cur.execute(sql)
      res = self.cur.fetchall()
      if not res:
        print(None)
      else:
        print(res[0]["Seconds_Behind_Master"])

    def check_slave_delay(self):
      sql = "show slave status"
      self.cur.execute(sql)
      res = self.cur.fetchall()
      if not res:
        print(None)
      else:
        print(res[0]["Seconds_Behind_Master"])

    def check_slave_io_running(self):
      sql = "show slave status"
      self.cur.execute(sql)
      res = self.cur.fetchall()
      if not res:
        print(None)
      else:
        print(res[0]["Slave_IO_Running"])

    def check_slave_sql_running(self):
      sql = "show slave status"
      self.cur.execute(sql)
      res = self.cur.fetchall()
      if not res:
        print(None)
      else:
        print(res[0]["Slave_SQL_Running"])

    def check_innodb_deadlock(self):
      sql = "show engine innodb status"
      self.cur.execute(sql)
      res = self.cur.fetchall()
      if not res:
        print(None)
      else:
        innodb_status = res[0]["Status"]
        print(innodb_status)



class Main(Checks):
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--username')
        parser.add_argument('--password')
        parser.add_argument('--address')
        parser.add_argument('--database')
        parser.add_argument('--port', type=int)

        subparsers = parser.add_subparsers()

        for name in dir(self):
            if not name.startswith("_"):
                p = subparsers.add_parser(name)
                method = getattr(self, name)
                argnames = inspect.getfullargspec(method).args[1:]
                for argname in argnames:
                    p.add_argument(argname)
                p.set_defaults(func=method, argnames=argnames)
        self.args = parser.parse_args()

    def db_connect(self):
        a = self.args
        username = a.username
        password = a.password
        address = a.address if a.address else '127.0.0.1'
        database = a.database
        port = a.port if a.port else 3306
        self.db = pymysql.connect(
          user=username, host=address, port=port, 
          password=password, database=database
        )
        self.cur = self.db.cursor(pymysql.cursors.DictCursor)

    def db_close(self):
        self.cur.close()
        self.db.close()

    def __call__(self):
        try:
            a = self.args
            callargs = [getattr(a, name) for name in a.argnames]
            self.db_connect()
            try:
                return self.args.func(*callargs)
            finally:
                self.db_close()
        except Exception as err:
            print(0)
            print(str(err))


if __name__ == "__main__":
    main = Main()
    main()
