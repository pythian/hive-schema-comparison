#!/usr/bin/env python

# This python script is used to generate hive schema definitions
# for a predetermined set of databases
# Using org.apache.hadoop.hive.metastore.api 
#        Class ThriftHiveMetastore.Client
# https://hive.apache.org/javadocs/r0.11.0/api/org/apache/hadoop/hive/metastore/api/ThriftHiveMetastore.Client.html 

from datetime import datetime
import json, collections
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from hive_metastore import ThriftHiveMetastore
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import sys


class configuration():

  def databases(self):
    return self.config_dict['databases_to_compare']

  def environments(self):
    env_dict = {}
    for hive_host in self.config_dict['Hive_Hosts']:
       env = self.Environment(*hive_host)
       env_dict[env.host_friendlyname]={}
       env_dict[env.host_friendlyname]['host_addr']=env.host_addr
       env_dict[env.host_friendlyname]['host_port']=env.host_port
    return env_dict


  def __init__(self,config_file_path):
    self.Environment = collections.namedtuple('Environment', ['host_friendlyname', 'host_addr','host_port'])
    self.config_dict = json.load(open(config_file_path))   


def get_schema(dbs, host_friendlyname, host, port=10001):
    try:
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
     
            global metastore_client
            metastore_client = ThriftHiveMetastore.Client(protocol)
            transport.open()

            data_dict={}
            for db in dbs:
                data_dict[db]={}
                tables = metastore_client.get_all_tables(db)

                for table in tables:
                    data_dict[db][table]=[]

                    print "HOST: {2} DB: {0} TABLE: {1}".format(db,table,host_friendlyname)
                    for field in metastore_client.get_fields(db, table):
                        data_dict[db][table].append(field)
                        print field, field.name

            f = open("{0}_schema.out".format(host_friendlyname),'w')
            f.write(str(data_dict))
    finally:
        pass


def main():
  try:
      config_file_path = sys.argv[1]   
  except:
      print 'usage: fetch_hive_schema.py <config_file_path>'
      sys.exit(2)

  config_file_path = sys.argv[1]   
  cfg = configuration(config_file_path)
  environments = cfg.environments()
  for env in environments:
    get_schema(cfg.databases(), env,  environments[env]['host_addr'],environments[env]['host_port'])

        
if __name__ == '__main__':
    main()
