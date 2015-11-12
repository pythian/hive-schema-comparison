import pandas as pd
import collections
# this is the namedtuple used by thrift...
FieldSchema = collections.namedtuple('FieldSchema', ['comment','type', 'name'])
# this namedtuple is used to make table comparison easier.
TableDiff = collections.namedtuple('TableDiff', ['db_name', 'table_name','is_diff','is_in_a','is_in_b', 'diff_df'])


class DictCompare:

    def get_fields_by_table(self, db, table):
        fields_a = self.dict_a[db][table] if table in self.dict_a[db] else False
        fields_b = self.dict_b[db][table] if table in self.dict_b[db] else False
        return fields_a, fields_b
    def get_databases(self):
        all_dbs = set([db for  db in self.dict_a]).union(set([db for  db in self.dict_b]))
        return all_dbs

    #From Compare_fields Return a TableDiff tuple

    def compare_fields(self, db, table):
        x,y =  self.get_fields_by_table(db,table)
        is_in_a=True
        is_in_b = True
        if not x or not y:
            description =""
            if not x:
                is_in_a = False
                description = "Table does not exist in {0}".format(self.a_name)
            if not y:
                is_in_b = False
                description = description + "Table does not exist in {0}".format(self.b_name)

            if __debug__:
                print "X",x
                print "Y",y
            return TableDiff(db,table,True,is_in_a,is_in_b,description)
        
        
        df_a = pd.DataFrame(x, columns=x[0]._fields)
        df_b = pd.DataFrame(y, columns=y[0]._fields)
        oj = df_a.merge(df_b, on="name", how="outer")
       
       

        # outer joining the fields for each table and checking NaNs
        df_diff = oj[(pd.isnull(oj["type_x"])) | (pd.isnull(oj["type_y"]))]


        ret_diff = ""
        if len(df_diff.index)==0:
            # "NO DIFFERENCE FOUND"
            ret_diff = TableDiff(db,table,False,True, True, "")
        else:
            ret_diff = TableDiff(db,table,True,True, True, "TABLE DIFFERENCES<BR>"+ df_diff.to_html())

        return ret_diff

    def compare_dbs(self, db):
            tables_a = [table for table in self.dict_a[db]]
            tables_b = [table for table in self.dict_b[db]]

            db_diffs_a =  set(tables_a).difference(set(tables_b))
            db_diffs_b =  set(tables_b).difference(set(tables_a))

            if len(db_diffs_a) + len(db_diffs_b) >0:
                if __debug__:
                    print "DATABASE TABLES DIFFERENCES,\n------------------\n TABLES in A Not in B \n------------------\n", db_diffs_a
                    print "------------------\n TABLES in B Not in A \n------------------\n", db_diffs_b
        
            if __debug__:
                print "\n\n---------------------"
                print "----COMPARE TABLE/FIELDS Between Databases:", db
                print "---------------------"


            all_db_diffs = []
            for table in set(tables_a).union(tables_b):
                tbl_diff = self.compare_fields(db, table)
                all_db_diffs.append(tbl_diff)
            print "all_db_diffs",all_db_diffs
            return all_db_diffs


    def __init__(self, schema_files_path, friendly_name_a, friendly_name_b):
        self.errors=[]

        print "Schema files ",schema_files_path

        schema_file_a =  open("{0}{1}_schema.out".format(schema_files_path,friendly_name_a),'r')
        schema_file_b =  open("{0}{1}_schema.out".format(schema_files_path,friendly_name_b),'r')

        self.dict_a = eval(schema_file_a.read())
        self.dict_b = eval(schema_file_b.read())

        self.a_name = friendly_name_a
        self.b_name = friendly_name_b

        print "INIT...COMPARING {0} to {1}".format(self.a_name, self.b_name)



