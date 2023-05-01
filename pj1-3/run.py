# Database Pj1-3 run.py
# SNUCSE 18
# OH, JIN SU
# 2018-19857

from lark import Lark, Transformer, v_args, LarkError
import sys
from berkeleydb import db
from os import remove
from datetime import date
import traceback
from typing import List

# Attribute class will represent the properties of the attributes
class Attribute:
    def __init__(self, column_name, data_type, char_length, not_null, primary, foreign, reference_table, reference_column):
        self.column_name = column_name
        self.data_type = data_type
        self.char_length = char_length
        self.not_null = not_null
        self.primary = primary
        self.foreign = foreign
        self.reference_table = reference_table
        self.reference_column = reference_column

    @classmethod
    def builder(cls):
        return AttributeBuilder()
    
    def is_char_length(self, num):
        self.char_length = num

    def is_not_null(self):
        self.not_null = True

    def is_primary(self):
        self.primary = True
        # if the attribute is primary, it is not_null automatically
        self.not_null = True

    def is_foreign(self, table, column):
        self.foreign = True
        self.reference_table = table
        self.reference_column = column

    # concat att to string with '/'
    def __att_to_string(att):
        return (f'{att.column_name}/'
            f'{att.data_type}/'
            f'{att.char_length}/'
            f'{att.not_null}/'
            f'{att.primary}/'
            f'{att.foreign}/'
            f'{att.reference_table}/'
            f'{att.reference_column}')
    
    # split string to att with '/'
    def __string_to_att(string):
        data = string.split('/')
        return Attribute.builder()\
            .set_column_name(data[0])\
            .set_data_type(data[1])\
            .set_char_length(int(data[2]))\
            .set_not_null(data[3] == "True")\
            .set_primary(data[4] == "True")\
            .set_foreign(data[5] == "True")\
            .set_reference_table(data[6])\
            .set_reference_column(data[7])\
            .build()
    
    # concat att_list to string with '?'
    def __att_list_to_string(att_list):
        string = '?'.join([Attribute.__att_to_string(att) for att in att_list])
        return string
    
    # split string to att_list with '?'
    def __string_to_att_list(string):
        data = string.split('?')
        att_list = []
        for datum in data:
            att = Attribute.__string_to_att(datum)
            att_list.append(att)
        return att_list
    
    # encode att_list to bytes
    @staticmethod
    def encode_att_list(att_list: List) -> bytes:
        return Attribute.__att_list_to_string(att_list).encode('utf-8')
    
    # decode bytes to att_list
    @staticmethod
    def decode_att_list(utf: bytes) -> List:
        string = utf.decode('utf-8')
        return Attribute.__string_to_att_list(string)

# AttributeBuilder class is a builder class for Attribute
class AttributeBuilder:
    
    # initialize values to default
    def __init__(self):
        self.column_name = None
        self.data_type = None
        self.char_length = 0
        self.not_null = False
        self.primary = False
        self.foreign = False
        self.reference_table = None
        self.reference_column = None
    
    def set_column_name(self, string):
        self.column_name = string
        return self
    
    def set_data_type(self, string):
        self.data_type = string
        return self
    
    def set_char_length(self, num):
        self.char_length = num
        return self
    
    def set_not_null(self, boo):
        self.not_null = boo
        return self
    
    def set_primary(self, boo):
        self.primary = boo
        return self
    
    def set_foreign(self, boo):
        self.foreign = boo
        return self
    
    def set_reference_table(self, string):
        self.reference_table = string
        return self
    
    def set_reference_column(self, string):
        self.reference_column = string
        return self
    
    # build Attribute with assinged values
    def build(self):
        return Attribute(self.column_name,
                         self.data_type,
                         self.char_length,
                         self.not_null,
                         self.primary,
                         self.foreign,
                         self.reference_table,
                         self.reference_column)

# Reference class will represent the list of the referenced table names
class Reference:
    def __init__(self):
        self.ref_list = []
    
    def add_reference(self, ref):
        self.ref_list.append(ref)
    
    @staticmethod
    def ref_list_to_string(ref_list: List[str]) -> str:
        return '?'.join(ref_list)
    
    @staticmethod
    def string_to_ref_list(string: str) -> List[str]:
        ref_list = []
        data = string.split('?')
        for datum in data:
            ref_list.append(datum)
        return ref_list

# Data class will represent the data with a single string
# Data only has static methods (i.e. Do not make any instance of Data)
class Data:

    @staticmethod
    def encode_data(data_list):
        string = '?'.join(str(x) for x in data_list)
        return string.encode('utf-8')
    
    @staticmethod
    def decode_data_to_att_list(utf, att_list):
        """
        Use this method when you have to handle/compare the data (i.e. where)
        """
        string = utf.decode('utf-8')
        this_list = string.split('?')
        my_list = []
        for i in range(len(att_list)):
            if att_list[i].type == "int":
                my_list.append(int(this_list[i]))
            elif att_list[i].type == "char":
                my_list.append(this_list[i])
            elif att_list[i].type == "data":
                my_list.append(date(this_list[i][0:4], this_list[i][5:7], this_list[8:9]))
        return my_list

    @staticmethod
    def decode_data_to_str_list(utf):
        """
        Use this method when your only goal is printing (i.e. select *)
        """
        string = utf.decode('utf-8')
        this_list = string.split('?')
        my_list = []
        for element in this_list:
            my_list.append(str(element))
        return my_list

infoDB = db.DB()

# open _info.db or create _info.db
try:
    infoDB.open('DB/_info.db', dbtype=db.DB_HASH)
except db.DBNoSuchFileError:
    infoDB.close()
    infoDB = db.DB()
    infoDB.open('DB/_info.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

@DeprecationWarning # used in only Pj1-1
def print_query(string):
    print("DB_2018-19857> '" + string + "' requested")

def print_success(string):
    print('DB_2018_19857> ' + string)

def print_error(string):
    print('DB_2018-19857> ' + string)

def encode(string: str) -> bytes:
    return string.encode('utf-8')

def decode(utf: bytes) -> str:
    return utf.decode('utf-8')

def encode_info_value(ref_list: List[str]) -> bytes:
    ref_list_str = Reference.ref_list_to_string(ref_list)
    return encode(ref_list_str)

def decode_info_value(utf: bytes) -> List[str]:
    string = decode(utf)
    ref_list = Reference.string_to_ref_list(string)
    return ref_list

class MyTransformer(Transformer):
    def create_table_query(self, items):
        table_name = items[2].children[0].lower()
        key = encode(table_name)

        # check TableExistenceError
        if infoDB.exists(key):
            print_error('Create table has failed: table with the same name already exists')
            return
        
        attribute_list = [] # Attribute
        primary_key_list = [] # column_name        
        foreign_key_list = [] # (column_name_list, foreign_table, column_name_list)

        column_definition_iter = items[3].find_data("column_definition")
        primary_key_constraint_iter = items[3].find_data("primary_key_constraint")
        referential_constraint_iter = items[3].find_data("referential_constraint")

        # handle column_definition        
        for column_definition in column_definition_iter:
            column_name = column_definition.children[0].children[0].value.lower()
            data_type = column_definition.children[1].children[0].value.lower()
            
            attribute = Attribute.builder()\
                .set_column_name(column_name)\
                .set_data_type(data_type)\
                .build()
            
            if data_type == "char":
                char_length = int(column_definition.children[1].children[2].value)
                
                # check CharLengthError
                if char_length < 1:
                    print_error('Char length should be over 0')
                    return
                else:
                    Attribute.is_char_length(attribute, char_length)
            
            if column_definition.children[2]:
                Attribute.is_not_null(attribute)
            
            attribute_list.append(attribute)

        # handle primary key constraint
        flag = False
        for primary_key_constraint in primary_key_constraint_iter:
            # check DuplicatePrimaryKeyDefError
            if flag:
                # primary_key_constraint should appear only once
                print_error('Create table has failed: primary key definition is duplicated')
                return
            column_name_iter = primary_key_constraint.find_data("column_name")
            for column_name in column_name_iter:
                primary_key_list.append(column_name.children[0].value.lower())
            flag = True

        # handle referential constraint
        for referential_constraint in referential_constraint_iter:
            foreign_name_iter = referential_constraint.children[2].find_data("column_name")
            foreign_name_list = []
            for foreign_name in foreign_name_iter:
                foreign_name_list.append(foreign_name.children[0].value.lower())
            reference_table_name = referential_constraint.children[4].children[0].value.lower()
            # check SelfReferenceError 
            if reference_table_name == table_name:
                print_error('Create table has failed: foreign key references table from itself')
                return
            reference_name_iter = referential_constraint.children[5].find_data("column_name")
            reference_name_list = []
            for reference_name in reference_name_iter:
                reference_name_list.append(reference_name.children[0].value.lower())
            # foreign_key_list is a list containing tuples (consist of foreign_name_list, reference_table_name, reference_name_list)
            foreign_key_list.append((foreign_name_list, reference_table_name, reference_name_list))

        # check DuplicateColumnDefError
        column_name_list = [attribute.column_name for attribute in attribute_list]
        if (len(column_name_list) != len(set(column_name_list))):
            print_error('Create table has failed: column definition is duplicated')
            return
        
        # check DuplicateColumnListError in primary key
        if len(primary_key_list) != len(set(primary_key_list)):
            print_error('Create table has failed: column is duplicated within a constraint list')
            return

        for column_name in primary_key_list:
            # check NonExistingColumnDefError(#colName) in primary_key
            if column_name not in column_name_list:
                print_error(f"Create table has failed: '{column_name}' does not exist in column definition")
                return
            
            for attribute in attribute_list:
                # find attribute
                if attribute.column_name != column_name:
                    continue
                Attribute.is_primary(attribute)
                break

        reference_table_list = [] # table_name

        # check foreign_key_list
        for (foreign_name_list, reference_table_name, reference_name_list) in foreign_key_list:
            
            # check ReferenceColumnMatchError 
            if len(foreign_name_list) != len(reference_name_list):
                print_error("Create table has failed: foreign keys don't match with reference keys")
                return
            
            # check DuplicateColumnListError in foreign key
            if len(primary_key_list) != len(set(primary_key_list)):
                print_error('Create table has failed: column is duplicated within a constraint list')
                return
            
            # check DuplicateColumnListError in reference key
            if len(reference_name_list) != len(set(reference_name_list)):
                print_error('Create table has failed: column is duplicated within a constraint list')
                return

            # check ReferenceTableExistenceError
            reference_key = encode(reference_table_name)
            if not infoDB.exists(reference_key):
                print_error('Create table has failed: foreign key references non existing table')
                return
            
            # open referenced tables to check whether referencing is valid or not
            refDB = db.DB()
            refDB.open(f'DB/{reference_table_name}.db', dbtype=db.DB_HASH)
            reference_attribute_list = Attribute.decode_att_list(refDB.get(encode('_attribute_list')))
            reference_column_list = [attribute.column_name for attribute in reference_attribute_list]

            reference_primary_list = []
            for reference_attribute in reference_attribute_list:
                if reference_attribute.primary:
                    reference_primary_list.append(reference_attribute.column_name)

            # check ReferenceNonPrimaryKeyError
            if len(reference_primary_list) != len(reference_name_list):
                print_error('Create table has failed: foreign key references non primary key column')
                return

            for i in range(len(foreign_name_list)):

                # check NonExistingColumnDefError(#colName) in foreign_key
                if foreign_name_list[i] not in column_name_list:
                    print_error(f"Create table has failed: '{foreign_name_list[i]}' does not exist in column definition")
                    return
                
                # check ReferenceColumnExistenceError
                if reference_name_list[i] not in reference_column_list:
                    print_error('Create table has failed: foreign key references non existing column')
                    return
                
                # check ReferenceNonPrimaryKeyError
                if reference_name_list[i] not in reference_primary_list:
                    print_error('Create table has failed: foreign key references non primary key column')
                    return

                for reference_attribute in reference_attribute_list:
                    # find reference_attribute
                    if reference_attribute.column_name != reference_name_list[i]:
                        continue
                    
                    for attribute in attribute_list:
                        # find attribute
                        if attribute.column_name != foreign_name_list[i]:
                            continue
                        
                        # check ReferenceTypeError
                        if reference_attribute.data_type != attribute.data_type:
                            print_error('Create table has failed: foreign key references wrong type')
                            return
                        elif attribute.data_type == "char" and reference_attribute.char_length != attribute.char_length:
                            # even data_type is same as char, it is type error if char_length is different
                            print_error('Create table has failed: foreign key references wrong type')
                            return
                        else:
                            # set attribute as foreign
                            Attribute.is_foreign(attribute, reference_table_name, reference_name_list[i])
                            reference_table_list.append(reference_table_name)
            refDB.close()

        # value for this table is the reference_table_list
        value = encode_info_value(reference_table_list)
        infoDB.put(key, value)

        # create new DB file
        newDB = db.DB()
        newDB.open(f'DB/{table_name}.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

        # _attribute_list is used whenever user inserts values or the other referencing table is being created
        newDB.put(encode("_attribute_list"), Attribute.encode_att_list(attribute_list))
        # _static_count is used as keys for inserting data
        newDB.put(encode("_static_count"), encode("0"))
        newDB.close()
        
        # print CreateTableSuccess(#tableName)
        print_success(f"'{table_name}' table is created")

    def drop_table_query(self, items):
        table_name = items[2].children[0].lower()
        key = encode(table_name)

        # check NoSuchTable
        if not infoDB.exists(key):
            print_error('No such table')
            return
        
        # check DropReferencedTableError(#tableName)
        cursor = infoDB.cursor()
        while x := cursor.next():            
            ref_list = decode_info_value(x[1])

            # if any other table references the table, the table should not be dropped
            if table_name in ref_list:
                print_error(f"Drop table has failed: {table_name} is referenced by other table")
                return
        
        # delete key-value in _info.db
        # remove db file
        del infoDB[key]
        remove(f"DB/{table_name}.db")

        # print DropSuccess(#tableName)
        print_success(f"'{table_name}' table is dropped")

    def explain_query(self, items):
        table_name = items[1].children[0].lower()
        key = encode(table_name)

        # check NoSuchTable
        if not infoDB.exists(key):
            print_error('No such table')
            return
        
        explainDB = db.DB()
        explainDB.open(f'DB/{table_name}.db', dbtype=db.DB_HASH)
        attributes = Attribute.decode_att_list(explainDB.get(encode("_attribute_list")))

        print("-----------------------------------------------------------------")
        print(f'table_name [{table_name}]')
        print('column_name'.ljust(17) + 'type'.ljust(15) + 'null'.ljust(15) + 'key'.ljust(15))
        # define column_name, data_type, not_null, key_type based on _attribute_list
        for attribute in attributes:
            column_name = attribute.column_name
            if attribute.data_type == "char":
                data_type = f"char({str(attribute.char_length)})"
            else:
                data_type = attribute.data_type
            if attribute.not_null:
                not_null = 'Y'
            else:
                not_null = 'N'
            if attribute.primary and attribute.foreign:
                key_type = "PRI/FOR"
            elif attribute.primary:
                key_type = "PRI"
            elif attribute.foreign:
                key_type = "FOR"
            else:
                key_type = ""
            print(column_name.ljust(17) + data_type.ljust(15) + not_null.ljust(15) + key_type.ljust(15))
        print("-----------------------------------------------------------------")
        explainDB.close()

    # same as explain_query    
    def describe_query(self, items):
        table_name = items[1].children[0].lower()
        key = encode(table_name)

        # check NoSuchTable
        if not infoDB.exists(key):
            print_error('No such table')
            return
        
        explainDB = db.DB()
        explainDB.open(f'DB/{table_name}.db', dbtype=db.DB_HASH)
        attributes = Attribute.decode_att_list(explainDB.get(encode("_attribute_list")))

        print("-----------------------------------------------------------------")
        print(f'table_name [{table_name}]')
        print('column_name'.ljust(17) + 'type'.ljust(15) + 'null'.ljust(15) + 'key'.ljust(15))
        for attribute in attributes:
            column_name = attribute.column_name
            if attribute.data_type == "char":
                data_type = f"char({str(attribute.char_length)})"
            else:
                data_type = attribute.data_type
            if attribute.not_null:
                not_null = 'Y'
            else:
                not_null = 'N'
            if attribute.primary and attribute.foreign:
                key_type = "PRI/FOR"
            elif attribute.primary:
                key_type = "PRI"
            elif attribute.foreign:
                key_type = "FOR"
            else:
                key_type = ""
            print(column_name.ljust(17) + data_type.ljust(15) + not_null.ljust(15) + key_type.ljust(15))
        print("-----------------------------------------------------------------")
        explainDB.close()

    # same as explain_query 
    def desc_query(self, items):
        table_name = items[1].children[0].lower()
        key = encode(table_name)

        # check NoSuchTable
        if not infoDB.exists(key):
            print_error('No such table')
            return
        
        explainDB = db.DB()
        explainDB.open(f'DB/{table_name}.db', dbtype=db.DB_HASH)
        attributes = Attribute.decode_att_list(explainDB.get(encode("_attribute_list")))

        print("-----------------------------------------------------------------")
        print(f'table_name [{table_name}]')
        print('column_name'.ljust(17) + 'type'.ljust(15) + 'null'.ljust(15) + 'key'.ljust(15))
        for attribute in attributes:
            column_name = attribute.column_name
            if attribute.data_type == "char":
                data_type = f"char({str(attribute.char_length)})"
            else:
                data_type = attribute.data_type
            if attribute.not_null:
                not_null = 'Y'
            else:
                not_null = 'N'
            if attribute.primary and attribute.foreign:
                key_type = "PRI/FOR"
            elif attribute.primary:
                key_type = "PRI"
            elif attribute.foreign:
                key_type = "FOR"
            else:
                key_type = ""
            print(column_name.ljust(17) + data_type.ljust(15) + not_null.ljust(15) + key_type.ljust(15))
        print("-----------------------------------------------------------------")
        explainDB.close()

    def show_tables_query(self, items):
        print('------------------------')
        cursor = infoDB.cursor()
        while x := cursor.next():
            print(decode(x[0]))
        print('------------------------')

    def select_query(self, items):
        
        # do not show values whose key is reserved for internal evaluation
        reserved_key = ["_attribute_list", "_static_count"]

        # assume that select * from table_name, table_name is unique in this case
        table_name = list(items[2].find_data("table_name"))[0].children[0].value.lower()
        key = encode(table_name)

        # check SelectTableExistenceError(#tableName)
        if not infoDB.exists(key):
            print_error(f"Selection has failed: '{table_name}' does not exist")
            return
        
        selectDB = db.DB()
        selectDB.open(f"DB/{table_name}.db", dbtype=db.DB_HASH)
        attributes = Attribute.decode_att_list(selectDB.get(encode("_attribute_list")))
        attribute_num = len(attributes)

        print('+' + "---------------+" * attribute_num)
        for attribute in attributes:
            print(f'|{attribute.column_name.ljust(15)}', end = '')
        print('|')
        print('+' + "---------------+" * attribute_num)

        cursor = selectDB.cursor()
        while x := cursor.next():
            key = decode(x[0])

            # continue if x[0] is reserved (e.g. "_attribute_list", "_static_count")
            if key in reserved_key:
                continue

            data_str_list = Data.decode_data_to_str_list(x[1])
            for data_str in data_str_list:
                print(f'|{data_str.ljust(15)}', end = '')
            print('|')

        print('+' + "---------------+" * attribute_num)
        selectDB.close()
        
    def insert_query(self, items):
        table_name = items[2].children[0].lower()
        key = encode(table_name)

        # check NoSuchTable
        if not infoDB.exists(key):
            print_error('No such table')
            return
        
        insertDB = db.DB()
        insertDB.open(f'DB/{table_name}.db', dbtype=db.DB_HASH)
        attributes = Attribute.decode_att_list(insertDB.get(encode("_attribute_list")))
        
        insert_value_list = []
        insert_value_iter = items[5].find_data("insert_value")
        for insert_value in insert_value_iter:
            insert_value_list.append(insert_value.children[0].value)

        data_list = []

        for i in range(len(attributes)):
            if attributes[i].data_type == "int":
                integer = int(insert_value_list[i])
                data_list.append(integer)
            elif attributes[i].data_type == "char":
                # get _STRING_ESC_INNER
                string_inner = insert_value_list[i][1:-1]
                # if data_type is char, get the char_length
                char_length = int(attributes[i].char_length)
                string_trunc = string_inner[:char_length]
                # trunc the string to char_length
                data_list.append(string_trunc)
            elif attributes[i].data_type == "date":
                date_data = date(insert_value_list[i])
                data_list.append(date_data)

        static_count = insertDB.get(encode("_static_count"))
        # use current static_count as a key
        insertDB.put(static_count, Data.encode_data(data_list))
        # update the value for _static_count
        insertDB.put(encode("_static_count"), encode(str(int(decode(static_count)) + 1)))
        
        # print InsertResult
        print_success('The row is inserted')

    def delete_query(self, items):
        print_error('Delete query not implemented')
    def update_tables_query(self, items):
        print_error('Update tables query not implemented')
    def exit(self, items):
        infoDB.close()
        sys.exit()

with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

while (True):    
    string = ''
    print('DB_2018-19857>', end = ' ')
    while (True):
        line = input()
        string += line + ' '
        if (line.strip().endswith(';')):
            break
    
    # if process str as query in raw, lark will determine the whole str as syntax error when there is an error in one query
    strs = string.strip().split(';')
    # split the str and process one by one
    for query in strs:
        # explicitly exclude an (maybe last) empty string which was made by split(';')
        if (len(query.strip()) == 0):
            continue
        try:
            # add semicolon again
            output = sql_parser.parse(query + ';')
            MyTransformer().transform(output)
        except LarkError as e:
            # traceback.print_exc()

            # if parsing error occurs, break the for loop and continue the while loop
            # breaking the for loop means ignoring the remaining queries
            print_error('Syntax error')
            break