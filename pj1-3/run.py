# Database Pj1-3 run.py
# SNUCSE 18
# OH, JIN SU
# 2018-19857

from lark import Lark, Transformer, v_args, LarkError, Token
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
        string = utf.decode('utf-8')
        this_list = string.split('?')
        my_list = []
        for i in range(len(att_list)):
            if this_list[i] == "None":
                my_list.append(None)
                continue

            if att_list[i].data_type == "int":
                my_list.append(int(this_list[i]))
            elif att_list[i].data_type == "char":
                my_list.append(this_list[i])
            elif att_list[i].data_type == "date":
                my_list.append(date(this_list[i][0:4], this_list[i][5:7], this_list[8:9]))
        return my_list

    @DeprecationWarning # used in only Pj1-2
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

class AmbiguousReferenceError(Exception):
    pass
class IncomparableError(Exception):
    pass
class TableNotSpecifiedError(Exception):
    pass
class ColumnNotExistError(Exception):
    pass

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

def print_header(select_att_str_list):
    select_att_num = len(select_att_str_list)
    print('+' + "---------------+" * select_att_num)
    for select_att_str in select_att_str_list:
        print(f'|{select_att_str.ljust(15)}', end = '')
    print('|')
    print('+' + "---------------+" * select_att_num)

def print_footer(select_att_str_list):
    select_att_num = len(select_att_str_list)
    print('+' + "---------------+" * select_att_num)

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

def get_index_of_column_name(table_name, column_name, att_list):
    if table_name is not None:
        # raise TableNotSpecifiedError: SelectTableExistenceError(#tableName) | WhereTableNotSpecified
        att_table_list = [att_table_name for (att_table_name, att_column_name) in att_list]
        if table_name not in att_table_list:
            raise TableNotSpecifiedError
        
        # raise ColumnNotExistError: SelectColumnResolveError(#colName) | WhereColumnNotExist
        for i in range(len(att_list)):
            (att_table_name, attribute) = att_list[i]
            if att_table_name == table_name and attribute.column_name == column_name:
                return i
        raise ColumnNotExistError
    else: # table_name is None
        idx = -1
        for i in range(len(att_list)):
            (att_table_name, attribute) = att_list[i]
            if attribute.column_name == column_name:
                # raise AmbiguousReferenceError: SelectColumnResolveError(#colName) | WhereAmbiguousReference
                if idx != -1:
                    raise AmbiguousReferenceError
                else:
                    idx = i

        # raise WhereColumnNotExistError: SelectColumnResolveError(#colName) | WhereColumnNotExist
        if idx == -1:
            raise ColumnNotExistError
        else:
            return idx

def get_where_clause_value(where_clause, att_list, data) -> bool:
    boolean_expr = where_clause.children[1]
    boolean_expr_value = get_boolean_expr_value(boolean_expr, att_list, data)
    if boolean_expr_value is None:
        # if final boolean_expr_value is Unknown, return False
        return False
    else:
        return boolean_expr_value

def get_boolean_expr_value(boolean_expr, att_list, data):
    return_None = False
    return_True = False
    for boolean_term in boolean_expr.children:
        # ignore Token 'OR'
        if isinstance(boolean_term, Token):
            continue

        boolean_term_value = get_boolean_term_value(boolean_term, att_list, data)
        if boolean_term_value is None:
            return_None = True
        elif boolean_term_value == True:
            return_True = True
        else:
            pass
    
    # short-circuit is not used to get all boolean_term_value to check validity
    if return_True:
        return True
    elif return_None:
        return None
    else:
        return False

def get_boolean_term_value(boolean_term, att_list, data):
    return_None = False
    return_True = True
    for boolean_factor in boolean_term.children:
        # ignore Token 'AND'
        if isinstance(boolean_factor, Token):
            continue

        boolean_factor_value = get_boolean_factor_value(boolean_factor, att_list, data)
        if boolean_factor_value is None:
            return_None = True
        elif boolean_factor_value == False:
            return_True = False
        else:
            pass

    # short-circuit is not used to get all boolean_term_value to check validity
    if not return_True:
        return False
    if return_None:
        return None
    else:
        return True

def get_boolean_factor_value(boolean_factor, att_list, data):
    boolean_test = boolean_factor.children[1]
    boolean_test_value = get_boolean_test_value(boolean_test, att_list, data)
    if boolean_test_value is None:
        return None
    
    if boolean_factor.children[0] is None: # boolean_test        
        return boolean_test_value
    else: # NOT boolean_test
        return not boolean_test_value
    
def get_boolean_test_value(boolean_test, att_list, data):
    if boolean_test.children[0].data == "predicate":
        predicate = boolean_test.children[0]
        return get_predicate_value(predicate, att_list, data)
    elif boolean_test.children[0].data == "parenthesized_boolean_expr":
        boolean_expr = boolean_test.children[0].children[1]
        return get_boolean_expr_value(boolean_expr, att_list, data)

def get_predicate_value(predicate, att_list, data):
    if predicate.children[0].data == "comparison_predicate":
        comparison_predicate = predicate.children[0]
        return get_comparison_predicate_value(comparison_predicate, att_list, data)
    elif predicate.children[0].data == "null_predicate":
        null_predicate = predicate.children[0]
        return get_null_predicate_value(null_predicate, att_list, data)

def get_comparison_predicate_value(comparison_predicate, att_list, data):
    comp_operand1 = get_comp_operand_value(comparison_predicate.children[0], att_list, data)
    comp_operand2 = get_comp_operand_value(comparison_predicate.children[2], att_list, data)

    if comp_operand1 is None or comp_operand2 is None:
        # if one of the value is null, return None (Unknown)
        return None

    if type(comp_operand1) != type(comp_operand2):
        # raise IncomparableError: WhereIncomparableError
        raise IncomparableError
    comp_op = comparison_predicate.children[1].children[0]
    if comp_op.type == "LESSTHAN":
        return comp_operand1 < comp_operand2
    elif comp_op.type == "LESSEQUAL":
        return comp_operand1 <= comp_operand2
    elif comp_op.type == "EQUAL":
        return comp_operand1 == comp_operand2
    elif comp_op.type == "GREATERTHAN":
        return comp_operand1 > comp_operand2
    elif comp_op.type == "GREATEREQUAL":
        return comp_operand1 >= comp_operand2
    elif comp_op.type == "NOTEQUAL":
        return comp_operand1 != comp_operand2

def get_comp_operand_value(comp_operand, att_list, data):
    if len(comp_operand.children) == 1:
        # comparable_value
        comparable_value = comp_operand.children[0]
        return get_comparable_value_value(comparable_value)
    else:
        # [table_name "."] column_name
        if comp_operand.children[0] is None:
            table_name = None
        else:
            table_name = comp_operand.children[0].children[0].value.lower()
        column_name = comp_operand.children[1].children[0].value.lower()
        
        # this may raise error, but select_query will handle the error
        idx = get_index_of_column_name(table_name, column_name, att_list)
        return data[idx]

def get_comparable_value_value(comparable_value):
    if comparable_value.children[0].type == "INT":
        return int(comparable_value.children[0].value)
    elif comparable_value.children[0].type == "STR":
        # trim first and last character \"
        return comparable_value.children[0].value[1:-1]
    elif comparable_value.children[0].type == "DATE":
        return date(comparable_value.children[0].value)
    else:
        return None

def get_null_predicate_value(null_predicate, att_list, data):   
    if null_predicate.children[0] is None:
        table_name = None
    else:
        table_name = null_predicate.children[0].children[0].value.lower()
    column_name = null_predicate.children[1].children[0].value.lower()

    null_operation = null_predicate.children[2]
    idx = get_index_of_column_name(table_name, column_name, att_list)

    if data[idx] is None:
        # data[idx] = null
        if null_operation.children[1] is None:
            # data[idx] is null
            return True
        else:
            # data[idx] is not null
            return False
    else:
        # data[idx] != null
        if null_operation.children[1] is None:
            # data[idx] is null
            return False
        else:
            # data[idx] is not null
            return True

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

        # find table_name in only table_reference_list
        table_name_iter = list(items[2].children[0].find_data("table_name"))
        table_name_list = [table_name.children[0].value.lower() for table_name in table_name_iter]
        key_name_pair_list = [(encode(table_name), table_name) for table_name in table_name_list]

        db_name_pair_list = []
        attribute_list = []

        # check SelectTableExistenceError(#tableName)
        for (key, table_name) in key_name_pair_list:
            if not infoDB.exists(key):
                print_error(f"Selection has failed: '{table_name}' does not exist")
                return
            selectDB = db.DB()
            selectDB.open(f"DB/{table_name}.db", dbtype=db.DB_HASH)
            db_name_pair_list.append((selectDB, table_name))
        
        # create attribute_list for select_att_str_list
        for (selectDB, att_table_name) in db_name_pair_list:
            attributes = Attribute.decode_att_list(selectDB.get(encode("_attribute_list")))
            for attribute in attributes:
                attribute_list.append((att_table_name, attribute))

        data_list = []

        # create data_list with len(table_name_list)
        # using one/two/three while loop, data_list consists of data which is concatenated data for each table
        if len(table_name_list) == 1:
            selectDB = db_name_pair_list[0][0]
            attributes = Attribute.decode_att_list(selectDB.get(encode("_attribute_list")))
            cursor = selectDB.cursor()
            while x := cursor.next():
                key = decode(x[0])
                if key in reserved_key:
                    continue
                data_att_list = Data.decode_data_to_att_list(x[1], attributes)

                data_list.append(data_att_list)
        elif len(table_name_list) == 2:
            selectDB = db_name_pair_list[0][0]
            attributes = Attribute.decode_att_list(selectDB.get(encode("_attribute_list")))
            cursor = selectDB.cursor()
            while x := cursor.next():
                key = decode(x[0])
                if key in reserved_key:
                    continue
                data_att_list = Data.decode_data_to_att_list(x[1], attributes)
                
                selectDB2 = db_name_pair_list[1][0]
                attributes2 = Attribute.decode_att_list(selectDB2.get(encode("_attribute_list")))
                cursor2 = selectDB2.cursor()                
                while y := cursor2.next():
                    key2 = decode(y[0])
                    if key2 in reserved_key:
                        continue
                    data_att_list2 = Data.decode_data_to_att_list(y[1], attributes2)

                    data_list.append(data_att_list + data_att_list2)
        elif len(table_name_list) == 3:
            selectDB = db_name_pair_list[0][0]
            attributes = Attribute.decode_att_list(selectDB.get(encode("_attribute_list")))
            cursor = selectDB.cursor()
            while x := cursor.next():
                key = decode(x[0])
                if key in reserved_key:
                    continue                
                data_att_list = Data.decode_data_to_att_list(x[1], attributes)

                selectDB2 = db_name_pair_list[1][0]
                attributes2 = Attribute.decode_att_list(selectDB2.get(encode("_attribute_list")))
                cursor2 = selectDB2.cursor()
                while y := cursor2.next():
                    key2 = decode(y[0])
                    if key2 in reserved_key:
                        continue
                    data_att_list2 = Data.decode_data_to_att_list(y[1], attributes2)
                    
                    selectDB3 = db_name_pair_list[2][0]
                    attributes3 = Attribute.decode_att_list(selectDB3.get(encode("_attribute_list")))
                    cursor3 = selectDB3.cursor()
                    while z := cursor3.next():
                        key3 = decode(z[0])
                        if key3 in reserved_key:
                            continue
                        data_att_list3 = Data.decode_data_to_att_list(z[1], attributes3)
                        
                        data_list.append(data_att_list + data_att_list2 + data_att_list3)
        else:
            # check SelectTableMaximumError
            print_error("Selection has failed: exceed maximum table")
            for (selectDB, table_name) in db_name_pair_list:
                selectDB.close()
            return

        # select_att_list will be used in matching the attribute order and print order
        select_att_str_list : List[str] = []
        select_att_idx_list : List[int] = []
        is_asterisk = len(list(items[1].find_data("selected_column"))) == 0
        if is_asterisk:
            # if asterisk, select all
            idx = 0
            for (att_table_name, attribute) in attribute_list:
                select_att_str_list.append(f"{attribute.column_name}")
                select_att_idx_list.append(idx)
                idx += 1
        else:
            # if not, get all idx for select_att_str_list using get_index_of_column_name()
            select_column_iter = items[1].find_data("selected_column")
            for select_column in select_column_iter:
                is_table_include = select_column.children[0] is not None
                if is_table_include:
                    # table_name is clarified
                    table_name = select_column.children[0].children[0].value.lower()
                    column_name = select_column.children[1].children[0].value.lower()
                    try:
                        idx = get_index_of_column_name(table_name, column_name, attribute_list)
                    except TableNotSpecifiedError:
                        # check SelectTableExistenceError(#tableName)
                        print_error(f"Selection has failed: {table_name} does not exist")
                        for (selectDB, table_name) in db_name_pair_list:
                            selectDB.close()
                        return
                    except ColumnNotExistError:
                        # check SelectColumnResolveError(#colName) - column not exist
                        print_error(f"Selection has failed: fail to resolve {column_name}")
                        for (selectDB, table_name) in db_name_pair_list:
                            selectDB.close()
                        return
                    else:
                        select_att_str_list.append(f"{table_name}.{column_name}")
                        select_att_idx_list.append(idx)
                else:
                    # table_name is not clarified, there is a possibility to reference ambiguously
                    column_name = select_column.children[1].children[0].value.lower()
                    try:
                        idx = get_index_of_column_name(None, column_name, attribute_list)
                    except AmbiguousReferenceError:
                        # check SelectColumnResolveError(#colName) - ambiguous reference
                        print_error(f"Selection has failed: fail to resolve {column_name}")
                        for (selectDB, table_name) in db_name_pair_list:
                            selectDB.close()
                        return
                    except ColumnNotExistError:
                        # check SelectColumnResolveError(#colName) - column not exist
                        print_error(f"Selection has failed: fail to resolve {column_name}")
                        for (selectDB, table_name) in db_name_pair_list:
                            selectDB.close()
                        return
                    else:
                        select_att_str_list.append(f"{column_name}")
                        select_att_idx_list.append(idx)

        # header will be printed once
        # if the error occurs during the analysis of where_clause, do not header_print
        header_print : bool = False

        where_clause = items[2].children[1]
        for data in data_list:
            if where_clause is None:
                # print all data
                if not header_print:
                    header_print = True
                    print_header(select_att_str_list)

                for idx in select_att_idx_list:
                    if data[idx] is None:
                        print(f"|{'null'.ljust(15)}", end = '')
                    else:
                        print(f"|{str(data[idx]).ljust(15)}", end = '')
                print('|')
            else:
                # get where_clause_value
                try:
                    where_clause_value = get_where_clause_value(where_clause, attribute_list, data)
                except AmbiguousReferenceError:
                    print_error("Where clause contains ambiguous reference")
                    for (selectDB, table_name) in db_name_pair_list:
                        selectDB.close()
                    return
                except TableNotSpecifiedError:
                    print_error("Where clause trying to reference tables which are not specified")
                    for (selectDB, table_name) in db_name_pair_list:
                        selectDB.close()
                    return
                except ColumnNotExistError:
                    print_error("Where clause trying to reference non existing column")
                    for (selectDB, table_name) in db_name_pair_list:
                        selectDB.close()
                    return
                except IncomparableError:
                    print_error("Where clause trying to compare incomparable values")
                    for (selectDB, table_name) in db_name_pair_list:
                        selectDB.close()
                    return
                else:
                    if where_clause_value:
                        if not header_print:
                            header_print = True
                            print_header(select_att_str_list)

                        # this data is to be printed
                        for idx in select_att_idx_list:
                            if data[idx] is None:
                                print(f"|{'null'.ljust(15)}", end = '')
                            else:
                                print(f"|{str(data[idx]).ljust(15)}", end = '')
                        print('|')
        
        if header_print:
            # successfully printed all data
            # print_footer
            print_footer(select_att_str_list)
        else:
            # there is no data to be printed
            # only print_header in this case
            print_header(select_att_str_list)

        # close selectDB
        for (selectDB, table_name) in db_name_pair_list:
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
        attributes_name_list = [attribute.column_name for attribute in attributes]

        insert_value_list = []
        insert_value_iter = items[5].find_data("insert_value")
        for insert_value in insert_value_iter:
            insert_value_list.append((insert_value.children[0].type, insert_value.children[0].value))

        if items[3] is None:
            column_index_list = list(range(len(attributes)))
            
            # check InsertTypeMismatchError when columns are not declared
            if len(column_index_list) != len(insert_value_list):
                print_error("Insertion has failed: Types are not matched")
                insertDB.close()
                return
            column_checked_list = [True] * len(attributes)
        else:
            column_index_list = []
            column_checked_list = [False] * len(attributes)
            column_name_iter = items[3].find_data("column_name")
            column_name_list = []

            for column_name in column_name_iter:
                column_name_list.append(column_name.children[0].value)

            # check InsertTypeMismatchError when columns are declared
            if len(column_name_list) != len(insert_value_list):
                print_error("Insertion has failed: Types are not matched")
                insertDB.close()
                return

            for column_name in column_name_list:
                
                # check InsertColumnExistenceError(#colName)
                if column_name not in attributes_name_list:
                    print_error(f"Insertion has failed: '{column_name}' does not exist")
                    insertDB.close()
                    return
                
                for i in range(len(attributes_name_list)):
                    if column_name == attributes_name_list[i]:
                        column_index_list.append(i)
                        column_checked_list[i] = True
                        break

        data_list = [None] * len(attributes)

        for i in range(len(attributes)):
            # column is not declared (i.e. null)
            if column_checked_list[i] is False:
                # check InsertColumnNonNullableError(#colName)
                if attributes[i].not_null:
                    print_error(f"Insertion has failed: '{attributes[i].column_name}' is not nullable")
                    insertDB.close()
                    return
                else:
                    data_list[i] = None
                    continue
        
        for i in range(len(insert_value_list)):
            (insert_type, insert_value) = insert_value_list[i]
            column_index = column_index_list[i]
            if insert_type == "INT":
                if attributes[column_index].data_type == "int":
                    data_list[column_index] = int(insert_value)
                else:
                    print_error("Insertion has failed: Types are not matched")
                    insertDB.close()
                    return
            elif insert_type == "STR":
                if attributes[column_index].data_type == "char":
                    # trim first and last character \"
                    string_inner = insert_value[1:-1]
                    # if data_type is char, get the char_length
                    string_trunc = string_inner[:attributes[column_index].char_length]
                    data_list[column_index] = string_trunc
                else:
                    print_error("Insertion has failed: Types are not matched")
                    insertDB.close()
                    return
            elif insert_type == "DATE":
                if attributes[column_index].data_type == "date":
                    data_list[column_index] = date(insert_value)
            elif insert_type == "NULL":
                # check InsertColumnNonNullableError(#colName)
                if attributes[column_index].not_null:
                    print_error(f"Insertion has failed: '{attributes[column_index].column_name}' is not nullable")
                    return
                else:
                    data_list[column_index] = None
            else:
                print_error("WTF")

        static_count = insertDB.get(encode("_static_count"))
        # use current static_count as a key
        insertDB.put(static_count, Data.encode_data(data_list))
        # update the value for _static_count
        insertDB.put(encode("_static_count"), encode(str(int(decode(static_count)) + 1)))
        
        # print InsertResult
        print_success('The row is inserted')
        insertDB.close()

    def delete_query(self, items):

        # do not show values whose key is reserved for internal evaluation
        reserved_key = ["_attribute_list", "_static_count"]
        
        table_name = items[2].children[0].value
        key = encode(table_name)

        # check NoSuchTable
        if not infoDB.exists(key):
            print_error("No such table")
            return

        delete_key_list = []
        att_list = []

        deleteDB = db.DB()
        deleteDB.open(f"DB/{table_name}.db", dbtype=db.DB_HASH)
        attributes = Attribute.decode_att_list(deleteDB.get(encode("_attribute_list")))
        for attribute in attributes:
            att_list.append((table_name, attribute))
        cursor = deleteDB.cursor()

        where_clause = items[3]
        if where_clause is None: # delete all
            count = 0
            while x := cursor.next():
                key = decode(x[0])
                if key not in reserved_key:
                    count = count + 1
                    cursor.delete()

            # print DeleteResult(#count)
            print_success(f"{count} row(s) are deleted")
            deleteDB.close()
            return
        else:
            # if there is a where_clause, first iterate all data to assert where_clause is valid
            count = 0
            while x := cursor.next():
                key = decode(x[0])
                if key in reserved_key:
                    continue
                data = Data.decode_data_to_att_list(x[1], attributes)
                
                try:
                    where_clause_value = get_where_clause_value(where_clause, att_list, data)
                except AmbiguousReferenceError:
                    print_error("Where clause contains ambiguous reference")
                    deleteDB.close()
                    return
                except TableNotSpecifiedError:
                    print_error("Where clause trying to reference tables which are not specified")
                    deleteDB.close()
                    return
                except ColumnNotExistError:
                    print_error("Where clause trying to reference non existing column")
                    deleteDB.close()
                    return
                except IncomparableError:
                    print_error("Where clause trying to compare incomparable values")
                    deleteDB.close()
                    return
                else:    
                    if where_clause_value:
                        delete_key_list.append(key)
        
        cursor = deleteDB.cursor()
        count = 0

        # if where_clause is valid, delete data in delete_key_list
        while x := cursor.next():
            key = decode(x[0])
            if key in delete_key_list:
                cursor.delete()
                count = count + 1
        
        # print DeleteResult(#count)
        print_success(f"{count} row(s) are deleted")
        deleteDB.close()

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
            traceback.print_exc()

            # if parsing error occurs, break the for loop and continue the while loop
            # breaking the for loop means ignoring the remaining queries
            print_error('Syntax error')
            break