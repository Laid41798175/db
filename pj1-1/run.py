from lark import Lark, Transformer, v_args, LarkError
import sys

def print_query(str):
    print("DB_2018-19857> '" + str + "' requested")
    
def print_error():
    print('DB_2018-19857> Syntax error')

class MyTransformer(Transformer):
    # processing DDL and DML will be implemented in project 1-2 and 1-3
    def create_table_query(self, items):
        print_query('CREATE TABLE')
    def drop_table_query(self, items):
        print_query('DROP TABLE')
    def explain_query(self, items):
        print_query('EXPLAIN')
    def describe_query(self, items):
        print_query('DESCRIBE')
    def desc_query(self, items):
        print_query('DESC')
    def show_tables_query(self, items):
        print_query('SHOW TABLES')
    def select_query(self, items):
        print_query('SELECT')
    def insert_query(self, items):
        print_query('INSERT')
    def delete_query(self, items):
        print_query('DELETE')
    def update_tables_query(self, items):
        print_query('UPDATE')
    def exit(self, items):
        sys.exit()

with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

while (True):    
    str = ''
    print('DB_2018-19857>', end = ' ')
    while (True):
        line = input()
        str += line + ' '
        if (line.strip().endswith(';')):
            break
    
    # if process str as query in raw, lark will determine the whole str as syntax error when there is an error in one query
    strs = str.strip().split(';')
    # split the str and process one by one
    for query in strs:
        # explicitly exclude an (maybe last) empty string which was made by split(';')
        if (len(query.strip()) == 0):
            continue
        try:
            # add semicolon again
            output = sql_parser.parse(query + ';')
            MyTransformer().transform(output)
        except LarkError:
            # if parsing error occurs, break the for loop and continue the while loop
            # breaking the for loop means ignoring the remaining queries
            print_error()
            break