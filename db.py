from lstore.table import Table
from lstore.page import Page
from lstore.bufferpool import BufferPool
import os
import json
import io

class Database():

    def __init__(self):
        self.tables = []
        self.path = None
        self.bufferpool = None

    # loads all the table data from disk back into memory so the database can pick up where it left off
    # should load pages into the bufferpool instead of directly into the table
    def open(self, path): # naomi
        self.path = path
        # create bufferpool when database is opened
        self.bufferpool = BufferPool(capacity=100, path=path)

        # create the folder where all our database files will live
        if not os.path.exists(path):
            os.makedirs(path)
        
        # if there's no metadata file, this is a brand new database, so nothing to load
        meta_path = path + '/metadata.json' # builds the full path to where the metadata file would be
        if not os.path.exists(meta_path):
            return

        # read the metadata file which has all the saved table info
        meta_file = io.open(meta_path, 'r')
        meta = json.load(meta_file) # converts the JSON file into a python dict
        meta_file.close()

        # recreate the table object with the same name, columns, and key as before
        for table_data in meta['tables']: # # loop through each table that was saved
            # get all the saved table info for this table
            name = table_data['name']
            num_columns = table_data['num_columns']
            key = table_data['key']
            rid = table_data['rid']
            page_directory = table_data['page_directory']
    
            # recreate the table object with the same info as before
            table = Table(name, num_columns, key, loading = True, db_path=path)
            table.bufferpool = self.bufferpool # give the table access to the shared buffer pool
            
            # restore the rid counter so we dont reuse old rids
            table.rid = rid
            # restore page directory, converting keys back to integers and values back to tuples
            directory = {}
            for k, v in page_directory.items():
                directory[int(k)] = tuple(v) 
            table.page_directory = directory
    
            # restore the indexes so we know which page range is the current one
            if 'cur_base_range_index' in table_data:
                # indexes are saved directly to load
                table.cur_base_range_index = table_data['cur_base_range_index']
                table.cur_tail_range_index = table_data['cur_tail_range_index']
            else:
                #for older formats, indexes not saved, look at length of the records list
                table.cur_base_range_index = len(table_data.get('base_num_records', [])) - 1
                table.cur_tail_range_index = len(table_data.get('tail_num_records', [])) - 1

            if table.cur_base_range_index < 0:
                table.new_base_page_range()

            table.index.needs_rebuild = True#marks index as need to rebuild
            self.tables.append(table)#append the table to tables

            

    def close(self): #naomi
        # if no path is set, nothing to save
        if not self.path:
            return
        # if bufferpool was never created, nothing to save
        if self.bufferpool is None:
            return
        # this will hold all the info we need to save for every table
        meta = {'tables': []}

        for table in self.tables:
            # all dirty pages get writen to disk
            table.bufferpool.flush_all()

            # save num_records for every page for when you reload the pages from disk in open
            base_num_records = self.save_num_records(table, 'base')
            tail_num_records = self.save_num_records(table, 'tail')

            # save everything to rebuild the table later in open function
            table_data = {
                'name': table.name,
                'num_columns': table.num_columns,
                'key': table.key,
                'rid': table.rid,  # save rid so we dont reuse old rids
                'cur_base_range_index': table.cur_base_range_index,
                'cur_tail_range_index': table.cur_tail_range_index,
                'base_num_records': base_num_records,
                'tail_num_records': tail_num_records,
            }
            
            # convert page directory keys to strings because json requires string keys
            directory = {}
            for k, v in table.page_directory.items():
                directory[str(k)] = list(v)
            table_data['page_directory'] = directory

            meta['tables'].append(table_data)
 
        # write metadata to a file so we can load it back later in open
        meta_path = self.path + '/metadata.json'
        meta_file = io.open(meta_path, 'w')
        json.dump(meta, meta_file) # converts Python data structures into the standardized JSON format
        meta_file.close()    

    
        
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):#Sage fixes for extended M2
        
        if self.bufferpool is None:#check if bufferpool does not exist
            if self.path is None:
                self.path = './ECS165'#set path if it somehow does not exist
            os.makedirs(self.path, exist_ok=True)
            self.bufferpool = BufferPool(capacity=100, path=self.path)#make bufferpool becasue it didnt exist before
        #bufferpool now for sure exists 
        self.tables = [table for table in self.tables if table.name != name]#set tables to each table in tables not named the name spesified: holy  what a sentence 
        table = Table(name, num_columns, key_index, loading=True, db_path=self.path)#make a table with all data 
        table.bufferpool = self.bufferpool#set the tables bufferpool to the known bufferpool 
        table.new_base_page_range()#allocate new base range
        self.tables.append(table)#append table 
        return table
    
    """
    # Deletes the specified table
    """
    def drop_table(self, name): # naomi
        # loop through tables and remove the one with the matching name
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)
                return

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name): # naomi
        # search through tables in reverse order (most recently added first)
        for table in reversed(self.tables):
            if table.name == name:
                return table
        return None



    def save_num_records(self, table, page_type):
        num_records_list = []#will hold num_records for every range and column
    
        #figure out how many ranges exist for this page type
        if page_type == 'base':
            num_ranges = table.cur_base_range_index + 1#index starts at 0
        else:
            num_ranges = table.cur_tail_range_index + 1
    
        for range_index in range(num_ranges):
            range_num_records = []#num_records for each column in this range
    
            for col in range(table.total_columns):
                #grab the page from the bufferpool
                page = table.bufferpool.get_page(table.name, page_type, range_index, col)
    
                if page is not None:
                    #write the page to disk using the disk manager
                    table.bufferpool.disk_manager.write_page(
                        table.name, page_type, range_index, col, page
                    )
                    range_num_records.append(page.num_records)#save how full this page was
                else:
                    range_num_records.append(0)#page didn't exist, record 0
    
            num_records_list.append(range_num_records)
    
        return num_records_list
