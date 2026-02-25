from lstore.index import Index
from lstore.page import Page
from lstore.bufferpool import BufferPool
from time import time

# Layout of columns in metadata: [0] indirection, [1] rid, [2] timestamp, [3] schema encoding
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid 
        self.key = key 
        self.indirection = None #set to None 
        self.schema_encoding = 0 
        self.start_time = None # setted later in insert and update 
        self.last_updated_time = None #set in update
        self.columns = columns
        

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, loading = False, db_path="./ECS165"):
        self.name = name
        #self.bufferpool = Bufferpool(capacity = 100) # Iris: sets up bufferpool
        #self.pagekey = list(range(100)) # Iris: page keys in this bufferpool will just be integers 
        # Here's how i interpret it, pls lmk if this is wrong:
        # bufferpool.pool{pagekey, value} with value being the page offset in table 
        # so pool looks like {0:somepage, 1:empty, ..., 100:somepage100}
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # dictionary to store data and offset under RIDS
        self.index = Index(self)
        self.merge_threshold_pages = 10  # The threshold to trigger a merge: M2 
        self.rid = 0
        self.base_pages = []
        self.total_columns = 4 + num_columns 
        self.tail_pages = []
        self.cur_tail_range_index = -1 # the greater range index for base pages
        self.cur_base_range_index = -1 # the greater range index for base pages

        self.bufferpool = BufferPool(capacity=50, path=db_path)#set bufferpool
    
    
    def get_page(self, page_type, idx, col):#Sage: helper function to get page from bufferpool
        return self.bufferpool.get_page(self.name, page_type, idx, col)
        
    def put_page(self, page_type, idx, col, page):#Sage: helper function to put page into bufferpool
        self.bufferpool.put_page(self.name, page_type, idx, col, page)
        
    def insert(self, values): # Nicholas & Sage 
        if len(values) == self.num_columns:#check 
            
            if not self.get_page('base', self.cur_base_range_index, 0).has_capacity():#Sge new bufferpool check capacity
                self.new_base_page_range()
            
            rid = self.rid # set rid for insert
            self.rid += 1 # increase rid by one to indicate new rid
                
                    
            # Insert the value into each column's page
            all_columns = [0, rid, int(time()), 0] + list(values) # this is the all column which stores [indirection, RID, time made, schema encoding] 
            offset = None # reset offset 
            for col, value in enumerate(all_columns):#iterate though each part of all columns and stores value and METADATA in col FIXED FOR BUFFERPOOL
                page = self.get_page('base', self.cur_base_range_index, col)#set page to be stored
                offset = page.write(value)#write the value to the page
                self.put_page('base', self.cur_base_range_index, col, page)#put the new page into bufferpool
            #store the range index and the offset to the page directory 
            self.page_directory[rid] = ('base', self.cur_base_range_index, offset)
            return rid              
            
        else:
            return False

    
    def update(self, rid, values): # Sage 
        if rid not in self.page_directory:#check if its not in the page directory 
            return False

        #get base range index and base offset from the page directory and page type M2
        page_type, base_range_index, base_offset = self.page_directory[rid]
        #Sage: new bufferpool implimentation 
        base_direction_page = self.get_page('base', base_range_index, INDIRECTION_COLUMN)#grab pages indirection page
        old_indirection = base_direction_page.read(base_offset)#set old indirection 
        
        #get the current record via the RID
        current_record = self.get_record(rid)
        #store a copy of current record into tail columns 
        tail_columns = current_record.columns.copy()
        for i, val in enumerate(values):
            if val is not None:
                tail_columns[i] = val
        # schema encoding calculation
        schema_encoding = 0
        for i, val in enumerate(values):
            if val is not None:
                schema_encoding += (1 << i)# reads the value of i as a bit map
        
        #create new tail RID 
        tail_rid = self.rid
        self.rid += 1

        tail_range_idx = self.get_current_tail_pages()#grab current tail pages
        all_columns = [old_indirection, tail_rid, int(time()), schema_encoding] + tail_columns#set all columns for iterations

        tail_offset = None#set tail offset 
        for col, value in enumerate(all_columns):#iterate over all columns 
            page = self.get_page('tail', tail_range_idx, col)#grab page
            tail_offset = page.write(value)#write page offset 
            self.put_page('tail', tail_range_idx, col, page)#put page back into bufferpool
            
        self.page_directory[tail_rid] = ('tail', tail_range_idx, tail_offset)#set the tail rid 

        # update base indirection to point to new tail
        base_direction_page.update(base_offset, tail_rid)
        self.put_page('base', base_range_index, INDIRECTION_COLUMN, base_direction_page)
        
        return True
        

        
    def delete(self, rid): # Nicholas
        
        #deletes base page as well as tail pages of record associated with rid
        #FIX BY Sage to include support for tail files and Btree Indexing AND NOW INCLUDES BUFFERPOOL M2
        if rid in self.page_directory:#check if the RID exists in page directory
            page_type, range_index, offset = self.page_directory[rid]#grab the three criteria as normal 
            
            for col in range(self.total_columns):#iterate over total columns 
                page = self.get_page('base', range_index, col)#grab page 
                page.update(offset, None)#update it to none to delete it 
                self.put_page('base', range_index, col, page)#put deleted apge back into directory 

            del self.page_directory[rid]#delete rid from page directory 
            return True
           
        else:
            print("RID Not Found in Page Directory")#not found in page directory 
            return False


    def new_base_page_range(self):# Sage fixeed for bufferpool
        new_idx = self.cur_base_range_index + 1#make index
        self.cur_base_range_index = new_idx#set index
        for col in range(self.total_columns):
            self.put_page('base', new_idx, col, Page(capacity=512))#put pages in bufferpool

    def new_tail_page_range(self):# Sage fixxed for bufferpool 
        new_idx = self.cur_tail_range_index + 1#make a new index 
        self.cur_tail_range_index = new_idx#set the new index
        for col in range(self.total_columns):
            self.put_page('tail', new_idx, col, Page(capacity=512))#put pages into bufferpool

    def get_current_tail_pages(self):# Sage 
        #get current tail page range and create if needed
        if self.cur_tail_range_index < 0 :# if this is the first page
            self.new_tail_page_range()# make new page range
        if not self.get_page('tail', self.cur_tail_range_index, 0).has_capacity():#Sage New bufferpool implimentation 
            self.new_tail_page_range()#return the page range 
        return self.cur_tail_range_index#return the index of the page range

    def get_record(self, rid, page_version = None):# Sage
        # Grabs a record from using its RID. If page is less than 0 then we grab a tail record instead.
        if rid in self.page_directory:#in the page directory 
            page_type, base_range_index, base_offset = self.page_directory[rid]# set the index and offset simultaniously via RID
            
            #pagekey = self.get_pagekey(base_offset) # Iris: inserts page into bufferpool, also checks in page is in bufferpool already, returns pagekey

            indirection = self.get_page('base', base_range_index, INDIRECTION_COLUMN).read(base_offset)
            columns = []
            
            for col in range(4,self.total_columns): # iterate through each column. Change 4 to METADATA COLUMN 
                value = self.get_page('base', base_range_index, col).read(base_offset)#new bufferpool setting and getting 
                columns.append(value)
            if indirection != 0: #If version of record is requested and record has tail pages then we apply tail updates.
                columns = self.tail_update(columns, indirection, page_version)#take all the columns and the in direction to update tail
            key = columns[self.key] 
            # Creates record and its indirection then returns full record
            record = Record(rid, key, columns)
            record.indirection = indirection 
            return record #return the full record
        else:
            return None#not in the page directory
            
    def tail_update(self, base_columns, tail_rid, version= None):# sage tail update and partial merge because select versions requires a tail update? 
        #updates the tail pages used in get record
        #follows tail pages and indirection pointers to get a spesific version 
        #not full merge as it does not change tail records and does not modify pysical storage
        # THIS WILL NEED TO BE CHANGED FOR A FULL MERGE IMPLEMENTATION!!!!!
        if tail_rid == 0 or tail_rid not in self.page_directory:#checks if the rid exists and is not 0 
            return base_columns
    
        # Build the tail chain to understand versions
        tail_chain = []
        current_tail = tail_rid# set current tail 
        while current_tail != 0 and current_tail in self.page_directory:# if it exists 
            tail_chain.append(current_tail)#append to the chain 
            tail_type, tail_range_index, tail_offset = self.page_directory[current_tail]#get range index and offset 

            current_tail = self.get_page('tail', tail_range_index, INDIRECTION_COLUMN).read(tail_offset)
            
        # Determine how many tails to apply based on version
        if version == 0 or version == None:
            # Apply all tails
            num_apply = len(tail_chain)
        else:
            num_skip = abs(version)
            num_apply = max(0, len(tail_chain) - num_skip)
        
        merged_columns = base_columns.copy()#Start with base columns
        
        process = tail_chain[::-1][:num_apply]#Apply tails from oldest to newest
        
        for item in process:#iterate through process 
            tail_type, tail_range_index, tail_offset = self.page_directory[item]#grab the range index and the offset 
            schema_encoding = self.get_page('tail', tail_range_index, SCHEMA_ENCODING_COLUMN).read(tail_offset)
            
            # Batch read all columns at once to reduce page reads with small buffer
            tail_columns = []
            for col in range(self.num_columns):
                tail_columns.append(self.get_page('tail', tail_range_index, 4 + col).read(tail_offset))
            
            #Apply updates from this tail record based on schema encoding
            for col in range(self.num_columns):
                if schema_encoding & (1 << col):# check if it was updated
                    merged_columns[col] = tail_columns[col]#set the update into merged columns 
        
        return merged_columns #return all the updates 
    
        
    def get_rid(self, rid): # Sage and Nicholas
        return Record(rid, key, columns) # Grabs record using get_record function above using RID

    def merge(self): #Sage fixed with bufferpool

        merge_rids = []# what rids need merging 
        for rid, location in list(self.page_directory.items()):#iterate throug the list form of the directory 
            page_type, range_index, offset = location#grab all the three normal criteria from the page directory 
            if page_type != 'base':#check if page type is a base page or tail page
                continue
            indirection = self.get_page('base', range_index, INDIRECTION_COLUMN).read(offset)#grab indirection from bufferpool
            if indirection != 0 and indirection in self.page_directory:#if the indirection exissts and is in the directory
                merge_rids.append(rid)#it needs merging 

        for rid in merge_rids:#iterate throug each rid in rids that need merging 
            if rid not in self.page_directory:#if the rid is not in the page directory skip it 
                continue

            page_type, base_range_index, base_offset = self.page_directory[rid]#grab the three normal cirteria 

            base_columns = []#initialize base columns 
            for col in range(4, self.total_columns):#iterate through total columsn 
                base_columns.append(self.get_page('base', base_range_index, col).read(base_offset))#append base columsn into base columsn from bufferpool

            indirection = self.get_page('base', base_range_index, INDIRECTION_COLUMN).read(base_offset)#grab indirectionfrom base 

            merged_columns = self.tail_update(base_columns, indirection, version=0)#apply tail updates int obase columns
            #writes merged values back into base pages
            for idx, value in enumerate(merged_columns):#iterate through merged columsn 
                page = self.get_page('base', base_range_index, idx + 4)#set page by getting it from bufferpool
                page.update(base_offset, value)#run an update
                self.put_page('base', base_range_index, idx + 4, page)#put the updated page back into buffer pool

            current_tail = indirection#set current tail using indirection 
            while current_tail != 0 and current_tail in self.page_directory:#while curent tail exists and is in the tail directory 
                tail_type, tail_range, tail_offset = self.page_directory[current_tail]#grab the three criteria 
                next_tail = self.get_page('tail', tail_range, INDIRECTION_COLUMN).read(tail_offset)#grab the new tail 
                del self.page_directory[current_tail]#delete current tail 
                current_tail = next_tail#set current tail to new tail to delete the whole chain 

            #reset base indirection with no more tails 
            page = self.get_page('base', base_range_index, INDIRECTION_COLUMN)
            page.update(base_offset, 0)
            self.put_page('base', base_range_index, INDIRECTION_COLUMN, page)

        # flush everything
        self.bufferpool.flush_all()
    def close(self):
        # flush all dirty pages to disk on shutdown
        self.bufferpool.flush_all()

