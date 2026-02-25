from lstore.page import Page
import os
import io
from collections import OrderedDict#optimization for dictionary method for speed


# Implementation Idea:
# - To write something onto a page
# - Code some disk management function that gets the page from a physical file
# - Insert into bufferpool
# - Then page.py write data onto page
# - after merge and stuff we need to write it back to the disk (page is dirty)
# - We need file management to update the physical location of the page (file)

class DiskManager(): # Iris
    def __init__(self, path):
        self.path = path # File path
        self.keys = [] # Keep track of a list of keys that's in the drive

    # This class should help with the transition of a page from disk (physical file) to the bufferpool (RAM)

    def write_page(self, table_name, page_type, r_idx, col, page): # Iris
        # Creates a new file with the inputted information, this input is also the key of the page
        key = table_name + "/" + page_type + "/range_" + str(r_idx) + "/col_" + str(col) + ".bin"
        if (table_name, page_type, r_idx, col) not in self.keys:
            self.keys.append((table_name, page_type, r_idx, col)) # Appends the key into a list so it can be used in bufferpool later
        file = self.path + "/" + key
        os.makedirs(os.path.dirname(file), exist_ok=True)
        file_open = io.open(file, 'wb') # Opens a file (page) prepares to write it
        file_open.write(page.data) # We input the page (from Page.py) into write_page so we can write the data (that should be written in page.py) into the disk
        file_open.close() # Once the updated data is written back into the disk, close the file

        # Note: if file path doesn't exist, it writes a new file at that path (new page)

    def get_page(self, table_name, page_type, r_idx, col): # Iris
        key = table_name + "/" + page_type + "/range_" + str(r_idx) + "/col_" + str(col) + ".bin"
        if (table_name, page_type, r_idx, col) not in self.keys:
            self.keys.append((table_name, page_type, r_idx, col)) # appends the key into a list so it can be used in bufferpool later
        file = self.path + "/" + key
        if not os.path.exists(file):
            # If the file path does not exist, then return none
            return None
        page = Page(capacity = 512) # Set standard 2 bytes capacity
        file_open = io.open(file, 'rb') # Opens a file (page) prepares it for read 
        page.data = bytearray(file_open.read()) # Specified bytes
        file_open.close() # Once page is read, file is closed, but the page is now in the buffer pool
        return page
        

class BufferPool():
    def __init__(self, capacity=100, path = None):
        self.disk_manager = DiskManager(path) # Iris: initializes diskmanager so we can pull pages into bufferpool
        # Initializes buffer pool and sets capacity for it
        # Enable LRU eviction with oldest entry at the front
        self.pool = OrderedDict() # Key calls to the page (value) of pool --> also acts as a key to the page for storage
        # Key template: table_name/page_type/rangeindex/column
        self.buffer_capacity = capacity
        self.dirty = set()


        
        '''
        SAGE: CHECK NEW IMPLEMENTATION OF HELPER FUNCTIONS  
        '''
    def get_page(self, table_name, page_type, r_idx, col): # Sage get page standerdized implimentation 
        key = (table_name, page_type, r_idx, col) # Set key to conditions in get page
        if key in self.pool: # Check if key is in pool to skip checking disk too ie slightly faster
            self.pool.move_to_end(key)
            return self.pool[key] # Because we found it in disk 
        # Not in pool, try disk
        page = self.disk_manager.get_page(table_name, page_type, r_idx, col) # Try get page function in disk manager 
        if page is None: # Not there
            return None
        self.buffer_insert(key, page) # Run an insert on the key and page to bufferpool
        return page
        
    def put_page(self, table_name, page_type, r_idx, col, page): # Put page in bufferpool if it exists
        key = (table_name, page_type, r_idx, col) # Grab key name 
        if key in self.pool: # Check key in pool
            self.pool[key] = page
            self.pool.move_to_end(key) # Move to end method in ordered dict for optimization of dictionary 
        else:
            self.buffer_insert(key, page) # Insert it if it is not in pool
        self.mark_dirty(key) # Mark the page dirty 

    # Flushes all the pages to disk 
    def flush_all(self):
        # Write all dirty pages back to disk (call on shutdown or after merge)
        for key in list(self.dirty):
            if key in self.pool:
                self.disk_manager.write_page(*key, self.pool[key])
        self.dirty.clear()

    def buffer_insert(self, key, value):  # Nicholas
        # Note from Iris: key is a tuple of (table_name, page_type, r_idx, col)
        if key not in self.pool:  # Checks if requested key is already in buffer pool and only moves forward if key is not in buffer pool
            if self.buffer_at_capacity():  # If bufferpool is at capacity then we must replace our oldest value with a new one
                # Were going to use Least Recently Used for deciding which page to evict from the buffer pool
                oldest_key, oldest_page = self.buffer_order.popitem(last = False)
                if oldest_key in self.dirty:
                    # If the oldest value in the buffer pool is not written to the storage drive then we need to flush it before eviction
                    self.disk_manager.write_page(*oldest_key, oldest_page)# Iris: write the page based off the oldest key the pool
                    self.dirty.discard(oldest_key)
            self.pool[key] = value

        elif key in self.pool: # If requested key is already in the buffer pool then we need to 
            self.pool[key] = value
            self.mark_dirty(key)
            self.pool.move_to_end(key) # Just grabs the value 
            return self.pool[key]

    # buffer_get is used for guaranteeing that we always get a page
    def buffer_get(self, key): # Nicholas and Iris
        # In order to ensure that we always get a page we check the pool (cache) and then the drive if its not in cache
        if key in self.disk_manager.keys and key in self.pool: # Iris: checks if key is in the drive
            # Here we just need to reset the requested pages position in the buffer_order and then return it from the buffer pool
            self.pool.move_to_end(key)
            return self.pool[key]
        else:
            if key not in self.disk_manager.keys:
                # This is just in case the requested page does not exist in the storage drive either
                return None
            # Iris: if key is in drive but not bufferpool, bring it into the bufferpool
            table_name = key[0] # Since key is a tuple, i'm deconstructing it for disk_manager
            page_type = key[1]
            r_idx = key[2]
            col = key[3]
            page = self.disk_manager.get_page(table_name, page_type, r_idx, col)
            self.buffer_insert(key, page)
            return page  # Returns the page that we are trying to access

    def mark_dirty(self, key):
        self.dirty.add(key)  # This tracks whether or not a page has been modified but hasn't been flushed to storage drive

    def buffer_at_capacity(self):  # Nicholas
        return len(
            self.pool) >= self.buffer_capacity  # Checks if there is capacity available in bufferpool and returns true/false

    def is_page_pinned(self, key):  # Iris
        # Checking if the pin count of the page is more than 0, if so then the page is locked from merging, eviction, etc.
        if self.pool[key].pin_count > 0:
            # Assuming self.pool[key] is calling the page??
            return True  # Returns true if page is pinned
        else:
            return False  # Other wise pin_count = 0, so the page is safe to evict, merge, etc.

    def evict_key(self, key):
        if self.is_page_pinned(key):  # Iris: checks if page is pinned before evicting it
            raise Exception("Eviction failed: page is currently being accessed.")
        # Checking if the page is dirty or not is included in buffer_insert, so I won't add it here
        del self.pool[key]  # Deletes key from buffer pool
