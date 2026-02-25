from lstore.Config import PAGE_SIZE, RECORD_SIZE

class Page:

    def __init__(self, capacity = None): # Nicholas and Sage
        self.num_records = 0
        # This is just a bytearray that is representative of pages raw data
        self.data = bytearray(PAGE_SIZE)
        # Calculates page capacity if capacity is not provided
        self.pin_count = 0 # Iris: number of users currently accessing the page (default is 0)
        # If pin_count > 0, then the page should be locked before merge happens
        if capacity is None:
            self.capacity = PAGE_SIZE // RECORD_SIZE # 512 records for a 4kb page
        else: 
            self.capacity = capacity

    def has_capacity(self): # Nicholas
        # Return booloean value indicating if page is has capacity left for new records
        return self.num_records < self.capacity

    
    def write(self, value): # Sage and Nicholas
        self.pin_count += 1 # Iris: add 1 to pin_count when user is writing to the page
        # Writes a value to next available slot on the page and returns -1 if page is full
        if self.has_capacity():
            # Calculates offset like standard Lstore 
            offset = self.num_records * RECORD_SIZE
            if value is None: 
                value = 0
            # Stores data as a 64-bit integer from offset to end of record as bytes 
            self.data[offset:offset + RECORD_SIZE] = value.to_bytes(RECORD_SIZE, byteorder='big', signed=True)
            self.num_records += 1 # Updates num_records to record the new number of total records
            self.pin_count -= 1 # Iris: reduce pin_count by 1 once write is done
            return offset 
        else:
            self.pin_count -= 1 # Iris: reduce pin_count by 1 when function terminates
            # Indicates that the page is full and a new page needs to be created
            return -1
            
    def read(self, offset): # Sage
        # Read a value from the page at the given offset.
        # Returns the integer value stored at that offset.
        self.pin_count += 1 # Iris: add 1 to pin_count when a value is being updated
        value_bytes = self.data[offset:offset + RECORD_SIZE] # Grabs the data from the offset to the end of the record 
        value = int.from_bytes(value_bytes, byteorder = 'big', signed=True) # Changes value_bytes to ints and returns them 
        self.pin_count -= 1 # Iris: once update is done, reduce pin_count by 1
        return value 

    def update(self, offset, value): # Sage
        # Updates a value at a specific offset in the page
        self.pin_count += 1 # Iris: add 1 to pin_count when a value is being updated
        if value is None: 
            value = 0 
        self.data[offset:offset + RECORD_SIZE] = value.to_bytes(RECORD_SIZE, byteorder='big', signed=True)
        self.pin_count -= 1 # Iris: once update is done, reduce pin_count by 1


    def get_num_records(self): # Sage
        # Returns the number of records stored in page
        return self.num_records

