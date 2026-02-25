from BTrees.OOBTree import OOBTree #Import that allows us to use the btree
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        #Alvin: This makes primary key column into a Btree for indexing
        self.table = table
        self.indices[table.key] = OOBTree()
        self.needs_rebuild = False  # Flag for lazy index rebuilding

    #Alvin: adding an insert function for the b-tree that appends values instead of replaces, this way keys (column values) can refer to multiple values (multiple RIDS)
    def insert_btree(self, column, key, value):
        if self.indices[column] is None:
            return 
        btree = self.indices[column]
        if key in btree:# sage fix to prevent bugs from privious version 
            btree[key].append(value)
        else:
            btree[key] = [value]

    #Alvin: NEW Function for deleting RID in a column (usually for removal of outdated recors)
    def delete_rid(self, column, valueInCol, RIDtoDelete):
        if self.indices[column] is not None:
            colIndex = self.indices[column]
            RIDOutput = colIndex[valueInCol]
            RIDOutput.remove(RIDtoDelete)
            if len(RIDOutput) == 0:
                del colIndex[valueInCol]
    """
    # returns the location of all records with the given value on column "column"
    """
    #Alvin: Example of how it works
    #locate(2, 100) for example, go to column two, and grab all records who have a the value 100 
    #Make sure to get the RID -> self.indices[column][value] = [rid1, rid2, rid3...]
    def locate(self, column, value):# Sage: bug fixes from VS code 
        if self.needs_rebuild:#Sage: fix for optimization 
            self.rebuild_indices()# calls the rebuild function 
        if self.indices[column] is None:#Sage: optimized and cleaned to implement MS extended cases added checking if the index was not defined
            matching_rids = []#initialize matching rids 
            for base_rid, location in self.table.page_directory.items():#grab base rid and location ie offset from page directory 
                page_type, range_index, offset = location# grab page type range index and offset from location/ offset
                if page_type != 'base':#skip tail pages as they do not need locating 
                    continue
                # Read the specific column value
                col_value = self.table.get_page('base', range_index, 4 + column).read(offset)#grab the column value from base pages and get function
                if col_value == value:
                    matching_rids.append(base_rid)#append the rid to matching rids if the valyes match 
            return matching_rids#return the matching rids list
        if value in self.indices[column]:#check value in column
            return self.indices[column][value]  
        else:
            return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    # Alvin: will return all RID (not in order but from records with values closer to begin first then to end)
    def locate_range(self, begin, end, column):
        if self.needs_rebuild:#Sage: efficency rebuild if needed
            self.rebuild_indices()
        if self.indices[column] is None: # sage: check none case to avoid potential errors that did happen
            matching_rids = []
            for base_rid, location in self.table.page_directory.items():
                page_type, range_index, offset = location
                if page_type != 'base':#skips tail records for speeeed
                    continue
                # Read the specific column value
                col_value = self.table.get_page('base', range_index, 4 + column).read(offset)
                if begin <= col_value <= end:#append the range if it is between begining and end
                    matching_rids.append(base_rid)
            return matching_rids
        valueExists = list(self.indices[column].keys(min=begin, max=end))
        RIDList = []
        #removes the lists format so only RID values are inputed into the list
        for value in valueExists:
            RIDList.extend(self.indices[column][value])#add all retrived values to ValidRIDs
        return RIDList
    def rebuild_indices(self):#Sage
        #Rebuild all indices from page_directory (called lazily on first query)
        if not self.needs_rebuild:#edge case check
            return
        
        for base_rid, location in self.table.page_directory.items():#grab location and base rid
            page_type, range_index, offset = location#set the other three 
            if page_type != 'base':#skip tail pages 
                continue
            # Read only indexed columns from base pages
            for col in range(self.table.num_columns):
                if self.indices[col] is not None:
                    value = self.table.get_page('base', range_index, 4 + col).read(offset)#do a rebuild
                    self.insert_btree(col, value, base_rid)
        
        self.needs_rebuild = False#se tthe rebuild to fale as it has been rebuilt

    """
    # optional: Create index on specific column
    """
    #Updated again on 2/21/26 to be able to handle cases where we removed outdated RID records, so we don't reenter then into index 
    #Alvin: Redone for M2 to allow making the index AFTER already having records inserted, will go through every RID
    # (base and tail) and add its column value into the newly created btree
    def create_index(self, column_number):
        if self.needs_rebuild:
            self.rebuild_indices()
        #creates the bTree for that column
        self.indices[column_number] = OOBTree()
        #Index/btree can be created at any point in time for non-primary key columns, so if creating index later, must get all values from before
        #Concept: for all current RID entries, access that specific column number and retrieve + index it to the B-tree
        flatList = [RID for sublist in list(self.indices[self.table.key].values()) for RID in sublist]
        for rid in flatList:
            if rid in self.table.page_directory: #checks to make sure rid is in the page directory
                current_record = self.table.get_record(rid)
                keyForTree = current_record.columns[column_number]
                self.table.index.insert_btree(column_number, keyForTree, rid)
        return True

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        #reset that index back to none
        self.indices[column_number] = None
        return True
