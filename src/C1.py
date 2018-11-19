import sys, os
from file_op_util import *

PAGE_SIZE = 4096 #Bytes
KEY_VALUE_SIZE = 16 #Bytes

#Set this based on your machine
BLOCK_SIZE_BYTES = 128 #Should be at the granularity of PAGE_SIZE
BLOCK_SIZE_KEYS = int(BLOCK_SIZE_BYTES/KEY_VALUE_SIZE) #Should be at the granularity of PAGE_SIZE

class C1:
    def __init__(self, c1_file):
        self.index_table = []
        self.c1_file = c1_file
        self.swap_file = c1_file+'.swp'
        self.swap_table = []
        self.swap_offset = 0


    def gen_index_table(self):
        """ Initialises the Index Table    """
        self.index_table = []
        fd = open(self.c1_file, 'r')
        offset = 0
        file_size = os.path.getsize(fd.name)
        while (offset < file_size):
            fd.seek(offset, 0)
            line = fd.readline()
            items = line.strip('\n ').split(' ')
            self.index_table.append((items[0], offset))
            offset += BLOCK_SIZE_BYTES
        fd.close()


    def get(self, key):
        fd = open(self.c1_file, 'r')
        r = binarySearch(self.index_table, 0, len(self.index_table)-1, key, get_key_it)
        search_block = islice(fd, self.index_table[r][1], BLOCK_SIZE_KEYS)
        r = binarySearch(search_block, 0, len(search_block)-1, key, get_key_sst)
        fd.close()
        if(get_key_sst(search_block[r]) == key):
            return search_block[r].strip('\n ').split(' ')[1]
        else:
            return None


    def __update(self, fd, entries):
        for entry in entries:
            if(self.swap_offset%BLOCK_SIZE_BYTES == 0):
                self.swap_table.append((entry.key, self.swap_offset))
            fd.write(make_string(entry.key, entry.value))
            self.swap_offset += KEY_VALUE_SIZE


    def __flush_and_update_itable(self, fd, entries, forced=False):
        if(forced == True):
            self.__update(fd, entries)
            return []
        else:
            self.__update(fd, entries[:BLOCK_SIZE_KEYS])
            return entries[BLOCK_SIZE_KEYS:]


    def merge(self, c0, isFile=True):
        if(isFile == False):
            self.__merge(c0)
        else:
            for line in c0:
                line = line.split(',')
                c0_list.append(LsmNode(line[0], line[1], line[3]))
            self.__merge(c0_list)

    def __merge(self, c0_list):
        i = 0
        entries = []
        c1_fd = open(self.c1_file, "r")
        swap_fd = open(self.swap_file, 'w+')
        swap_table = []

        for line in c1_fd:
            old_entry = LsmNode(line.split(' ')[0], line.split(' ')[1])
            while(i < len(c0_list)):
                new_entry = c0_list[i]
                if(new_entry.key > old_entry.key):
                    entries.append(old_entry)
                    break
                elif(new_entry.key < old_entry.key):
                    i += 1
                    if(new_entry.tombstone == False): #add to file if it is not a delete request
                        entries.append(new_entry)
                else:    # both keys are equal - Dedup
                    i += 1
                    if(new_entry.tombstone == False):
                        entries.append(new_entry)
                    break
            if(i == len(c0_list)):
                break
            entries = self.__flush_and_update_itable(swap_fd, entries)
    
        for line in c1_fd:
            #print('Writing '+line)
            old_entry = LsmNode(line.split(' ')[0], line.split(' ')[1])
            entries.append(new_entry)
            enries = self.__flush_and_update_itable(swap_fd, entries)
    
        while(i < len(c0_list)):
            new_entry = c0_list[i]
            i += 1
            if(new_entry.tombstone == False):
                entries.append(new_entry)

        #Final flushing
        entries = self.__flush_and_update_itable(swap_fd, entries, True)
    
        swap_fd.close()
        c1_fd.close()


if __name__ == "__main__":

    init_c1_file() #use to init C1 file
    c1 = C1("C1.txt")

    c1_fd = open('C1.txt', 'r')

    c1.gen_index_table() #generate index table

    #testing presence of all the keys of names.txt in C1.txt
    my_fd = open('names.txt', 'r')
    for line in my_fd.readlines():
        line = line.strip('\n ').split(' ')
        value = c1.get(line[0])
        if(value):
            #print('Key found!')
            continue
        else:
            print('Key '+line[0]+' not found!')
            exit()

    #testing merging
    c1_fd.seek(0, 0)
    c0_list = []
    c0_list.append(LsmNode('Aarav', 'Ruth')) #inserting a new key at the beginning of the file
    c0_list.append(LsmNode('Aman', 'Jain')) #inserting a new entry in the middle
    c0_list.append(LsmNode('Novak', 'Dvic', True)) #deleting a non-existent key
    c0_list.append(LsmNode('Tonia', 'Jain', True))    #deleting an existing entry
    c0_list.append(LsmNode('Warren', 'Jain', False)) #Updating an entry
    c0_list.append(LsmNode('Zzaid', 'Moph', False)) #Adding an entry at the end of file
    c1.merge(c0_list, False)

    print(c1.index_table)

    os.rename(c1.swap_file, c1.c1_file)
    c1.gen_index_table() #generate index table
    print(c1.index_table)
    print(c1.swap_table)
