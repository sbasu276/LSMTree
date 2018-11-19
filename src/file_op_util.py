import sys, os, time
from lsm_node import LsmNode

PAGE_SIZE = 4096 #Bytes
KEY_VALUE_SIZE = 1024 #Bytes

#Set this based on your machine
BLOCK_SIZE_BYTES = 4096 #Should be at the granularity of PAGE_SIZE
BLOCK_SIZE_KEYS = int(BLOCK_SIZE_BYTES/KEY_VALUE_SIZE) #Should be at the granularity of PAGE_SIZE

C1_FILE = 'C1.txt' #Main SSTable file name

index_table = []

def make_string(key, value):
    line = str(key)+','+str(value)+"\n"
    """
    if(len(line) < KEY_VALUE_SIZE):
        i = KEY_VALUE_SIZE - len(line)
        while(i > 0):
            line += ' '
            i -= 1
    """
    return line

def init_c1_file():
    my_dict = {}
    names_fd = open('names.txt', 'r+')
    c1_fd = open('C1.txt', 'w')
    for line in names_fd.readlines():
        line = line.strip('\n').split(' ')
        my_dict[line[0]] = line[1]    

    for key in sorted(my_dict):
        line = make_string(key, my_dict[key])
        c1_fd.write(line)
    c1_fd.close()

def gen_index_table(fd):
    global index_table 
    index_table= []
    offset = 0
    fd.seek(0, 0)
    file_size = os.path.getsize(fd.name)
    while (offset < file_size):
        line = fd.readline()
        items = line.strip('\n ').split(',')
        index_table.append((items[0], offset))
        offset += BLOCK_SIZE_BYTES
        fd.seek(offset, 0)

def get_key_it(a):
    return a[0]

def get_key_sst(a):
    return a.strip('\n ').split(',')[0]

def binarySearch(arr, l, r, x, extract_key): 
    while l <= r: 
        mid = int(l + (r - l)/2);
        arr_mid = extract_key(arr[mid])
        if arr_mid == x: 
            return mid 
        elif arr_mid < x: 
            l = mid + 1
        else: 
            r = mid - 1
    return r

def islice(fd, offset, numLines):
    lines = []
    fd.seek(offset, 0)
    for i in range(numLines):
        line = fd.readline().strip('\n ')
        if line:
            lines.append(line)
    return lines

def get(fd, key):
    r = binarySearch(index_table, 0, len(index_table)-1, key, get_key_it)
    search_block = islice(fd, index_table[r][1], BLOCK_SIZE_KEYS)
    r = binarySearch(search_block, 0, len(search_block)-1, key, get_key_sst)
    if(get_key_sst(search_block[r]) == key):
        return search_block[r].strip('\n ').split(',')[1]
    else:
        return None

def merge(c1_fd, c0_list):
#    Generates a new merged file with the same name as c1_fd + .tmp extension\
#    TODO: Replace the .txt file with the .tmp file

    temp_file_name = c1_fd.name.strip('.txt')+'.tmp'
    temp_file = open(temp_file_name, 'w+')
    i = 0
    index = 0
    new_index_table = []
    for line in c1_fd:
        old_entry = LsmNode(line.split(',')[0], line.split(',')[1])
        while(i < len(c0_list)):
            new_entry = c0_list[i]
            if(new_entry.key > old_entry.key):
                temp_file.write(make_string(old_entry.key, old_entry.value))
                index += 1
                break
            elif(new_entry.key < old_entry.key):
                i += 1
                if(new_entry.tombstone == False): #add to file if it is not a delete request
                    temp_file.write(make_string(new_entry.key, new_entry.value))
                    index += 1
            else:    # both keys are equal - Dedup
                i += 1
                if(new_entry.tombstone == False):
                    #print('Deleting '+new_entry.key)
                    break
                else:
                    temp_file.write(make_string(new_entry.key, new_entry.value))
                    index += 1
        if(i == len(c0_list)):
            break

    for line in c1_fd:
        #print('Writing '+line)
        temp_file.write(line)    

    while(i < len(c0_list)):
        new_entry = c0_list[i]
        i += 1
        if(new_entry.tombstone == False):
            temp_file.write(make_string(new_entry.key, new_entry.value))

    temp_file.close()

if __name__ == "__main__":

    init_c1_file() #use to init C1 file

    c1_fd = open('C1.txt', 'r')

    gen_index_table(c1_fd) #generate index table

    #testing presence of all the keys of names.txt in C1.txt
    my_fd = open('names.txt', 'r')
    for line in my_fd.readlines():
        line = line.strip('\n ').split(' ')
        value = get(c1_fd, line[0])
        if(value):
            #print('Key found!')
            continue
        else:
            print('Key '+line[0]+' not found!')
            exit()

    #testing merging
    c1_fd.seek(0, 0)
    c0_list = []
    c0_list.append(Request('Aarav', 'Ruth')) #inserting a new key at the beginning of the file
    c0_list.append(Request('Aman', 'Jain')) #inserting a new entry in the middle
    c0_list.append(Request('Novak', 'Dvic', 'D')) #deleting a non-existent key
    c0_list.append(Request('Tonia', 'Jain', 'D'))    #deleting an existing entry
    c0_list.append(Request('Warren', 'Jain', 'W')) #Adding an entry
    c0_list.append(Request('Zzaid', 'Moph', '')) #Adding an entry at the end of file
    merge(c1_fd, c0_list)

    
