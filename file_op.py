import sys, os

PAGE_SIZE = 4096 #Bytes
KEY_VALUE_SIZE = 16 #Bytes
BLOCK_SIZE_BYTES = 128 #Should be at the granularity of PAGE_SIZE
BLOCK_SIZE_KEYS = int(BLOCK_SIZE_BYTES/KEY_VALUE_SIZE) #Should be at the granularity of PAGE_SIZE

C1_FILE = 'C1.txt' #Main SSTable file name
C0_FILE = 'C0.txt' #C0 component

index_table = []

def init_c1_file():
	my_dict = {}
	names_fd = open('names.txt', 'r+')
	c1_fd = open('C1.txt', 'w')
	for line in names_fd.readlines():
		line = line.strip('\n').split(' ')
		my_dict[line[0]] = line[1]	

	for key in sorted(my_dict):
		line = key+' '+my_dict[key]
		if(len(line) < 16):
			i = 15 - len(line)
			while(i > 0):
				line += ' '
				i -= 1
			c1_fd.write(line+'\n')
	c1_fd.close()

def init_index_table():
	global index_table 
	index_table= []
	offset = 0
	file_size = os.path.getsize(C1_FILE)
	c1_file = open(C1_FILE, "r+")
	while (offset < file_size):
		line = c1_file.readline()
		items = line.strip('\n').split(' ')
		index_table.append((items[0], offset))
		offset += BLOCK_SIZE_BYTES
		c1_file.seek(offset, 0)

def get_key_it(a):
	return a[0]

def get_key_sst(a):
	return a.strip('\n').split(' ')[0]

def binarySearch(arr, l, r, x, extract_key): 
	while l <= r: 
		mid = int(l + (r - l)/2);
		print(mid, l, r)
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
	print(index_table[r])
	search_block = islice(fd, index_table[r][1], BLOCK_SIZE_KEYS)
	print(search_block)
	r = binarySearch(search_block, 0, len(search_block)-1, key, get_key_sst)
	if(get_key_sst(search_block[r]) == key):
		print("Key found!!")
	else:
		print("Key NOT found!!")

def merge():
	print('')
#    while i < len(L) and j < len(R):
#        
#        if L[i][0] < R[j][0]:
#            arr.insert( k, L[i] )
#            i+=1
#            k+=1
#        else:
#            arr.insert( k, R[j] )
#            j+=1
#            k+=1
#
#    # Checking if any element is left
#    while i < len(L):
#        arr.insert( k, L[i] )
#        i+=1
#        k+=1
#    while j < len(R):
#        arr.insert( k, R[j] )
#        j+=1
#        k+=1
#
#
#    L_fd.close()
#    R_fd.close()
#    return arr
#
##Open files
#
#dest_fd=open('dest.txt','w')
#
#
##Merged tuple list
#fname1='SST_1029.txt'
#fname2='SST_1090.txt'
#dest=merge(fname1,fname2)
#print("DEST:",dest)
#
##Converting to required format for file
#final_list=format_into_list_of_strings(dest)
#print(final_list)
#
##Writing line by line to the destination file
#dest_fd.writelines(final_list)
#
##Closing files
#
#dest_fd.close()

if __name__ == "__main__":
	init_c1_file()
	init_index_table()
	c1_fd = open('C1.txt', 'r')
	get(c1_fd, 'Rafael')
	get(c1_fd, 'zzz')
