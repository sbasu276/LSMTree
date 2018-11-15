'''
CSE 511: Lab 1 : LRU Cache

Cache and File System Implementation

The key-value pairs will be stored within two levels of memory hierarchy:
(i) an in-memory cache following a least-recently used (LRU) replacement policy which holds
the most recently accessed key-value pairs and 
(ii) a persistent store (based on a magnetic or flash drive) which will hold the entire
database of key-value pairs.
You may implement the persistent store in whatever way you deem appropriate,
e.g., using a single file, using multiple files, or using some pre-existing database.
'''


##########################################  CIRCULAR D-LINKED LIST  #####################################################



class Node(object):

    def __init__(self, value, prev=None, next=None):
        self.value = value
        self.prev = prev
        self.next = next

class CircularDoublyLinkedList(object):
    
    def __init__(self, value=None):
        if value == None:
            self.root = None
            self.length=0
        else:
            self.root = Node(value)
            self.root.prev = self.root
            self.root.next = self.root
            self.length=1


    def get_first(self):
        return self.root

    def get_last(self):
        return self.root.prev

    def insert_at_beginning(self, value):
        
        new = Node(value)
        if self.root == None:
            self.root = new
            self.root.next= new
            self.root.prev= new
            self.length+=1
            print ("Added one item. It is the only item\n")
            return
        
        new.next = self.root
        new.prev = self.root.prev
        self.root.prev.next = new
        self.root.prev = new
        self.root = new
        self.length+=1
        print ("Added one item. It is one of many items\n")
        return

    def insert_at_end(self, value):
        
        new = Node(value)
        if self.root == None:
            self.root = new
            self.root.next= new
            self.root.prev = new
            self.length+=1
            print ("Added one item. It is the only item\n")
            return
        last = self.root.prev
        last.next = new
        new.prev = last
        new.next = self.root
        self.root.prev = new
        self.length+=1
        print ("Added one item. It is one of many items\n")
        return

    def remove_from_beginning(self):
        
        x = self.root.value
        new_root = self.root.next
        new_root.prev = self.root.prev
        self.root.prev.next = new_root
        self.root = new_root
        self.length-=1
        print("{} got removed".format(x))
        return

    def remove_from_end(self):
        
        x = self.root.prev.value
        self.get_last().prev.next = self.root
        self.root.prev = self.get_last().prev
        self.length-=1
        print("{} got removed".format(x))
        return

    def size(self):
       return self.length

    def printQ(self):
        current = self.root
        c=0
        while( c < self.length ):
            print(current.value)
            current = current.next
            c+=1
        return
        
####################################  END OF CIRCULAR D-LINKED LIST  ##################################################

def find_from_storage(First_name):
    with open('names.txt') as f:
        flag=0
        for num,line in enumerate(f, 0):
            if First_name in line:
                linenum=num
                print("Present at line #: {}".format(linenum))
                flag=1
                break
        if flag==0:
            print("Name not present in file!")
            exit()
        with open('names.txt') as f2:
            startpos=f2.read().find(First_name)
            print("Present at character#: {}".format(startpos+linenum))
            f2.seek(startpos+linenum)
            return(f2.readline().replace('\n',''))



#######################################  BEGINNING OF LRU CACHE  #######################################################


class LRUCache(object):
    
    def __init__(self, maxsize=5):
        self.cache_list = CircularDoublyLinkedList()
        self.cache_table = {}
        self.misses = 0
        self.hits = 0
        self.requests = 0
        self.maxsize = maxsize

    def get_size(self):
        
        return self.cache_list.length #Current size of cache

    def reference(self, nodeval):
        #Nodeval is the first name which also serves as the key
        nodeindex = nodeval #Simple Hash Function
        if self.cache_table.get(nodeindex, None) != None:
            # already in cache
            desired_node = self.cache_table[nodeindex]
            self.hits += 1
            self.updateQ(desired_node)# To Do!!! Moves Existing node in List to the front
            print("There was a hit in the cache!!! Your value is now being returned...")
            return desired_node.value
        else:
            # not in cache
            # Fetch from Persistent storage (In our case, a single file)
            FullNodeVal= find_from_storage(nodeval)
                
            desired_node = Node(FullNodeVal)#For now, nodeindex and nodeval will be the same
            self.cache_table[nodeindex] = desired_node
            self.misses += 1
            if self.is_full():
                self.delete_oldest()
            self.updateQ(desired_node)
            print("Oops... There was a miss in the cache!!! However, we have fetched your value from the Persistent storage and it is now being returned...")
            return desired_node.value
    
    def delete_oldest(self):
        oldest = self.cache_list.root.prev
        oldest.prev.next = self.cache_list.root
        self.cache_list.root.prev = oldest.prev
        self.cache_list.length-=1
        oldest = None
        

    def is_full(self):
       
        if self.get_size() >= self.maxsize:
            return True
        else:
            return False

    def updateQ(self, recent_node):
        
        if self.cache_list.root == None:
            # list is empty
            recent_node.prev = recent_node
            recent_node.next = recent_node
            self.cache_list.root = recent_node
            self.cache_list.length+=1
        elif recent_node.next != None and recent_node.prev != None:
            # node is already in list somewhere
            self.remove_from_middle(recent_node)
            self.add_to_head(recent_node)
        else:
            # node not in list
            self.add_to_head(recent_node)

    def add_to_head(self, recent_node):
        old_root = self.cache_list.root
        tail = old_root.prev
        tail.next = recent_node
        recent_node.prev = tail
        old_root.prev = recent_node
        recent_node.next = old_root
        self.cache_list.root = recent_node#Finished adding recent_node to the head
        self.cache_list.length+=1

    def remove_from_middle(self, recent_node):
        recent_node.next.prev = recent_node.prev
        recent_node.prev.next = recent_node.next
        recent_node.next=recent_node
        recent_node.prev=recent_node #Finished removing recent_node temporarily (Will be added to the head)
        self.cache_list.length-=1

    '''
    def make_hashed_func_object(self, func, *args, **kwargs):
        """
        >>> x = LRUCache()
        >>> foo = lambda x, y: x + y
        >>> hashed_fun = x.make_hashed_func_object(foo, (1, 2))
        >>> hashed_fun == hash(repr([foo, ((1, 2),), {}]))
        True
        """
        return hash(repr([func, args, kwargs]))
   '''
    def get_first(self):
        
        return self.cache_list.root

    def get_last(self):
       
        return self.cache_list.root.previous

    def print_cache_contents(self):
        
        l = []
        item = self.cache_list.root
        if item == None:
            print("Nothing in the cache at the moment")
            return
        
        while item.next != self.cache_list.root:
            l.append(item.value)
            item = item.next
        l.append(item.value)
        print("Here are the cache contents in list form: {}".format(l))
        return 
 
##############################################  END OF LRU CACHE  #########################################################
