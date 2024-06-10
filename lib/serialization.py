"""
Client reads from csv file with DictReader and serializes the dicts by
transforming them into a string, with the fields separated by FIELD_SEPARATOR = "@|@"

All the serialized dicts that enter in one batch are joined, separated by the ROW_SEPARATOR = "$|$"

To deserialize, first thhee rows have to be splitted by the ROW_SEPARATOR = "$|$".
Then each row has to be splitted by the FIELD_SEPARATOR = "@|@" 
"""

ID_MSG_SEPARATOR = "~|~"
ID_SEPARATOR = '_'
FIELD_SEPARATOR = "@|@"
ROW_SEPARATOR = "$|$"
KEY_VAL_SEPARATOR = "#|#"
VALUES_SEPARATOR = ","
EOF = 'EOF_'
RESULT_SLICE_FIELD = "RESULT_SLICE"
EOF_CLIENT_ID_INDEX = 4
EOF_WORKER_ID_INDEX = 6
NO_ID = ''

def add_id(message, id):
    return id + ID_MSG_SEPARATOR + message

def split_message_info(message):
    message = Message(message).decode()
    return message.split(ID_MSG_SEPARATOR)

def serialize_batch(batch):
    return [serialize_dict(filtered_dictionary) for filtered_dictionary in batch]

def serialize_message(message_items, id=NO_ID):
    """
    Serialize a message (list of items) by adding a separator
    on each item and deleting the newline character
    """
    message = ROW_SEPARATOR.join(message_items)
    message = add_id(message, id)
    return message

def deserialize_titles_message(bytes):
    # Get the id and the message separated 
    client_id, message = split_message_info(bytes)
     
    return client_id, [deserialize_into_titles_dict(row) for row in message.split(ROW_SEPARATOR)] 

def serialize_dict(dict_to_serialize):
    msg = ''
    for key, value in dict_to_serialize.items():
        if isinstance(value, set):
            value = serialize_set(value)
        elif isinstance(value, list):
            value = serialize_list(value)

        msg += key + KEY_VAL_SEPARATOR + value + FIELD_SEPARATOR
        
    return msg[:-len(FIELD_SEPARATOR)]

def serialize_list(list_to_serialize):
    return VALUES_SEPARATOR.join(map(str,list_to_serialize))

def serialize_set(set_to_serialize):
    serialized_set = ''
    for element in set_to_serialize:
        serialized_set += element + VALUES_SEPARATOR
    
    return serialized_set[:-len(VALUES_SEPARATOR)]

def deserialize_into_titles_dict(row):
    splitted_row = row.split(FIELD_SEPARATOR)
    title_dict = {}
    for field in splitted_row:
        try:
            key, value = field.split(KEY_VAL_SEPARATOR, 1)
            title_dict[key] = value
        except Exception as e:
            print(f'El error es: {e} con la row: {row}')
            raise e

    return title_dict

def is_EOF(msg_bytes):
    return msg_bytes[:EOF_CLIENT_ID_INDEX] == bytes(EOF, 'utf-8')

def get_EOF_client_id(bytes):
    return str(bytes.decode('utf-8')[EOF_CLIENT_ID_INDEX])

def get_EOF_worker_id(bytes):
    return str(bytes.decode('utf-8')[EOF_WORKER_ID_INDEX])

def create_EOF(client_id, worker_id):
    return EOF + client_id + ID_SEPARATOR + worker_id

def hash_title(s):                                                                                                                                
    hash = 5381
    for x in s:
        hash = (( hash << 5) + hash) + ord(x)
    return hash & 0xFFFFFFFF

def create_queue_name(queue, client_id):
    return queue + ID_SEPARATOR + client_id

def create_log_file_name(log, worker_id):
    log_file_name = log + ID_SEPARATOR + '.tx'
    return 


class Message:

    def __init__(self, msg = ""):
        self.msg = msg
        self.end_flag = False

    def push(self, msg):
        if self.end_flag:
            raise Exception("Message already ended")
        self.msg += msg

    def set_end_flag(self):
        self.end_flag = True
    
    def is_ended(self):
        return self.end_flag

    def get_message(self):
        return self.msg
    
    def encode(self):
        return self.msg.encode('utf-8')
    
    def decode(self):
        try:
            msg = self.msg.decode('utf-8')
        except:
            msg = self.msg
        return msg

    def __str__(self):
        return self.msg
    
    def clean(self):
        self.msg= ""

    def add_id(self, id):
        self.msg = add_id(self.msg, id)
