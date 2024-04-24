"""
Client reads from csv file with DictReader and serializes the dicts by
transforming them into a string, with the fields separated by FIELD_SEPARATOR = "@|@"

All the serialized dicts that enter in one batch are joined, separated by the ROW_SEPARATOR = "-|-"

To deserialize, first thhee rows have to be splitted by the ROW_SEPARATOR = "-|-".
Then each row has to be splitted by the FIELD_SEPARATOR = "@|@" 
"""

FIELD_SEPARATOR = "@|@"
ROW_SEPARATOR = "-|-"

def serialize_item(item):
    """
    Serialize a file item by adding a separator 
    and deleting the newline character
    """
    return item.rstrip('\n') + '|'

def serialize_message(message_items):
    """
    Serialize a message (list of items) by adding a separator
    on each item and deleting the newline character
    """
    return ROW_SEPARATOR.join(message_items)
    #return (''.join([serialize_item(item) for item in message_items]))[:-1]

def deserialize_item(item):
    """
    Deserialize a file item by splitting 
    it using the separator
    """
    return item.split(FIELD_SEPARATOR)

def deserialize_titles_message(bytes):
    """
    Deserialize a message (list of items) by splitting
    it using the separator
    """
    message = str(bytes)
    #print(f'[deserialize_titles_message] Me llego el mensaje: {message}')
    return [deserialize_into_titles_dict(row) for row in message.split(ROW_SEPARATOR)]
    #return [deserialize_item(item) for item in message.split(ROW_SEPARATOR)]

def serialize_dict(dict):
    msg = ''
    for value in dict.values():
        msg += value + FIELD_SEPARATOR
    
    return msg[:-len(FIELD_SEPARATOR)]

def deserialize_into_titles_dict(row):
    #print(f"[deserialize_into_titles_dict] Me llego la row: {row}")
    # TODO: See how to make this prettier
    dict_keys = ['title', 'description', 'authors', 'image', 'preview_link', 'publisher', 'published_date', 'info_link', 'categories', 'ratings_count']
    splitted_row = row.split(FIELD_SEPARATOR)
    title_dict = {}
    for i, key in enumerate(dict_keys):
        if i >= len(splitted_row): # If the row has less fields than expected, the resting fields will have a empty string as value
            title_dict[key] = ''
        else:
            title_dict[key] = splitted_row[i]

    return title_dict
    #try: 
    #    title_dict['title'] = splitted_row[0]
    #    title_dict['description'] = splitted_row[1]
    #    title_dict['authors'] = splitted_row[2]
    #    title_dict['image'] = splitted_row[3]
    #    title_dict['preview_link'] = splitted_row[4]
    #    title_dict['publisher'] = splitted_row[5]
    #    title_dict['published_date'] = splitted_row[6]
    #    title_dict['info_link'] = splitted_row[7]
    #    title_dict['categories'] = splitted_row[8]
    #    title_dict['ratings_count'] = splitted_row[9]
    #except IndexError:
    #    # If the row has less fields than expected  
    #    raise e



#def deserialize_into_reviews_dict(row):


