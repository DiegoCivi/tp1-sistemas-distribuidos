

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
    return (''.join([serialize_item(item) for item in message_items]))[:-1]

def deserialize_item(item):
    """
    Deserialize a file item by splitting 
    it using the separator
    """
    return item.split(',')

def deserialize_message(message):
    """
    Deserialize a message (list of items) by splitting
    it using the separator
    """
    return [deserialize_item(item) for item in message.split('|')]