from csv import DictReader
from serialization import serialize_dict

def create_file_reader(file_path):
    """
    Create a file reader object
    """
    try:
        file = open(file_path, 'r')
    except:
        return None
    
    reader = DictReader(file)
    return file, reader

def read_csv_batch(file_reader, threshold=200):
    """
    Read a batch of rows from a CSV file
    """
    batch = []
    # EOF reached
    if file_reader is None:
        return batch
    
    for i, dictionary in enumerate(file_reader):
        serialized_dict = serialize_dict(dictionary)
        batch.append(serialized_dict)

        if i >= threshold:
            break
        
    return batch


"""
CAMBIOS PARA DESP MIRAR SI REALMENTE VAN:
    - En la funcion is_queue_finished() de MultipleQueueWorker se agrego un if. Chequear si la logica esta bien y se puede agreegar
    - Se cambio assemble_results() en ResultCoordinator. Esto si se puede agregar ya que solo se fija si es que no lelgo ningun res de esa query
"""

