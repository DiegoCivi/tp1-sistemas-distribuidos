import pickle
import os

VOLUME_PATH = './persisted_data/'

class Logger:

    def __init__(self, file_name):
        self.file_path = VOLUME_PATH + file_name

    def persist(self, content):
        f = open(self.file_path, 'ab')
        pickle.dump(content, f)
        f.write(b'\n')
        f.flush()
        f.close()
    
    def read_persisted_data(self):
        f = open(self.file_path, 'ab')
        try:  # catch OSError in case of a one line file 
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        last_line = pickle.load(f)
        
        f.close()

        return last_line