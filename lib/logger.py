import pickle
import os

class Logguer:

    def __init__(self, file):
        self.file = file

    def persist(self, content):
        f = open(self.file, 'ab')
        pickle.dump(content)
        f.write(b'\n')
        f.flush()
        f.close()
    
    def read_persisted_data(self):
        f = open(self.file, 'ab')
        with open('./log.txt', 'rb') as f:
            try:  # catch OSError in case of a one line file 
                f.seek(-2, os.SEEK_END)
                while f.read(1) != b'\n':
                    f.seek(-2, os.SEEK_CUR)
            except OSError:
                f.seek(0)
            last_line = pickle.load(f)
        
        f.close()

        return last_line