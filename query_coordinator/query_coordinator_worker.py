#from serialization import deserialize_titles_message, deserialize_into_titles_dict, ROW_SEPARATOR
from query_coordinator import QueryCoordinator
import os

def main():

    eof_titles_max_subscribers = int(os.getenv('EOF_TITLES_MAX_SUBS'))
    eof_reviews_max_subscribers = int(os.getenv('EOF_REVIEWS_MAX_SUBS'))

    try:
        query_coordinator = QueryCoordinator(eof_titles_max_subscribers, eof_reviews_max_subscribers)
        query_coordinator.run()
    except:
        print('SIGTERM received')
main()