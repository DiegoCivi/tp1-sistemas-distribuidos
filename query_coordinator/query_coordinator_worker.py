#from serialization import deserialize_titles_message, deserialize_into_titles_dict, ROW_SEPARATOR
from middleware import Middleware
from query_coordinator import QueryCoordinator
import time
import os





def main():
    middleware = Middleware()

    eof_titles_max_subscribers = int(os.getenv('EOF_TITLES_MAX_SUBS'))
    eof_reviews_max_subscribers = int(os.getenv('EOF_REVIEWS_MAX_SUBS'))

    query_coordinator = QueryCoordinator(middleware, eof_titles_max_subscribers, eof_reviews_max_subscribers)
    query_coordinator.run()




main()