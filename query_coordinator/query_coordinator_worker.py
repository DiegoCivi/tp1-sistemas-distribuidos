#from serialization import deserialize_titles_message, deserialize_into_titles_dict, ROW_SEPARATOR
from query_coordinator import QueryCoordinator
import os

def main():

    workers_q1 = int(os.getenv('WORKERS_Q1'))
    workers_q2 = int(os.getenv('WORKERS_Q2'))
    workers_q3_titles = int(os.getenv('WORKERS_Q3_TITLES'))
    workers_q3_reviews = int(os.getenv('WORKERS_Q3_REVIEWS'))
    workers_q5_titles = int(os.getenv('WORKERS_Q5_TITLES'))
    workers_q5_reviews = int(os.getenv('WORKERS_Q5_REVIEWS'))

    query_coordinator = QueryCoordinator(workers_q1, workers_q2, workers_q3_titles, workers_q3_reviews, workers_q5_titles, workers_q5_reviews)
    query_coordinator.run()

main()