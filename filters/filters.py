# Module dedicated to the implementation of the filters used in the queries

# Reviews dataset format:
# id, title, price, user_id, profile_name, helpfulness, score, time, summary, text

# Books dataset format:
# title, description, authors, image, preview_link, publisher, published_date, info_link, categories, ratings_count


# Generic filter that returns the desired rows from a dataset according to a given condition and value
def filter_by(dataset, condition, values):
    """
    Filter the dataset by a given condition function and values
    """
    return [row for row in dataset if condition(row, values)]

