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

def title_condition(item, value):
    """
    Check if the title of the item is in the values list
    """
    return value.lower() in item[0].lower()

def category_condition(item, value):
    """
    Check if the category of the item is in the values list
    """
    return value.lower() == item[8].lower() 

def year_range_condition(item, values):
    """
    Check if the published date of the item is in the values list
    """
    year_info = item[6].split('-')[0]
    return values[0] <= int(year_info) <= values[1]



