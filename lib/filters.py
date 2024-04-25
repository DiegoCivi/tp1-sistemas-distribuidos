# Module dedicated to the implementation of the filters used in the queries

# Reviews dataset format:
# id, title, price, user_id, profile_name, helpfulness, score, time, summary, text
# 0 , 1    , 2    , 3      , 4           , 5          , 6    , 7   , 8      , 9

# Books dataset format:
# title, description, authors, image, preview_link, publisher, published_date, info_link, categories, ratings_count
# 0    , 1          , 2      , 3    , 4           , 5        , 6             , 7        , 8         , 9

import re

# Generic filter that returns the desired rows from a dataset according to a given condition and value
def filter_by(batch, condition, values):
    """
    Filter the batch by a given condition function and values
    """
    return [row_dictionary for row_dictionary in batch if condition(row_dictionary, values)]

def title_condition(row_dictionary, value):
    """
    Check if the title of the item is in the values list
    """
    #print(row_dictionary)
    return value.lower() in row_dictionary['Title'].lower()

def category_condition(row_dictionary, value):
    """
    Check if the category of the item is in the values list
    """
    #if row_dictionary['title'] == 'Windows 98 Hints & Hacks':
    #    print(f'La categoria del titulo es: {row_dictionary['categories']} y la categoria a buscar es: {value}')
    title_category = row_dictionary['categories']
    title_category = re.sub(r'[^a-zA-Z]', '', title_category)

    return value == title_category 

def year_range_condition(row_dictionary, values):
    """
    Check if the published date of the item is in the values list
    """
    year_info = row_dictionary['publishedDate'].split('-')[0]
    try:
        year = int(year_info)
    except:
        return False
 
    return values[0] <= year <= values[1]

def different_decade_counter(batch):
    """
    Summarize the number of different decades in which
    each author published a book
    """
    authors = {}
    for row_dict in batch:
        authors = row_dict['authors']
        parsed_authors = re.sub(r'[^a-zA-Z,]', '', authors).split(',')
        year = int(row_dict['published_date'].split('-')[0])
        for author in parsed_authors:
            if author not in authors:
                authors[author] = set()
            authors[author].add(str(year - year%10))
    return authors

def calculate_review_sentiment(batch):
    """
    Calculate the sentiment of the reviews
    """
    sentiment = {}
    for row in batch:
        text = row[9]
        title = row[1]
        tokens = nltk.word_tokenize(text)
        #sentiment[title] = nltk.sentiment.util.demo_liu_hu_lexicon(tokens)
    return sentiment

def calculate_percentile(sentiment_scores, percentile):
    """
    Calculate the titles above a certain percentile
    """
    titles = []
    for title, score in sentiment_scores.items():
        if score > percentile:
            titles.append(title) # TODO: This is much more complex than this
    return titles
    

def hash_title(batch, title_index):
    for row in batch:
        title = row[title_index]
        hashed_title = hash(title) # TODO: This returns an int. Maybe we need a string
        row.append(hashed_title)  

    return batch

