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
    authors_dict = {}
    # Get the decades for each author
    for row_dict in batch:
        if not row_dict['publishedDate'] or not row_dict['authors']:
            continue
        authors = row_dict['authors']
        splitted_authors = re.sub(r'[^a-zA-Z,]', '', authors).split(',')
        year = row_dict['publishedDate'].split('-')[0]
        year = re.sub(r'\D', '', year)
        year = int(year)
        for author in splitted_authors:
            if author not in authors_dict:
                authors_dict[author] = set()

            authors_dict[author].add(str(year - year%10))

    return [authors_dict]

def parse_authors_dict(authors_dict):
    authors_decades = {}
    for author, decades_str in authors_dict.items():
        splitted_val = decades_str.split(',')
        decades = set()
        for decade in splitted_val:
            decades.add(decade)
        authors_decades[author] = decades

    return authors_decades

def accumulate_authors_decades(batch, decades_accumulator):
    authors_decades = parse_authors_dict(batch[0])

    for author, decades_set in authors_decades.items():
        if author not in decades_accumulator:
            decades_accumulator[author] = set()
        decades_accumulator[author] = decades_accumulator[author].union(decades_set)


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

