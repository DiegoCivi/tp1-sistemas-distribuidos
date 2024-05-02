# Module dedicated to the implementation of the filters used in the queries

# Reviews dataset format:
# id, title, price, user_id, profile_name, helpfulness, score, time, summary, text
# 0 , 1    , 2    , 3      , 4           , 5          , 6    , 7   , 8      , 9

# Books dataset format:
# title, description, authors, image, preview_link, publisher, published_date, info_link, categories, ratings_count
# 0    , 1          , 2      , 3    , 4           , 5        , 6             , 7        , 8         , 9

import re
from textblob import TextBlob


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

def review_quantity_value(batch, value):
    """
    Check if the the review quantity is greater than the value
    If it is, then we add it to the result list of titles information
    """
    filtered_dict = {}
    for row_dictionary in batch:
        #print(row_dictionary)
        for title, values in row_dictionary.items():
            if int(values.split(',')[0]) >= value:
                filtered_dict[title] = values
            
    return [filtered_dict]

def year_range_condition(row_dictionary, values):
    """
    Check if the published date of the item is in the values list
    """
    year_regex = re.compile('(?<!\*)[^\d]*(\d{4})[^\d]*(?<!\*)')
    result = year_regex.search(row_dictionary['publishedDate'])
    try:
        year = int(result.group(1))
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
        splitted_authors = re.sub(r'[^\w,\s]', '',authors).split(',')
        #splitted_authors = re.sub(r'[^\D]', '', authors).split(',')
        year = row_dict['publishedDate'].split('-')[0]
        year = re.sub(r'\D', '', year)
        year = int(year)
        for author in splitted_authors:
            if author == '':
                continue
            elif author.startswith(' ') or author.endswith(' '):
                author = author.lstrip(' ').rstrip(' ')
            
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
    result = []
    for row_dictionary in batch:
        sentiment_dict = {}
        text = row_dictionary['review/text']
        if text == '':
            continue
        title = row_dictionary['Title']
        blob = TextBlob(text)
        text_sentiment = blob.sentiment.polarity
        sentiment_dict['Title'] = title
        sentiment_dict['text_sentiment'] = str(text_sentiment)
        result.append(sentiment_dict)
    return result

def calculate_percentile(sentiment_scores, percentile):
    """
    Calculate the titles above a certain percentile
    """
    titles = []
    for title, score in sentiment_scores.items():
        if score > percentile:
            titles.append(title) # TODO: This is much more complex than this
    return titles

def hash_djb2(s):                                                                                                                                
    hash = 5381
    for x in s:
        hash = (( hash << 5) + hash) + ord(x)
    return hash & 0xFFFFFFFF    

def hash_title(batch):
    for row_dictionary in batch:
        title = row_dictionary['Title']
        hashed_title = hash_djb2(title) # TODO: This returns an int. Maybe we need a string
        row_dictionary['hashed_title'] = hashed_title  

    return batch

def get_top_n(batch, top, top_n, last):
    """
    In this case the batch only has 1 dictionary that will have different titles 
    with their counter. The counter is a string with this format -> 'reviews_quantity,ratings_summation,authors'.

    If last is set to True, it means this funcion is being called by the top 10 worker calculator in the last layer. This means
    that the job to do is the same as the other workers in the other layers, but the format of the message is not the same. Being in the
    last layer means the worker wont have to calculate the mean_rating since it is already in the message.
    """
    titles_counters_dict = batch[0]
    batch_top = []
    for title, value in titles_counters_dict.items():
        if last:
            mean_rating = float(value)
        else:
            splitted_counter = value.split(',', 2) # Only split until the second ','. This is to not split the authors field which may have also a ','.
            mean_rating = float(splitted_counter[1]) / float(splitted_counter[0])
        
        batch_top.append((title, mean_rating))

    batch_top.sort(key=sorting_key, reverse=True)
    top = top + batch_top[:top_n]
    top.sort(key=sorting_key, reverse=True)
    return top[:top_n]


def sorting_key(tup):
    return tup[1]

def titles_in_the_n_percentile(review_sentiment_dict, n):
    vals = list(review_sentiment_dict.values())
    vals = sorted(vals)
    percentile_index = int(len(vals) * 0.9)
    percentile_val = vals[percentile_index]
    percentile_val = 0.37467883042883043
    print("############### El percentil 90 es: ", percentile_val)
    titles_in_percentile_n = [title for title, valor in review_sentiment_dict.items() if valor >= percentile_val]

    print("######### SI ROMPE LA SERIALIZACION/DESERIALIZACION MIRAR QUE ACA DEVOLVEMOS UNA LISTA #########")
    return titles_in_percentile_n





