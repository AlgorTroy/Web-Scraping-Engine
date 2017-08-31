import Crawler as venom
import pandas as pd
import os.path
from tqdm import tqdm
import re
import random
from bs4 import BeautifulSoup


def read_info(keyfile, websitefile):
    """
    # The file are to be passed in csv format only
    # Files should be in Data folder only
    :param keyfile: file name of the keywords to be extracted from
    :param websitefile: file name of the website info to be used for
    :return: data frames of keyfile and websitefile
    """

    keyfile_path = os.path.abspath('..') + "\Data" + "\\" + keyfile
    websitefile_path = os.path.abspath('..') + "\Data" + "\\" + websitefile

    try:
        keyfile_df, websiteinfo_df = pd.read_csv(keyfile_path, encoding='ISO-8859-1', engine='c', dtype=str),\
                                     pd.read_csv(websitefile_path, encoding='ISO-8859-1', engine='c', dtype=str)
    except PermissionError:
        raise PermissionError("The files are already open!! Please close and try again")
    except FileNotFoundError:
        raise FileNotFoundError("File not found")
    return keyfile_df, websiteinfo_df


def get_attr(html, tag, class_name):
    """

    :param html:
    :param tag:
    :param class_name:
    :return:
    """
    soup = BeautifulSoup(html, 'html.parser')
    data = soup.find(tag, attrs={'class': class_name})

    if data is None:
        data = soup.find(tag, attrs={'id': class_name})
    return data


def get_data_from_html(web_df, html, domain_name, domain_url, product_link_dict):
    href = ''
    visible_text = ''
    if html is not None:

        link_tags = web_df[web_df['Name'] == domain_name]['LinkTagType'].tolist()
        link_classes = web_df[web_df['Name'] == domain_name]['LinkClass'].tolist()
        text_tags = web_df[web_df['Name'] == domain_name]['TextTagType'].tolist()
        text_classes = web_df[web_df['Name'] == domain_name]['TextClass'].tolist()

        for i in range(len(text_tags)):
            if href == '' and visible_text == '':

                link_class = link_classes[i]
                link_tag = link_tags[i]
                text_tag = text_tags[i]
                text_class = text_classes[i]

                if "|" in text_class:
                    find_fields = text_class.split("|")
                    find_tags = text_tag.split("|")
                else:
                    find_fields = [text_class]
                    find_tags = [text_tag]

                link_element = get_attr(html, tag=link_tag, class_name=link_class)

                if link_element is not None:
                    link = link_element.find('a')
                    # If Link element does not have 'a' tag then set link to the whole link element
                    if link is None:
                        link = link_element
                    # If href is not found then set the entire link as output
                    try:
                        href = link['href']
                    except Exception:
                        href = link

                for field in find_fields:
                    found_text = get_attr(html, tag=find_tags[find_fields.index(field)], class_name=field)
                    if found_text is not None:
                        visible_text = visible_text + "\n\n" + found_text.text

                if visible_text is None:
                    visible_text = ''

                visible_text = re.sub("\n+", "\n", str(visible_text).strip())

                # Add base url
                if href != '':
                    try:
                        href = product_link_dict[domain_name] + href
                    except:
                        href = domain_url

    return href, visible_text


def create_keywords(key_dataframe, header_list):
    """
    :param key_dataframe:
    :param header_list:
    :return:
    """
    key_dataframe.fillna('', inplace=True)
    keywords = []
    print('\nCreating keywords....')
    for i in tqdm(range(key_dataframe.shape[0])):
        keyword = ''
        for header in header_list:
            header_data = str(key_dataframe[header].iloc[i])
            if header_data != '':
                if "/" in header_data:
                    header_data = re.sub("/", "%2", header_data)
                keyword += "+" + header_data
        if keyword.startswith("+"):
            keyword = keyword[1:]
        keywords.append(keyword)
    key_dataframe['Keywords'] = pd.Series(keywords).values
    return key_dataframe


def create_random_search_query(keyword_df, domain_dict, try_new_domain=False):
    """
    :param keyword_df:
    :param domain_dict:
    :return:
    """
    keyword_df['Search_URL'] = ''
    keyword_df['URL'] = ''

    print('\nCreating random urls....')

    for i in range(keyword_df.shape[0]):
        keyword = keyword_df['Keywords'].iloc[i]
        domain_name = (str(keyword_df['DomainName'].iloc[i])).strip()

        if domain_name == '':
            domain_names = list(domain_dict.keys())
            search_query_list = list(domain_dict.values())
            index = random.randint(0, len(search_query_list)-1)
            keyword_df.set_value(i, 'Search_URL', {domain_names[index]: search_query_list[index]+keyword})
            keyword_df.set_value(i, 'URL', search_query_list[index]+keyword)
        else:
            keyword_df.set_value(i, 'Search_URL', {domain_name: domain_dict[domain_name] + keyword})
            keyword_df.set_value(i, 'URL', domain_dict[domain_name] + keyword)

    return keyword_df


def activate_bot(save_after_itr, url_jump_lag, num_retries, wait_range, continue_from_last):
    output_file_path = os.path.abspath('..') + "\Data\Output\Crawled.csv"
    save_count = 0
    save_after_itr = save_after_itr
    url_jump_lag = url_jump_lag
    num_retries = num_retries
    wait_range = wait_range
    continue_from_last = continue_from_last

    output_columns = ['Domain Name', 'Product Description', 'Product Link', 'URL',
                           'MATERIAL_NO', 'MANUFACTURER_NAME_1', 'MANUFACTURER_PT_NO_1',
                           'NOUN_ENGLISH', 'MODIFIER_ENGLISH', 'Keywords', 'Status Flag']

    if continue_from_last:
        # Load output file
        print('Loading output file...')
        output_df = pd.read_csv(output_file_path, engine='c', encoding='ISO-8859-1', dtype=str)

        print('Loading input files....')
        key_df, web_df = read_info('Input\SearchKeys.csv', 'Input\Website Info.csv')

        output_df.fillna('', inplace=True)

        # Remove empty rows from df i.e the uncrawled data
        success_df = output_df[(output_df['Product Description'] != '') |
                                                  (output_df['Product Link'] != '')]

        # Get list of all successfully crawled urls
        success_url_list = success_df['MATERIAL_NO'].unique().tolist()
        print(success_url_list)

        # Remove all these urls from input file
        found_keys_df = key_df[key_df['MATERIAL_NO'].isin(success_url_list)]
        key_df = key_df.drop(found_keys_df.index).reset_index(drop=True)
        print(key_df)

    else:
        # Load the website & search key details
        print('Loading input files....')
        key_df, web_df = read_info('Input\SearchKeys.csv', 'Input\Website Info.csv')

        # Reset output folder
        crawled_df = pd.DataFrame(columns=output_columns)
        crawled_df.to_csv(output_file_path, index=False, columns=output_columns)

        print('Output folder cleared...')

    # Get the merge list
    merge_list = ['MANUFACTURER_NAME_1', 'MANUFACTURER_PT_NO_1', 'NOUN_ENGLISH', 'MODIFIER_ENGLISH']

    # Create keywords
    key_df = create_keywords(key_df, merge_list)

    search_query_list = web_df['SearchQuery'].tolist()
    domain_names = web_df['Name'].tolist()
    base_url_list = web_df['BaseProductURL'].tolist()
    product_link_dict = dict(zip(domain_names, base_url_list))

    domain_dict = dict(zip(domain_names, search_query_list))

    # Generate random search query urls
    keyword_df = create_random_search_query(key_df, domain_dict)
    keyword_df.to_csv(os.path.abspath('..') + "\Data\Output\Continuation\KeywordUrls.csv", index=False)

    random_url_list = keyword_df['Search_URL'].tolist()


    for i in range(0, len(random_url_list), save_after_itr):

        list_of_dicts = []
        save_count += 1
        random_urls_sub = random_url_list[i:i + save_after_itr]

        html_dict = venom.download_randomly(random_urls_sub, url_jump_lag=url_jump_lag,
                                            num_retries=num_retries, wait_range=wait_range)

        for domain_url, domain_data in html_dict.items():
            # link_tag, link_class, text_tag, text_class = '', '', '', ''
            html = list(domain_data.values())[0]
            domain_name = list(domain_data.keys())[0]
            href = ''
            visible_text = ''
            status_flag = ''

            if html is not None:
                href, visible_text = get_data_from_html(web_df, html, domain_name, domain_url, product_link_dict)

                if href == '' and visible_text == '':
                    status_flag = 'No data found'

            else:
                print('Web page not scrapped:', domain_url)
                status_flag = 'Access Denied'

            # Add data to list
            list_of_dicts.append({"Domain Name": domain_name, "URL": domain_url,
                                  "Product Link": href, "Product Description": visible_text, 'Status Flag': status_flag})

        crawled_df = pd.DataFrame(list_of_dicts)
        crawled_df = crawled_df.merge(keyword_df, on='URL', how='left')
        # crawled_df = crawled_df.drop_duplicates(['MATERIAL_NO'], keep='last')

        with open(os.path.abspath('..') + "\Data\Output\Crawled.csv", 'a') as f:
            crawled_df.to_csv(f, header=False, index=False,
                              columns=output_columns)
        print('\nSaved for itr:', save_count)