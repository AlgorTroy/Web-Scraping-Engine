# ----------------------------------------------
# Project Name : Venom
# The web scraping engine for Utopia.inc
# Project Author(s): Akash and Atiya
# Project allocation by : Tanmay
# Project Manifestation: 28-12-16(dd-mm-yy)
# Crawler Inspired by: Richard Lawson
# ----------------------------------------------

"""
ShadowBot is nothing fancy, name was chosen randomly.
ShadowBot requires Crawler lib for all the works to be done.

"""

import Crawler as venom
import pandas as pd
import os.path
from tqdm import tqdm
import re
import random
from bs4 import BeautifulSoup
import time
import ast


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
        keyfile_df, websiteinfo_df = pd.read_csv(keyfile_path, encoding='ISO-8859-1', engine='c'),\
                                     pd.read_csv(websitefile_path, encoding='ISO-8859-1', engine='c')
    except PermissionError:
        raise PermissionError("The files given are not in csv format, please change to csv and try again")
    except FileNotFoundError:
        raise FileNotFoundError("File not found")
    return keyfile_df, websiteinfo_df


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

        if try_new_domain:
            for domain, query in domain_dict.items():
                if str(domain_name).strip() != str(domain).strip():
                    domain_name = domain
                    break
            keyword_df.loc[i, 'Search_URL'] = {domain_name: domain_dict[domain_name] + keyword}
            keyword_df.loc[i, 'URL'] = domain_dict[domain_name] + keyword
            keyword_df.loc[i, 'DomainName'] = str(domain_name)

        else:
            if domain_name == '':
                domain_names = list(domain_dict.keys())
                search_query_list = list(domain_dict.values())
                index = random.randint(0, len(search_query_list)-1)
                # merged_list.append({domain_names[index]:search_query_list[index]+keyword})
                keyword_df.loc[i, 'Search_URL'] = {domain_names[index]: search_query_list[index]+keyword}
                keyword_df.loc[i, 'URL'] = search_query_list[index]+keyword
            else:
                print(keyword_df)
                keyword_df.loc[i, 'Search_URL'] = {domain_name: domain_dict[domain_name] + keyword}
                keyword_df.loc[i, 'URL'] = domain_dict[domain_name] + keyword

    return keyword_df


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


def activate_shadowbot(url_jump_lag=(0, 5), num_retries=2, wait_range=(20, 60),
                       save_after_itr=20, continue_from_last=False):
    """

    :param url_jump_lag:
    :param num_retries:
    :param wait_range:
    :param save_after_itr:
    :param continue_from_last:
    :return:
    """
    print('Processing started @', time.strftime("%a %H:%M:%S"))

    if not continue_from_last:
        # Read the files
        key_df, web_df = read_info('Input\SearchKeys.csv', 'Input\Website Info.csv')

        # Create and save an empty crawled output file
        crawled_df = pd.DataFrame(columns=['Domain Name', 'Product Description', 'Product Link',
                                           'URL', 'MATERIAL_NO', 'MANUFACTURER_NAME_1',
                                           'MANUFACTURER_PT_NO_1', 'NOUN_ENGLISH', 'MODIFIER_ENGLISH',
                                           'Keywords', 'Status Flag'])
        crawled_df.to_csv(os.path.abspath('..') + "\Data\Output\Crawled.csv", index=False,
            columns=['Domain Name', 'Product Description', 'Product Link', 'URL',
                       'MATERIAL_NO', 'MANUFACTURER_NAME_1', 'MANUFACTURER_PT_NO_1',
                       'NOUN_ENGLISH', 'MODIFIER_ENGLISH', 'Keywords', 'Status Flag']
        )
        # Get the merge list
        merge_list = ['MANUFACTURER_NAME_1', 'MANUFACTURER_PT_NO_1', 'NOUN_ENGLISH', 'MODIFIER_ENGLISH']

        # Create keywords
        key_df = create_keywords(key_df, merge_list)

        search_query_list = web_df['SearchQuery'].tolist()
        domain_names = web_df['Name'].tolist()
        base_url_list = web_df['BaseProductURL'].tolist()
        base_dict = dict(zip(domain_names, base_url_list))

        domain_dict = dict(zip(domain_names, search_query_list))

        # Generate random search query urls
        keyword_df = create_random_search_query(key_df, domain_dict)
        keyword_df.to_csv(os.path.abspath('..') + "\Data\Output\Continuation\KeywordUrls.csv", index=False)

        random_url_list = keyword_df['Search_URL'].tolist()
    else:

        # Read the files
        key_df, web_df = read_info('Input\SearchKeys.csv', 'Input\Website Info.csv')

        domain_names = web_df['Name'].tolist()
        base_url_list = web_df['BaseProductURL'].tolist()
        base_dict = dict(zip(domain_names, base_url_list))

        keyword_df = pd.read_csv(os.path.abspath('..') + "\Data\Output\Continuation\KeywordUrls.csv")
        # Remove all the fetched urls from crawled.csv
        # Read existing crawled file
        existing_crawled_df = pd.read_csv(os.path.abspath('..') + "\Data\Output\Crawled.csv",
                                          encoding='ISO-8859-1', engine='c')
        existing_crawled_df.fillna('', inplace=True)

        # Remove empty rows from df i.e the uncrawled data
        existing_crawled_df = existing_crawled_df[(existing_crawled_df['Product Description'] != '') |
                                                  (existing_crawled_df['Product Link'] != '')]

        # Get list of all crawled urls
        crawled_url_list = existing_crawled_df['URL'].unique().tolist()

        # Remove all these urls from keywordUrl file
        keyword_df = keyword_df[~keyword_df['URL'].isin(crawled_url_list)]

        # keyword_df = keyword_df[~keyword_df['URL'].isin(crawled_url_list)]
        keyword_df.to_csv(os.path.abspath('..') + "\Data\Output\Continuation\KeywordUrls.csv", index=False)

        # # Remove empty rows from df
        # existing_crawled_df = existing_crawled_df[(existing_crawled_df['Product Description'] != '') |
        #                                           (existing_crawled_df['Product Link'] != '')]

        # Save updated df to local
        existing_crawled_df.to_csv(os.path.abspath('..') + "\Data\Output\Crawled.csv",
                                   index=False,
                                    columns=['Domain Name', 'Product Description', 'Product Link', 'URL',
                                   'MATERIAL_NO', 'MANUFACTURER_NAME_1', 'MANUFACTURER_PT_NO_1',
                                   'NOUN_ENGLISH', 'MODIFIER_ENGLISH', 'Keywords', 'Status Flag'])

        random_url_list = keyword_df['Search_URL'].tolist()

        random_url_list_updated =[]
        for random_url in random_url_list:
            random_url_list_updated.append(ast.literal_eval(random_url))
        random_url_list = list(random_url_list_updated)

    save_count = 1
    for i in range(0, len(random_url_list), save_after_itr):
        crawled_df = crawl_urls(random_url_list, web_df, keyword_df, i, base_dict, save_after_itr, url_jump_lag, num_retries, wait_range)
        # status_flag = ''
        # random_urls_sub = random_url_list[i:i+save_after_itr]
        #
        # html_dict = venom.download_randomly(random_urls_sub, url_jump_lag=url_jump_lag,
        #                                     num_retries=num_retries, wait_range=wait_range)
        # list_of_dicts = []
        #
        # for domain_url, domain_data in html_dict.items():
        #     # link_tag, link_class, text_tag, text_class = '', '', '', ''
        #     html = list(domain_data.values())[0]
        #     domain_name = list(domain_data.keys())[0]
        #     href = ''
        #     visible_text = ''
        #
        #     if html is not None:
        #         href, visible_text = get_data_from_html(web_df, html, domain_name)
        #
        #     else:
        #         print('Web page not scrapped:', domain_url)
        #         status_flag = 'Page Not Crawled'
        #
        #     list_of_dicts.append({"Domain Name": domain_name, "URL": domain_url,
        #                           "Product Link": href, "Product Description": visible_text, 'Status Flag': status_flag})
        #
        # crawled_df = pd.DataFrame(list_of_dicts)
        # crawled_df['Product Description'].fillna('', inplace=True)
        # crawled_df['Product Link'].fillna('', inplace=True)
        #
        # # keyword_df = keyword_df.merge(crawled_df, on='URL', how='left')
        # crawled_df = crawled_df.merge(keyword_df, on='URL', how='left')
        # crawled_df.drop('Search_URL', axis=1, inplace=True)

        # Get all failed crawls

        failed_crawls_df = crawled_df[(crawled_df['Product Description'] == '') |
                                            (crawled_df['Product Link'] == '')]

        # crawled_df = crawled_df[(crawled_df['Product Description'] != '') |
        #                                     (crawled_df['Product Link'] != '')]
        if not failed_crawls_df.empty:
            print('Retrying the failed keywords...')

            # Create keywords
            # Get the merge list
            merge_list = ['MANUFACTURER_NAME_1', 'MANUFACTURER_PT_NO_1', 'NOUN_ENGLISH', 'MODIFIER_ENGLISH']

            key_df_failed = failed_crawls_df.drop(['Domain Name', 'Product Description', 'Product Link',
                                                   'Keywords','Status Flag'], axis=1).reset_index(drop=True)

            key_df_failed = create_keywords(key_df_failed, merge_list)

            domain_names = web_df['Name'].tolist()
            search_query_list = web_df['SearchQuery'].tolist()

            domain_dict = dict(zip(domain_names, search_query_list))

            base_url_list = web_df['BaseProductURL'].tolist()
            base_dict = dict(zip(domain_names, base_url_list))

            # Generate random search query urls
            keyword_df_failed = create_random_search_query(key_df_failed, domain_dict, try_new_domain=True)

            random_url_list_fail = keyword_df_failed['Search_URL'].tolist()

            retry_crawl_df = crawl_urls(random_url_list_fail, web_df, keyword_df_failed, i, base_dict, save_after_itr, url_jump_lag, num_retries,
                                    wait_range)

            if not retry_crawl_df.empty:
                crawled_df = pd.concat([crawled_df, retry_crawl_df])

                crawled_df = crawled_df.drop_duplicates(['MATERIAL_NO'], keep='last')

        with open(os.path.abspath('..') + "\Data\Output\Crawled.csv", 'a') as f:
            crawled_df.to_csv(f, header=False, index=False,
                              columns=['Domain Name', 'Product Description', 'Product Link', 'URL',
                                       'MATERIAL_NO', 'MANUFACTURER_NAME_1', 'MANUFACTURER_PT_NO_1',
                                       'NOUN_ENGLISH', 'MODIFIER_ENGLISH', 'Keywords', 'Status Flag'])
        print('\nSaved for itr:', save_count)
        save_count += 1

    # crawled_df = pd.DataFrame(list_of_dicts)
    # crawled_df['Product Description'].fillna('Data Not Found', inplace=True)
    # crawled_df['Product Link'].fillna('Data Not Found', inplace=True)
    #
    # # keyword_df = keyword_df.merge(crawled_df, on='URL', how='left')
    #
    # keyword_df.drop('Search_URL', axis=1, inplace=True)
    # keyword_df.to_excel(os.path.abspath('..') + "\Data\Output\KeywordUrls.xlsx")

    crawled_df = pd.read_csv(os.path.abspath('..') + "\Data\Output\Crawled.csv",
                                      encoding='ISO-8859-1', engine='c')

    crawled_df.drop_duplicates(subset=['MATERIAL_NO'], keep='last', inplace=True)

    # Save updated df to local
    crawled_df.to_csv(os.path.abspath('..') + "\Data\Output\Crawled.csv",
                               index=False,
                               columns=['Domain Name', 'Product Description', 'Product Link', 'URL',
                                        'MATERIAL_NO', 'MANUFACTURER_NAME_1', 'MANUFACTURER_PT_NO_1',
                                        'NOUN_ENGLISH', 'MODIFIER_ENGLISH', 'Keywords', 'Status Flag'])

    print('Processing Done @', time.strftime("%a %H:%M:%S"))


def get_data_from_html(web_df, html, domain_name, base_dict):
    href = ''
    visible_text = ''
    if html is not None:

        link_tags = web_df[web_df['Name'] == domain_name]['LinkTagType'].tolist()
        link_classes = web_df[web_df['Name'] == domain_name]['LinkClass'].tolist()
        text_tags = web_df[web_df['Name'] == domain_name]['TextTagType'].tolist()
        text_classes = web_df[web_df['Name'] == domain_name]['TextClass'].tolist()

        for i in range(len(link_tags)):
            if href == '' and visible_text == '':
                # Reset values
                href = ''
                visible_text = ''

                link_class = link_classes[i]
                link_tag = link_tags[i]
                text_tag = text_tags[i]
                text_class = text_classes[i]

                link_element = get_attr(html, tag=link_tag, class_name=link_class)

                # if link_element is not None:
                #     link = link_element.find('a')
                #     # If Link element does not have 'a' tag then set link to the whole link element
                #     if link is None:
                #         link = link_element
                #     # If href is not found then set the entire link as output
                #     try:
                #         href = link['href']
                #     except Exception:
                #         href = link

                visible_text = get_attr(html, tag=text_tag, class_name=text_class)
                if visible_text is not None:
                    visible_text = visible_text.text
                else:
                    visible_text = ''

                visible_text = re.sub("\n+", "\n", str(visible_text).strip())

                # Add base url
                if href != '':
                    href = base_dict[domain_name] + href


                # Reset the link if no product link is provided

                if str(base_dict[domain_name]).strip() == '' or str(base_dict[domain_name]).strip() == 'nan':
                    href = 'huehue:' + str(base_dict[domain_name])
    return href, visible_text


def crawl_urls(random_url_list, web_df, keyword_df, i, base_dict, save_after_itr, url_jump_lag, num_retries, wait_range):
    status_flag = ''
    random_urls_sub = random_url_list[i:i + save_after_itr]

    html_dict = venom.download_randomly(random_urls_sub, url_jump_lag=url_jump_lag,
                                        num_retries=num_retries, wait_range=wait_range)
    list_of_dicts = []

    for domain_url, domain_data in html_dict.items():
        # link_tag, link_class, text_tag, text_class = '', '', '', ''
        html = list(domain_data.values())[0]
        domain_name = list(domain_data.keys())[0]
        href = ''
        visible_text = ''
        status_flag = ''

        if html is not None:
            href, visible_text = get_data_from_html(web_df, html, domain_name, base_dict)

        else:
            print('Web page not scrapped:', domain_url)
            status_flag = 'Access Denied'

        list_of_dicts.append({"Domain Name": domain_name, "URL": domain_url,
                              "Product Link": href, "Product Description": visible_text, 'Status Flag': status_flag})

    crawled_df = pd.DataFrame(list_of_dicts)

    if crawled_df.empty or (crawled_df is None):
        # Create empty dataframe
        crawled_df = pd.DataFrame(columns=['Domain Name', 'Product Description', 'Product Link',
                                           'URL', 'MATERIAL_NO', 'MANUFACTURER_NAME_1',
                                           'MANUFACTURER_PT_NO_1', 'NOUN_ENGLISH', 'MODIFIER_ENGLISH',
                                           'Keywords', 'Status Flag'])
    else:
        crawled_df['Product Description'].fillna('', inplace=True)
        crawled_df['Product Link'].fillna('', inplace=True)

        crawled_df = crawled_df.merge(keyword_df, on='URL', how='left')
        crawled_df.drop('Search_URL', axis=1, inplace=True)

    return crawled_df