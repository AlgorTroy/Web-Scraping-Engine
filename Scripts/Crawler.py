# ----------------------------------------------
# Project Name : Venom
# The web scraping engine for Utopia.inc
# Project Author(s): Akash and Atiya
# Project allocation by : Tanmay
# Project Manifestation: 28-12-16(dd-mm-yy)
# Crawler Inspired by: Richard Lawson
# ----------------------------------------------

"""
The major issue faced by poorly made web crawler is the certainty of the scraping function
applied each time on the target website, Venom tries to mimic human behaviour by randomly
crawling through the target website and extracting information when called but
not over-loading the server or slowing down the target website.

*Note: The engine is to be used ethically for data extraction purpose alone.

"""

from urllib.request import urlopen
from urllib.request import Request
from urllib.error import URLError
from random import randint
from time import sleep
from tqdm import tqdm


__all__ = ['download', 'download_randomly']

__version__ = "1.0.3"

"""
User Agent List:

------------------------------------------------------------------------------------
Linux / Firefox 44: Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:44.0) Gecko/20100101 Firefox/44.0
Mac OS X/ Safari: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2
 Safari/601.3.9
Windows / IE 11: Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko
Windows / Edge: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0
 Safari/537.36 Edge/13.10586

Android / Chrome 40: Mozilla/5.0 (Linux; Android 5.1.1; Nexus 4 Build/LMY48T) AppleWebKit/537.36 (KHTML, like Gecko)
 Chrome/40.0.2214.89 Mobile Safari/537.36
iOS / Safari 9: Mozilla/5.0 (iPhone; CPU iPhone OS 9_2_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko)
Version/9.0 Mobile/13D15 Safari/601.1

Google Bot: Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
PS4: Mozilla/5.0 (PlayStation 4 3.15) AppleWebKit/537.73 (KHTML, like Gecko)
Curl: curl/7.43.0
ABrowse: Mozilla/5.0 (compatible; U; ABrowse 0.6; Syllable) AppleWebKit/420+ (KHTML, like Gecko)
------------------------------------------------------------------------------------

"""

user_agents = {
    "Linux/Firefox 44": "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:44.0) Gecko/20100101 Firefox/44.0",
    "Mac OS X/Safari": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) "
                       "Version/9.0.2 Safari/601.3.9",
    "Windows/IE 11": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Windows/Edge": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
                    " Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586",
    "Android/Chrome 40": "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 4 Build/LMY48T) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/40.0.2214.89 Mobile Safari/537.36",
    "iOS/Safari 9": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_2_1 like Mac OS X) AppleWebKit/601.1.46 "
                    "(KHTML, like Gecko) Version/9.0 Mobile/13D15 Safari/601.1",
    "Google Bot": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "PS4": "Mozilla/5.0 (PlayStation 4 3.15) AppleWebKit/537.73 (KHTML, like Gecko)",
    "Curl": "curl/7.43.0",
    "ABrowse": "Mozilla/5.0 (compatible; U; ABrowse 0.6; Syllable) AppleWebKit/420+ (KHTML, like Gecko)"
}


def download(url, user_agent_dict=user_agents, num_retries=2, wait_randomly=True, wait_range=(20, 60)):
    """
    :param url: The url of the website to be scrapped.
    :param num_retries: If the request fails with an error 500 and above how many times to try after it.
    :param user_agent_dict: User agent to be picked randomly from this list.
    :param wait_randomly: True if download request has to be made random after error to download when access is denied.
    :param wait_range: Random wait range for wait_randomly True, by default set between 20's to 60's.
    :return: Page or Html.
    """
    # print("\nDownloading:", url)
    user_agent_list = list(user_agent_dict.keys())
    user_agent_key = user_agent_list[randint(0, len(user_agent_list) - 1)]
    user_agent = user_agent_dict[user_agent_key]
    # print("\nUsing user_agent", user_agent_key)
    headers = {'User-agent': user_agent}
    request = Request(url, headers=headers)
    html = None
    try:
        html = urlopen(request).read()
        if (html is None) or (html.strip() == ''):
            raise KeyError("Empty Page found...Retry")
    except KeyError:
        print('\nHandling key error')
        html = None
        if num_retries > 0:
            if wait_randomly:
                start, end = wait_range
                wait_time = randint(start, end)
                # print('\nWaiting for', wait_time, 'seconds before retry...')
                # print("\nUsing user_agent", user_agent)
                sleep(wait_time)
                # recursively retry 5xx HTTP errors
                return download(url, user_agents, num_retries - 1, wait_randomly=wait_randomly,
                                wait_range=wait_range)

    except URLError as e:
        print('\nHandling url error')
        # print('\nDownload error:', e.reason)
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and (500 <= e.code < 600):
                # Make the random wait option here
                if wait_randomly:
                    start, end = wait_range
                    wait_time = randint(start, end)
                    # print('\nWaiting for', wait_time, 'seconds before retry...')
                    # print("\nUsing user_agent", user_agent)
                    sleep(wait_time)
                    # recursively retry 5xx HTTP errors
                    return download(url, user_agents, num_retries-1, wait_randomly=wait_randomly,
                                    wait_range=wait_range)

    except UnicodeEncodeError:
        print('\nEncoding error')
        pass

    if html == '':
        return None
    return html


def download_randomly(url_list, agents_dict=user_agents, make_url_jumps_random=True, url_jump_lag=(10, 15),
                      num_retries=2, wait_range=(20, 60)):
    """
    :param url_list: urls to be crawled
    :param agents_dict: url agent to be specified
    :param make_url_jumps_random: specify if the url jumps have to be random or straight
    :param url_jump_lag: time between two url jumps
    :param num_retries: how many times the program should retry after it encounters fails or busy server
    :param wait_range: the amount of time it should wait before next try
    :return: html_dict, format = {url:html}
    """
    print("\n Downloading websites randomly.....")
    if make_url_jumps_random:
        url_list_clone = list(url_list)
        html_dict = {}
        for i in tqdm(range(len(url_list_clone))):
            url_index = randint(0, len(url_list_clone)-1)
            url_dict = url_list_clone[url_index]

            domain_name = list(url_dict.keys())[0]
            domain_url = list(url_dict.values())[0]

            # html = download(url_list_clone[url_index], agents_dict)
            html = download(domain_url, agents_dict, num_retries=num_retries, wait_range=wait_range)

            html_dict[domain_url] = {domain_name: html}
            del url_list_clone[url_index]
            if url_list_clone:
                start, end = url_jump_lag
                wait_time = randint(start, end)
                # print('\nWaiting for', wait_time, 'seconds before jumping to new url...')
                sleep(wait_time)

    return html_dict
