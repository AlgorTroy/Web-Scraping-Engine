from urllib.request import urlopen
from bs4 import BeautifulSoup
import re

html = urlopen('https://www.dataquest.io/blog/web-scraping-tutorial-python/').read()
soup = BeautifulSoup(html, 'html.parser')
texts = soup.findAll(text=True)


def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element)):
        return False
    return True

out = []

for element in texts:
    if visible(element):
        element = element.replace('\n', ' ')
        element = element.replace('\t', ' ')
        out.append(element)

# visible_texts = filter(visible, texts)

print(''.join(out))
# print(texts)
