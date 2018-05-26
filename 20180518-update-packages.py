import datetime, logging, re
from ckancrawler import Crawler
from config import CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('update-packages')

crawler = Crawler(ckan_url=CONFIG['ckanurl'], apikey=CONFIG['apikey'])

for package in crawler.packages(fq='organization:unhcr'):
    if re.match('^refugees-(originating|residing)-[a-z]{3}$', package['name']):
        logger.info("Updating %s...", package['name'])
        crawler.ckan.call_action('package_hxl_update', package)
    else:
        logger.warn('Skipping %s...', package['name'])
    
