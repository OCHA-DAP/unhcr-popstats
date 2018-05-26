"""2018-05-25 patch for UNHCR PopStats datasets
Remove demographics from "originating" datasets
Fix HXL Proxy recipe for "residing" datasets
"""
import ckancrawler, logging, re
from config import CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix-demographics")

DELAY=1
"""Delay in seconds between ops"""

crawler = ckancrawler.Crawler(ckan_url=CONFIG['ckanurl'], apikey=CONFIG['apikey'], delay=DELAY)

for package in crawler.packages(fq='organization:unhcr'):
    m = re.match('^refugees-(originating|residing)-[a-z]{3}$', package['name'])
    if m:
        logger.info("Updating %s...", package['name'])
        loc = m.group(1)
        for resource in package['resources']:
            url = resource['url']
            if 'demographics' in url:
                if loc == 'originating':
                    try:
                        crawler.ckan.call_action('resource_delete', resource)
                        logger.info('Deleted %s', url)
                    except Exception as e:
                        logger.exception(e)
                else:
                    # need to correct the select filter
                    url = url.replace('country%2Borigin=', 'country%2Bresidence=')
                    if '&filter02=rename' not in url:
                        # need to fix a column hashtag that's wrong in the UNHCR source
                        url += '&filter02=rename&rename-oldtag02=country%2Borigin&rename-newtag02=loc%2Bname'
                    resource['url'] = url
                    try:
                        crawler.ckan.call_action('resource_update', resource)
                        logger.info('Updated %s', url)
                    except Exception as e:
                        logger.exception(e)
    else:
        logger.warn('Skipping %s...', package['name'])
    



