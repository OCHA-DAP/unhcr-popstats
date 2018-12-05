"""2018-12-05 patch for UNHCR PopStats datasets
Put each resource into its own dataset
"""
import ckancrawler, copy, logging, re, pprint
from config import CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("update")

DEFAULT_DELAY = 1

def crawl_unhcr_packages(ckanurl, apikey, delay=1):
    crawler = ckancrawler.Crawler(ckan_url=ckanurl, apikey=apikey, delay=delay)
    for package in crawler.packages(fq='organization:unhcr'):
        m = re.match('^refugees-(originating|residing)-([a-z]{3})$', package['name'])
        if m:
            logger.info('Updating %s...', package['name'])
            notes = "Information about UNHCR's populations of concern {context} {country}. The data appears in two formats: a \"wide\" format, with all indicators in the same row, and a \"long\" (timeline) format, with one indicator on each row. All are disaggregated by country of origin, country of residence, and year.\n\nPopulations of concern include refugees, asylum seekers, internally-displaced people (IDPs), returned IDPs, returned refugees, stateless people, and others of concern.".format(
                context="residing in" if m.group(1) == "residing" else "originating from",
                country=package['groups'][0]['display_name']
            )
            print(notes)
            package['notes'] = notes
            result = crawler.ckan.call_action('package_update', package)
        else:
            logger.warn('Skipping %s...', package['name'])


if __name__ == '__main__':
    crawl_unhcr_packages(CONFIG['ckanurl'], CONFIG['apikey'], DEFAULT_DELAY);
