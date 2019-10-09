"""2019-10-09 fix caveats
Add new caveats as requested by UNHCR.
"""

import ckancrawler, logging, re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix caveats")
"""Python logging object"""

DEFAULT_DELAY = 0
"""Default delay between API calls, in seconds"""

NEW_CAVEATS = """In the most-recent data, figures between 1 and 4 have been replaced with an asterisk (*). These represent situations where figures are being kept confidential to protect the anonymity of individuals. Such figures are not included in any totals. Due to retroactive adjustments implemented by States, totals in this dataset may differ from annual totals published by the competent national authorities. Dataset may be empty if the UNHCR dataset does not currently contain any matching records."""

def update_resource_metadata(ckan, package):
    """Match a dataset short name against all known patterns.
    Invoked by scan_datasets()
    If successful, extract the type and country ISO3 code and invoke
    add_quickcharts().
    @param ckan: the CKAN API access object
    @param package: the CKAN package (dataset) structure
    """
    result = re.fullmatch(r"unhcr-(asylum-seekers-determination)-([a-z]{3})", package["name"]):
    if result:
        logger.info("Updating %s", package["name"])
        package["caveats"] = NEW_CAVEATS
        ckan.call_action("package_update", package)

def scan_datasets(ckanurl, apikey, delay=1):
    """Update resource metadata in matching datasets.
    This is the main external entry point.
    @param ckanurl: the URL of the CKAN installation
    @param apikey: the key for CKAN API access
    @param delay: the delay in seconds between API calls
    """
    crawler = ckancrawler.Crawler(ckan_url=ckanurl, apikey=apikey, delay=delay)
    for package in crawler.packages(fq="organization:unhcr"): # scan only UNHCR datasets
        update_resource_metadata(crawler.ckan, package)

#
# Invoke as a command-line script using the info in config.py
#
if __name__ == '__main__':
    from config import CONFIG
    scan_datasets(CONFIG['ckanurl'], CONFIG['apikey'], DEFAULT_DELAY);
