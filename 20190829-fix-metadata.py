"""2019-02-04 fix resource metadata
All resources must have url_type="api" and resource_type="api"
"""

import ckanapi, ckancrawler, copy, logging, re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix-resource-metadata")
"""Python logging object"""

DEFAULT_DELAY = 0
"""Default delay between API calls, in seconds"""

PATTERNS = [
    r"(refugees-residing)-([a-z]{3})",
    r"(refugees-originating)-([a-z]{3})",
    r"unhcr-(time-series-residing)-([a-z]{3})",
    r"unhcr-(time-series-originating)-([a-z]{3})",
    r"unhcr-(demographics-residing)-([a-z]{3})",
    r"unhcr-(asylum-seekers-determination)-([a-z]{3})",
    r"unhcr-(asylum-seekers-residing)-([a-z]{3})",
    r"unhcr-(asylum-seekers-originating)-([a-z]{3})",
    r"unhcr-(resettlement-residing)-([a-z]{3})",
    r"unhcr-(resettlement-originating)-([a-z]{3})",
]
"""Regular expressions to match dataset shortnames.
First group is the type, and second is the ISO3 country code
"""

def update_resource_metadata(ckan, package):
    """Match a dataset short name against all known patterns.
    Invoked by scan_datasets()
    If successful, extract the type and country ISO3 code and invoke
    add_quickcharts().
    @param ckan: the CKAN API access object
    @param package: the CKAN package (dataset) structure
    """
    for pattern in PATTERNS:
        result = re.fullmatch(pattern, package["name"])
        if result:
            logger.info("Updating %s", package["name"])
            for resource in package["resources"]:
                resource["url_type"] = "api"
                resource["resource_type"] = "api"
                ckan.call_action("resource_update", resource)
            return
    logger.warning("Skipping %s", package["name"])
    
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
