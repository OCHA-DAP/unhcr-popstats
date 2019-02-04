"""2019-02-04 enable Quick Charts for UNHCR datasets"""

import ckancrawler, copy, logging, re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("add-quickcharts")

DEFAULT_DELAY = 1

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

def add_quickcharts(package, dataset_type, iso3):
    """Add Quick Charts to a dataset after a match.
    @param package: the CKAN package (dataset) structure
    @param dataset_type: a string identifying the dataset type (e.g. "refugees-originating")
    @param iso3: the ISO3 code for the country
    """
    print(package["name"], dataset_type, iso3)

def try_patterns(package):
    """Scan a dataset short name against all known patterns.
    If sucessful, extract the type and country ISO3 code and invoke
    add_quickcharts().
    @param package: the CKAN package (dataset) structure
    """
    for pattern in PATTERNS:
        result = re.fullmatch(pattern, package["name"])
        if result:
            add_quickcharts(package, result.group(1), result.group(2))
            return
    logger.warning("Skipping %s", package["name"])
    
def scan_datasets(ckanurl, apikey, delay=1):
    """Add Quick Charts to matching datasets
    This is the main external entry point.
    @param ckanurl: the URL of the CKAN installation
    @param apikey: the key for CKAN API access
    @param delay: the delay in seconds between API calls
    """
    crawler = ckancrawler.Crawler(ckan_url=ckanurl, apikey=apikey, delay=delay)
    for package in crawler.packages(fq="organization:unhcr"):
        try_patterns(package)

#
# Invoke as a command-line script using the info in config.py
#
if __name__ == '__main__':
    from config import CONFIG
    scan_datasets(CONFIG['ckanurl'], CONFIG['apikey'], DEFAULT_DELAY);

