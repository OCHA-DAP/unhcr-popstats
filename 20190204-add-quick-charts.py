"""2019-02-04 enable Quick Charts for UNHCR datasets"""

import ckancrawler, copy, logging, re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("add-quickcharts")
"""Python logging object"""

DEFAULT_DELAY = 1
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

MODEL_QUICKCHARTS = [
    ["refugees-residing", "refugees-residing-jor"],
    ["refugees-originating", "refugees-originating-mmr"],
    ["time-series-residing", "unhcr-time-series-residing-nld"],
    ["time-series-originating", "unhcr-time-series-originating-mli"],
    ["demographics-residing", "unhcr-demographics-residing-nga"],
    ["asylum-seekers-determination", "unhcr-asylum-seekers-determination-dnk"],
    ["asylum-seekers-residing", "unhcr-asylum-seekers-residing-ita"],
    ["asylum-seekers-originating", "unhcr-asylum-seekers-originating-afg"],
    ["resettlement-residing", "unhcr-resettlement-residing-can"],
    ["resettlement-originating", "unhcr-resettlement-originating-syr"],
]
"""Datasets containing model Quick Charts configurations to copy to others of the same type"""

quickcharts_configurations = {}
"""Quick Charts configuration strings, keyed by dataset type (to be loaded)"""

def load_models(ckan):
    """Load model Quick Charts configurations
    Populates quickcharts_configurations
    @param ckan: the CKAN API access object
    """
    for entry in MODEL_QUICKCHARTS:
        package = ckan.action.package_show(id=entry[1])
        resource_id = package["resources"][0]["id"]
        views = ckan.action.resource_view_list(id=resource_id)
        for view in views:
            # Find the Quick Charts view
            if view["view_type"] == "hdx_hxl_preview":
                quickcharts_configurations[entry[0]] = view["hxl_preview_config"]
                logger.info("Loaded Quick Charts configuration for %s from %s", entry[0], entry[1])
                break
        if entry[0] not in quickcharts_configurations:
            # Oops! Couldn't find config.
            raise Exception("Failed to load model configuration for {}".format(entry[0]))

def add_quickcharts(ckan, package, dataset_type, iso3):
    """Add Quick Charts to a dataset after a match.
    Invoked by try_patterns()
    @param ckan: the CKAN API access object
    @param package: the CKAN package (dataset) structure
    @param dataset_type: a string identifying the dataset type (e.g. "refugees-originating")
    @param iso3: the ISO3 code for the country
    """

    logger.info("Adding Quick Charts to %s", package["name"])

    # Set up the package to preview
    package["dataset_preview"] = "first_resource",
    package["has_quickcharts"] = True
    ckan.call_action("package_update", package)

    # Set the Quick Charts configuration
    resource_id = package["resources"][0]["id"]
    views = ckan.action.resource_view_list(id=resource_id)
    for view in views:
        # Find the Quick Charts view and set the configuration
        if view["view_type"] == "hdx_hxl_preview":
            view["hxl_preview_config"] = quickcharts_configurations[dataset_type]
            ckan.call_action("resource_view_update", view)
            return

    # If we get to here, then we need to add the view
    logger.warn("Missing Quick Charts view for %s (creating)", package["name"])
    view = {
        "description": "",
        "title": "Quick Charts",
        "resource_id": resource_id,
        "view_type": "hdx_hxl_preview",
        "package_id": package["id"],
        "hxl_preview_config": quickcharts_configurations[dataset_type],
    }
    ckan.call_action("resource_view_create", view)

def try_patterns(ckan, package):
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
            add_quickcharts(ckan, package, result.group(1), result.group(2))
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
    load_models(crawler.ckan)
    for package in crawler.packages(fq="organization:unhcr"): # scan only UNHCR datasets
        try_patterns(crawler.ckan, package)

#
# Invoke as a command-line script using the info in config.py
#
if __name__ == '__main__':
    from config import CONFIG
    scan_datasets(CONFIG['ckanurl'], CONFIG['apikey'], DEFAULT_DELAY);
