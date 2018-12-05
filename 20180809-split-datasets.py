"""2018-07-04 patch for UNHCR PopStats datasets
Put each resource into its own dataset
"""
import ckancrawler, copy, logging, re, pprint
from config import CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix-demographics")

DEFAULT_DELAY = 1

SPECS = [
    {
        "stub_pattern": r"unhcr_time_series_residence_([a-z]{3}).csv",
        "title": "Time-series data for UNHCR's populations of concern residing in {country_name}",
        "name": "unhcr-time-series-residing-{country_code}",
        "notes": "information about UNHCR's populations of concern for a given year and country of residence. Data is presented as a yearly time series across the page.",
        "caveats": "In the data for the most-recent year, figures between 1 and 4 have been replaced with an asterisk (*). These represent situations where the figures are being kept confidential to protect the anonymity of individuals. Such figures are not included in any totals. Dataset may be empty if the UNHCR dataset does not currently contain any matching records.",
        "tags": ['asylum-seekers', 'hxl', 'idps', 'migration', 'refugee', 'refugees', 'returnees', 'stateless'],
        "url": "http://popstats.unhcr.org/en/time_series",
        "subnational": False,
        "start_date": "01/01/1951",
    },
    {
        "stub_pattern": r"unhcr_demographics_residence_([a-z]{3}).csv",
        "title": "Demographics for UNHCR's populations of concern residing in {country_name}",
        "name": "unhcr-demographics-residing-{country_code}",
        "notes": "Information about persons of concern broken down by sex and age, as well as by location within the country of residence (where such information is available). Such data is available since 2000.",
        "caveats": "Note that data broken down in this way is not always available, so it may not be possible to reconcile the figures on this page with those on the Persons of Concern and Time Series pages. Dataset may be empty if the UNHCR dataset does not currently contain any matching records.",
        "tags": ['hxl', 'migration', 'refugee', 'refugees', 'sadd'],
        "url": "http://popstats.unhcr.org/en/demographics",
        "subnational": True,
        "start_date": "01/01/2005",
    },
    {
        "stub_pattern": r"unhcr_asylum_seekers_residence_([a-z]{3}).csv",
        "title": "Refugee status determinations for asylum seekers in {country_name}",
        "name": "unhcr-asylum-seekers-determination-{country_code}",
        "notes": "Information about asylum applications in a given year and the progress of asylum-seekers through the refugee status determination process.",
        "caveats": "Data is available from 2000. Dataset may be empty if the UNHCR dataset does not currently contain any matching records.",
        "tags": ['asylum-seekers', 'hxl', 'migration'],
        "url": "http://popstats.unhcr.org/en/asylum_seekers",
        "subnational": False,
        "start_date": "01/01/2000",
    },
    {
        "stub_pattern": r"unhcr_asylum_seekers_monthly_residence_([a-z]{3}).csv",
        "title": "Monthly data on asylum seekers residing in {country_name}",
        "name": "unhcr-asylum-seekers-residing-{country_code}",
        "notes": "Information about asylum applications lodged in 38 European and 6 non-European countries. Data are broken down by month and origin. Where possible, figures exclude repeat/re-opened asylum applications and applications lodged on appeal or with courts. For some countries, the monthly data are available since 1999 while for others at a later period.",
        "caveats": "In the most-recent data, figures between 1 and 4 have been replaced with an asterisk (*). These represent situations where figures are being kept confidential to protect the anonymity of individuals. Such figures are not included in any totals. Due to retroactive adjustments implemented by States, totals in this dataset may differ from annual totals published by the competent national authorities. Dataset may be empty if the UNHCR dataset does not currently contain any matching records.",
        "tags": ['asylum-seekers', 'hxl', 'migration', 'refugee', 'refugees'],
        "url": "http://popstats.unhcr.org/en/asylum_seekers_monthly",
        "subnational": False,
        "start_date": "01/01/1999",
    },
    {
        "stub_pattern": r"unhcr_resettlement_residence_([a-z]{3}).csv",
        "title": "Resettlement arrivals of refugees in {country_name}",
        "name": "unhcr-resettlement-residing-{country_code}",
        "notes": "This page presents information on resettlement arrivals of refugees, with or without UNHCR assistance. This dataset is based on Government statistics and, in principle, excludes humanitarian admissions.",
        "caveats": "In the most-recent data, figures between 1 and 4 have been replaced with an asterisk (*). These represent situations where the figures are being kept confidential to protect the anonymity of individuals. Such figures are not included in any totals. Dataset may be empty if the UNHCR dataset does not currently contain any matching records.",
        "tags": ['hxl', 'migration', 'refugee', 'refugees', 'resettlement'],
        "url": "http://popstats.unhcr.org/en/resettlement",
        "subnational": False,
        "start_date": "01/01/1959",
    },
    {
        "stub_pattern": r"unhcr_time_series_origin_([a-z]{3}).csv",
        "title": "Time-series data for UNHCR's populations of concern originating from {country_name}",
        "name": "unhcr-time-series-originating-{country_code}",
        "notes": "information about UNHCR's populations of concern for a given year and country of origin. Data is presented as a yearly time series across the page.",
        "caveats": "In the data for the most-recent year, figures between 1 and 4 have been replaced with an asterisk (*). These represent situations where the figures are being kept confidential to protect the anonymity of individuals. Such figures are not included in any totals. Dataset may be empty if the UNHCR dataset does not currently contain any matching records.",
        "tags": ['asylum-seekers', 'hxl', 'idps', 'migration', 'refugee', 'refugees', 'returnees', 'stateless'],
        "url": "http://popstats.unhcr.org/en/time_series",
        "subnational": False,
        "start_date": "01/01/1951",
    },
    {
        "stub_pattern": r"unhcr_asylum_seekers_monthly_origin_([a-z]{3}).csv",
        "title": "Monthly data on asylum seekers originating from {country_name}",
        "name": "unhcr-asylum-seekers-originating-{country_code}",
        "notes": "Information about asylum applications lodged in 38 European and 6 non-European countries. Data are broken down by month and origin. Where possible, figures exclude repeat/re-opened asylum applications and applications lodged on appeal or with courts. For some countries, the monthly data are available since 1999 while for others at a later period.",
        "caveats": "In the most-recent data, figures between 1 and 4 have been replaced with an asterisk (*). These represent situations where figures are being kept confidential to protect the anonymity of individuals. Such figures are not included in any totals. Due to retroactive adjustments implemented by States, totals in this dataset may differ from annual totals published by the competent national authorities. Dataset may be empty if the UNHCR dataset does not currently contain any matching records.",
        "tags": ['asylum-seekers', 'hxl', 'migration', 'refugee', 'refugees'],
        "url": "http://popstats.unhcr.org/en/asylum_seekers_monthly",
        "subnational": False,
        "start_date": "01/01/1999",
    },
    {
        "stub_pattern": r"unhcr_resettlement_origin_([a-z]{3}).csv",
        "title": "Resettlement arrivals of refugees originating from {country_name}",
        "name": "unhcr-resettlement-originating-{country_code}",
        "notes": "This page presents information on resettlement arrivals of refugees, with or without UNHCR assistance. This dataset is based on Government statistics and, in principle, excludes humanitarian admissions.",
        "caveats": "In the most-recent data, figures between 1 and 4 have been replaced with an asterisk (*). These represent situations where the figures are being kept confidential to protect the anonymity of individuals. Such figures are not included in any totals. Dataset may be empty if the UNHCR dataset does not currently contain any matching records.",
        "tags": ['hxl', 'migration', 'refugee', 'refugees', 'resettlement'],
        "url": "http://popstats.unhcr.org/en/resettlement",
        "subnational": False,
        "start_date": "01/01/1959",
    },
]

def crawl_unhcr_packages(ckanurl, apikey, delay=1):
    crawler = ckancrawler.Crawler(ckan_url=ckanurl, apikey=apikey, delay=delay)
    for package in crawler.packages(fq='organization:unhcr'):
        m = re.match('^refugees-(originating|residing)-([a-z]{3})$', package['name'])
        if m:
            logger.info("Splitting {}".format(package['name']))
            situation = m.group(1)
            country_code = m.group(2)
            m = re.match(r'^UNHCR\'s populations of concern (?:originating from|residing in) (.+)$', package['title'])
            country_name = m.group(1)
            split_popstats_package(crawler.ckan, package, situation, country_name, country_code)
        else:
            logger.warn('Skipping %s...', package['name'])

def split_popstats_package(ckan, package, situation, country_name, country_code):
    resources = copy.deepcopy(package['resources'])
    for resource in resources:
        for spec in SPECS:
            m = re.match(spec['stub_pattern'], resource['name'])
            if m:
                
                name = spec['name'].format(country_code=country_code)
                
                new_package = copy.deepcopy(package)
                for key in ['id', 'dataset_preview', 'has_quickcharts', 'num_resources', 'num_tags', 'maintainer_email']:
                    del new_package[key]
                
                new_resource = copy.deepcopy(resource)
                for key in ['id', 'package_id', 'position']:
                    del new_resource[key]
                new_resource['name'] = name + ".csv"

                # new package info
                new_package['name'] = name
                new_package['title'] = spec['title'].format(country_name=country_name)
                new_package['notes'] = spec['notes']
                new_package['caveats'] = spec['caveats']
                new_package['tags'] = [{'name': tag} for tag in spec['tags']]
                new_package['subnational'] = spec['subnational']
                new_package['resources'] = [new_resource]

                # general cleanup
                new_package['data_update_frequency'] = '0' # live
                new_package['author'] = 'Laurent Pitoiset'
                new_package['author_email'] = 'pitoiset@unhcr.org'
                new_package['dataset_date'] = '{start_date}-12/31/2025'.format(start_date=spec['start_date'])
                new_package['maintainer'] = '7ae95211-71dd-484e-8538-2c625315eb56' # David Megginson

                #pprint.pprint(new_package)
                #exit()

                logger.info("    Creating dataset " + new_package['name'])
                ckan.call_action('package_create', new_package)

                for i, old_resource in enumerate(package['resources']):
                    if old_resource['id'] == resource['id']:
                        del package['resources'][i]
                        break
                
                # ckancrawler.action.call('resource_delete', resource)
                break

    logger.info("Updating dataset to remove old resources")

    package['notes'] = "Information about UNHCR's populations of concern in {country}. The data appears in two formats: a \"wide\" format, with all indicators in the same row, and a \"long\" (timeline) format, with one indicator on each row. All are disaggregated by country of origin, country of residence, and year."
    package['tags'] += [
        {"name": "idps"},
        {"name": "migration"},
        {"name": "refugees"},
        {"name": "returnees"},
        {"name": "stateless"},
    ]
    
    package['data_update_frequency'] = '0'
    package['author'] = 'Laurent Pitoiset'
    package['author_email'] = 'pitoiset@unhcr.org'
    package['dataset_date'] = '01/01/1951-12/31/2025'
    package['maintainer'] = '7ae95211-71dd-484e-8538-2c625315eb56'
    ckan.call_action('package_update', package)

if __name__ == '__main__':
    crawl_unhcr_packages(CONFIG['ckanurl'], CONFIG['apikey'], DEFAULT_DELAY);
