#!/usr/bin/python3
"""Create per-country datasets from the HXL data at the UNHCR Population statistics site.

http://popstats.unhcr.org/en/overview

For each country, there will be two datasets: one listing refugees
originating from the country, and one listing refugees resident in the
country.

The script relies on a Google Sheet.  If that sheet becomes unavailable, there are backups
of its data tabs in Inputs/.

See README.md for more details.

Started 2015-10-13 by David Megginson
"""

import config

import ckanapi
import hxl
import urllib

import pprint

#
# Constants
#
COUNTRIES_URL = 'https://docs.google.com/spreadsheets/d/1tHbzC8F79wQhpLos7Zw2qLQJI-UzccddDt0ds7R88F8/edit#gid=1998541723'
DATASETS_URL = 'https://docs.google.com/spreadsheets/d/1tHbzC8F79wQhpLos7Zw2qLQJI-UzccddDt0ds7R88F8/edit#gid=778105659'
RESOURCES_URL = 'https://docs.google.com/spreadsheets/d/1tHbzC8F79wQhpLos7Zw2qLQJI-UzccddDt0ds7R88F8/edit#gid=828285269'


#
# Classes with logic for handling countries, datasets, and resources
#

class Country(object):
    """Logic for a country."""
    
    def __init__(self, hxl_row):
        self.row = hxl_row

    @property
    def code(self):
        return self.row.get('country+code+iso3')

    @property
    def name(self):
        return self.row.get('country+name+display')

    @property
    def unhcr_name(self):
        return self.row.get('country+name+unhcr')

    @property
    def article(self):
        return self.row.get('country+article+display')

    @property
    def full_name(self):
        if self.article:
            return "{} {}".format(self.article, self.name)
        else:
            return self.name

class Dataset(object):
    """Logic for a dataset."""

    def __init__(self, hxl_row, country):
        self.row = hxl_row
        self.country = country

    @property
    def stub(self):
        return self.row.get('id').format(self.country.code.lower())

    @property
    def category(self):
        return self.row.get('category')

    @property
    def title(self):
        s = self.row.get('title')
        return s.format(self.country.full_name)

    @property
    def description(self):
        return self.row.get('description+general').format(self.country.full_name)

class Resource(object):
    """Logic for a resource."""

    URL_PATTERN = 'http://proxy.hxlstandard.org/data.csv?url={url}&filter01=select&select-query01-01={pattern}={country}'

    def __init__(self, hxl_row, dataset):
        self.row = hxl_row
        self.dataset = dataset

    @property
    def name(self):
        return self.row.get('x_filename').format(self.dataset.category, self.dataset.country.code.lower())

    @property
    def description(self):
        s = self.row.get('title+' + self.dataset.category)
        return s.format(self.dataset.country.full_name)

    @property
    def url(self):
        source_url = urllib.parse.quote(self.row.get('x_resource+link+source'))
        pattern = urllib.parse.quote(self.dataset.row.get('x_pattern'))
        country_name = urllib.parse.quote(self.dataset.country.unhcr_name)
        return self.URL_PATTERN.format(url=source_url, pattern=pattern, country=country_name)

#
# Create or update the datasets
#
datasets = hxl.data(DATASETS_URL, True).cache() # cache for repeated use
resources = hxl.data(RESOURCES_URL, True).cache() # cache for repeated use

ckan = ckanapi.RemoteCKAN(config.CONFIG['ckanurl'], apikey=config.CONFIG['apikey'])

for country_row in hxl.data(COUNTRIES_URL, True):
    country = Country(country_row)
    for dataset_row in datasets:
        dataset = Dataset(dataset_row, country)
        tags = [{'name': tag.strip()} for tag in dataset.row.get('description+tags').split("\n")]
        dataset_object = {
            'name': dataset.stub,
            'title': dataset.title,
            'notes': dataset.description,
            'dataset_source': dataset.row.get('source'),
            'owner_org': 'unhcr',
            'package_creator': config.CONFIG['creator'],
            'license_id': dataset.row.get('description+license'),
            'methodology': dataset.row.get('description+method'),
            'caveats': dataset.row.get('description+caveats'),
            'groups': [{'id': country.row.get('country+code+iso3').lower()}],
            'tags': tags,
            'resources': []
        }
        for resource_row in resources:
            resource = Resource(resource_row, dataset)
            resource_object = {
                'name': resource.name,
                'description': resource.description,
                'url': resource.url,
                'mimetype': 'text/csv',
                'format': 'CSV'
            }
            dataset_object['resources'].append(resource_object)
        try:
            ckan.call_action('package_create', dataset_object)
            print("Created {}...".format(dataset.stub))
        except:
            ckan.call_action('package_update', dataset_object)
            print("Updated {}...".format(dataset.stub))

# end
