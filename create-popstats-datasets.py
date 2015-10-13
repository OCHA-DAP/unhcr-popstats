#!/usr/bin/python

import ckan
import hxl

class Country(object):
    """Logic for a country."""
    
    def __init__(self, hxl_row):
        self.row = hxl_row

    @property
    def name(self):
        return self.row.get('country+name+display')

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
    def category(self):
        return self.row.get('category')

    @property
    def title(self):
        s = self.row.get('title')
        return s.format(self.country.full_name)

class Resource(object):
    """Logic for a resource."""

    def __init__(self, hxl_row, dataset):
        self.row = hxl_row
        self.dataset = dataset

    @property
    def title(self):
        s = self.row.get('title+' + self.dataset.category)
        return s.format(self.dataset.country.full_name)

    @property
    def title_residence(self):
        s = self.row.get('title+residence')
        return s.format(self.dataset.country.full_name)


#
# HXL-tagged input data
#
COUNTRIES_URL = 'https://docs.google.com/spreadsheets/d/1tHbzC8F79wQhpLos7Zw2qLQJI-UzccddDt0ds7R88F8/edit#gid=1998541723'
DATASETS_URL = 'https://docs.google.com/spreadsheets/d/1tHbzC8F79wQhpLos7Zw2qLQJI-UzccddDt0ds7R88F8/edit#gid=778105659'
RESOURCES_URL = 'https://docs.google.com/spreadsheets/d/1tHbzC8F79wQhpLos7Zw2qLQJI-UzccddDt0ds7R88F8/edit#gid=828285269'

datasets = hxl.data(DATASETS_URL).cache()
resources = hxl.data(RESOURCES_URL).cache()

for country_row in hxl.data(COUNTRIES_URL):
    country = Country(country_row)
    print(country.name)
    for dataset_row in datasets:
        dataset = Dataset(dataset_row, country)
        print('  ' + dataset.title)
        for resource_row in resources:
            resource = Resource(resource_row, dataset)
            print('    ' + resource.title)
