"""
This program is designed to scrape metadata from a DHIS2 system and output it in human-readable format.
It expects there to be a JSON file containing base URL, username and password information, with
the location of the file stored in a .env variable.
"""

from xml.dom import minidom
import argparse
import getpass
import os
import re
import urllib2
import csv
import requests
import json

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

fieldnames = [
  'Indicator name',
  'Numerator description',
  'Denominator description',
  'Calculation',
  'Definition validation',
  'Data validation',
  'Comments'
]
    
# constructs the url the given data - inputs are the element type, id, and name.
def constructDisplayUrl(base_url, element_type, element_id, friendlyName):
	output_url = 'https://' + base_url + '/api/' + element_type + '/' + element_id
	return '=HYPERLINK(\"' + output_url + '\",' + '\"' + friendlyName + '\")'
  

# We expect dhis_params_dict to be a dictionary keyed by country; we expect
# values to be dicts having "baseUrl", "username", and "password" as keys.
# This dictionary should be stored in a JSON file.
# The path to this file should be stored in an environment variable named
# DHIS2_PARAMS_FILE.
# This returns a pair [full_login_url, display_url]; the former has username/
# password inherent in it and is never put into output.
def constructDhisUrls(country):
  dhis_params_dict = {}
  dhis_params_file = os.environ['DHIS2_PARAMS_FILE']
  with open(dhis_params_file, 'r') as ofh:
    dhis_params_dict = json.load(ofh)

  return ['https://' + dhis_params_dict[country]['username'] + ':' +
            dhis_params_dict[country]['password'] + '@' +
            dhis_params_dict[country]['baseUrl'],
          'https://' + dhis_params_dict[country]['baseUrl']]


class dhisParser():
  """ A class to parse DHIS2 system metadata
  
      :param country: country/DHIS2 system identifier
      :param indicator_group: specific indicatorGroup/dataElementGroup of interest
  """
  def __init__(self, country, group_id):
    self.country = country
    self.full_login_url, self.display_url = constructDhisUrls(country)
    self.group = group_id
    
    group_metadata_url = self.full_login_url + '/api/identifiableObjects/' +\
                         self.group
    r = requests.get(group_metadata_url)
    parsed_metadata = minidom.parse(r.content)

    group_url = parsed_metadata.getElementsByTagName('identifiableObject').get('href')
    group_type = group_url.split('/')[-2]
    authenticated_group_url = self.full_login_url + '/api' + group_type + '/' +\
                              self.group
    
    # This contains the parsed XML DOM of the indicator group, from which we can
    # retrieve a list of indicator ids.
    group_xml = minidom.parse(requests.get(authenticated_group_url).content)
    self.element_type = 'indicators' if (group_type == 'indicatorGroup') else 'dataElements'
    self.element_ids = group_xml.getElementsByTagName(self.element_type)
    self.element_names = {}
    self.values = {}
    
  def constructElementUrl(element_id):
    return self.full_login_url + '/api/' + self.element_type + '/' + element_id

  def getUnknownTypeMetadata(element_id):
    url = self.full_login_url + '/api/identifiableObjects/' + element_id
    idobj_metadata = minidom.parse(requests.get(url).content)
    md_url = idobj_metadata.getElementsByTagName('identifiableObject').get('href')
    
    return minidom.parse(requests.get(md_url).content)
    
  def getKnownTypeMetadata(element_id, element_type):
    url = self.full_login_url + '/api/' + element_type + '/' + element_id   
    return minidom.parse(requests.get(url).content)

  def getElementName(element_id):
    if element_id in self.element_names:
      return self.element_names[element_id]

    elementXml = getKnownTypeMetadata(element_id, self.element_type)
      if element_id in self.element_ids
      else getUnknownTypeMetadata(element_id);
    elements = elementXml.getElementsByTagName('displayName')

    for elem in elements:
      d_name = elem.firstChild.data
      
    self.element_names[element_id] = d_name
    return d_name 

  def getIndicatorDescription(indicator_id):
    indicator_xml = self.getKnownTypeMetadata(indicator_id, 'indicators');
  
    # create dictionary of values to write into csv file
    values = { key: '' for key in fieldnames }
	
    # store display name
    displayName = indicator_xml.getElementsByTagName('displayName')
    dNameValue = displayName[0].firstChild.data
    values['Indicator name'] = constructHyperLink('indicators', indicatorId, dNameValue)

    # store the numerator description
    numDesc = indicator_xml.getElementsByTagName('numeratorDescription')
    numDescValue = numDesc[0].firstChild.data
    values['Numerator description'] = numDescValue

    # store the denominator description
    denDesc = indicator_xml.getElementsByTagName('denominatorDescription')
    denDescValue = denDesc[0].firstChild.data
    values['Denominator description'] = denDescValue

    # get the numerator ids - currently with ids instead of friendly name(temporarily opening the direct file)
    numerator = indicator_xml.getElementsByTagName('numerator')
    numDescription = numerator[0].firstChild.data

    # get the denominator ids - currently with ids instead of friendly name
    denominator = indicator_xml.getElementsByTagName('denominator')
    denDescription = denominator[0].firstChild.data

    # convert the numerator and denominator dataElement ids with their descriptions
    # 	all possible elements: #{xxxxxx}, sometimes #{xxxxx.xxxxx}, operators (+,-,*), and numbers (int)
    #   create a list of id's, navigate to their url, and replace the num/den id's with the descriptions
    parsedNumDesc = re.finditer('(#\{\w*\.?\w*\})|[\+\-\/\*]|(\d*)', numDescription)
    parsedDenDesc = re.finditer('(#\{\w*\.?\w*\})|[\+\-\/\*]|(\d*)', denDescription)

    # iterate through parsed descriptions; extract friendly names of elements
    # and pass operators/numbers through as is.
    values['Calculation'] = '('
    for numItem in parsedNumDesc:
      if numItem.group(0).isdigit() || re.match('[\+\-\/\*]', numItem.group(0)):
        values['Calculation'] += ' ' + numItem.group(0)
      else:
        elements = re.match('#\{(\w*)\.?(\w*)\}', numItem.group(0))
        if elements:
          values['Calculation'] += ' ' + self.getElementName(elements.group(1))
          if elements.group(2):
            values['Calculation'] += ' ' + self.getElementName(elements.group(2))
    values['Calculation'] += ' ) / ('
    for denItem in parsedDenDesc:
      if (denItem.group(0).isdigit() || re.match('[\+\-\/\*]', denItem.group(0))):
        values['Calculation'] += ' ' + denItem.group(0)
      else:
        elements = re.match('#\{(\w*)\.?(\w*)\}', denItem.group(0))
        if elements:
          values['Calculation'] += ' ' + self.getElementName(elements.group(1))
          if elements.group(2):
            values['Calculation'] += ' ' + self.getElementName(elements.group(2))  
    values['Calculation'] += ' )'
    
    return values
    
  def outputAllIndicators():
    if self.element_type != 'indicators':
      return []
      
    output_values = []
    for indicator_id in self.element_ids:
      output_values.append(self.getIndicatorDescription(indicator_id))
      
    return output_values


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--country', default='Senegal',
                      help='Which country\'s DHIS2 system are we scraping')
  parser.add_argument('--output', default='testoutput.csv', help='Output file')
  parser.add_argument('--group_id', default='',
                      help='Specific indicatorGroup / dataElementGroup of interest')
  args = parser.parse_args()
  
  dhis_parser = dhisParser(args.country, args.group_id)
  
  output_values = dhis_parser.outputAllIndicators()

  with open(args.output, 'w') as ofh:
    ofh.write(fieldnames.join(','))
    for value in output_values:
      line = ''
      for field in fieldnames:
        line += (value[field] || '') + ','
      ofh.write(line[:-1])


