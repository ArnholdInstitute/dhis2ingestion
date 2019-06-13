"""
This program is designed to scrape metadata from a DHIS2 system and output it
in human-readable format. It expects either 1) a JSON file containing base
URL, username and password information, with the location of the file stored
in a .env variable or 2) an OAuth2 token.
"""

import argparse
import concurrent.futures
import json
import os
import re
import requests
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

NUM_THREADS = 10

fieldnames = [
  'Indicator name',
  'Numerator description',
  'Denominator description',
  'Calculation',
  'Definition validation code',
  'Data validation code',
  'Validation comments'
]

# bitmap error codes for indicator calculation validation
validation_errcodes = {
  1: 'calculation error',
  2: 'missing numerator/denominator',
  4: 'other missing fields (e.g. descriptions)',
  8: 'invalid id -- field is missing metadata',
  16: 'bad id -- field is not in registry'
}

dhis_params_dict = {}
try:
  with open(os.environ['DHIS2_PARAMS_FILE'], 'r') as ofh:
    dhis_params_dict = json.load(ofh)
except:
  print("No DHIS2_PARAMS_FILE env variable found", file=sys.stderr)


def translateErrCode(input_errcode):
  outputs = []
  for errcode, error in validation_errcodes:
    if (errcode & input_errcode):
      outputs.append(error)
  return ' & '.join(outputs) | 'valid'

  
# constructs the url the given data - inputs are the element type, id, and name.
def constructDisplayUrl(base_url, element_type, element_id, friendlyName):
  output_url = 'https://' + base_url + '/api/' + element_type + '/' + element_id
  display_url = '=HYPERLINK(\"' + output_url + '\";\"' + friendlyName + '\")'
  return output_url, display_url


# This returns a pair [full_login_url, display_url]; the former has username/
# password inherent in it and is never put into output.
def constructDhisUrls(auth_dict):
  return ['https://' + auth_dict['username'] + ':' + auth_dict['password'] +
            '@' + auth_dict['baseUrl'],
          'https://' + auth_dict[country]['baseUrl']]


def getAuthorizedJson(auth_dict, url):
  result = { 'text' : None }
  if 'token' in auth_dict:
    headers = { 'Authorization': 'Bearer ' + auth_dict['token'] }
    result = requests.get('https://' + url, headers=headers)
  else:
    auth_url = 'https://' + auth_dict['username'] + ':' + auth_dict['password']\
      + '@' + url
    result = requests.get(auth_url)
  return json.loads(result.text)

          
def getGroupIdsFromGroupDesc(auth_dict, group_desc):
  groups_url = auth_dict['baseUrl'] + '/api/indicatorGroups.json?paging=false'
  group_list = getAuthorizedJson(auth_dict, groups_url)
    
  # This contains the parsed JSON DOM of the indicator group list, from which we
  # can match indicator group display names against the group description.
  
  group_ids = []
  for indicator_group in group_list['indicatorGroups']:
    if re.search(group_desc, indicator_group['displayName'], re.IGNORECASE):
      group_ids.append(indicator_group['id'])

  return group_ids

  
# Takes in a dict, list, string, or number.  For a dict will take keys written
# as human-readable text, and convert to camel-cased strings for JSON output.
def camelCaseKeys(value_dict):
  if not type(value_dict) is dict: return value_dict
  output_dict = {}
  for key in value_dict:
    keySubstrings = key.split(' ')
    ccaseKeySubstrs = list(map(lambda x: x.lower().capitalize(), keySubstrings))
    capitalKey = ''.join(ccaseKeySubstrs)
    camelCaseKey = capitalKey[:1].lower() + capitalKey[1:]
    output_dict[camelCaseKey] = camelCaseKeys(value_dict[key])
    
  return output_dict
    
          
class dhisParser():
  """ A class to parse DHIS2 system metadata
  
      :param auth_dict: Dictionary of DHIS2 authorization data
  """
  def __init__(self, auth_dict):
    self.auth = auth_dict
    self.elt_id_to_desc = {}
    self.element_names = {}
    self.values = {}
    self.group = None
    self.group_desc = None
    self.element_type = None
    self.element_ids = []
    
  # group_id: DHIS2-internal id of indicatorGroup/dataElementGroup of interest
  def setGroupId(self, group_id):
    self.group = group_id
    group_metadata_url = self.auth['baseUrl'] + '/api/identifiableObjects/' + self.group
    parsed_metadata = getAuthorizedJson(self.auth, group_metadata_url)
    
    try:
      group_type = parsed_metadata['href'].split('/')[-2]
    except:
      print("No valid metadata found for group id {}".format(self.group), file=sys.stderr)
      raise
    group_url = self.auth['baseUrl'] + '/api/' + group_type + '/' + self.group
    
    # This contains the parsed JSON DOM of the indicator group, from which we
    # can retrieve a list of indicator ids.
    group_metadata = getAuthorizedJson(self.auth, group_url)
    self.group_desc = group_metadata['displayName']
    
    self.element_type = ('indicators'
      if (group_type == 'indicatorGroups') else 'dataElements')
    self.element_ids = list(map(lambda x: x['id'],
                                group_metadata[self.element_type]))
    
  def constructElementUrl(self, element_id):
    return self.auth['baseUrl'] + '/api/' + self.element_type + '/' + element_id

  # returns a validation warning if the element is not found.
  def getUnknownTypeMetadata(self, element_id):
    url = self.auth['baseUrl'] + '/api/identifiableObjects/' + element_id
    idobj_metadata = getAuthorizedJson(self.auth, url)
    if not idobj_metadata:
      return None, 16
    elt_type = idobj_metadata['href'].split('/')[-2]
    md_url = self.auth['baseUrl'] + '/api/' + elt_type + '/' + element_id
    
    return getAuthorizedJson(self.auth, md_url), 0
    
  def getKnownTypeMetadata(self, element_id, element_type):
    url = self.auth['baseUrl'] + '/api/' + element_type + '/' + element_id
    return getAuthorizedJson(self.auth, url), 0

  # This is the only place where we can get a race condition; indicator ids
  # will only show up once in the list of element ids and so we will query
  # their metadata exactly once. Data element ids however can show up in
  # the metadata of multiple indicators, so we need to be sure we only
  # query their metadata once.
  def getElementName(self, element_id):
    if element_id in self.element_names:
      return self.element_names[element_id]

    element_json, valid_code = \
      self.getKnownTypeMetadata(element_id, self.element_type) \
      if element_id in self.element_ids \
      else self.getUnknownTypeMetadata(element_id)
    # If the validation error is that the field is not in the registry, we want
    # that error (16) to get passed through -- if the error is that the field is
    # in the registry but the metadata is not there, we want to return error (8)
    if not element_json:
      return None, (valid_code or 8)
    d_name = element_json['displayName']
    if not d_name:
      self.element_names[element_id] = [None, 8]

    self.element_names[element_id] = [d_name, 0]
    return self.element_names[element_id] 

  def getIndicatorDescription(self, indicator_id):
    indicator_json, _ = self.getKnownTypeMetadata(indicator_id, 'indicators');
  
    # create dictionary of values to write into csv file
    values = { key: '' for key in fieldnames }
    values['Group Description'] = self.group_desc
    values['Definition validation code'] = 0

    if not indicator_json:
      values['Definition validation code'] = 8
      values['Validation comments'] = \
        'Indicator ' + indicator_id + ' not found - has no registry entry.'
      return values

    values['Validation comments'] = []

    # store display name
    displayName = '??????'
    if 'displayName' in indicator_json:
      displayName = indicator_json['displayName']
    else:
      values['Definition validation code'] = 4
      values['Validation comments'].append(
        'Indicator ' + indicator_id + ' has no display name.'
      )

    indicator_number_match = re.search(
      'pour\s*(\d+)|per\s*(\d+)|[\*\/]\s*(\d+)|(\d+)\*\s*',
      re.sub('\s', '', displayName)
    )
    indicator_number = None
    if indicator_number_match:
      indicator_number = int(
        indicator_number_match.group(1) or
        indicator_number_match.group(2) or
        indicator_number_match.group(3) or
        indicator_number_match.group(4)
      )

    values['Indicator name'] = displayName
    values['Indicator Url'], values['Display Url'] =\
      constructDisplayUrl(self.auth['baseUrl'],
                          'indicators',
                          indicator_id,
                          displayName)

    # store the numerator description
    values['Numerator description'] = '??????'
    if 'numeratorDescription' in indicator_json:
      values['Numerator description'] = indicator_json['numeratorDescription']
    else:
      values['Definition validation code'] = 4
      values['Validation comments'].append('No description of the numerator.')

    numerator_number_match = re.search(
      'pour\s*(\d+)|per\s*(\d+)|[\*\/]\s*(\d+)|(\d+)\*\s*',
      values['Numerator description']
    )
    numerator_number = None
    if numerator_number_match:
      numerator_number = int(
        numerator_number_match.group(1) or
        numerator_number_match.group(2) or
        numerator_number_match.group(3) or
        numerator_number_match.group(4)
      )
      
    # store the denominator description
    values['Denominator description'] = '1'
    if ('denominatorDescription' in indicator_json and 
        indicator_json['denominatorDescription'] != ''):
      values['Denominator description'] =\
        indicator_json['denominatorDescription']
    else:
      values['Definition validation code'] = 4
      values['Validation comments'].append(
        'No description of the denominator; we assume it is 1.'
      )
        
    denominator_number_match = re.search(
      'pour\s*(\d+)|per\s*(\d+)|[\*\/]\s*(\d+)|(\d+)\*\s*',
      values['Denominator description']
    )
    denominator_number = None
    denominator_number = None
    if denominator_number_match:
      denominator_number = int(
        denominator_number_match.group(1) or
        denominator_number_match.group(2) or
        denominator_number_match.group(3) or
        denominator_number_match.group(4)
      )
      if denominator_number == 1:
        denominator_number = None

    if (indicator_number and indicator_number != denominator_number and
        indicator_number != numerator_number):
      values['Definition validation code'] |= 1
      values['Validation comments'].append(
        'Indicator description has a number in it (' + str(indicator_number) +
        ') which does not appear in numerator or denominator descriptions.'
      )

    # get the numerator formula
    numerator = '??????'
    if 'numerator' in indicator_json:
      numerator = indicator_json['numerator']
    else:
      values['Definition validation code'] |= 2
      values['Validation comments'].append('Numerator has no formula.')

    # get the denominator formula
    denominator = '??????'
    if 'denominator' in indicator_json:
      denominator = indicator_json['denominator']
    else:
      values['Definition validation code'] |= 2
      values['Validation comments'].append('Denominator has no formula.')
    if (denominator == '1') ^ (values['Denominator description'] == '1'):
      values['Definition validation code'] |= 1
      values['Validation comments'].append(
        'Denominator formula does not match description.')

    # parse the numerator and denominator dataElement formulas to English
    # 	all possible elements: #{xxxxxx}, sometimes #{xxxxx.xxxxx}, 
    #   operators (+,-,*), and numbers (int).
    #   create a list of id's, navigate to their url, and replace the num/den
    #   id's with the descriptions
    parsed_num_form = re.finditer('(#\{\w*\.?\w*\})|[\+\-\/\*]|(\d*)',
                                  numerator)
    parsed_den_form = re.finditer('(#\{\w*\.?\w*\})|[\+\-\/\*]|(\d*)',
                                  denominator)

    # iterate through parsed formulas; extract friendly names of elements
    # and pass operators/numbers through as is.
    numerator_number_seen = (numerator_number is None)
    denominator_number_seen = (denominator_number is None)
    values['Calculation'] = '{'
    for num_item in parsed_num_form:
      if (num_item.group(0).isdigit() or
          re.match('[\+\-\/\*]', num_item.group(0))):
        values['Calculation'] += ' ' + num_item.group(0)
        if (num_item.group(0).isdigit() and
            int(num_item.group(0)) == numerator_number):
          numerator_number_seen = True
      else:
        elements = re.match('#\{(\w*)\.?(\w*)\}', num_item.group(0))
        if elements:
          data_elt_name, elt_vcode = self.getElementName(elements.group(1))
          values['Calculation'] += ' ' + (data_elt_name or '??????')
          if elt_vcode:
            values['Definition validation code'] |= 1 | elt_vcode
            vcomment = 'dataElement ' + elements.group(1) +\
              ' in numerator formula is not well defined - '
            vcomment += ('has no registry entry.' if elt_vcode == 16 else \
                         'has no valid metadata.')
            values['Validation comments'].append(vcomment)
          if elements.group(2):
            coc_name, coc_vcode = self.getElementName(elements.group(2))
            values['Calculation'] += ' ' + (coc_name or '??????')
            if coc_vcode:
              values['Definition validation code'] |= 1 | coc_vcode
              vcomment = 'categoryOptionCombo ' + elements.group(2) +\
                ' in numerator formula is not well defined - '
              vcomment += ('has no registry entry.' if elt_vcode == 16 else \
                         'has no valid metadata.')
              values['Validation comments'].append(vcomment)              

    if not numerator_number_seen:
      values['Validation comments'].append(
        'The numerator description contains a number (' +
        str(numerator_number) +
        ') which does not appear in the calculation of the numerator.'
      )
        
    values['Calculation'] += ' } / {'
    for den_item in parsed_den_form:
      if (den_item.group(0).isdigit() or
          re.match('[\+\-\/\*]', den_item.group(0))):
        values['Calculation'] += ' ' + den_item.group(0)
        if (den_item.group(0).isdigit() and
            denominator_number == int(den_item.group(0))):
          denominator_number_seen = True
      else:
        elements = re.match('#\{(\w*)\.?(\w*)\}', den_item.group(0))
        if elements:
          data_elt_name, elt_vcode = self.getElementName(elements.group(1))
          values['Calculation'] += ' ' + (data_elt_name or '??????')
          if elt_vcode:
            values['Definition validation code'] |= 1 | elt_vcode
            vcomment = 'dataElement ' + elements.group(1) +\
              ' in denominator formula is not well defined - '
            vcomment += ('has no registry entry.' if elt_vcode == 16 else \
                         'has no valid metadata.')
            values['Validation comments'].append(vcomment)
          if elements.group(2):
            coc_name, coc_vcode = self.getElementName(elements.group(2))
            values['Calculation'] += ' ' + (coc_name or '??????')
            if coc_vcode:
              vcomment = 'categoryOptionCombo ' + elements.group(2) +\
                ' in denominator formula is not well defined - '
              vcomment += ('has no registry entry.' if elt_vcode == 16 else \
                           'has no valid metadata.')
              values['Validation comments'].append(vcomment) 

    values['Calculation'] += ' }'

    if not denominator_number_seen:
      values['Validation comments'].append(
        'The denominator description contains a number (' +
        str(denominator_number) +
        ') which does not appear in the calculation of the denominator.'
      )
    
    return values
    
  def addDescToDict(self, indicator_id):
    if indicator_id in self.elt_id_to_desc: return
    self.elt_id_to_desc[indicator_id] =\
      self.getIndicatorDescription(indicator_id)
    return
    
  def outputAllIndicators(self):
    if self.element_type != 'indicators':
      return []
      
    output_values = []
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
      executor.map(self.addDescToDict, self.element_ids)
      
    for indicator_id in self.element_ids:
      output_values.append(self.elt_id_to_desc[indicator_id].copy())
      
    return output_values


def main(args):
  output_values = [] 
  output_format = 'csv'
  if args.output.lower() == 'json': output_format = 'json'
  
  auth = {}
  if args.country:
    auth = dhis_params_dict[args.country]
  if args.base_url:
    auth['baseUrl'] = args.base_url
  if args.auth_token:
    auth['token'] = args.auth_token

  dhis_parser = dhisParser(auth)

  group_ids = []
  if args.group_ids:
    group_ids = args.group_ids.split(',')
  elif args.group_desc:
    group_ids = getGroupIdsFromGroupDesc(auth, args.group_desc)
    
  for group_id in group_ids:
    try:
      dhis_parser.setGroupId(group_id)
      output_values += dhis_parser.outputAllIndicators()
    except:
      print("Failed to output indicators for group id {}".format(group_id), file=sys.stderr)

  if output_format == 'csv':
    print(','.join(fieldnames))
    for value in output_values:
      line = ''
      if 'Validation comments' in value:
        value['Validation comments'] = '\"' + \
            '\n'.join(value['Validation comments']) + '\"'
      if 'Indicator name' in value:
        value['Indicator name'] = value['Display Url']
      if 'Definition validation code' in value:
        value['Definition validation code'] =\
          str(value['Definition validation code'])
      for field in fieldnames:
        line += (value[field] or '') + ','
      print(line[:-1])
  elif output_format == 'json':
    final_output_vals = []
    for value in output_values:
      del value['Display Url']
      final_output_vals.append(camelCaseKeys(value))
    print(json.dumps({'indicators': final_output_vals}))


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--country', default='',
                      help='Which country\'s DHIS2 system are we scraping')
  parser.add_argument('--base_url', default='',
                      help='Base URL of DHIS2 system, assuming not stored in JSON')
  parser.add_argument('--auth_token', default='',
                      help='Authorization token for DHIS2 system, assuming access creds not stored')
  parser.add_argument('--output', default='csv', help='Output format (CSV or JSON)')
  parser.add_argument('--group_ids', default='',
                      help='Ids of specific indicatorGroups of interest (comma-separated)')
  parser.add_argument('--group_desc', default='',
                      help='One-word description of indicatorGroup of interest')
  args = parser.parse_args()
  main(args)
