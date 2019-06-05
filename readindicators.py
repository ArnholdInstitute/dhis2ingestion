"""
This program is designed to scrape metadata from a DHIS2 system and output it
in human-readable format. It expects there to be a JSON file containing base
URL, username and password information, with the location of the file stored
in a .env variable.
"""

import argparse
import os
import re
import requests
import json
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

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
with open(os.environ['DHIS2_PARAMS_FILE'], 'r') as ofh:
  dhis_params_dict = json.load(ofh)


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
  if 'token' in auth_dict:
    headers = { 'Authorization': 'Bearer ' + auth_dict['token'] }
    return json.loads(requests.get('https://' + url, header=headers).text)
  else:
    auth_url = 'https://' + auth_dict['username'] + ':' + auth_dict['password']\
      + '@' + url
    return json.loads(requests.get(auth_url).text)

          
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
  
      :param group_id: DHIS2-internal id of indicatorGroup/dataElementGroup of interest
  """
  def __init__(self, auth_dict, group_id):
    self.auth = auth_dict
    self.group = group_id
    
    group_metadata_url = auth_dict['baseUrl'] + '/api/identifiableObjects/' + self.group
    parsed_metadata =  getAuthorizedJson(self.auth, group_metadata_url)

    group_type = parsed_metadata['href'].split('/')[-2]
    group_url = auth_dict['baseUrl'] + '/api/' + group_type + '/' + self.group
    
    # This contains the parsed JSON DOM of the indicator group, from which we
    # can retrieve a list of indicator ids.
    group_metadata = getAuthorizedJson(self.auth, group_url)
    self.group_desc = group_metadata['displayName']

    self.element_type = ('indicators'
      if (group_type == 'indicatorGroups') else 'dataElements')
    self.element_ids = list(map(lambda x: x['id'],
                                group_metadata[self.element_type]))
    self.element_names = {}
    self.values = {}
    
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
    
    values['Definition validation code'] = \
      str(values['Definition validation code'])
#    values['Validation comments'] = '\"' + \
#      '\n'.join(values['Validation comments']) + '\"'
    
    return values
    
  def outputAllIndicators(self):
    if self.element_type != 'indicators':
      return []
      
    output_values = []
    for indicator_id in self.element_ids:
      output_values.append(self.getIndicatorDescription(indicator_id).copy())
      
    return output_values


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--country', default='Senegal',
                      help='Which country\'s DHIS2 system are we scraping')
  parser.add_argument('--base_url', default='',
                      help='Base URL of DHIS2 system, assuming not stored in JSON')
  parser.add_argument('--auth_token', default='',
                      help='Authorization token for DHIS2 system, assuming access creds not stored')
  parser.add_argument('--output', default='testoutput.csv',
                      help='Output file (CSV or JSON)')
  parser.add_argument('--group_id', default='',
                      help='Id of specific indicatorGroup / dataElementGroup of interest')
  parser.add_argument('--group_desc', default='',
                      help='One-word description of indicatorGroup of interest')
  args = parser.parse_args()
  
  output_values = []
  
  output_format = 'csv'
  if re.search('\.json', args.output): output_format = 'json'
  
  auth = {}
  if args.country:
    auth = dhis_params_dict[args.country]
  if args.base_url:
    auth['baseUrl'] = args.base_url
  if args.auth_token:
    auth['token'] = args.auth_token
  
  if args.group_id:
    dhis_parser = dhisParser(auth, args.group_id)
    output_values = dhis_parser.outputAllIndicators()
  elif args.group_desc:
    group_ids = getGroupIdsFromGroupDesc(auth, args.group_desc)
    for group_id in group_ids:
      dhis_parser = dhisParser(auth, group_id)
      output_values += dhis_parser.outputAllIndicators()

  with open(args.output, 'w') as ofh:
    if output_format == 'csv':
      ofh.write(','.join(fieldnames) + '\n')
      for value in output_values:
        line = ''
        if 'Validation comments' in value:
          value['Validation comments'] = '\"' + \
              '\n'.join(value['Validation comments']) + '\"'
        if 'Indicator name' in value:
          value['Indicator name'] = value['Display Url']
        for field in fieldnames:
          line += (value[field] or '') + ','
        ofh.write(line[:-1] + '\n')
    elif output_format == 'json':
      final_output_vals = []
      for value in output_values:
        del value['Display Url']
        final_output_vals.append(camelCaseKeys(value))
      ofh.write(json.dumps({'indicators': final_output_vals}))


