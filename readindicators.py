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
from enum import Enum
load_dotenv(find_dotenv())

NUM_THREADS = 10

fieldnames = [
  'Group Description',
  'Indicator id',
  'Indicator name',
  'Numerator description',
  'Denominator description',
  'Calculation'
]

# Error messages for validation.
# English language messages are provided as a template;
#  output will be message id and fields for the fill-in-the-blanks.
class ValidationErrCode(Enum):
  NO_ERRORS = (0, 'No errors found in indicator', 0)
  INDIC_NOT_IN_REG = (1, 'Indicator ___ not in registry', 1)
  INDIC_NO_DISPLAY_NAME = (2, 'Indicator ___ has no display name', 1)
  NUMER_NO_DESC = (3, 'No description of the numerator', 0)
  DENOM_NO_DESC = (4, 'No description of the denominator; we assume it is 1', 0)
  NUMER_NO_FORMULA = (5, 'Numerator has no formula', 0)
  DENOM_NO_FORMULA = (6, 'Denominator has no formula', 0)
  DENOM_FORMULA_NO_MATCH = (7, 'Denominator formula does not match description', 0)
  INDIC_NUMBER_MISSING = (8, ('Indicator description has a number in it (___)'
                              ' which does not appear in numerator or'
                              ' denominator descriptions or the indicator'
                              ' type'), 1)
  FORMULA_NUMBER_MISSING = (9, ('___ description contains a number (___)'
                              ' which does not appear in the formula'), 2)
  VBL_NOT_IN_REG = (10, ('Variable ___ appearing in the formula for ___ is not'
                         ' in the registry'), 2)
  VBL_NO_METADATA = (11, ('Variable ___ of type ___ appearing in the formula'
                          ' for ___ has no valid metadata'), 3)
  NUMER_EQS_DENOM = (12, ('Numerator and denominator have the same formula'), 0)
  INDIC_PARSE_FAILED = (13, ('Parsing of indicator ___ failed'), 1)


  def __init__(self, index, eng_errmsg_template, num_blanks):
    self.index = index
    self.eng_errmsg_template = eng_errmsg_template
    self.num_blanks = num_blanks
    
  # takes in a list of values to fill in the blanks in the error message
  # template; fills in those blanks and returns the full error message.
  def eng_errmsg(self, fillin_list):
    if len(fillin_list) != self.num_blanks:
      raise ValueError("Validation error code #" + str(self.index) + " of type "
                       + self.name + " takes in " + str(self.num_blanks) +\
                       " values")
    errmsg = self.eng_errmsg_template
    for i in range(self.num_blanks):
      errmsg = re.sub('___', str(fillin_list[i]), errmsg)
    return errmsg


dhis_params_dict = {}
try:
  with open(os.environ['DHIS2_PARAMS_FILE'], 'r') as ofh:
    dhis_params_dict = json.load(ofh)
except:
  print("No DHIS2_PARAMS_FILE env variable found", file=sys.stderr)

  
# constructs the url the given data - inputs are the element type, id, and name.
def construct_display_url(base_url, element_type, element_id, friendly_name):
  output_url = 'https://' + base_url + '/api/' + element_type + '/' + element_id
  display_url = '=HYPERLINK(\"' + output_url + '\";\"' + friendly_name + '\")'
  return output_url, display_url


# This returns a pair [full_login_url, display_url]; the former has username/
# password inherent in it and is never put into output.
def construct_dhis_urls(auth_dict):
  return ['https://' + auth_dict['username'] + ':' + auth_dict['password'] +
            '@' + auth_dict['baseUrl'],
          'https://' + auth_dict[country]['baseUrl']]


def get_authorized_json(auth_dict, url):
  result = { 'text' : None }
  if 'token' in auth_dict:
    headers = { 'Authorization': 'Bearer ' + auth_dict['token'] }
    result = requests.get('https://' + url, headers=headers)
  else:
    auth_url = 'https://' + auth_dict['username'] + ':' + auth_dict['password']\
      + '@' + url
    result = requests.get(auth_url)
  try:
    return json.loads(result.text)
  except:
    return json.loads({ 'text' : None })

          
def get_group_ids_from_group_desc(auth_dict, group_desc):
  if not 'baseUrl' in auth_dict: return []
  groups_url = auth_dict['baseUrl'] + '/api/indicatorGroups.json?paging=false'
  group_list = get_authorized_json(auth_dict, groups_url)
    
  # This contains the parsed JSON DOM of the indicator group list, from which we
  # can match indicator group display names against the group description. 
  group_ids = []
  if 'indicatorGroups' in group_list:
    for indicator_group in group_list['indicatorGroups']:
      if 'displayName' in indicator_group and \
        re.search(group_desc, indicator_group['displayName'], re.IGNORECASE):
        group_ids.append(indicator_group['id'])

  return group_ids


# Takes in a string, depluralizes it (English-only)
def deplural(in_string):
  if not in_string or not type(in_string) is string: return ''
  if in_string[-1] == 's' and in_string[-2] != 's':
    return in_string[:-1]
  else: return in_string

  
# Takes in a dict, list, string, or number.  For a dict will take keys written
# as human-readable text, and convert to camel-cased strings for JSON output.
def camel_case_keys(value_dict):
  if not type(value_dict) is dict: return value_dict
  output_dict = {}
  for key in value_dict:
    key_substrings = str(key).split(' ')
    ccase_key_substrs = list(map(lambda x: x.lower().capitalize(),
                                 key_substrings))
    capital_key = ''.join(ccase_key_substrs)
    camel_case_key = capital_key[:1].lower() + capital_key[1:]
    # We want to exclude validation codes from camel-casing
    if camel_case_key == 'validationCodes':
      output_dict[camel_case_key] = value_dict[key]
    else:
      output_dict[camel_case_key] = camel_case_keys(value_dict[key])
    
  return output_dict


# Extracts a numerical factor from display text.
# TODO: More intelligent parsing
#   1) multiple languages
#   2) figure out what to do about American/European difference in definition
#      of million, billion etc.
#   3) generally improve i18n
#   4) handle decimal extraction
def extract_numerical_factor(display_text, is_multiplicative=False):
  factor_dict = { 'ten': 10, 'hundred': 100, 'thousand': 1000,
                  'million': 1000000, 'billion': 1000000000, 'percent': 100 }
  factor_regex = '(?:^|\s)(' + '|'.join(factor_dict.keys()) + ')(?:\s|$)'
  
  # If we are looking for a multiplicative factor, we either want 'per 1000'
  # or '* 1000' or '1000 *', for example. Only relevant to display text for
  # indicators/numerators/denominators -- generally not relevant to 
  # indicatorType display text.
  number_regex = '(?:pour|per|par|[\*\/])(\d+)|(\d+)\*' if is_multiplicative \
    else '(\d+)'
  number_match = re.search(number_regex, re.sub('\s|,', '', display_text))
  if number_match:
    number = int(number_match.group(1)) if len(number_match.groups()) < 3 else \
      int(number_match.group(1) or number_match.group(2))
    return number

  # Bit of a cheaty way of capturing "per thousand", "per ten thousand", and
  # "per ten-thousand". Might be better to do a split on [_\W], and match
  # against the keys. TODO?
  factor_match = re.search(factor_regex,
                           re.sub('\s|\-', '  ', display_text))
  if factor_match:
    number = 1
    for factor in re.finditer(factor_regex,
                              re.sub('\s|\-', '  ', display_text)):
      number *= factor_dict[factor.group(1)]
    return number

  return None

          
class DHIS2Parser():
  """ A class to parse DHIS2 system metadata for indicatorGroups.
      Will technically also work for dataElementGroups.
  
      :param auth_dict: Dictionary of DHIS2 authorization data
  """
  def __init__(self, auth_dict):
    self.values = {}                    # Stores output
    self.group = None                   # Stores group id of current group being looked up
    self.group_desc = None              # Stores display name of current group

    if not 'baseUrl' in auth_dict:
      raise ValueError('Invalid DHIS2 system URL')
    self._auth = auth_dict              # Stores authorization for DHIS2 instance
    self._indic_to_desc = {}            # Maps indicator to description
    self._vbl_names = {}                # Maps vbl_id to [display name, lookup error code, variable type]
    self._element_type = None           # Stores "type" of group elements -- generally indicators
    self._element_ids = []              # Stores list of group elements in current group
    self._indicator_type_map = {}       # Maps indicatorType id to a number.
    
    try:
      self._get_indicator_type_map()      # Populates above map.
    except err:
      raise
    
  # Indicator types are e.g. "number", "percent", "per thousand";
  # we want a mapping from indicator type id to numerical factor in the denominator.
  def _get_indicator_type_map(self):
    it_url = self._auth['baseUrl'] + '/api/indicatorTypes'
    it_metadata = get_authorized_json(self._auth, it_url)

    if not 'indicatorTypes' in indic_type:
      raise ValueError('DHIS2 system misconfigured? Missing indicatorTypes')

    for indic_type in it_metadata['indicatorTypes']:
      if 'id' in indic_type:
        md_url = it_url + '/' + indic_type['id']
        parsed_metadata = get_authorized_json(self._auth, md_url)
        if 'factor' in parsed_metadata:
          self._indicator_type_map[indic_type['id']] = \
            parsed_metadata['factor']
        else:
          number = 1
          if 'displayName' in indic_type:
            number = extract_numerical_factor(indic_type['displayName'],
                                              False)
            if not number:
              number = 1
          self._indicator_type_map[indic_type['id']] = number
    
  # group_id: DHIS2-internal id of indicatorGroup/dataElementGroup of interest
  def set_group_id(self, group_id):
    self.group = group_id
    group_metadata_url = self._auth['baseUrl'] + '/api/identifiableObjects/' +\
      self.group
    parsed_metadata = get_authorized_json(self._auth, group_metadata_url)
    
    try:
      group_type = parsed_metadata['href'].split('/')[-2]
    except:
      errmsg = 'Group id ' + self.group + ' not found in registry'
      print(errmsg, file=sys.stderr)
      raise ValueError(errmsg)

    group_url = self._auth['baseUrl'] + '/api/' + group_type + '/' + self.group
    
    # This contains the parsed JSON DOM of the indicator group, from which we
    # can retrieve a list of indicator ids.
    group_metadata = get_authorized_json(self._auth, group_url)

    if not 'displayName' in group_metadata:
      raise ValueError(
        'Group id ' + self.group + ' does not have valid metadata'
      )
    self.group_desc = group_metadata['displayName']
    
    self._element_type = re.sub('Group', '', group_type, re.IGNORECASE)
    self._element_ids = list(map(lambda x: x['id'],
                                 group_metadata[self._element_type]))
    
  def _construct_element_url(self, element_id):
    return self._auth['baseUrl'] + '/api/' + self._element_type + '/' +\
      element_id

  # returns a validation warning if the element is not found.
  def _get_unknown_type_metadata(self, element_id):
    url = self._auth['baseUrl'] + '/api/identifiableObjects/' + element_id
    idobj_metadata = get_authorized_json(self._auth, url)
    if not idobj_metadata or 'href' not in idobj_metadata:
      return None, ValidationErrCode.VBL_NOT_IN_REG, None
    elt_type = idobj_metadata['href'].split('/')[-2]
    md_url = self._auth['baseUrl'] + '/api/' + elt_type + '/' + element_id
    
    return get_authorized_json(self._auth, md_url), \
      ValidationErrCode.NO_ERRORS, elt_type
    
  def _get_known_type_metadata(self, element_id, element_type):
    url = self._auth['baseUrl'] + '/api/' + element_type + '/' + element_id
    return get_authorized_json(self._auth, url), \
      ValidationErrCode.NO_ERRORS, element_type

  # This is the only place where we can get a race condition; indicator ids
  # will only show up once in the list of element ids and so we will query
  # their metadata exactly once. Data element ids however can show up in
  # the metadata of multiple indicators, so we'd like to be sure we only
  # query their metadata once.
  def _get_variable_name(self, vbl_id):
    if vbl_id in self._vbl_names:
      return self._vbl_names[vbl_id]

    vbl_json, valid_code, vbl_type = \
      self._get_known_type_metadata(vbl_id, self._element_type) \
      if vbl_id in self._element_ids \
      else self._get_unknown_type_metadata(vbl_id)
    vbl_type = deplural(vbl_type)

    # If the validation error is that the field is not in the registry, we want
    # that error (16) to get passed through -- if the error is that the field is
    # in the registry but the metadata is not there, we want to return error (8)
    if not vbl_json:
      if valid_code == ValidationErrCode.NO_ERRORS:
        valid_code = ValidationErrCode.VBL_NO_METADATA
      self._vbl_names[vbl_id] = [None, valid_code, vbl_type]
      return self._vbl_names[vbl_id]
    if 'displayName' in vbl_json and vbl_json['displayName']:
      self._vbl_names[vbl_id] = \
        [ vbl_json['displayName'], ValidationErrCode.NO_ERRORS, vbl_type ]
    else:
      self._vbl_names[vbl_id] = \
        [ None, ValidationErrCode.VBL_NO_METADATA, vbl_type ]

    return self._vbl_names[vbl_id] 
     
  # Parses the formula for numerator or denominator, outputs a human-readable
  # calculation and a list of validation "values".
  def _parse_formula(self, formula, number, quantity_type):
    calculation = ''
    vvalues = []
    
    # We are going to want to keep track of variables that show up in a given
    # formula so that we don't duplicate error messages.
    vbls_seen = []
  
    # all possible formula terms: ([#ACDIR]|OUG){xxxx.xxx.xx} sometimes 
    #   with wildcards (*) or fewer sub-terms, operators (+,-,*), and numbers.
    # see https://docs.dhis2.org/master/en/developer/html/webapi_indicators.html
    # create a list of id's, navigate to their url, and replace the num/den
    #   id's with the descriptions.
    vbl_prefix_regex = '(?:[#ACDIR]|OUG)'
    vbl_regex = '(' + vbl_prefix_regex +\
                '\{([\w|\*]*)\.?([\w|\*]*)\.?([\w|\*|_]*)\})'
    oper_regex = '([\+\-\/\*])'
    number_regex = '(\d+\.\d+)'
    total_regex = vbl_regex + '|' + oper_regex + '|' + number_regex
    
    parsed_formula = re.finditer(total_regex, formula)
    number_seen = (number is None)
    
    for term in parsed_formula:
      if (re.match(number_regex, term.group(0)) or
          re.match(oper_regex, term.group(0))):
        calculation += ' ' + term.group(0)
        if (term.group(0).isdigit() and
            int(term.group(0)) == number):
          number_seen = True
      else:
        elements = re.match(vbl_regex, term.group(0))
        # Our variable naming convention assumes we are dealing with a variable
        # of type #{dataElement.categoryOptionCombo.attributeOptionCombo}.
        # Our _logic_, however, will work with all indicator variable types.
        if elements:
          data_elt_name, elt_vcode, elt_type =\
            self._get_variable_name(elements.group(2))
          calculation += ' ' + (data_elt_name or '??????')
          if not elements.group(2) in vbls_seen:
            vbls_seen.append(elements.group(2))
            if elt_vcode != ValidationErrCode.NO_ERRORS:
              elt_vvalues = [elements.group(2), elt_type, quantity_type] \
                if elt_type else [elements.group(2), quantity_type]
              vvalues.append([ elt_vcode, elt_vvalues ])
          if elements.group(3) and elements.group(3) != '*':
            coc = elements.group(3)
            # If elements.group(2) is a dataset, then group(3) could be a metric.
            # In which case we want to insert it into the calculation as is and
            # not report an error.
            if elt_type == 'dataSet' and re.search('_', coc):
              calculation += ' ' + coc
            else:
              coc_name, coc_vcode, coc_type = self._get_variable_name(coc)
              calculation += ' ' + (coc_name or '??????')
              if not coc in vbls_seen: 
                vbls_seen.append(coc)
                if coc_vcode != ValidationErrCode.NO_ERRORS:
                  coc_vvalues = [coc, coc_type, quantity_type] if coc_type \
                    else [coc, quantity_type]
                  vvalues.append([ coc_vcode, coc_vvalues ])
          if elements.group(4) and elements.group(4) != '*':
            aoc = elements.group(4)
            aoc_name, aoc_vcode, aoc_type = self._get_variable_name(aoc)
            calculation += ' ' + (aoc_name or '??????')
            if not aoc in vbls_seen: 
              vbls_seen.append(aoc)
              if aoc_vcode != ValidationErrCode.NO_ERRORS:
                aoc_vvalues = [aoc, aoc_type, quantity_type] if aoc_type \
                  else [aoc, quantity_type]
                vvalues.append([ aoc_vcode, aoc_vvalues ])
    if not number_seen:
      vvalues.append(
        [ValidationErrCode.FORMULA_NUMBER_MISSING, [quantity_type, str(number)]]
      )

    return calculation, vvalues
    
  def _get_indicator_description(self, indicator_id):
    indicator_json, _, _ = self._get_known_type_metadata(indicator_id,
                                                         'indicators');
 
    # create dictionary of values to write into csv file
    values = { key: '' for key in fieldnames }
    values['Validation values'] = []
    values['Indicator id'] = indicator_id

    if not indicator_json:
      values['Validation values'].append(
        [ ValidationErrCode.INDIC_NOT_IN_REG, [indicator_id] ]
      )
      return values

    # store display name
    display_name = ''
    if 'displayName' in indicator_json:
      display_name = indicator_json['displayName']
    else:
      values['Validation values'].append(
        [ ValidationErrCode.INDIC_NO_DISPLAY_NAME, [indicator_id] ]
      )
    values['Indicator name'] = display_name

    indicator_type_number = 1
    if 'indicatorType' in indicator_json and \
      'id' in indicator_json['indicatorType']:
      it_id = indicator_json['indicatorType']['id']
      if it_id in self._indicator_type_map:
        indicator_type_number = self._indicator_type_map[it_id]
    indicator_number = extract_numerical_factor(display_name, True)
    if indicator_number == 1: indicator_number = None

    values['Indicator Url'], values['Display Url'] =\
      construct_display_url(self._auth['baseUrl'],
                            'indicators',
                            indicator_id,
                            display_name)

    # store the numerator description
    values['Numerator description'] = '??????'
    if 'numeratorDescription' in indicator_json:
      values['Numerator description'] = indicator_json['numeratorDescription']
    else:
      values['Validation values'].append([ValidationErrCode.NUMER_NO_DESC, []])

    numerator_number = extract_numerical_factor(
      values['Numerator description'], True
    )
      
    # store the denominator description
    values['Denominator description'] = '1'
    if ('denominatorDescription' in indicator_json and 
        indicator_json['denominatorDescription'] != ''):
      values['Denominator description'] =\
        indicator_json['denominatorDescription']
    else:
      values['Validation values'].append([ValidationErrCode.DENOM_NO_DESC, []])
        
    denominator_number = extract_numerical_factor(
      values['Denominator description'], True
    )
    if denominator_number == 1:
      denominator_number = None

    if (indicator_number and
        indicator_number != denominator_number and
        indicator_number != numerator_number and
        indicator_number != indicator_type_number):
      values['Validation values'].append(
        [ ValidationErrCode.INDIC_NUMBER_MISSING, [str(indicator_number)] ]
      )

    # get the numerator formula
    numerator = '??????'
    if 'numerator' in indicator_json:
      numerator = indicator_json['numerator']
    else:
      values['Validation values'].append(
        [ ValidationErrCode.NUMER_NO_FORMULA, [] ]
      )

    # get the denominator formula
    denominator = '??????'
    if 'denominator' in indicator_json:
      denominator = indicator_json['denominator']
    else:
      values['Validation values'].append(
        [ ValidationErrCode.DENOM_NO_FORMULA, [] ]
      )
    if (denominator == '1') ^ (values['Denominator description'] == '1'):
      values['Validation values'].append(
        [ ValidationErrCode.DENOM_FORMULA_NO_MATCH, [] ]
      )
      
    # TODO: make this check also test term reordering (that is, A+B+C = A+C+B)
    if numerator == denominator:
      values['Validation values'].append(
        [ ValidationErrCode.NUMER_EQS_DENOM, [] ]
      )

    # do the parsing of the calculation
    values['Calculation'] = '{'

    numer_calc, numer_vvalues = self._parse_formula(numerator,
                                                    numerator_number,
                                                    'numerator')
    values['Calculation'] += numer_calc
    values['Validation values'].extend(numer_vvalues)
      
    values['Calculation'] += ' } / {'

    denom_calc, denom_vvalues = self._parse_formula(denominator,
                                                    denominator_number,
                                                    'denominator')
    values['Calculation'] += denom_calc
    values['Validation values'].extend(denom_vvalues)

    values['Calculation'] += ' }'

    return values
    
  def _add_desc_to_dict(self, indicator_id):
    if indicator_id in self._indic_to_desc: return
    self._indic_to_desc[indicator_id] =\
      self._get_indicator_description(indicator_id)
    return
    
  def output_all_indicators(self):
    if self._element_type != 'indicators':
      return []
      
    output_values = []
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
      executor.map(self._add_desc_to_dict, self._element_ids)
      
    for indicator_id in self.element_ids:
      tmp_desc = dict(zip(fieldnames, map(lambda x: '', fieldnames)))
      tmp_desc['Validation values'] = [
        [ ValidationErrCode.INDIC_PARSE_FAILED, [indicator_id] ]
      ]
      if indicator_id in self.indic_to_desc:
        tmp_desc = self.indic_to_desc[indicator_id].copy()
      tmp_desc['Group Description'] = self.group_desc
      output_values.append(tmp_desc)
      
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

  dhis_parser = DHIS2Parser(auth)

  group_ids = []
  if args.group_ids:
    group_ids = args.group_ids.split(',')
  elif args.group_desc:
    group_ids = get_group_ids_from_group_desc(auth, args.group_desc)
    
  for group_id in group_ids:
    try:
      dhis_parser.set_group_id(group_id)
      output_values += dhis_parser.output_all_indicators()
    except:
      print("Failed to output indicators for group id {}".format(group_id), file=sys.stderr)

  if output_format == 'csv':
    print(','.join(fieldnames) + ',Validation Comments')
    for value in output_values:
      line = ''
      if 'Validation values' in value:
        # For CSV output we will only supply English error messages.
        vcomments = list(map(lambda x: x[0].eng_errmsg(x[1]),
                                       value['Validation values']))
        value['Validation comments'] = '\"' + '\n'.join(vcomments) + '\"'
      else: value['Validation comments'] = '\"\"'
      if 'Indicator name' in value:
        value['Indicator name'] = value['Display Url']
      for field in fieldnames:
        line += (value[field] or '') + ','
      line += value['Validation comments']
      print(line)
  elif output_format == 'json':
    indicator_groups = {}
    for value in output_values:
      del value['Display Url']
      value['Validation codes'] = {}
      for code in value['Validation values']:
        if not code[0].name in value['Validation codes']:
          value['Validation codes'][code[0].name] = []
        value['Validation codes'][code[0].name].append(code[1])
      del value['Validation values']
      if len(value['Validation codes']) == 0:
        value['Validation codes'] = { ValidationErrCode.NO_ERRORS.name: [] }
      igroup = value['Group Description']
      del value['Group Description']
      if not igroup in indicator_groups:
        indicator_groups[igroup] = []
      indicator_groups[igroup].append(camel_case_keys(value))

    final_output_vals = []
    for igroup in indicator_groups:
      final_output_vals.append({ 'groupDescription': igroup,\
        'indicators': indicator_groups[igroup] })
    vcode_dict = {}
    for name, member in ValidationErrCode.__members__.items():
      vcode_dict[name] = member.eng_errmsg_template
    print(json.dumps(
            { 'indicatorGroups': final_output_vals, 
              'validationCodeDict': vcode_dict
            }, indent=4, sort_keys=True)
         )

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
