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

dhis_params_dict = {}
#dhis_url = 'senegal.dhis2.org/dhis'

# prints all indicators in the malaria indicator group; 
# higher level function that retrieves the numerator and denominator of
# all indicators in the group.
def parse(filename):
	parsedFile = minidom.parse(filename)
	getIndicatorNames(parsedFile)

	ids = parsedFile.getElementsByTagName('indicator')
	for elem in ids:
		indicatorId = elem.attributes['id'].value
		#getDescription(indicatorId)
		print(indicatorId)


# prints the input indicator's displayName; currently expects a file as input.
def getIndicatorNames(parsedFile):
	indicators = parsedFile.getElementsByTagName('displayName')

	for elem in indicators:
		d_name = elem.firstChild.data
		#print d_name
		
	return d_name 
	

def getDescription(indicatorId):
	# create dictionary of values to write into csv file
	fieldnames = ['Indicator name', 'Numerator description', 'Denominator description', 'Calculation', 'Definition validation', 'Data validation', 'Comments' ]
	values = {key: '' for key in fieldnames}
	print(values)

	# navigate to the indicator url
	parsedFile = minidom.parse(indicatorId + '.xml')
	
	# store display name
	displayName = parsedFile.getElementsByTagName('displayName')
	dNameValue = displayName[0].firstChild.data
	values['Indicator name'] = constructHyperLink('indicators', indicatorId, dNameValue)

	# store the numerator description
	numDesc = parsedFile.getElementsByTagName('numeratorDescription')
	numDescValue = numDesc[0].firstChild.data
	values['Numerator description'] = numDescValue

	# store the denominator description
	denDesc = parsedFile.getElementsByTagName('denominatorDescription')
	denDescValue = denDesc[0].firstChild.data
	values['Denominator description'] = denDescValue

	# get the numerator ids - currently with ids instead of friendly name(temporarily opening the direct file)
	numerator = parsedFile.getElementsByTagName('numerator')
	numDescription = numerator[0].firstChild.data
	print(numDescription)


	# get the denominator ids - currently with ids instead of friendly name
	denominator = parsedFile.getElementsByTagName('denominator')
	denDescription = denominator[0].firstChild.data
	print(denDescription)


	# convert the numerator and denominator dataElement id's with their descriptions
	# 	all possible elements: #{xxxxxx}, sometimes #{xxxxx}.xxxxx, operators (+,-,*), and numbers (int)
	#   create a list of id's, navigate to their url, and replace the num/den id's with the descriptions

	# TODO memoization - as you find ids, add them to a global mapping b/t id and description; 
	# TODO look for operators so that you can display the calculation
	numIds = re.findall('[^#{ }+/\-*()]+', numDescription)
	denIds = re.findall('[^#{ }+/\-*()]+', denDescription)
	#print(numIds)

	# find all operators so that you can reconstruct the 'calculation' column 
	numOps = re.findall('[\+\-\*\/]', numDescription)
	denOps = re.findall('[\+\-\*\/]', denDescription)


	# iterate through numIds and denIds (the matched dataElement ids or digits)
	# and find the friendly name of the id and add it to the dictionary along
	# with the next operator (+,-,*) in the list of matched operators.
	for ids in numIds:
		# if it's a number, then insert it straight into 'calculation'
		if (ids.isdigit()):
			#print (ids)
			values['Calculation'] += ids + (numOps.pop(0) if numOps else ' ')
		else:
			#navigate to url of the dataElement using the id 

			# separate out the category option combos, if any (index 0 will be the isolated indicator id, index 1 will be the category option combo)
			a_id = re.findall('[^.]+', ids)

			combo_name = getIndicatorNames(minidom.parse(a_id[1] + '.xml')) if len(a_id) > 1 else ''
			#print(combo_name)

			delem_name = getIndicatorNames(minidom.parse(a_id[0] + '.xml'))
			

			#print(ids + ': ' + delem_name)
			values['Calculation'] += delem_name + combo_name + (numOps.pop(0) if numOps else ' ')

	values['Calculation'] += '/'

	for ids in denIds:
		# if it's a number, then print it
		if (ids.isdigit()):
			#print (ids)
			values['Calculation'] += ids + (denOps.pop(0) if denOps else ' ')
		else:
			#navigate to url of the dataElement using the id; be sure to separate out the category option combos, if any
			a_id = re.findall('[^.]+', ids)
			combo_name = getIndicatorNames(minidom.parse(a_id[1] + '.xml')) if len(a_id) > 1 else ''

			delem_name = getIndicatorNames(minidom.parse(a_id[0] + '.xml'))
			

			# TODO: construct url from the ids and write the hyperlink into the csv (easy)
			#print(ids + ': ' + delem_name)
			values['Calculation'] += delem_name + combo_name + (denOps.pop(0) if denOps else ' ')

	# open csv file and declare column names
	with open('testoutput.csv', mode='w') as csv_file:

		writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
		writer.writeheader()
		# encode the values in utf-8 before writing the rows to csv.
		for key, value in values.items():
			values[key] = value.encode('utf-8')
		writer.writerow(values)


# currently I am redirected to the login page for DHIS2 - want to look up how to log in w/ credentials then navigate to the proper url
def navigate(full_login_url, lvl, iid, session):
  navigate_url = 'https://' + full_login_url + '/api/' + lvl + '/' + iid
  r = requests.get(navigate_url)
	return r.content


# constructs the url the given data - inputs are the level and the id
def constructUrl(display_url, lvl, iid, friendlyName):
	output_url = 'https://' + display_url + '/api/' + lvl + '/' + iid
	return '=HYPERLINK(\"' + output_url + '\",' + '\"' + friendlyName + '\")'
  

# We expect dhis_params_dict to be a dictionary keyed by country; we expect
# values to be dicts having "baseUrl", "username", and "password" as keys.
# This dictionary should be stored in a JSON file.
# The path to this file should be stored in an environment variable named
# DHIS2_PARAMS_FILE.
# This returns a pair [full_login_url, display_url]; the former has username/
# password inherent in it and is never put into output.
def constructDhisUrls(country):
  if country not in dhis_params_dict:
    dhis_params_file = os.environ['DHIS2_PARAMS_FILE']
    with open(dhis_params_file, 'r') as ofh:
      dhis_params_dict = json.load(ofh)

  return ['https://' + dhis_params_dict[country]['username'] + ':' +
            dhis_params_dict[country]['password'] + '@' +
            dhis_params_dict[country]['baseUrl'],
          'https://' + dhis_params_dict[country]['baseUrl']]
         
# with requests.Session() as s:
# 	p = s.post(login_url, data=payload)
# 	print p.text

# 	r = s.get('https://senegal.dhis2.org/dhis/api/indicators/iCA0KZXvXuZ')
# 	print r.text
# 	#navigate('indicators', 'iCA0KZXvXuZ', s)

#parse('malariaindicators.xml')
#getDescription('TdWw71NnOoQ')
#getDescription('gQvoDVLOyh1')
#navigate('indicators', 'iCA0KZXvXuZ')

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

    group_url = parsedFile.getElementsByTagName('identifiableObject').get('href')
    group_type = group_url.split('/')[-2]
    authenticated_group_url = self.full_login_url + '/api' + group_type + '/' +\
                              self.group
    
    # This contains the parsed XML DOM of the indicator group, from which we can
    # retrieve a list of indicator ids.
    group_xml = minidom.parse(requests.get(authenticated_group_url).content)
    self.element_type = (group_type == 'indicatorGroup') ? 'indicator' : 'dataElement'
    self.element_ids = group_xml.getElementsByTagName('elt_type')
    
  def constructElementUrl(element_id):
    return self.full_login_url + '/api/' + self.element_type + '/' + element_id
    
  def getElementMetadata(element_id):
    element_url = self.constructElementUrl(element_id)
    return minidom.parse(requests.get(element_url).content)
    

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--country', default='Senegal',
                      help='Which country\'s DHIS2 system are we scraping')
  parser.add_argument('--output', default='testoutput.csv', help='Output file')
  parser.add_argument('--group_id', default='',
                      help='Specific indicatorGroup / dataElementGroup of interest')
  args = parser.parse_args()
  
  dhis_parser = dhisParser(args.country, args.group_id)

