from xml.dom import minidom
import getpass
import os
import re
import urllib2
import csv
import requests


dhis_url = 'senegal.dhis2.org/dhis'

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
def navigate(lvl, iid, session):
	return None


# constructs the url the given data - inputs are the level and the id
def constructHyperLink(lvl, iid, friendlyName):
	base_url = 'https://' + dhis_url + '/api/' + lvl + '/' + iid
	return '=HYPERLINK(\"' + base_url + '\",' + '\"' + friendlyName + '\")'
	
# with requests.Session() as s:
# 	p = s.post(login_url, data=payload)
# 	print p.text

# 	r = s.get('https://senegal.dhis2.org/dhis/api/indicators/iCA0KZXvXuZ')
# 	print r.text
# 	#navigate('indicators', 'iCA0KZXvXuZ', s)

#parse('malariaindicators.xml')
getDescription('TdWw71NnOoQ')
#getDescription('gQvoDVLOyh1')
#navigate('indicators', 'iCA0KZXvXuZ')