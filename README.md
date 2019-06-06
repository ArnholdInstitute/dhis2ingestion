Usage: "python ./readindicators.py [--args] > output_file"

This program is designed to scrape metadata from a DHIS2 system and output it
in human-readable format (either CSV for consumption by Excel/OpenOffice, or
JSON for use by other programs).  The program outputs to stdout.

It expects access credentials to be provided for the DHIS2 system in question;
these access credentials can either be in the form of an OAuth2 token (provided
as a command-line argument) or as username/password stored locally in a JSON file
(**NOT PREFERRED**).

In the event of username/passwords being stored locally, the JSON file should be
formatted as  

    {  
      "country1": {  
        "username": "username1",  
        "password": "password1",  
        "baseUrl": "URL1"  
      },
      "country2": {  
        ...  
      }  
      ...  
    }  
    
with the URLs being everything between "https://" and "/api" in the DHIS2 API calls.  
The location of the JSON file should be stored in the "DHIS2_PARAMS_FILE" environment
variable.  

The parser takes in the following command-line arguments:  
"country" -- a key from the DHIS2 params file, to be used only if username/passwords are
  stored locally.
  
"base_url" -- the base url of the DHIS2 system, will override the "baseUrl" field from the
  DHIS2 params file if both are provided. 
  
"auth_token" -- an OAuth2 authentication token; if both "country" and "auth_token" are
  provided, the "auth_token" field takes priority.  
  
"output" -- desired output format, CSV is default but JSON is also supported.
  
"group_desc" -- a description of the indicatorGroup(s) to be analyzed. The program will check
  the display names of all indicator groups for substrings which match "group_desc" in a
  case-insensitive fashion. So for example, group_desc="Paludism" would match indicatorGroups
  with display names "Carte Score:PALUDISME", "Paludisme", "Paludisme_CU_Milda", and
  "Paludisme hebdomadaire".  
  
"group_id" -- internal DHIS2 identifier of the indicatorGroup to be analyzed.  
**Will override "group_desc" argument.**  
