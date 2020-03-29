'''
Fetchs COVID-19 spread data from 4 sources: 
1. CSSE at JHU ArcGIS, 
2. CSSE at JHU github repo
3. Worldometer website
4. Manual Input (WHO, Official Data)

Combines the data and uploads on a Amazon S3-type Cloud-Storage

'''
import os
import csv
import json
import requests
from io import StringIO
from datetime import datetime

from boto3 import session
from botocore.client import Config
from boto3.s3.transfer import S3Transfer

from bs4 import BeautifulSoup


'''
Amazon S3-type Storage Configuration 
Should be set up as env variables
'''
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
AWS_STORAGE_BUCKET_NAME = os.environ.get('AWS_STORAGE_BUCKET_NAME')
AWS_S3_ENDPOINT_URL = os.environ.get('AWS_S3_ENDPOINT_URL')
AWS_S3_CUSTOM_DOMAIN = os.environ.get('AWS_S3_CUSTOM_DOMAIN')
AWS_S3_OBJECT_PARAMETERS = {
    'CacheControl': 'max-age=600',
}


COUNTRIES = {'CHN': 'China', 'ITA': 'Italy', 'IRN': 'Iran', 'KOR': 'Korea, South', 'ESP': 'Spain', 'DEU': 'Germany', 'FRA': 'France', 'USA': 'United States of America', 'CHE': 'Switzerland', 'NOR': 'Norway', 'DNK': 'Denmark', 'SWE': 'Sweden', 'NLD': 'Netherlands', 'GBR': 'United Kingdom', 'JPN': 'Japan', 'BEL': 'Belgium', 'AUT': 'Austria', 'QAT': 'Qatar', 'AUS': 'Australia', 'FIN': 'Finland', 'BHR': 'Bahrain', 'CAN': 'Canada', 'SGP': 'Singapore', 'MYS': 'Malaysia', 'GRC': 'Greece', 'ISR': 'Israel', 'BRA': 'Brazil', 'CZE': 'Czech Republic', 'SVN': 'Slovenia', 'HKG': 'Hong Kong S.A.R.', 'ISL': 'Iceland', 'PRT': 'Portugal', 'EST': 'Estonia', 'IRQ': 'Iraq', 'KWT': 'Kuwait', 'PHL': 'Philippines', 'ROU': 'Romania', 'IDN': 'Indonesia', 'LBN': 'Lebanon', 'EGY': 'Egypt', 'IRL': 'Ireland', 'SAU': 'Saudi Arabia', 'ARE': 'United Arab Emirates', 'IND': 'India', 'POL': 'Poland', 'THA': 'Thailand', 'SMR': 'San Marino', 'TWN': 'Taiwan', 'VNM': 'Vietnam', 'RUS': 'Russia', 'CHL': 'Chile', 'SRB': 'Serbia', 'ALB': 'Albania', 'LUX': 'Luxembourg', 'PER': 'Peru', 'DZA': 'Algeria', 'HRV': 'Croatia', 'BRN': 'Brunei', 'PAN': 'Panama', 'PSE': 'The Palestinian Territories', 'ARG': 'Argentina', 'SVK': 'Slovakia', 'BGR': 'Bulgaria', 'GEO': 'Georgia', 'PAK': 'Pakistan', 'BLR': 'Belarus', 'ECU': 'Ecuador', 'LVA': 'Latvia', 'CRI': 'Costa Rica', 'HUN': 'Hungary', 'ZAF': 'South Africa', 'SEN': 'Senegal', 'CYP': 'Cyprus', 'OMN': 'Oman', 'BIH': 'Bosnia and Herzegovina', 'MLT': 'Malta', 'TUN': 'Tunisia', 'COL': 'Colombia', 'AZE': 'Azerbaijan', 'ARM': 'Armenia', 'MEX': 'Mexico', 'MKD': 'North Macedonia', 'AFG': 'Afghanistan', 'MAC': 'Macau S.A.R', 'BOL': 'Bolivia', 'FRO': 'Faroe Islands', 'MDV': 'Maldives', 'MAR': 'Morocco', 'LKA': 'Sri Lanka', 'JAM': 'Jamaica', 'KHM': 'Cambodia', 'LTU': 'Lithuania', 'NZL': 'New Zealand', 'GUF': 'French Guiana', 'KAZ': 'Kazakhstan', 'MDA': 'Moldova', 'PRY': 'Paraguay', 'DOM': 'Dominican Republic', 'TUR': 'Turkey', 'CUB': 'Cuba', 'LIE': 'Liechtenstein', 'URY': 'Uruguay', 'UKR': 'Ukraine', 'BGD': 'Bangladesh', 'PYF': 'French Polynesia',
             'PRI': 'Puerto Rico', 'MCO': 'Monaco', 'NGA': 'Nigeria', 'ABW': 'Aruba', 'BFA': 'Burkina Faso', 'CMR': 'Cameroon', 'GHA': 'Ghana', 'HND': 'Honduras', 'NAM': 'Namibia', 'MAF': 'Saint Martin', 'TTO': 'Trinidad and Tobago', 'VEN': 'Venezuela', 'GUY': 'Guyana', 'SDN': 'Sudan', 'AND': 'Andorra', 'JOR': 'Jordan', 'NPL': 'Nepal', 'ATG': 'Antigua and Barbuda', 'BTN': 'Bhutan', 'CYM': 'Cayman Islands', 'CIV': "Ivory Coast (Côte d'Ivoire)", 'CUW': 'Curaçao', 'ETH': 'Ethiopia', 'GAB': 'Gabon', 'GTM': 'Guatemala', 'GIN': 'Guinea', 'VAT': 'Vatican (Holy See)', 'KEN': 'Kenya', 'MRT': 'Mauritania', 'MNG': 'Mongolia', 'RWA': 'Rwanda', 'LCA': 'Saint Lucia', 'VCT': 'Saint Vincent and the Grenadines', 'SUR': 'Suriname', 'TGO': 'Togo', 'REU': 'Réunion', 'MTQ': 'Martinique', 'GLP': 'Guadeloupe', 'UZB': 'Uzbekistan', 'KGZ': 'Kyrgyz Republic', 'KOS': 'Kosovo', 'MNE': 'Montenegro', 'TKM': 'Turkmenistan', 'TJK': 'Tajikistan', 'COG': 'Congo', 'LBR': 'Liberia', 'CAF': 'Central African Republic', 'TZA': 'Tanzania', 'SOM': 'Somalia', 'GRL': 'Greenland', 'BEN': 'Benin', 'BHS': 'Bahamas', 'SYC': 'Seychelles', 'GUM': 'Guam', 'BLM': 'St. Barths', 'COD': 'Democratic Republic of the Congo', 'GNQ': 'Equatorial Guinea', 'VIR': 'U.S. Virgin Islands', 'ZMB': 'Zambia', 'NCL': 'New Caledonia', 'BRB': 'Barbados', 'GMB': 'Gambia', 'MSR': 'Montserrat', 'DJI': 'Djibouti', 'GBX': 'Channel Islands', 'MYT': 'Mayotte', 'SWZ': 'Eswatini', 'GIB': 'Gibraltar', 'DPX': 'Diamond Princess (Cruise Ship)', 'MUS': 'Mauritius', 'NIC': 'Nicaragua', 'FJI': 'Fiji', 'SLV': 'El Salvador', 'BMU': 'Bermuda', 'TCD': 'Chad', 'HTI': 'Haiti', 'AGO': 'Angola', 'CPV': 'Cape Verde', 'IMN': 'Isle of Man', 'NER': 'Niger', 'PNG': 'Papua New Guinea', 'MDG': 'Madagascar', 'ZWE': 'Zimbabwe', 'ERI': 'Eritrea', 'GRD': 'Grenada', 'MOZ': 'Mozambique', 'SYR': 'Syria', 'UGA': 'Uganda', 'TLS': 'Timor-Leste', 'DMA': 'Dominica', 'BLZ': 'Belize', 'LAO': 'Laos', 'LBY': 'Libya', 'MMR': 'Myanmar', 'MLI': 'Mali', 'GNB': 'Guinea-Bissau', 'KNA': 'Saint Kitts and Nevis', 'VGB': 'British Virgin Islands', 'MZX': 'MS Zaandam (Cruise Ship)'}


CODES = {}

'''
Used to normalize country titles as long as each data source has own naming standard
'''
TITLES = {
    'Iran (Islamic Republic of)': 'Iran',
    'US': 'United States of America',
    'USA': 'United States of America',
    'UK': 'United Kingdom',
    'Republic of Moldova': 'Moldova',
    'Mainland China': 'China',
    'Viet Nam': 'Vietnam',
    'Macao SAR': 'Macau S.A.R',
    'Macao': 'Macau S.A.R',
    'Russian Federation': 'Russia',
    'Hong Kong SAR': 'Hong Kong S.A.R.',
    'Hong Kong': 'Hong Kong S.A.R.',
    'Holy See': 'Vatican (Holy See)',
    'Vatican (Holy Sea)': 'Vatican (Holy See)',
    'Vatican City': 'Vatican (Holy See)',
    'occupied Palestinian territory': 'The Palestinian Territories',
    'Palestine': 'The Palestinian Territories',
    'West Bank and Gaza': 'The Palestinian Territories',
    'Republic of Korea': 'Korea, South',
    'S. Korea': 'Korea, South',
    'Czechia': 'Czech Republic',
    'Taiwan*': 'Taiwan',
    'Cote d\'Ivoire': 'Ivory Coast (Côte d\'Ivoire)',
    'Ivory Coast': 'Ivory Coast (Côte d\'Ivoire)',
    'UAE': 'United Arab Emirates',
    'Faeroe Islands': 'Faroe Islands',
    'St. Vincent Grenadines': 'Saint Vincent and the Grenadines',
    'CAR': 'Central African Republic',
    'St. Barth': 'St. Barths',
    'DRC': 'Democratic Republic of the Congo',
    'Congo (Kinshasa)': 'Democratic Republic of the Congo',
    'Kyrgyzstan': 'Kyrgyz Republic',
    'Diamond Princess': 'Diamond Princess (Cruise Ship)',
    'MS Zaandam': 'MS Zaandam (Cruise Ship)',
    'Cruise Ship': 'Diamond Princess (Cruise Ship)',
    'Cabo Verde': 'Cape Verde',
    'East Timor': 'Timor-Leste',
    'Congo (Brazzaville)': 'Congo',
    'Curacao': 'Curaçao',
    'Burma': 'Myanmar',
    '': '',
}


MANUAL_DATA = {
    'KOS': [86, 1, 1, '2020-03-29 10:59:59', 'Official Data'],
}


class CovidDataFactory(object):

    def __init__(self):

        self.covid_data = {}  # App Data Storage

        for code in COUNTRIES:
            CODES[COUNTRIES[code]] = code

    def execute(self):
        self.covid_data = self.read_arcgis()
        self.combine_data()

        if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
            print(
                '\nNo S3-type Storage Available.\nCOVID-19 data is stored in self.covid_data property.')
            return True

        result = self.save_to_cloud()
        print('Saving JSON to Cloud Storage:', 'OK' if result else 'ERROR')
        return result

    def add_country_data(self, country_name=None, confirmed=0, deaths=0, recovered=0, latest_update=None, source=''):
        '''
        Normalize Country title
        Return a dictionary with COVID-19 country data with source, latest update label and county ISO code
        '''
        if country_name in TITLES:
            country_name = TITLES[country_name]

        if country_name in CODES:
            country_code = CODES[country_name]

            return {
                'code': country_code,
                'confirmed': confirmed,
                'deaths': deaths,
                'recovered': recovered,
                'latest_update': latest_update,
                'source': source
            }

        print('Country title not found: ', country_name)

        return None

    def read_covid_csse(self):
        '''
        Fetch data from CSSE at JHU COVID-19 github repo.
        Always retrieve yesterday CVS as long as CSSE updates it on a daily basis
        Returns a dictionary with data
        '''

        covid_data = {}
        day = datetime.now().day - 1

        r = requests.get('https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_daily_reports/{0}-{1}-2020.csv'.format(
            datetime.now().strftime('%m'), str(day) if day > 9 else '0{}'.format(day)), timeout=40)

        if r.status_code != requests.codes.ok:
            exit('Died from Coronavirus trying to fetch latest data from github')

        csv_reader = csv.reader(StringIO(r.text), delimiter=',')
        line = 0

        for row in csv_reader:

            if line > 0:
                country_name = row[3]
                latest_update = row[4]
                confirmed = int(row[7])
                deaths = int(row[8])
                recovered = int(row[9])

                obj = self.add_country_data(
                    country_name, confirmed, deaths, recovered, latest_update, 'JHU CSSE')

                covid_data[obj['code']] = obj

            line += 1

        return covid_data

    def read_arcgis(self):
        '''
        Fetch data from CSSE at JHU COVID-19 ArcGIS service
        Return a dictionary with data
        '''

        data = {}

        response = requests.get('https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases/FeatureServer/2/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&outSR=102100&resultOffset=0&resultRecordCount=250&cacheHint=true&quantizationParameters=%7B%22mode%22%3A%22edit%22%7D', timeout=120)

        if response.status_code != requests.codes.ok:
            exit(
                'Died from Coronavirus trying to fetch latest data from CSSE at JHU ArcGIS')

        features = json.loads(response.text)

        for item in features['features']:

            obj = self.add_country_data(
                country_name=item['attributes']['Country_Region'], confirmed=item['attributes']['Confirmed'], deaths=item['attributes']['Deaths'], recovered=item['attributes']['Recovered'], latest_update=datetime.fromtimestamp(item['attributes']['Last_Update'] / 1000).strftime("%Y/%m/%d, %H:%M:%S"), source='JHU CSSE')

            data[obj['code']] = obj

        return data

    def read_worldometer(self):
        '''
        Fetch data from COVID-19 page on Worldometer website
        https://www.worldometers.info/coronavirus/

        Return a dictionary with data
        '''

        data = {}

        url = 'https://www.worldometers.info/coronavirus/'
        response = requests.get(url, timeout=40)

        if response.status_code != requests.codes.ok:
            exit('Died from Coronavirus trying to fetch latest data from Worldometer')

        html = BeautifulSoup(response.text, "html.parser")
        table = html.find('table', id='main_table_countries_today')

        for row in table.find('tbody').find_all('tr'):
            cells = row.find_all('td')

            obj = self.add_country_data(country_name=cells[0].text.strip(), confirmed=self.parse_num(cells[1].text), deaths=self.parse_num(
                cells[3].text), recovered=self.parse_num(cells[5].text), latest_update=datetime.now().strftime("%Y/%m/%d, %H:%M:%S"), source='Worldometer')

            if obj:
                data[obj['code']] = obj

        return data

    def combine_data(self):
        '''
        Walks through all data sources and combines data using the rule:
        * First priority — ArcGIS data added into covid_data
        * If a country not found in ArcGIS but presents in CSSE git repo — append it to covid_data
        * If a country presents in Worldometer and cannot be found in our storage (or country code is among the list: SRB, KGZ, KAZ, RUS, UKR, MZX, UZB) - append it
        * If a country presents in MANUAL_DATA dictionary — overwrite the data in storage

        Returns a dictionary with data
        '''

        wom_data = self.read_worldometer()
        csse = self.read_covid_csse()

        print('\n', 'Total items in ARCGIS', len(self.covid_data))
        print('Total items in CSSE', len(csse))
        print('Total items in Worldometer', len(wom_data), '\n')

        print("-" * 57 + "|")

        print("{code:8s} | {c:13s} | {d:13s} | {r:13s} |".format(
            code='CODE', c='  CONFIRMED', d='   DEATHS', r='  RECOVERED'))
        print("-" * 57 + "|")

        for code in csse:
            if code in self.covid_data:
                title = code
                if self.covid_data[code]['confirmed'] < csse[code]['confirmed'] or self.covid_data[code]['deaths'] < csse[code]['deaths'] or self.covid_data[code]['recovered'] < csse[code]['recovered']:
                    extra = ''
                    if self.covid_data[code]['confirmed'] < csse[code]['confirmed']:
                        self.covid_data[code][
                            'confirmed'] = csse[code]['confirmed']
                        extra += 'C'
                    if self.covid_data[code]['deaths'] < csse[code]['deaths']:
                        self.covid_data[code]['deaths'] = csse[code]['deaths']
                        extra += 'D'
                    if self.covid_data[code]['recovered'] < csse[code]['recovered']:
                        self.covid_data[code][
                            'recovered'] = csse[code]['recovered']
                        extra += 'R'
                    self.covid_data[code]['latest_update'] = csse[code]['latest_update'].replace(
                        'T', ' ')
                    self.covid_data[code]['source'] = 'JHU CSSE'
                    title = "> {1}+{0}".format(extra, title)

                print("{code:8s} | {c1:5d} | {c2:5d} | {d1:5d} | {d2:5d} | {r1:5d} | {r2:5d} |".format(code=title, c1=self.covid_data[code]['confirmed'], c2=csse[code][
                      'confirmed'], d1=self.covid_data[code]['deaths'], d2=csse[code]['deaths'], r1=self.covid_data[code]['recovered'], r2=csse[code]['recovered']))
            else:
                # print('Adding', code, 'from CSSE')
                print("{code:8s} | {c1:5d} | {c2:5d} | {d1:5d} | {d2:5d} | {r1:5d} | {r2:5d} |".format(code="+ " + code, c1=0, c2=csse[code][
                      'confirmed'], d1=0, d2=csse[code]['deaths'], r1=0, r2=csse[code]['recovered']))
                self.covid_data[code] = csse[code]
                self.covid_data[code]['latest_update'] = self.covid_data[code]['latest_update'].replace(
                    'T', ' ')
                self.covid_data[code]['source'] = 'JHU CSSE'
                # print(self.covid_data[code]['latest_update'])

        print("-" * 57 + "|")
        print("-" * 16 + " ADDING FROM WORLDOMETER " + "-" * 16 + '|')
        print("-" * 57 + "|")

        for code in wom_data:
            if code not in self.covid_data or code in ('SRB', 'KGZ', 'KAZ', 'RUS', 'UKR', 'MZX', 'UZB',):
                if code in COUNTRIES:
                    print("{code:8s} | {c1:5d} | {c2:5d} | {d1:5d} | {d2:5d} | {r1:5d} | {r2:5d} |".format(
                        code="++ " + code, c1=self.covid_data[code]['confirmed'] if 'code' in self.covid_data else 0, c2=wom_data[code]['confirmed'], d1=self.covid_data[code]['deaths'] if 'code' in self.covid_data else 0, d2=wom_data[code]['deaths'], r1=self.covid_data[code]['recovered'] if 'code' in self.covid_data else 0, r2=wom_data[code]['recovered']))
                    self.covid_data[code] = {
                        'confirmed': wom_data[code]['confirmed'],
                        'deaths': wom_data[code]['deaths'],
                        'recovered': wom_data[code]['recovered'],
                        'latest_update': wom_data[code]['latest_update'],
                        'source': wom_data[code]['source']
                    }

                else:
                    print('! CODE NOT FOUND', code)

        print("-" * 57 + "|")

        print("-" * 16 + " ADDING FROM MANUAL INPUT " + "-" * 15 + "|")

        print("-" * 57 + "|")

        if(len(MANUAL_DATA)):
            for code in MANUAL_DATA:
                print("{code:8s} | {c1:5d} | {c2:5d} | {d1:5d} | {d2:5d} | {r1:5d} | {r2:5d} |".format(
                    code="++ " + code, c1=self.covid_data[code]['confirmed'] if 'code' in self.covid_data else 0, c2=MANUAL_DATA[code][0], d1=self.covid_data[code]['deaths'] if 'code' in self.covid_data else 0, d2=MANUAL_DATA[code][1], r1=self.covid_data[code]['recovered'] if 'code' in self.covid_data else 0, r2=MANUAL_DATA[code][2]))
                self.covid_data[code] = {
                    'confirmed': MANUAL_DATA[code][0],
                    'deaths': MANUAL_DATA[code][1],
                    'recovered': MANUAL_DATA[code][2],
                    'latest_update': MANUAL_DATA[code][3],
                    'source': MANUAL_DATA[code][4]
                }

            print("-" * 57 + "|")

        return None

    def save_to_cloud(self):
        '''
        Store our data inside Amazon S3-type Cloud Storage
        '''
        try:
            s3session = session.Session()
            client = s3session.client('s3',
                                      region_name=AWS_S3_CUSTOM_DOMAIN,
                                      endpoint_url=AWS_S3_ENDPOINT_URL,
                                      aws_access_key_id=AWS_ACCESS_KEY,
                                      aws_secret_access_key=AWS_SECRET_KEY)

            client.put_object(Bucket=AWS_STORAGE_BUCKET_NAME, Key='covid-19/map.json',
                              Body=json.dumps(this.covid_data, ensure_ascii=False))
            response = client.put_object_acl(
                ACL='public-read', Bucket=AWS_STORAGE_BUCKET_NAME, Key="covid-19/map.json")
            return True

        except:
            return False

    def parse_num(self, text=None):
        '''
        Format numeric data from Worldometer
        Return 0 or a valid number
        '''
        text = text.strip().replace(',', '')
        if len(text):
            try:
                return int(text)
            except:
                return 0
        return 0


def update_covid19_data(event=None, context=None):

    cdf = CovidDataFactory()
    cdf.execute()


if __name__ == "__main__":
    update_covid19_data()
