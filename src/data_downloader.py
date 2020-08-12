import re
import os
import ast
import json
import time
import pickle
import datetime
import requests
import urllib.request

from PIL import Image
from io import BytesIO
from bs4 import BeautifulSoup as bs
from concurrent.futures import ThreadPoolExecutor


class CarCrawler( object ):
    """
    This class is responsible for iterating over all of the available cars on www.hasznaltauto.hu
    and downloading all the necessary data.
    """

    def __init__( self ):
        """
        Constructor. Loads the parameters and starts the downloading.
        """   
        self.load_params( 'params/website_params.json' )
        self.iterate_over_available_cars()

    def load_params( self, filename ):
        """
        Loads the given JSON parameter file and stores it in self.params.
        """
        with open( filename, 'r' ) as input_file:
            self.params = json.load( input_file )
 
    def iterate_over_available_cars( self, max_workers=10 ):
        """
        Iterates over all of the available car pages and passes the car links to parse_car_page function.
        It uses ThreadPoolExecutor to make the whole process faster as the CPU is in idle when waiting for
        response from the website.
        """
        self.vehicle_data            = []           # Stores the collected data
        self.new_vehicle_links       = set()        # Stores new (unprocessed) car links
        self.broken_vehicle_links    = set()        # Stores broken (exception, no connection/response, etc) car links
        self.processed_vehicle_links = set()        # Stores successfully processed vehicle links

        # Start the iteration from the first page and always get the next page link until it is empty
        next_page_url = self.params['website']
        next_page_html = bs( requests.get( next_page_url ).content, 'lxml' )
        next_page_button_html = next_page_html.find( 'li', class_='next' ).find( 'a' )

        # If next page button's html is none the last page is reached
        while( next_page_button_html != None ):

            # Find all vehicle links on the current page
            vehicle_htmls = next_page_html.find_all( 'a', title=True, class_="", href=re.compile( '/szemelyauto/' ) )
            self.new_vehicle_links.update( [ vehicle_html.get( 'href' ) for vehicle_html in vehicle_htmls ] )

            start = time.time()
            with ThreadPoolExecutor( max_workers=max_workers ) as executor:
                for vehicle_link in self.new_vehicle_links:
                    executor.submit( self.parse_car_page, ( vehicle_link ) )

            print( '{} has been crawled in {:.2f}s'.format( next_page_url.split( '/' )[-1], time.time() - start ) )

            self.save_data()
            
            # Find next page's link
            next_page_url = next_page_button_html.get( 'href' )

            next_page_html = bs( requests.get( next_page_url ).content, 'lxml' )
            next_page_button_html = next_page_html.find( 'li', class_='next' ).find( 'a' )
    
    def save_data( self, limit=1000 ):
        """
        Saves and empty the collected vehicle_data when there are at least 'limit' amount. This is needed to avoid
        memory full issues. The filename will be 'current_date_current_time_export.pkl'. It also saves the
        broken_vehicle_links list so the user can investigate what went wrong.
        """
        if len( self.vehicle_data ) >= limit:
            current_datetime_str = datetime.datetime.now( ).strftime( '%Y_%m_%d_%H_%M_%S' )
            filename = 'data/' + current_datetime_str + '_export.pkl'
            
            data_folder_path = os.path.join( os.getcwd(), 'data')
            if not os.path.exists( data_folder_path ):
                os.mkdir( data_folder_path )

            with open( filename, 'wb' ) as output_file:
                pickle.dump( self.vehicle_data, output_file )
            
            with open( 'data/broken_links.pkl', 'wb' ) as output_file:
                pickle.dump( self.broken_vehicle_links, output_file )

            print( '{} saved.'.format( filename ) )
            self.vehicle_data = []

    def parse_car_page( self, vehicle_link ):
        """
        Parses all the necessary data from a car's page. If parsing was succesfull it removes the link
        from new_vehicle_links and adds it to processed_vehicle_links. If there was an exception it adds
        the link to broken_vehicle_links.
        """
        try:
            data = dict()
            data['link'] = vehicle_link
            data['id'] = vehicle_link.split( '-' )[-1]
            car_page_html = bs( requests.post( url=vehicle_link, headers=self.params['headers'], cookies=self.params['cookies']).content, 'lxml' )
            self.parse_images( data, car_page_html )
            self.parse_common_data( data, car_page_html )
            self.parse_details_data( data, car_page_html )
            self.parse_description_data( data, car_page_html )

            self.vehicle_data.append( data )
            self.processed_vehicle_links.add( vehicle_link )
            self.new_vehicle_links.remove( vehicle_link )

        except Exception as e:
            self.new_vehicle_links.remove( vehicle_link )
            self.broken_vehicle_links.add( vehicle_link )
            print( 'Exception ({}) in {}'.format( e, vehicle_link ) )

    @staticmethod
    def parse_images( data, car_page_html ):
        """
        Parses the thumbnail images from a car's page.
        """
        data['images'] = []

        images_html = car_page_html.find_all( 'img', attrs={ 'itemprop' : 'thumbnail' } )
        image_links = [ image_html.get( 'src' ) for image_html in images_html ]

        for image_link in image_links:
            with urllib.request.urlopen( image_link ) as url:
                bytes_image = BytesIO( url.read() )
                decoded_image = Image.open( bytes_image )
                data['images'].append( decoded_image )

    @staticmethod
    def parse_common_data( data, car_page_html ):
        """
        Parses common data from a car's page like: price, color, power, mileage, etc.
        """
        data['common'] = {}

        data['common']['brand'] = car_page_html.find( 'a', attrs={ 'type' : 'marka' } ).getText()
        data['common']['model'] = car_page_html.find( 'a', attrs={ 'type' : 'modell' } ).getText()
        
        model_group_html = car_page_html.find( 'a', attrs={ 'type' : 'modellcsoport' } )
        data['common']['model_group'] = '' if model_group_html is None else model_group_html.getText()

        table = car_page_html.find( 'table', attrs={ 'class' : 'hirdetesadatok' } )
        rows = table.find_all( 'tr' )

        for row in rows:
            cols = [ ele.text.strip() for ele in row.find_all( 'td' ) if ele.text.strip() ]
            elements = [ ele for ele in cols if ele ]
            if len( elements ) == 2:
                data['common'][elements[0]] = elements[1].replace( u'\xa0', u'' )

    @staticmethod
    def parse_details_data( data, car_page_html ):
        """
        Parses the details section from a car's page.
        """
        data['details'] = {}

        details = car_page_html.find_all( 'div', attrs={ 'class' : 'col-xs-28 col-sm-14' } )
        for detail in details:
            for title in ['Beltér', 'Műszaki', 'Kültér', 'Multimédia / Navigáció']:
                if title in detail.text:
                    options = detail.text.split( '\n' )[:-1]
                    data['details'][title] = options[1:]

    @staticmethod
    def parse_description_data( data, car_page_html ):
        """
        Parses the description sections from a car's page.
        """
        data['description'] = {}

        description_html = car_page_html.find( 'div', attrs={ 'leiras' } )
        description = '' if description_html is None else description_html.text.split( '\n' )[2]
        data['description']['Leírás'] = description

        else_description_html = car_page_html.find( 'div', attrs={ 'egyebinformacio' } )
        else_description = '' if else_description_html is None else else_description_html.text.split( '\n' )[3:-1]
        data['description']['Egyéb információ'] = else_description 

if __name__ == '__main__':
    cc = CarCrawler()