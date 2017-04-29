# GeoCivics Assets

from cull import cull
from rone import assets, rone
from faraday import faraday, xform_schema as xform
from srproj import extract_cscard as extract, load_cscard as load


ASSET_PATH = '/home/jgp/geocivics/rone/assets'
    
def make_add_geo_point():
    def latlongpt(header, row):
        """
        Takes a row and returns a value.
        Returns a string of SRID 4326 Point String
        """
        lat_val = None
        lon_val = None
        row_list = row.split(',')
        col_list = header.split(',')
        for col, val in zip(col_list, row_list):
            if col == 'LATITUDE':
                lat_val = val
            if col == 'LONGITUDE':
                lon_val = val
            if lat_val is not None and lon_val is not None:
                try:
                    lat = float(lat_val)
                    lon = float(lon_val)
                except:
                    return ""
                return '\"POINT(' + str(lon_val) + ' '+ str(lat_val) + ')\"'
        return ""
    
    add_geo_point = xform.Transformation('COORDINATE', 'geography(POINT, 4326)', latlongpt, 1)
    return add_geo_point

def get_assets():
    print("Getting assets: ")
    asset_list = []
    
    college_scorecard = assets.Asset(
        url="https://ed-public-download.apps.cloud.gov/downloads/Most-Recent-Cohorts-All-Data-Elements.csv",
        filetype="text/csv",
        filename="college_scorecard.csv")
    college_scorecard.transformations = [make_add_geo_point()]
    college_scorecard.cull_regex_list = ['L4', 'RPY', 'CIP', 'NPT4', 'WDRAW', 'NUM4', 'C150', '_INC_', 'TRANS', '_RT'] 
    asset_list.append(college_scorecard)

    chi_landmarks = assets.Asset(
        url="",
        filetype="text/csv",
        filename="chi_landmarks.csv")
    chi_landmarks.transformations = [make_add_geo_point()]
    chi_landmarks.cull_regex_list = []

    return asset_list

def download_assets(asset_list):
    """
    Use Rone to download assets.
    """
    return rone.update_assets(asset_list)

def cull_columns(asset_list):
    """
    Use cull to remove unneeded columns
    """
    return cull.cull_from_assets(asset_list)

def extract_schema(asset_list):
    """
    Extract a schema from the asset.
    """
    return srproj.extract_assets_schema(asset_list)

def transform_columns(asset_list):
    """
    Use Faraday to transform the values in the file
    """
    return faraday.transform_assets(asset_list)
    
def load_data(asset_list):
    """
    Load data into the database
    """
    pass

def ETL():
    """
    Run the ETL Pipeline.
    """
    asset_list = get_assets()
    print("Downloading assets:")
    download_assets(asset_list)
    cull.cull_from_assets(asset_list)
    extract.extract_assets_schema(asset_list)
    faraday.transform_assets(asset_list)
    load.load_data(asset_list)

if __name__ == '__main__':
    ETL()
