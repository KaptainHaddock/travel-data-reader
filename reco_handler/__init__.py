import azure.functions as func
import logging
import sys
import gzip
import json
import datetime
import os
import time
import tempfile
from collections import Counter

# Attempt to import neobase with error handling
try:
    import neobase
    _HAS_NEOBASE = True
except ImportError:
    _HAS_NEOBASE = False
    logging.warning("neobase module not available. Geography features will be limited.")
    # Define a simple fallback class
    class NeoBaseFallback:
        def get(self, city, field):
            return "XX"  # Default country code
        
        def distance(self, origin, destination):
            return 0  # Default distance

_RECO_LAYOUT = ["version_nb", "search_id", "search_country",
                "search_date", "search_time", "origin_city", "destination_city", "request_dep_date",
                "request_return_date", "passengers_string",
                "currency", "price", "taxes", "fees", "nb_of_flights"]
 
_FLIGHT_LAYOUT = ["dep_airport", "dep_date", "dep_time",
                  "arr_airport", "arr_date", "arr_time",
                  "operating_airline", "marketing_airline", "flight_nb", "cabin"]
 
logging.basicConfig(format='%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
neob = None
def get_neob():
    global neob
    if neob is None:
        logger.info("Init geography module neobase")
        if _HAS_NEOBASE:
            neob = neobase.NeoBase()
        else:
            logger.warning("Using fallback geography module")
            neob = NeoBaseFallback()
    return neob
 
# Currency rates
def load_rates(rates_file):
    header = None
    rates = []
    with open(rates_file, 'r') as f:
        for line in f:
            array = line.rstrip().split(',')
            if len(array)<=1: return None
            array = [x.lstrip() for x in array if x != ""]
            if header == None:
                header = array
            else:
                rate_date = datetime.datetime.strptime(array[0], "%d %B %Y").strftime("%Y-%m-%d")
                array = [rate_date] + list(map(float, array[1:]))
                rates.append(dict(zip(header, array)))
    rates = rates[-1]
    return rates
 
def decode_line(line):
    try:
        if isinstance(line, bytes):
            line = line.decode()
        array = line.rstrip().split('^')
        if len(array)<=1:
            return None
        reco = dict(zip(_RECO_LAYOUT, array))
        read_columns_nb=len(_RECO_LAYOUT)
        reco["nb_of_flights"] = int(reco["nb_of_flights"])
        reco["flights"]=[] 
        for i in range(reco["nb_of_flights"]):
            flight=dict(zip(_FLIGHT_LAYOUT, array[read_columns_nb:]))
            read_columns_nb+=len(_FLIGHT_LAYOUT)
            reco["flights"].append(flight)
    except:
        return None
    return reco
 
_SEARCH_FIELDS = ["version_nb", "search_id", "search_country", "search_date", "search_time", "origin_city", "destination_city", "request_dep_date", "request_return_date", "passengers_string", "currency"]
 
def group_and_decorate(recos_in, rates):
    if recos_in is None or len(recos_in) == 0:
        return None
    recos = [reco for reco in recos_in if reco is not None]
 
    def to_euros(amount):
        if search["currency"] == "EUR":
            return amount
        return round(amount / rates.get(search["currency"], 1.0), 2)
 
    search = {key: recos[0][key] for key in _SEARCH_FIELDS}
    search["recos"] = [{key: value for key, value in reco.items() if key not in _SEARCH_FIELDS} for reco in recos]
 
    search_date = datetime.datetime.strptime(search["search_date"],'%Y-%m-%d')
    request_dep_date = datetime.datetime.strptime(search["request_dep_date"],'%Y-%m-%d')
    search["advance_purchase"] = (request_dep_date - search_date).days
 
    search["stay_duration"] = -1 if search["request_return_date"] == "" else (datetime.datetime.strptime(search["request_return_date"],'%Y-%m-%d') - request_dep_date).days
    search["trip_type"] = "OW" if search["stay_duration"] == -1 else "RT"
 
    search['passengers'] = [{"passenger_type": p.split("=")[0], "passenger_nb": int(p.split("=")[1])} for p in search['passengers_string'].rstrip().split(',')]
    search["origin_country"] = get_neob().get(search["origin_city"], 'country_code')
    search["destination_country"] = get_neob().get(search["destination_city"], 'country_code')
    search["geo"] = "D" if search["origin_country"] == search["destination_country"] else "I" 
    search["OnD"] = f"{search['origin_city']}-{search['destination_city']}"
    search["OnD_distance"] = round(get_neob().distance(search["origin_city"], search["destination_city"]))
 
    return search
 
def encoder_json(search):
    return json.dumps(search)
 
encoders = {"json": encoder_json}
 
def process(args):
    rates = load_rates(args.rates_file)
    cnt = Counter()
    recos, current_search_id = [], 0
    with gzip.open(args.input_file, 'r') as f:
        for line in f:
            reco = decode_line(line)
            if reco:
                if reco["search_id"] != current_search_id:
                    current_search_id = reco["search_id"]
                    if recos:
                        search = group_and_decorate(recos, rates)
                        if search:
                            yield search
                        recos = []
                recos.append(reco)
    if recos:
        search = group_and_decorate(recos, rates)
        if search:
            yield search

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        file_data = req.get_body()
        with tempfile.NamedTemporaryFile(delete=False, suffix='.gz') as temp_file:
            temp_file.write(file_data)
            temp_filename = temp_file.name

        class Args:
            def __init__(self, input_file):
                self.input_file = input_file
                self.rates_file = os.path.join(os.path.dirname(__file__), "../etc/eurofxref.csv")

        try:
            results = [search for search in process(Args(temp_filename))]
        finally:
            # Make sure we always clean up the temp file
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)

        return func.HttpResponse(body=json.dumps(results), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )