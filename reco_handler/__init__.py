import azure.functions as func
import logging
from recoReader import decode_line

def main(event: func.EventHubEvent):
    line = event.get_body().decode()
    logging.info(f"Message reçu : {line}")
    reco = decode_line(line)
    if reco:
        logging.info(f"Reco OK : {reco['search_id']}")
