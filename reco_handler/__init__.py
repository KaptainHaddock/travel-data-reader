import logging
import azure.functions as func
from recoReader import decode_line, group_and_decorate, load_rates

RATES_FILE = "etc/eurofxref.csv"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🔔 Requête HTTP reçue")

    body = req.get_body().decode()

    # Chaque ligne du body est une reco
    lines = body.splitlines()
    recos = [decode_line(line) for line in lines if decode_line(line)]

    if len(recos) == 0:
        return func.HttpResponse("Aucune reco décodée", status_code=400)

    rates = load_rates(RATES_FILE)
    decorated = group_and_decorate(recos, rates)

    if not decorated:
        return func.HttpResponse("Erreur dans la décoration", status_code=500)

    return func.HttpResponse(f"Décoration OK pour search_id: {decorated['search_id']}", status_code=200)
