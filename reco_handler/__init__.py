import logging
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🎉 Requête reçue avec succès dans Azure Function HTTP !")

    return func.HttpResponse(
        "✅ Hello Vincent ! Ta fonction fonctionne 🎯",
        status_code=200
    )
