import os
from dotenv import load_dotenv


def obter_config_api():
    load_dotenv()  # carrega .env para os.environ
    api_key = os.getenv("API_KEY", "").strip()
    api_base_url = os.getenv("API_BASE_URL", "").strip()

    if not api_key or not api_base_url:
        raise EnvironmentError(
            "Variáveis de ambiente API_KEY e API_BASE_URL são obrigatórias e devem estar preenchidas"
        )

    return api_key, api_base_url