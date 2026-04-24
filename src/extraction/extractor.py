"""
extractor.py
------------
Scraping de normativas ANI.

Lógica extraída íntegramente de la Lambda original:
  - clean_quotes
  - get_rtype_id
  - is_valid_created_at
  - normalize_datetime
  - extract_title_and_link
  - extract_summary
  - extract_creation_date
  - scrape_page

Punto de entrada público: run_extraction(num_pages) -> list[dict]
"""

import re
import logging
from datetime import datetime

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes (idénticas a la Lambda)
# ---------------------------------------------------------------------------
ENTITY_VALUE = "Agencia Nacional de Infraestructura"
FIXED_CLASSIFICATION_ID = 13
URL_BASE = (
    "https://www.ani.gov.co/informacion-de-la-ani/normatividad"
    "?field_tipos_de_normas__tid=12&title=&body_value="
    "&field_fecha__value%5Bvalue%5D%5Byear%5D="
)

CLASSIFICATION_KEYWORDS = {
    "resolución": 15,
    "resolucion": 15,
    "decreto": 14,
}
DEFAULT_RTYPE_ID = 14


# ---------------------------------------------------------------------------
# Helpers (sin cambios respecto a la Lambda)
# ---------------------------------------------------------------------------

def clean_quotes(text: str) -> str:
    if not text:
        return text
    quotes_map = {
        "\u201C": "", "\u2018": "", "\u2019": "", "\u00AB": "", "\u00BB": "",
        "\u201E": "", "\u201A": "", "\u2039": "", "\u203A": "", '"': "",
        "'": "", "´": "", "`": "", "′": "", "″": "",
    }
    cleaned = text
    for char, replacement in quotes_map.items():
        cleaned = cleaned.replace(char, replacement)
    pattern = r'["\'\u201C\u201D\u2018\u2019\u00AB\u00BB\u201E\u201A\u2039\u203A\u2032\u2033]'
    cleaned = re.sub(pattern, "", cleaned)
    return " ".join(cleaned.strip().split())


def get_rtype_id(title: str) -> int:
    title_lower = title.lower()
    for keyword, rtype_id in CLASSIFICATION_KEYWORDS.items():
        if keyword in title_lower:
            return rtype_id
    return DEFAULT_RTYPE_ID


def is_valid_created_at(value) -> bool:
    if not value:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, datetime):
        return True
    return False


def normalize_datetime(dt):
    if dt is None:
        return None
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


# ---------------------------------------------------------------------------
# Extractores de campos (sin cambios en criterios de omisión)
# ---------------------------------------------------------------------------

def extract_title_and_link(row, norma_data: dict, verbose: bool, row_num: int) -> bool:
    title_cell = row.find("td", class_="views-field views-field-title")
    if not title_cell:
        if verbose:
            logger.debug("Fila %s: sin celda de título. Saltando.", row_num)
        return False

    title_link = title_cell.find("a")
    if not title_link:
        if verbose:
            logger.debug("Fila %s: sin enlace. Saltando.", row_num)
        return False

    raw_title = title_link.get_text(strip=True)
    cleaned_title = clean_quotes(raw_title)

    if len(cleaned_title) > 65:
        if verbose:
            logger.debug(
                "Saltando norma con título demasiado largo: '%s' (%d chars)",
                cleaned_title,
                len(cleaned_title),
            )
        return False

    norma_data["title"] = cleaned_title

    external_link = title_link.get("href")
    if external_link and not external_link.startswith("http"):
        external_link = "https://www.ani.gov.co" + external_link

    norma_data["external_link"] = external_link
    norma_data["gtype"] = "link" if external_link else None

    if not norma_data["external_link"]:
        if verbose:
            logger.debug(
                "Saltando norma '%s': sin enlace externo válido.", norma_data["title"]
            )
        return False

    return True


def extract_summary(row, norma_data: dict) -> None:
    summary_cell = row.find("td", class_="views-field views-field-body")
    if summary_cell:
        raw = summary_cell.get_text(strip=True)
        norma_data["summary"] = clean_quotes(raw).capitalize()
    else:
        norma_data["summary"] = None


def extract_creation_date(row, norma_data: dict, verbose: bool, row_num: int) -> bool:
    fecha_cell = row.find("td", class_="views-field views-field-field-fecha--1")
    if fecha_cell:
        fecha_span = fecha_cell.find("span", class_="date-display-single")
        if fecha_span:
            raw = fecha_span.get("content", fecha_span.get_text(strip=True))
            if "T" in raw:
                norma_data["created_at"] = raw.split("T")[0]
            elif "/" in raw:
                try:
                    day, month, year = raw.split("/")
                    norma_data["created_at"] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except Exception:
                    norma_data["created_at"] = raw
            else:
                norma_data["created_at"] = raw
        else:
            norma_data["created_at"] = fecha_cell.get_text(strip=True)
    else:
        norma_data["created_at"] = None

    if not is_valid_created_at(norma_data["created_at"]):
        if verbose:
            logger.debug(
                "Saltando norma '%s': fecha inválida (%s).",
                norma_data.get("title"),
                norma_data["created_at"],
            )
        return False

    return True


# ---------------------------------------------------------------------------
# Scraping por página
# ---------------------------------------------------------------------------

def scrape_page(page_num: int, verbose: bool = False) -> list:
    url = URL_BASE if page_num == 0 else f"{URL_BASE}&page={page_num}"
    logger.info("Scrapeando página %d: %s", page_num, url)

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Error HTTP en página %d: %s", page_num, exc)
        return []

    try:
        soup = BeautifulSoup(response.content, "html.parser")
        tbody = soup.find("tbody")
        if not tbody:
            logger.warning("Página %d: sin tabla <tbody>.", page_num)
            return []

        rows = tbody.find_all("tr")
        logger.debug("Página %d: %d filas encontradas.", page_num, len(rows))

        page_data = []
        for i, row in enumerate(rows, 1):
            try:
                norma_data = {
                    "created_at": None,
                    "update_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "is_active": True,
                    "title": None,
                    "gtype": None,
                    "entity": ENTITY_VALUE,
                    "external_link": None,
                    "rtype_id": None,
                    "summary": None,
                    "classification_id": FIXED_CLASSIFICATION_ID,
                }

                if not extract_title_and_link(row, norma_data, verbose, i):
                    continue
                extract_summary(row, norma_data)
                if not extract_creation_date(row, norma_data, verbose, i):
                    continue

                norma_data["rtype_id"] = get_rtype_id(norma_data["title"])
                page_data.append(norma_data)

            except Exception as exc:
                logger.warning("Error en fila %d, página %d: %s", i, page_num, exc)
                continue

        return page_data

    except Exception as exc:
        logger.error("Error procesando página %d: %s", page_num, exc)
        return []


# ---------------------------------------------------------------------------
# Punto de entrada público
# ---------------------------------------------------------------------------

def run_extraction(num_pages: int = 9) -> list[dict]:
    """
    Scrapea las primeras `num_pages` páginas de ANI.

    Returns:
        list[dict]: registros extraídos y válidos según criterios de scraping.
    """
    logger.info("Iniciando extracción — páginas: 0..%d", num_pages - 1)
    all_records: list[dict] = []

    for page_num in range(num_pages):
        page_data = scrape_page(page_num)
        all_records.extend(page_data)
        if (page_num + 1) % 3 == 0:
            logger.info(
                "Progreso: %d/%d páginas — %d registros acumulados.",
                page_num + 1,
                num_pages,
                len(all_records),
            )

    logger.info("Extracción completada — total registros: %d", len(all_records))
    return all_records
