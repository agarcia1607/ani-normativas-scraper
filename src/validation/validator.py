"""
validator.py
------------
Etapa de validación entre extracción y escritura.

Lee las reglas desde configs/validation_rules.yaml y aplica:
  - Verificación de tipo de dato
  - Verificación de regex
  - Si campo requerido falla  → fila descartada
  - Si campo opcional falla   → campo queda NULL, fila continúa

Punto de entrada público: run_validation(records) -> list[dict]
"""

import re
import logging
import os
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ruta al archivo de reglas
# ---------------------------------------------------------------------------
_DEFAULT_RULES_PATH = Path(__file__).parents[2] / "configs" / "validation_rules.yaml"
RULES_PATH = Path(os.environ.get("VALIDATION_RULES_PATH", _DEFAULT_RULES_PATH))


# ---------------------------------------------------------------------------
# Carga de reglas
# ---------------------------------------------------------------------------

def load_rules(path: Path = RULES_PATH) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("fields", {})


# ---------------------------------------------------------------------------
# Validadores de tipo
# ---------------------------------------------------------------------------

def _check_type(value: Any, expected_type: str) -> bool:
    if value is None:
        return False
    if expected_type == "str":
        return isinstance(value, str) and bool(value.strip())
    if expected_type == "int":
        if isinstance(value, bool):
            return False
        return isinstance(value, int) or (isinstance(value, str) and value.strip().lstrip("-").isdigit())
    if expected_type == "bool":
        return isinstance(value, bool) or value in (0, 1, "true", "false", "True", "False")
    if expected_type == "date":
        if not isinstance(value, str):
            return False
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value.strip()[:10]))
    return True  # tipo desconocido → pass


def _check_regex(value: Any, pattern: str) -> bool:
    if not isinstance(value, str):
        return False
    return bool(re.match(pattern, value, re.DOTALL))


# ---------------------------------------------------------------------------
# Validación de un registro
# ---------------------------------------------------------------------------

def validate_record(record: dict, rules: dict) -> tuple[dict | None, list[str]]:
    """
    Valida un registro contra las reglas.

    Returns:
        (validated_record, issues)
        validated_record es None si debe descartarse por campo requerido inválido.
        issues es la lista de problemas encontrados (para logging).
    """
    result = dict(record)  # copia
    issues: list[str] = []

    for field, rule in rules.items():
        expected_type = rule.get("type")
        pattern = rule.get("regex")
        required = rule.get("required", False)

        value = result.get(field)

        # --- verificación de tipo ---
        type_ok = True
        if expected_type and value is not None:
            type_ok = _check_type(value, expected_type)
            if not type_ok:
                issues.append(f"{field}: tipo inválido (esperado={expected_type}, valor={value!r})")

        # --- verificación de regex ---
        regex_ok = True
        if pattern and value is not None and type_ok:
            regex_ok = _check_regex(str(value), pattern)
            if not regex_ok:
                issues.append(f"{field}: regex no cumplida (pattern={pattern}, valor={value!r})")

        field_valid = type_ok and regex_ok

        # --- valor None también es inválido si required ---
        if value is None and required:
            issues.append(f"{field}: NULL en campo obligatorio")
            field_valid = False

        if not field_valid:
            if required:
                return None, issues  # descartar fila
            else:
                result[field] = None  # nullear campo opcional

    return result, issues


# ---------------------------------------------------------------------------
# Punto de entrada público
# ---------------------------------------------------------------------------

def run_validation(records: list[dict], rules_path: Path = RULES_PATH) -> list[dict]:
    """
    Valida una lista de registros.

    Returns:
        list[dict]: registros que pasaron validación (campos opcionales
                    inválidos quedan como NULL).
    """
    rules = load_rules(rules_path)
    logger.info("Reglas de validación cargadas desde %s (%d campos).", rules_path, len(rules))

    total_in = len(records)
    validated: list[dict] = []
    discarded = 0

    for record in records:
        result, issues = validate_record(record, rules)
        if result is None:
            discarded += 1
            logger.debug(
                "Fila descartada [title=%r]: %s",
                record.get("title"),
                "; ".join(issues),
            )
        else:
            if issues:
                logger.debug(
                    "Fila con campos nulleados [title=%r]: %s",
                    record.get("title"),
                    "; ".join(issues),
                )
            validated.append(result)

    logger.info(
        "Validación completada — entrada: %d | aprobados: %d | descartados: %d",
        total_in,
        len(validated),
        discarded,
    )
    return validated
