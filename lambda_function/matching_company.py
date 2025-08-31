import os
import re
import time
import unicodedata
import requests
from typing import List, Dict, Any, Optional

# ========= CONFIG PROPRIÉTÉS HUBSPOT =========

HS_PROPS_ADDRESS      = "address"
HS_PROPS_ADDRESS2     = "address2"
HS_PROPS_ZIP          = "zip"  
HS_PROPS_NAME         = "name"
HS_PROPS_CLIENT_NAALI = "client_naali"  
HS_PROPS_CITY         = "city"  

BASE_URL = "https://api.hubapi.com/crm/v3/objects/companies/search"
REQUEST_TIMEOUT = 15
RETRY_MAX = 4

# ========= SIMILARITÉ (rapidfuzz si dispo) =========
try:
    from rapidfuzz import fuzz
    def ratio(a, b):
        return fuzz.token_set_ratio(a or "", b or "")
except Exception:
    def ratio(a, b):
        a_set = set((a or "").split())
        b_set = set((b or "").split())
        if not a_set or not b_set:
            return 0
        return int(100 * len(a_set & b_set) / max(1, len(a_set | b_set)))

# ========= TOKEN =========
def _get_token() -> str:
    ACCESS_TOKEN_HUBSPOT = os.getenv("ACCESS_TOKEN_HUBSPOT")
    if not ACCESS_TOKEN_HUBSPOT:
        raise RuntimeError(
            "HUBSPOT_PRIVATE_APP_TOKEN absent. "
            "Exporte la variable d’environnement avec le token d’app privée HubSpot."
        )
    return ACCESS_TOKEN_HUBSPOT.strip()

HUBSPOT_TOKEN = _get_token()

# ========= NORMALISATION =========
def _strip_accents_lower(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _normalize_name(s: str) -> str:
    s = _strip_accents_lower(s)
    s = re.sub(r"\b(pharmacie|pharma|pharm|parapharmacie|para)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _normalize_address(s: str) -> str:
    s = _strip_accents_lower(s)
    replacements = {
        r"\bav\b": "avenue",
        r"\bav\.\b": "avenue",
        r"\bar\b": "avenue",
        r"\br\b": "rue",
        r"\br\.\b": "rue",
        r"\bbd\b": "boulevard",
        r"\bctr?e?\b": "centre",
        r"\bctal?\b": "centre",
        r"\bctal?\.\b": "centre",
        r"\ball(ee|e|é)e?\b": "allee",
        r"\bste\b": "sainte",
        r"\bst\b": "saint",
        r"centre cial": "centre commercial",
        r"ctre cial": "centre commercial",
        r"c cial": "centre commercial",
    }
    for pat, rep in replacements.items():
        s = re.sub(pat, rep, s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _street_token(address: str) -> str:
    """
    Extrait un jeton 'rue': [numéro?] + 1-3 mots significatifs.
    Ex: "10 Rue Porte Baron" -> "10 porte baron"
    """
    norm = _normalize_address(address)
    parts = norm.split()
    if not parts:
        return ""
    num = parts[0] if parts and parts[0].isdigit() else ""
    stop = {
        "rue","avenue","boulevard","allee","impasse","chemin","route","place",
        "square","quai","cours","centre","commercial","ctre"
    }
    core = [w for w in parts if w not in stop and len(w) > 2][:3]
    token = " ".join(([num] if num else []) + core)
    return token.strip()

# ========= TOKENS LIEU / NOM (fallbacks) =========
MALL_KEYWORDS = {
    "centre commercial","centre cial","ctre cial","c cial","cc",
    "cora","auchan","carrefour","geant","geant casino","leclerc",
    "beausejours","beausejour","beauséjours","beauséjour",
    "val d europe","val d’europe","rivoli","grand littoral","rives d arcins",
}

def _place_token(s: str) -> str:
    t = _strip_accents_lower(s)
    hits = [k for k in MALL_KEYWORDS if k in t]
    return " ".join(sorted(set(hits)))[:60]

def _name_token(name: str) -> str:
    n = _normalize_name(name or "")
    parts = [w for w in n.split() if len(w) > 2][:4]
    return " ".join(parts)

# ========= APPELS API HUBSPOT =========
def _hs_search(filter_groups: List[Dict[str, Any]], properties: List[str], limit: int = 100) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "filterGroups": filter_groups,
        "properties": properties,
        "limit": limit,
    }
    for attempt in range(RETRY_MAX):
        resp = requests.post(BASE_URL, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        if resp.status_code == 401:
            raise RuntimeError(
                "401 Unauthorized depuis HubSpot.\n"
                "• Vérifie le token d’app privée et le portail.\n"
                "• Scopes requis: 'crm.objects.companies.read'.\n"
                f"• Réponse: {resp.text}"
            )
        if not resp.ok:
            raise RuntimeError(f"HubSpot API error {resp.status_code}: {resp.text}")
        data = resp.json()
        return data.get("results", []) or []
    return []

def hubspot_healthcheck():
    url = "https://api.hubapi.com/crm/v3/objects/companies?limit=1&properties=name"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if r.status_code == 401:
        raise RuntimeError(
            "Healthcheck 401: Token invalide ou scopes insuffisants (crm.objects.companies.read). "
            f"Réponse: {r.text}"
        )
    if not r.ok:
        raise RuntimeError(f"Healthcheck error {r.status_code}: {r.text}")
    return True

# ========= SCORING =========
def _score_candidate(input_addr: str, input_name: str, cand: Dict[str, Any]) -> int:
    props = cand.get("properties", {})
    hs_addr = " ".join(filter(None, [
        props.get(HS_PROPS_ADDRESS, ""),
        props.get(HS_PROPS_ADDRESS2, "")
    ]))
    hs_name = props.get(HS_PROPS_NAME, "")

    a_in = _normalize_address(input_addr)
    a_hs = _normalize_address(hs_addr)
    n_in = _normalize_name(input_name or "")
    n_hs = _normalize_name(hs_name or "")

    addr_score = ratio(a_in, a_hs)
    name_score = ratio(n_in, n_hs) if n_in and n_hs else 0
    return int(0.7 * addr_score + 0.3 * name_score)

def _pick_best(input_item: Dict[str, str], candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None
    best = None
    best_score = -1
    for c in candidates:
        s = _score_candidate(input_item.get("adresse",""), input_item.get("nom",""), c)
        if s > best_score:
            best = c
            best_score = s
    if best is None:
        return None
    best["__match_score"] = best_score
    return best

# ========= UTIL: cast CLIENT NAALI =========
def _to_bool_or_oui_non(v, as_oui_non=False):
    if v is None:
        return None
    s = str(v).strip().lower()
    truthy = {"true","1","yes","y","oui"}
    falsy  = {"false","0","no","n","non"}
    if s in truthy:
        return "Oui" if as_oui_non else True
    if s in falsy:
        return "Non" if as_oui_non else False
    return v  # valeur inattendue: renvoyer brut

# ========= FONCTION PRINCIPALE =========
def find_hubspot_company_ids(items: List[Dict[str, str]], min_score: int = 70) -> List[Dict[str, Any]]:
    """
    items: [{"nom":..., "adresse":..., "code_postal":...}, ...]
    Retourne pour chaque item: hs_object_id, matched_name, client_naali, score, method
    """
    out = []
    props = [HS_PROPS_NAME, HS_PROPS_ADDRESS, HS_PROPS_ADDRESS2, HS_PROPS_ZIP, HS_PROPS_CLIENT_NAALI]
    # Si tu veux la ville:
    # props.append(HS_PROPS_CITY)

    for it in items:
        nom = it.get("nom", "")
        adr = it.get("adresse", "")
        cp  = (it.get("code_postal") or "").strip()

        street_tok = _street_token(adr)
        chosen = None
        method = None

        # 1) zip AND address CONTAINS_TOKEN(street_tok)
        if cp and street_tok:
            filters = [{
                "filters": [
                    {"propertyName": HS_PROPS_ZIP, "operator": "EQ", "value": cp},
                    {"propertyName": HS_PROPS_ADDRESS, "operator": "CONTAINS_TOKEN", "value": street_tok}
                ]
            }]
            cand = _hs_search(filters, props)
            chosen = _pick_best(it, cand)
            method = "zip+address_token" if chosen else None

        # 2) zip AND address2 CONTAINS_TOKEN(street_tok)
        if not chosen and cp and street_tok:
            filters = [{
                "filters": [
                    {"propertyName": HS_PROPS_ZIP, "operator": "EQ", "value": cp},
                    {"propertyName": HS_PROPS_ADDRESS2, "operator": "CONTAINS_TOKEN", "value": street_tok}
                ]
            }]
            cand = _hs_search(filters, props)
            chosen = _pick_best(it, cand)
            method = "zip+address2_token" if chosen else None

        # 3) zip only + scoring
        if not chosen and cp:
            filters = [{
                "filters": [
                    {"propertyName": HS_PROPS_ZIP, "operator": "EQ", "value": cp}
                ]
            }]
            cand = _hs_search(filters, props)
            chosen = _pick_best(it, cand)
            method = "zip_only" if chosen else None

        # 3bis) zip AND (address|address2) CONTAINS_TOKEN(place_token) si centre commercial détecté
        if (not chosen or chosen.get("__match_score", 0) < min_score) and cp:
            place_tok = _place_token(adr)
            if place_tok:
                filters = [{
                    "filters": [
                        {"propertyName": HS_PROPS_ZIP, "operator": "EQ", "value": cp},
                        {"propertyName": HS_PROPS_ADDRESS, "operator": "CONTAINS_TOKEN", "value": place_tok}
                    ]
                }]
                cand = _hs_search(filters, props)
                tmp = _pick_best(it, cand)
                if tmp and (not chosen or _score_candidate(adr, nom, tmp) > chosen.get("__match_score", -1)):
                    chosen = tmp
                    method = "zip+place_in_address"

                if not chosen or chosen.get("__match_score", 0) < min_score:
                    filters = [{
                        "filters": [
                            {"propertyName": HS_PROPS_ZIP, "operator": "EQ", "value": cp},
                            {"propertyName": HS_PROPS_ADDRESS2, "operator": "CONTAINS_TOKEN", "value": place_tok}
                        ]
                    }]
                    cand = _hs_search(filters, props)
                    tmp = _pick_best(it, cand)
                    if tmp and (not chosen or _score_candidate(adr, nom, tmp) > chosen.get("__match_score", -1)):
                        chosen = tmp
                        method = "zip+place_in_address2"

        # 3ter) zip AND name CONTAINS_TOKEN(name_token) fallback sur le nom
        if (not chosen or chosen.get("__match_score", 0) < min_score) and cp:
            n_tok = _name_token(nom)
            if n_tok:
                filters = [{
                    "filters": [
                        {"propertyName": HS_PROPS_ZIP, "operator": "EQ", "value": cp},
                        {"propertyName": HS_PROPS_NAME, "operator": "CONTAINS_TOKEN", "value": n_tok}
                    ]
                }]
                cand = _hs_search(filters, props)
                tmp = _pick_best(it, cand)
                if tmp and (not chosen or _score_candidate(adr, nom, tmp) > chosen.get("__match_score", -1)):
                    chosen = tmp
                    method = "zip+name_token"

        # Sortie
        if chosen and chosen.get("__match_score", 0) >= min_score:
            props_chosen = chosen.get("properties", {}) or {}
            out.append({
                "input": it,
                "match": "found",
                "hs_object_id": chosen.get("id"),
                "matched_name": props_chosen.get(HS_PROPS_NAME, ""),
                "score": chosen.get("__match_score", 0),
                "method": method,
                "client_naali": _to_bool_or_oui_non(props_chosen.get(HS_PROPS_CLIENT_NAALI), as_oui_non=True)
            })
        else:
            out.append({
                "input": it,
                "match": "no_match",
                "hs_object_id": None,
                "matched_name": None,
                "score": int(chosen.get("__match_score", 0)) if chosen else 0,
                "method": method,
                "client_naali": None
            })

    return out
