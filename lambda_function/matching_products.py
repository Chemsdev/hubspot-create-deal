import os
import re
import time
import json
import unicodedata
import requests
from typing import List, Dict, Any, Optional, Union, Tuple

# ===================== CONFIG HUBSPOT =====================
HS_PRODUCTS_LIST_URL = "https://api.hubapi.com/crm/v3/objects/products"

# Propriétés produits qu’on récupère une fois pour toutes
HS_PROD_NAME  = "name"
HS_PROD_PRICE = "price"          # adapte si autre champ prix
HS_PROD_SKU   = "hs_sku"         # si tu l'utilises
HS_PROD_CODE  = "hs_product_id"  # code interne HubSpot (facultatif)
HS_PROD_DESC  = "description"    # utile pour EAN/GTIN ou tags (UG/PLV)
PRODUCT_PROPERTIES = [HS_PROD_NAME, HS_PROD_PRICE, HS_PROD_SKU, HS_PROD_CODE, HS_PROD_DESC]

REQUEST_TIMEOUT = 20
PAGE_LIMIT = 100

# ===================== SIMILARITÉ =====================
try:
    from rapidfuzz import fuzz
    def name_ratio(a, b):
        return fuzz.token_set_ratio(a or "", b or "")
except Exception:
    def name_ratio(a, b):
        # fallback très simple
        a_set = set((a or "").split())
        b_set = set((b or "").split())
        if not a_set or not b_set:
            return 0
        return int(100 * len(a_set & b_set) / max(1, len(a_set | b_set)))

# ===================== NORMALISATION / EXTRACTIONS =====================
def _strip_accents_lower(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w\s\-]", " ", s)  # on garde tirets pour "citron-vert"
    s = re.sub(r"\s+", " ", s).strip()
    return s

# tokens utiles : tailles, categories, aromes, EAN
SIZE_RE   = re.compile(r"\bx\s?(\d{1,3})\b")      # "x42", "x 60"
EAN_RE    = re.compile(r"\b(\d{13})\b")           # EAN-13
AROMES    = {"fraise","orange","citron","citron-vert","menthe"}
CATEGORIES= {"ug","plv","presentoir","présentoir","sachet","echantillon","échantillon","pack","trousse","carte","panneau","stop","meuble"}

def normalize_name_for_match(s: str) -> str:
    s = _strip_accents_lower(s)
    # enlever marque/termes trop génériques si besoin (naali, gommies…)
    noise = {
        "naali","gummies","gummie","gomme","gommes","gums","gummys",
        "pilulier","boite","b","x","de","du","la","le","les","des","et","vide","pack"
    }
    parts = [w for w in s.split() if len(w) > 2 and w not in noise]
    return " ".join(parts)

def extract_size_token(s: str) -> Optional[int]:
    m = SIZE_RE.search(_strip_accents_lower(s))
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None

def extract_aromes(s: str) -> set:
    t = _strip_accents_lower(s)
    hits = set()
    for a in AROMES:
        if a in t:
            hits.add(a)
    # normaliser "citron vert" -> "citron-vert"
    if "citron vert" in t:
        hits.add("citron-vert")
    return hits

def extract_categories(s: str) -> set:
    t = _strip_accents_lower(s)
    hits = set()
    for c in CATEGORIES:
        if c in t:
            # map “présentoir” → “presentoir” (clé unique)
            hits.add("presentoir" if c in {"presentoir","présentoir"} else c)
    return hits

def extract_eans(s: str) -> List[str]:
    return EAN_RE.findall(s or "")

# ===================== HUBSPOT FETCH =====================
def _get_token() -> str:
    ACCESS_TOKEN_HUBSPOT = os.getenv("ACCESS_TOKEN_HUBSPOT")
    if not ACCESS_TOKEN_HUBSPOT:
        raise RuntimeError(f"{ACCESS_TOKEN_HUBSPOT} absent. Exporte le token d’app privée HubSpot.")
    return ACCESS_TOKEN_HUBSPOT

HUBSPOT_TOKEN = _get_token()

def fetch_all_hubspot_products(properties: List[str] = PRODUCT_PROPERTIES, max_pages: Optional[int]=None) -> List[Dict[str, Any]]:
    """
    Liste complète des products HubSpot (pagination). On ramène les propriétés utiles.
    """
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    params = {
        "limit": PAGE_LIMIT,
        "properties": ",".join(properties),
        "archived": "false",
    }
    results = []
    after = None
    pages = 0
    while True:
        if after:
            params["after"] = after
        r = requests.get(HS_PRODUCTS_LIST_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code == 401:
            raise RuntimeError("401 HubSpot (products). Vérifie token/portail et scope 'crm.objects.products.read'.")
        if not r.ok:
            raise RuntimeError(f"HubSpot error {r.status_code}: {r.text}")
        data = r.json()
        batch = data.get("results", []) or []
        results.extend(batch)
        paging = data.get("paging", {})
        nextp  = (paging.get("next") or {}).get("after")
        pages += 1
        if max_pages and pages >= max_pages:
            break
        if not nextp:
            break
        after = nextp
        # throttle léger
        time.sleep(0.05)
    return results

# ===================== INDEX LOCAL =====================
class ProductCatalog:
    def __init__(self, hubspot_products: List[Dict[str, Any]]):
        self.rows = []
        for row in hubspot_products:
            props = row.get("properties", {}) or {}
            name     = props.get(HS_PROD_NAME, "") or ""
            price    = props.get(HS_PROD_PRICE, None)
            desc     = props.get(HS_PROD_DESC, "") or ""
            sku      = props.get(HS_PROD_SKU, None)
            hs_code  = props.get(HS_PROD_CODE, None)
            norm     = normalize_name_for_match(name)
            size     = extract_size_token(name)
            aromas   = extract_aromes(name)
            cats     = extract_categories(name) | extract_categories(desc)
            eans     = set(extract_eans(desc))

            self.rows.append({
                "id": row.get("id"),
                "name": name,
                "norm_name": norm,
                "price": _safe_float(price),
                "sku": sku,
                "hs_code": hs_code,
                "desc": desc,
                "size": size,
                "aromas": aromas,
                "cats": cats,
                "eans": eans,
            })

def _safe_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except:
        return None

# ===================== MATCHING =====================
def score_candidate(input_name: str,
                    input_price: Optional[float],
                    cand: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    """
    Score global = score_nom + bonus (prix, size, aromes, categories, ean).
    Renvoie (score_total, details_bonus)
    """
    details = {"name_score": 0, "price_bonus": 0, "size_bonus": 0, "aroma_bonus": 0, "cat_bonus": 0, "ean_bonus": 0}

    # 1) score de nom (fuzzy)
    in_norm  = normalize_name_for_match(input_name)
    cand_norm= cand["norm_name"]
    s_name   = name_ratio(in_norm, cand_norm)
    details["name_score"] = s_name

    total = s_name

    # 2) bonus prix (si >0), plus c’est proche, plus bonus
    if input_price is not None and input_price > 0 and cand["price"] not in (None, 0):
        diff = abs(cand["price"] - input_price)
        rel  = diff / max(1e-6, input_price)
        if rel <= 0.01:   # <=1%
            bonus = 12
        elif rel <= 0.03: # <=3%
            bonus = 9
        elif rel <= 0.07: # <=7%
            bonus = 6
        elif rel <= 0.12: # <=12%
            bonus = 3
        else:
            bonus = 0
        details["price_bonus"] = bonus
        total += bonus
    else:
        # si l’un vaut 0 et l’autre non → pénalité légère
        if (input_price == 0 and (cand["price"] or 0) > 0) or ((input_price or 0) > 0 and (cand["price"] or 0) == 0):
            total -= 3

    # 3) bonus size (x42, x60…)
    in_size = extract_size_token(input_name)
    if in_size and cand["size"] == in_size:
        details["size_bonus"] = 6
        total += 6

    # 4) bonus aromes (fraise, orange, citron-vert/menthe…)
    in_aromas = extract_aromes(input_name)
    common_aromas = in_aromas & cand["aromas"]
    if common_aromas:
        add = 6 if len(common_aromas) >= 1 else 0
        details["aroma_bonus"] = add
        total += add

    # 5) bonus categories (UG/PLV/Présentoir/Sachet/Échantillon/Pack/Trousse…)
    in_cats = extract_categories(input_name)
    common_cats = in_cats & cand["cats"]
    if common_cats:
        add = 8 if ("ug" in common_cats or "presentoir" in common_cats) else 5
        details["cat_bonus"] = add
        total += add

    # 6) bonus EAN si détectable des deux côtés
    in_eans = set(extract_eans(input_name))
    if in_eans and cand["eans"]:
        if in_eans & cand["eans"]:
            details["ean_bonus"] = 20
            total += 20

    return total, details

def match_one_item(catalog: ProductCatalog,
                   item: Dict[str, Any],
                   min_score: int = 78) -> Dict[str, Any]:
    """
    item = {'nom_produit': ..., 'prix_unitaire': ...}
    Retourne un dict avec match ou no_match + détails.
    """
    name_in  = item.get("nom_produit") or ""
    price_in = _safe_float(item.get("prix_unitaire"))

    best = None
    best_score = -10**9
    best_details = {}
    for cand in catalog.rows:
        sc, det = score_candidate(name_in, price_in, cand)
        if sc > best_score:
            best = cand
            best_score = sc
            best_details = det

    if best and best_score >= min_score:
        return {
            "input": item,
            "match": "found",
            "hs_object_id": best["id"],
            "matched_name": best["name"],
            "matched_price": best["price"],
            "score": int(best_score),
            "method": "fuzzy+signals",
            "details": best_details
        }
    else:
        return {
            "input": item,
            "match": "no_match",
            "hs_object_id": None,
            "matched_name": None,
            "matched_price": None,
            "score": int(best_score if best else 0),
            "method": "fuzzy+signals",
            "details": best_details if best else {}
        }

# ===================== WRAPPER (listes imbriquées) =====================
_product_cache_catalog: Optional[ProductCatalog] = None

def ensure_catalog(force_refresh: bool = False) -> ProductCatalog:
    global _product_cache_catalog
    if _product_cache_catalog is None or force_refresh:
        hub = fetch_all_hubspot_products(PRODUCT_PROPERTIES)
        _product_cache_catalog = ProductCatalog(hub)
    return _product_cache_catalog

Nested = Union[List[Any], Dict[str, Any]]

def match_products_preserve_shape(nested: Nested, min_score: int = 78, force_refresh: bool = False) -> Nested:
    """
    Accepte:
      - liste plate d’items produits
      - liste de listes
      - n niveaux d’imbrication
    Retourne la même structure, mais avec les objets résultat.
    """
    catalog = ensure_catalog(force_refresh=force_refresh)

    if isinstance(nested, list):
        if not nested:
            return []
        if all(isinstance(x, dict) for x in nested):
            return [match_one_item(catalog, x, min_score=min_score) for x in nested]
        return [match_products_preserve_shape(x, min_score=min_score, force_refresh=False) for x in nested]
    else:
        raise TypeError("L'entrée doit être une liste d’items ou une liste de listes.")