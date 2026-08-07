from flask import Flask, render_template, jsonify, request
import urllib.request
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

@app.after_request
def no_cache_api(response):
    """Los endpoints /api/* nunca deben cachearse — HOT y los scores dependen
    de datos en vivo, y el navegador (Safari/iOS en particular) puede cachear
    fetch() agresivamente sin este header, dejando la UI 'como una foto'
    aunque el JS pida datos nuevos correctamente."""
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response

DATABASE_URL = os.environ.get("DATABASE_URL", "")
INJECT_TOKEN = os.environ.get("INJECT_TOKEN", "")   # token para /api/inject (sin token, el endpoint queda cerrado)

# ── DB CONNECTION ─────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signal_log (
                id          SERIAL PRIMARY KEY,
                logged_at   TIMESTAMP DEFAULT NOW(),
                log_date    DATE DEFAULT CURRENT_DATE,
                sym         VARCHAR(20),
                event_id    VARCHAR(50),
                score       INTEGER,
                dir         VARCHAR(10),
                rsi         FLOAT,
                price_entry FLOAT,
                price_exit  FLOAT,
                chg_pct     FLOAT,
                correct     BOOLEAN
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS event_state (
                event_id        VARCHAR(50) PRIMARY KEY,
                last_escalation DATE
            );
        """)
        # Contenido inyectado manualmente (tweets/notas) que suma como titular
        # del evento — ver /api/inject y fetch_injected. Caduca por ventana de tiempo.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS injected_signals (
                id         SERIAL PRIMARY KEY,
                event_id   VARCHAR(50),
                title      TEXT,
                source     VARCHAR(80) DEFAULT 'manual',
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print("DB init error:", e)

# ── SCORE MAP ─────────────────────────────────────────────────────────────────
SCORE_MAP = {
    "HEATWAVE":  {"CEG":80,"VST":75,"NRG":70,"AES":60,"ETR":65,"LNG":70,"EQT":65,"AR":60,"SWN":55,"LII":75,"CARR":70,"TT":65,"NEE":40,"DUK":35,"SO":35,"AEP":35},
    "HURRICANE": {"GNRC":85,"SHYF":60,"POWI":55,"HD":75,"LOW":70,"SHW":65,"MLM":70,"VMC":70,"MPC":60,"VLO":55,"PSX":50,"RE":-70,"RNR":-65,"MKL":-60},
    "DROUGHT":   {"MOS":80,"NTR":75,"CF":78,"ICL":65,"CTVA":70,"FMC":65,"AWK":60,"WTRG":55,"CWT":50,"CPB":-55,"SJM":-50,"CAG":-55,"MKC":-45},
    "FLOOD":     {"XYL":75,"AWK":65,"VMC":60,"MLM":55,"NUE":50,"ALL":-70,"TRV":-65,"CB":-60,"HIG":-65,"ADM":40,"BG":35,"INGR":30},
    "PANDEMIC":  {"MRNA":90,"PFE":80,"BNTX":85,"NVAX":75,"QDEL":80,"BDX":70,"DHR":65,"TMO":65,"MMM":60,"HOLX":55,"UAL":-80,"DAL":-80,"MAR":-70,"HLT":-65},
    "FLU":       {"GILD":75,"ABBV":65,"JNJ":60,"MRK":65,"QDEL":80,"BDX":65,"CVS":60,"WBA":55,"HCA":50,"THC":45,"CYH":40},
    "HORMUZ":    {"XOM":80,"CVX":75,"OXY":78,"COP":72,"EOG":70,"SLB":65,"HAL":60,"BKR":58,"UAL":-70,"DAL":-68,"AAL":-65,"ZIM":50,"MATX":45,"STNG":55},
    "TAIWAN":    {"LMT":80,"RTX":78,"NOC":75,"GD":72,"HII":68,"GLD":70,"IAU":68,"NEM":60,"GOLD":62,"NVDA":-65,"AMD":-60,"AMAT":-68,"LRCX":-65,"AAPL":-55,"AMZN":-40,"WMT":-35},
    "NATO":      {"LMT":82,"RTX":80,"NOC":78,"GD":75,"LDOS":65,"XOM":65,"CVX":60,"LNG":70,"CQP":65,"GLD":75,"TLT":60,"IAU":72,"SPY":-60,"QQQ":-65,"IWM":-58},
    "AI_BOOM":   {"NVDA":90,"AMD":80,"AVGO":78,"MRVL":72,"CEG":70,"VST":68,"ETR":65,"NRG":62,"VRT":75,"SMCI":70,"CIEN":65,"CSCO":55,"JNPR":50},
    "CYBER":     {"CRWD":88,"PANW":85,"ZS":80,"FTNT":78,"S":72,"LDOS":65,"SAIC":62,"BAH":60,"CACI":58,"JPM":-60,"BAC":-55,"NEE":-50,"DUK":-48},
    "SUPERBOWL": {"PEP":70,"KO":65,"TAP":60,"STZ":62,"NFLX":65,"GOOGL":60,"META":68,"TTD":72,"DASH":65,"UBER":60,"WMT":50,"TGT":48,"COST":45},
    "BTS":       {"WMT":65,"TGT":62,"AMZN":68,"COST":60,"AAPL":70,"DELL":65,"HPQ":58,"BBY":62,"UPS":60,"FDX":58,"NKE":55,"PVH":50,"RL":48},
}

SECTOR_MAP = {
    "HEATWAVE":  [{"name":"Power Generation","dir":"LONG","tickers":["CEG","VST","NRG","AES","ETR"]},{"name":"Natural Gas","dir":"LONG","tickers":["LNG","EQT","AR","SWN"]},{"name":"HVAC","dir":"LONG","tickers":["LII","CARR","TT"]},{"name":"Utilities","dir":"WATCH","tickers":["NEE","DUK","SO","AEP"]}],
    "HURRICANE": [{"name":"Generators","dir":"LONG","tickers":["GNRC","SHYF","POWI"]},{"name":"Construction","dir":"LONG","tickers":["HD","LOW","SHW","MLM","VMC"]},{"name":"Refiners","dir":"LONG","tickers":["MPC","VLO","PSX"]},{"name":"Insurance","dir":"SHORT","tickers":["RE","RNR","MKL"]}],
    "DROUGHT":   [{"name":"Fertilizers","dir":"LONG","tickers":["MOS","NTR","CF","ICL"]},{"name":"Agro Seeds","dir":"LONG","tickers":["CTVA","FMC"]},{"name":"Water","dir":"LONG","tickers":["AWK","WTRG","CWT"]},{"name":"Food Processing","dir":"SHORT","tickers":["CPB","SJM","CAG","MKC"]}],
    "FLOOD":     [{"name":"Water Infra","dir":"LONG","tickers":["XYL","AWK"]},{"name":"Construction","dir":"LONG","tickers":["VMC","MLM","NUE"]},{"name":"Insurance","dir":"SHORT","tickers":["ALL","TRV","CB","HIG"]},{"name":"Agro Commodities","dir":"WATCH","tickers":["ADM","BG","INGR"]}],
    "PANDEMIC":  [{"name":"Vaccines mRNA","dir":"LONG","tickers":["MRNA","PFE","BNTX","NVAX"]},{"name":"Diagnostics","dir":"LONG","tickers":["QDEL","BDX","DHR","TMO"]},{"name":"Medical Supplies","dir":"LONG","tickers":["MMM","HOLX"]},{"name":"Airlines Hotels","dir":"SHORT","tickers":["UAL","DAL","MAR","HLT"]}],
    "FLU":       [{"name":"Antivirals","dir":"LONG","tickers":["GILD","ABBV","JNJ","MRK"]},{"name":"Diagnostics","dir":"LONG","tickers":["QDEL","BDX"]},{"name":"Pharmacy","dir":"LONG","tickers":["CVS","WBA"]},{"name":"Hospitals","dir":"WATCH","tickers":["HCA","THC","CYH"]}],
    "HORMUZ":    [{"name":"Oil E&P","dir":"LONG","tickers":["XOM","CVX","OXY","COP","EOG"]},{"name":"Oilfield Svcs","dir":"LONG","tickers":["SLB","HAL","BKR"]},{"name":"Airlines","dir":"SHORT","tickers":["UAL","DAL","AAL"]},{"name":"Shipping","dir":"WATCH","tickers":["ZIM","MATX","STNG"]}],
    "TAIWAN":    [{"name":"Defense","dir":"LONG","tickers":["LMT","RTX","NOC","GD","HII"]},{"name":"Gold","dir":"LONG","tickers":["GLD","IAU","NEM","GOLD"]},{"name":"Semiconductors","dir":"SHORT","tickers":["NVDA","AMD","AMAT","LRCX"]},{"name":"Consumer","dir":"SHORT","tickers":["AAPL","AMZN","WMT"]}],
    "NATO":      [{"name":"Defense","dir":"LONG","tickers":["LMT","RTX","NOC","GD","LDOS"]},{"name":"Energy","dir":"LONG","tickers":["XOM","CVX","LNG","CQP"]},{"name":"Safe Haven","dir":"LONG","tickers":["GLD","TLT","IAU"]},{"name":"Risk-Off","dir":"SHORT","tickers":["SPY","QQQ","IWM"]}],
    "AI_BOOM":   [{"name":"AI Chips","dir":"LONG","tickers":["NVDA","AMD","AVGO","MRVL"]},{"name":"DC Power","dir":"LONG","tickers":["CEG","VST","ETR","NRG"]},{"name":"Cooling","dir":"LONG","tickers":["VRT","SMCI"]},{"name":"Fiber","dir":"LONG","tickers":["CIEN","CSCO","JNPR"]}],
    "CYBER":     [{"name":"Cybersecurity","dir":"LONG","tickers":["CRWD","PANW","ZS","FTNT","S"]},{"name":"Defense IT","dir":"LONG","tickers":["LDOS","SAIC","BAH","CACI"]},{"name":"Banks affected","dir":"SHORT","tickers":["JPM","BAC"]},{"name":"Utilities affected","dir":"SHORT","tickers":["NEE","DUK"]}],
    "SUPERBOWL": [{"name":"Beverages Snacks","dir":"LONG","tickers":["PEP","KO","TAP","STZ"]},{"name":"Streaming Ad","dir":"LONG","tickers":["NFLX","GOOGL","META","TTD"]},{"name":"Delivery","dir":"LONG","tickers":["DASH","UBER"]},{"name":"Retail","dir":"WATCH","tickers":["WMT","TGT","COST"]}],
    "BTS":       [{"name":"Retail","dir":"LONG","tickers":["WMT","TGT","AMZN","COST"]},{"name":"Electronics","dir":"LONG","tickers":["AAPL","DELL","HPQ","BBY"]},{"name":"Logistics","dir":"LONG","tickers":["UPS","FDX"]},{"name":"Apparel","dir":"WATCH","tickers":["NKE","PVH","RL"]}],
}

INTENSITY_MAP = {
    "HEATWAVE":40,"HURRICANE":35,"DROUGHT":30,"FLOOD":30,
    "PANDEMIC":45,"FLU":35,"HORMUZ":55,"TAIWAN":60,
    "NATO":50,"AI_BOOM":70,"CYBER":50,"SUPERBOWL":40,"BTS":35,
}

DEMAND_SUMMARY = {
    "HEATWAVE":  "Ola de calor dispara consumo electrico por uso masivo de AC.",
    "HURRICANE": "Huracan destruye infraestructura generando demanda urgente de generadores y construccion.",
    "DROUGHT":   "Sequia dana cosechas disparando precios de commodities y fertilizantes.",
    "FLOOD":     "Inundaciones generan demanda de infraestructura hidrica y reconstruccion.",
    "PANDEMIC":  "Brote pandemico dispara demanda de vacunas, diagnosticos y EPP.",
    "FLU":       "Temporada de gripe severa impulsa ventas de antivirales y tests rapidos.",
    "HORMUZ":    "Bloqueo en Hormuz restringe oferta de crudo, beneficiando productores.",
    "TAIWAN":    "Escalada militar eleva gasto en defensa y activos refugio.",
    "NATO":      "Conflicto OTAN dispara contratos de defensa y activos seguros.",
    "AI_BOOM":   "Boom de IA escala demanda de chips, energia y datacenters.",
    "CYBER":     "Ataque cibernetico genera demanda urgente de soluciones de seguridad.",
    "SUPERBOWL": "Evento masivo impulsa consumo de bebidas, snacks y streaming.",
    "BTS":       "Temporada escolar dispara gasto en electronica y logistica.",
}

KEYWORDS = {
    "HEATWAVE":  ["heat wave","heatwave","extreme heat","temperature record","electricity demand"],
    "HURRICANE": ["hurricane","tropical storm","cyclone","storm surge","landfall"],
    "DROUGHT":   ["drought","crop damage","harvest","rainfall deficit","water shortage"],
    "FLOOD":     ["flood","flooding","heavy rain","levee","inundation"],
    "PANDEMIC":  ["pandemic","outbreak","WHO","pathogen","virus","epidemic"],
    "FLU":       ["flu","influenza","respiratory","antiviral","hospitalization"],
    "HORMUZ":    ["hormuz","strait","tanker","oil supply","iran","gulf","crude"],
    "TAIWAN":    ["taiwan","strait","china military","pla","semiconductor","tsmc"],
    "NATO":      ["nato","russia","ukraine","escalation","military","missile"],
    "AI_BOOM":   ["artificial intelligence","AI investment","data center","nvidia","chips"],
    "CYBER":     ["cyberattack","ransomware","hack","breach","infrastructure attack"],
    "SUPERBOWL": ["super bowl","world cup","championship","nfl","fifa","advertising"],
    "BTS":       ["back to school","retail sales","consumer spending","electronics demand"],
}

RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/topNews",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
]

# ── Ajuste dinámico de intensidad según el contenido real de las noticias ───
# INTENSITY_MAP es solo el punto de partida ("baseline") de cada episodio.
# Antes esto quedaba FIJO para siempre — ahora se le suma/resta según si los
# titulares más recientes muestran escalada o desescalada real.
ESCALATION_PHRASES = [
    "ceasefire is over", "ceasefire ended", "ceasefire collapsed", "truce broken",
    "resumes strikes", "renewed strikes", "resumed attacks",
]
ESCALATION_WORDS = [
    "strike","strikes","struck","attack","attacked","bomb","bombing","bombed",
    "explosion","seized","blockade","invasion","missile","killed","clash","offensive",
]
DEESCALATION_WORDS = [
    "ceasefire","truce","peace deal","de-escalate","withdraw",
    "talks resume","agreement reached","calm returns","reopened",
]

# ── Decay de intensidad: sin escalada reciente, el evento se enfría solo ────
# Antes la intensidad quedaba alta "por inercia": si HORMUZ subía a 73 y
# después el tema desaparecía de las noticias (sin titulares de desescalada
# explícitos), quedaba en 73 para siempre. Ahora pierde 2 pts/día desde la
# última escalada detectada (cap -14, o sea el decay se agota en ~1 semana).
DECAY_PTS_POR_DIA = 2
DECAY_MAX = 14

def _get_last_escalation(event_id):
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT last_escalation FROM event_state WHERE event_id=%s", (event_id,))
        row = cur.fetchone(); cur.close(); conn.close()
        if row and row[0]: return row[0]
    except Exception as e:
        print(f"[DECAY get {event_id}] {e}")
    return None

def _set_last_escalation(event_id, d):
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("""INSERT INTO event_state (event_id, last_escalation) VALUES (%s,%s)
                       ON CONFLICT (event_id) DO UPDATE SET last_escalation=EXCLUDED.last_escalation""",
                    (event_id, d))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        print(f"[DECAY set {event_id}] {e}")

def intensity_decay(event_id, delta_hoy):
    """Retorna los puntos de decay a restar. Si hoy hubo escalada (delta>0),
    se registra la fecha y el decay es 0. Si no, se descuenta según los días
    transcurridos desde la última escalada registrada. Cualquier error de DB
    devuelve 0 — el decay es un refinamiento, nunca debe romper el score."""
    try:
        hoy = date.today()
        if delta_hoy > 0:
            _set_last_escalation(event_id, hoy)
            return 0
        ultima = _get_last_escalation(event_id)
        if not ultima:
            return 0  # sin historial — no castigar sin datos
        dias = (hoy - ultima).days
        return min(DECAY_MAX, max(0, dias * DECAY_PTS_POR_DIA))
    except Exception:
        return 0

def headline_intensity_delta(headlines):
    """+/- puntos según escalada/desescalada real en los titulares recientes.
    Frases de escalada fuerte (ej. 'ceasefire is over') priman sobre el
    match genérico de 'ceasefire' como palabra suelta, que sería desescalada
    engañosa en ese contexto."""
    delta = 0
    for h in headlines[:8]:
        title = (h.get("title","") + " " + h.get("summary","")).lower()
        # sacar comillas (rectas y curvas) para que "ceasefire is 'over'"
        # matchee igual que "ceasefire is over"
        title = title.replace("'", "").replace('"', "").replace("’", "").replace("‘", "")
        if any(p in title for p in ESCALATION_PHRASES):
            delta += 10
            continue
        esc = any(w in title for w in ESCALATION_WORDS)
        des = any(w in title for w in DEESCALATION_WORDS)
        if esc and not des:
            delta += 8
        elif des and not esc:
            delta -= 8
        # si matchean ambos sin frase fuerte -> ambiguo, no suma (evita ruido)
    return max(-25, min(25, delta))

# ── Clasificador de intensidad con IA (Claude) + fallback a keywords ─────────
# El matcheo por palabras clave funciona pero es frágil ante titulares
# creativos. Si hay ANTHROPIC_API_KEY configurada en Railway, se usa Claude
# (Haiku, barato) para clasificar escalada/desescalada con contexto real.
# Si no hay key, o el llamado falla, cae automáticamente al método de
# keywords — NUNCA rompe el endpoint.
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
_ai_delta_cache = {}   # {event_id: (timestamp, delta)} — TTL 15min para no
                        # pagar un llamado por cada refresh del dashboard

def intensity_delta(event_id, headlines):
    """Delta de intensidad -25..+25. Intenta IA, cae a keywords si falla."""
    if not ANTHROPIC_API_KEY or not headlines:
        return headline_intensity_delta(headlines), "keywords"

    import time as _t
    cached = _ai_delta_cache.get(event_id)
    if cached and _t.time() - cached[0] < 900:
        return cached[1], "ai"

    try:
        titulares = "\n".join(
            f"- {h.get('title','')}. {h.get('summary','')[:120]}"
            for h in headlines[:8])
        prompt = (
            f"Evento monitoreado: {event_id}.\n"
            f"Titulares recientes:\n{titulares}\n\n"
            "¿Estos titulares indican ESCALADA o DESESCALADA del evento? "
            "Respondé SOLO un JSON: {\"delta\": N} donde N es un entero entre "
            "-25 (fuerte desescalada) y 25 (fuerte escalada), 0 si es ambiguo "
            "o irrelevante. Sin texto adicional.")
        body = json.dumps({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 50,
            "messages": [{"role": "user", "content": prompt}],
        }).encode()
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages", data=body,
            headers={"Content-Type": "application/json",
                     "x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        texto = "".join(b.get("text","") for b in data.get("content",[])
                        if b.get("type") == "text")
        texto = texto.replace("```json","").replace("```","").strip()
        delta = int(json.loads(texto).get("delta", 0))
        delta = max(-25, min(25, delta))
        _ai_delta_cache[event_id] = (_t.time(), delta)
        return delta, "ai"
    except Exception as e:
        print(f"[AI DELTA {event_id}] fallback a keywords: {e}")
        return headline_intensity_delta(headlines), "keywords"

# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch_price(symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=30d"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=8) as r:
        data = json.loads(r.read())
    result = data["chart"]["result"][0]
    meta   = result["meta"]
    closes = [c for c in result.get("indicators",{}).get("quote",[{}])[0].get("close",[]) if c]
    price  = meta.get("regularMarketPrice", closes[-1] if closes else None)
    prev   = closes[-2] if len(closes) >= 2 else meta.get("chartPreviousClose", price)
    state  = meta.get("marketState","CLOSED")
    session = {"REGULAR":"LIVE","PRE":"PRE","POST":"POST","POSTPOST":"POST"}.get(state,"CLOSED")
    meta["_prevClose"]    = prev
    meta["_sessionLabel"] = session
    return data

# ── FUENTE 4: StockTwits — sentimiento social en tiempo real ────────────────
# Solo se llama para los finalistas del top5 (no para todo el universo)
# para no comerse el rate limit público (~200 req/hora sin auth).
def fetch_stocktwits_sentiment(symbol):
    """Retorna {'bullish': bool, 'ratio': float, 'volumen': int} en base a los
    últimos mensajes con sentimiento etiquetado en StockTwits para el símbolo."""
    try:
        url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=6) as r:
            data = json.loads(r.read())
        msgs = data.get("messages", [])
        bull = bear = 0
        for m in msgs:
            sent = (m.get("entities", {}) or {}).get("sentiment") or {}
            basic = sent.get("basic")
            if basic == "Bullish": bull += 1
            elif basic == "Bearish": bear += 1
        total = bull + bear
        if total < 3:  # muestra insuficiente para confiar en el ratio
            return {"bullish": False, "ratio": None, "volumen": len(msgs)}
        ratio = bull / total
        return {"bullish": ratio >= 0.65, "ratio": round(ratio, 2), "volumen": len(msgs)}
    except Exception as e:
        print(f"[STOCKTWITS] {symbol} error: {e}")
        return {"bullish": False, "ratio": None, "volumen": 0}

# ── FUENTE 5: SEC Form 4 — compras de insiders en los últimos 10 días ───────
_cik_cache = {}
def _get_cik(symbol):
    """Mapea ticker→CIK usando el archivo público de la SEC (se cachea en memoria)."""
    global _cik_cache
    if not _cik_cache:
        try:
            url = "https://www.sec.gov/files/company_tickers.json"
            req = urllib.request.Request(url, headers={"User-Agent": "WorldDash contact@americancotton.com.ar"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            for row in data.values():
                _cik_cache[row["ticker"].upper()] = str(row["cik_str"]).zfill(10)
        except Exception as e:
            print(f"[SEC] Error cargando company_tickers: {e}")
    return _cik_cache.get(symbol.upper())

def fetch_insider_activity(symbol, dias=10):
    """Busca Form 4 filed en los últimos `dias` días y chequea si hay
    transactionCode 'P' (compra en mercado abierto) en el documento.
    Solo se llama para los finalistas del top5 (rate-limit friendly)."""
    try:
        cik = _get_cik(symbol)
        if not cik: return {"insider_buy": False, "n_filings": 0}
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        req = urllib.request.Request(url, headers={"User-Agent": "WorldDash contact@americancotton.com.ar"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
        recent = data.get("filings", {}).get("recent", {})
        forms  = recent.get("form", [])
        dates  = recent.get("filingDate", [])
        accs   = recent.get("accessionNumber", [])
        docs   = recent.get("primaryDocument", [])
        limite = (date.today() - timedelta(days=dias)).isoformat()

        compras = 0
        cik_int = int(cik)
        for i, form in enumerate(forms):
            if form != "4" or dates[i] < limite: continue
            try:
                acc_nodash = accs[i].replace("-", "")
                doc_url = (f"https://www.sec.gov/Archives/edgar/data/"
                           f"{cik_int}/{acc_nodash}/{docs[i]}")
                dreq = urllib.request.Request(doc_url, headers={"User-Agent": "WorldDash contact@americancotton.com.ar"})
                with urllib.request.urlopen(dreq, timeout=6) as dr:
                    xml_txt = dr.read().decode("utf-8", errors="ignore")
                if "<transactionCode>P</transactionCode>" in xml_txt:
                    compras += 1
            except: continue
        return {"insider_buy": compras > 0, "n_filings": compras}
    except Exception as e:
        print(f"[SEC FORM4] {symbol} error: {e}")
        return {"insider_buy": False, "n_filings": 0}

def fetch_rss(event_id):
    keywords = [k.lower() for k in KEYWORDS.get(event_id, [])]
    headlines = []
    for feed_url in RSS_FEEDS:
        try:
            req = urllib.request.Request(feed_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=6) as r:
                root = ET.fromstring(r.read())
            for item in root.findall(".//item")[:30]:
                title = (item.find("title").text or "") if item.find("title") is not None else ""
                desc  = (item.find("description").text or "") if item.find("description") is not None else ""
                combined = (title + " " + desc).lower()
                if any(kw in combined for kw in keywords):
                    neg = sum(1 for w in ["decline","fall","drop","risk","warn","fear"] if w in combined)
                    pos = sum(1 for w in ["surge","rise","gain","jump","boost","record"] if w in combined)
                    impact = "BEARISH" if neg > pos else "NEUTRAL" if neg == pos else "BULLISH"
                    headlines.append({"title":title[:120],"source":feed_url.split("/")[2].replace("www.","").replace("feeds.",""),"impact":impact,"summary":desc[:100].strip()})
                    if len(headlines) >= 5: break
        except: continue
        if len(headlines) >= 5: break
    return headlines[:5]

def fetch_injected(event_id, ttl_hours=36):
    """Titulares INYECTADOS manualmente (vía /api/inject) para el evento, dentro
    de una ventana de ttl_hours. Mismo formato que fetch_rss (title/summary/
    source/impact) → se mergean con el RSS y los puntúa el MISMO scorer (IA/
    keywords). Caducan solos por la ventana: no dejan el evento caliente para
    siempre (además el delta total sigue acotado a +25, así que una inyección
    es suplementaria, nunca spikea sola)."""
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT title, source FROM injected_signals
                       WHERE event_id=%s AND created_at > NOW() - (%s * INTERVAL '1 hour')
                       ORDER BY created_at DESC LIMIT 5""", (event_id, ttl_hours))
        rows = cur.fetchall(); cur.close(); conn.close()
        return [{"title": r["title"], "summary": "",
                 "source": (r["source"] or "manual") + " (inyectado)", "impact": "HIGH"}
                for r in rows]
    except Exception as e:
        print(f"[fetch_injected {event_id}] {e}")
        return []

# ── LOG TODAY'S TOP SIGNALS ──────────────────────────────────────────────────
def log_signals():
    """Called daily — logs top tickers with entry price"""
    try:
        conn = get_db()
        cur  = conn.cursor()
        today = date.today()
        # Check if already logged today
        cur.execute("SELECT COUNT(*) FROM signal_log WHERE log_date = %s", (today,))
        if cur.fetchone()[0] > 0:
            cur.close(); conn.close(); return

        # Get top 10 tickers across all events
        all_tickers = []
        for ev_id, scores in SCORE_MAP.items():
            for sym, score in scores.items():
                all_tickers.append({"sym":sym,"score":score,"event_id":ev_id})

        # Deduplicate — keep max abs score
        dedup = {}
        for t in all_tickers:
            if t["sym"] not in dedup or abs(t["score"]) > abs(dedup[t["sym"]]["score"]):
                dedup[t["sym"]] = t
        top10 = sorted(dedup.values(), key=lambda x: abs(x["score"]), reverse=True)[:10]

        for t in top10:
            try:
                data   = fetch_price(t["sym"])
                result = data["chart"]["result"][0]
                meta   = result["meta"]
                closes = [c for c in result.get("indicators",{}).get("quote",[{}])[0].get("close",[]) if c]
                price  = meta.get("regularMarketPrice", closes[-1] if closes else None)
                # RSI
                rsi = None
                if len(closes) >= 15:
                    gains = losses = 0
                    for i in range(1,15):
                        d = closes[i]-closes[i-1]
                        if d > 0: gains += d
                        else: losses += abs(d)
                    ag, al = gains/14, losses/14
                    rsi = round(100-(100/(1+ag/al)),1) if al else 100
                # Dir
                sec_data = SECTOR_MAP.get(t["event_id"],[])
                dir_ = "LONG"
                for sec in sec_data:
                    if t["sym"] in sec["tickers"]: dir_ = sec["dir"]; break
                cur.execute("""
                    INSERT INTO signal_log (sym, event_id, score, dir, rsi, price_entry)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (t["sym"], t["event_id"], t["score"], dir_, rsi, price))
            except: continue
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print("Log error:", e)

def update_exits():
    """Updates price_exit and correct for entries older than 1 day"""
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        yesterday = date.today() - timedelta(days=1)
        cur.execute("""
            SELECT id, sym, score, dir, price_entry
            FROM signal_log
            WHERE log_date <= %s AND price_exit IS NULL AND price_entry IS NOT NULL
        """, (yesterday,))
        rows = cur.fetchall()
        for row in rows:
            try:
                data   = fetch_price(row["sym"])
                result = data["chart"]["result"][0]
                meta   = result["meta"]
                price_exit = meta.get("regularMarketPrice")
                if not price_exit or not row["price_entry"]: continue
                chg_pct = round((price_exit - row["price_entry"]) / row["price_entry"] * 100, 2)
                # Correct = price moved in direction of score
                correct = (row["score"] > 0 and chg_pct > 0) or (row["score"] < 0 and chg_pct < 0)
                cur.execute("""
                    UPDATE signal_log SET price_exit=%s, chg_pct=%s, correct=%s WHERE id=%s
                """, (price_exit, chg_pct, correct, row["id"]))
            except: continue
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print("Exit update error:", e)

# ── ROUTES ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/price/<symbol>")
def price(symbol):
    try:
        data = fetch_price(symbol)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Cache de intensidad por evento (RSS + IA/keywords) ──────────────────────
# fetch_rss() pega contra 4 feeds por evento — para 1 evento en
# /api/score/<id> no pasa nada, pero /api/hot necesita la intensidad de
# TODOS los eventos que aparecen entre sus candidatos, y sin cache eso son
# hasta ~13×4 fetches por request. TTL 5min: las noticias no cambian tan
# rápido como para justificar refetchear en cada refresh del dashboard/bot.
_intensity_cache = {}   # {event_id: (timestamp, dict)}
INTENSITY_CACHE_TTL = 300

def get_event_intensity(event_id):
    import time as _t
    cached = _intensity_cache.get(event_id)
    if cached and _t.time() - cached[0] < INTENSITY_CACHE_TTL:
        return cached[1]
    base_intensity = INTENSITY_MAP.get(event_id, 40)
    # Inyectados (manuales) PRIMERO + RSS. El mismo scorer los evalúa; el delta
    # sigue acotado a ±25, así que lo inyectado es suplementario, no dominante.
    headlines = (fetch_injected(event_id) + fetch_rss(event_id))[:8]
    delta, fuente = intensity_delta(event_id, headlines)
    decay = intensity_decay(event_id, delta)
    intensity = max(0, min(100, base_intensity + delta - decay))
    result = {"intensity": intensity, "base": base_intensity, "delta": delta,
              "decay": decay, "fuente": fuente, "headlines": headlines}
    _intensity_cache[event_id] = (_t.time(), result)
    return result

@app.route("/api/score/<event_id>")
def score(event_id):
    try:
        sectors    = SECTOR_MAP.get(event_id, [])
        scores_raw = SCORE_MAP.get(event_id, {})
        demand     = DEMAND_SUMMARY.get(event_id, "")
        ei = get_event_intensity(event_id)
        intensity, base_intensity, delta, decay, fuente, headlines = \
            ei["intensity"], ei["base"], ei["delta"], ei["decay"], ei["fuente"], ei["headlines"]
        sectors_out = []
        for sec in sectors:
            tickers_out = [{"sym":sym,"score":scores_raw.get(sym,0),"reason":""} for sym in sec["tickers"]]
            sectors_out.append({"name":sec["name"],"dir":sec["dir"],"tickers":tickers_out})
        signal = "ACTIVE" if intensity>=70 else "ELEVATED" if intensity>=45 else "QUIET"
        return jsonify({
            "eventIntensity": intensity, "signal": signal,
            "intensityBase": base_intensity, "intensityDelta": delta,
            "intensityDecay": decay,
            "intensitySource": fuente,
            "lastSignal": headlines[0]["title"] if headlines else "",
            "demandSummary": demand, "headlines": headlines, "sectors": sectors_out,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/inject", methods=["POST"])
def api_inject():
    """Inyecta contenido (tweet/nota) como titular de un evento → sube su
    intensidad de forma SUPLEMENTARIA (delta acotado a +25) y con la MISMA
    corroboración del scorer IA/keywords. Requiere token (env INJECT_TOKEN);
    sin token configurado el endpoint queda cerrado. Body JSON:
    {"event_id":"CYBER","text":"...","source":"@cuenta"}. event_id debe ser uno
    conocido. Lo inyectado caduca solo a las 36h. Invalida el cache para que
    tome efecto al instante."""
    token = request.headers.get("X-Inject-Token") or request.args.get("token", "")
    if not INJECT_TOKEN or token != INJECT_TOKEN:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True, silent=True) or {}
    event_id = (data.get("event_id") or "").upper().strip()
    text = (data.get("text") or data.get("title") or "").strip()
    source = (data.get("source") or "manual").strip()[:80]
    if event_id not in SCORE_MAP:
        return jsonify({"error": f"event_id inválido: {event_id}",
                        "validos": sorted(SCORE_MAP.keys())}), 400
    if not text:
        return jsonify({"error": "falta 'text'"}), 400
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("INSERT INTO injected_signals (event_id, title, source) VALUES (%s,%s,%s)",
                    (event_id, text[:300], source))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    _intensity_cache.pop(event_id, None)   # que /api/hot y /api/score recomputen ya
    _ai_delta_cache.pop(event_id, None)
    return jsonify({"ok": True, "event_id": event_id, "text": text[:300], "source": source,
                    "nota": "Suma como titular del evento (delta ±25, suplementario); caduca a las 36h."})

@app.route("/api/injected")
def api_injected():
    """Lista las inyecciones vigentes (últimas 36h) — para verificar qué está activo."""
    try:
        conn = get_db(); cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT event_id, title, source, created_at FROM injected_signals
                       WHERE created_at > NOW() - INTERVAL '36 hours'
                       ORDER BY created_at DESC LIMIT 50""")
        rows = cur.fetchall(); cur.close(); conn.close()
        return jsonify({"injected": [
            {"event_id": r["event_id"], "title": r["title"], "source": r["source"],
             "created_at": r["created_at"].isoformat() if r["created_at"] else None}
            for r in rows]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/hot")
def hot():
    """Top tickers rankeados por hotScore = 60% extremo de RSI + 40%
    intensidad de noticias del evento (ver get_event_intensity)."""
    try:
        UNIVERSE = {
            "HEATWAVE":  [("LONG",["CEG","VST","NRG","AES","ETR","LNG","EQT","AR","LII","CARR"]),("WATCH",["NEE","DUK","SO","AEP"])],
            "HURRICANE": [("LONG",["GNRC","HD","LOW","SHW","MLM","VMC","MPC","VLO"]),("SHORT",["RE","RNR","MKL"])],
            "DROUGHT":   [("LONG",["MOS","NTR","CF","ICL","CTVA","FMC","AWK","WTRG"]),("SHORT",["CPB","SJM","CAG","MKC"])],
            "FLOOD":     [("LONG",["XYL","AWK","VMC","MLM","NUE"]),("SHORT",["ALL","TRV","CB","HIG"])],
            "PANDEMIC":  [("LONG",["MRNA","PFE","BNTX","NVAX","QDEL","BDX","DHR","TMO"]),("SHORT",["UAL","DAL","MAR","HLT"])],
            "FLU":       [("LONG",["GILD","ABBV","JNJ","MRK","CVS","WBA"]),("WATCH",["HCA","THC"])],
            "HORMUZ":    [("LONG",["XOM","CVX","OXY","COP","EOG","SLB","HAL","BKR"]),("SHORT",["UAL","DAL","AAL"]),("WATCH",["ZIM","MATX"])],
            "TAIWAN":    [("LONG",["LMT","RTX","NOC","GD","HII","GLD","IAU","NEM"]),("SHORT",["NVDA","AMD","AMAT","LRCX","AAPL"])],
            "NATO":      [("LONG",["LMT","RTX","NOC","XOM","CVX","LNG","GLD","TLT"]),("SHORT",["SPY","QQQ","IWM"])],
            "AI_BOOM":   [("LONG",["NVDA","AMD","AVGO","MRVL","CEG","VST","VRT","SMCI","CIEN","CSCO"])],
            "CYBER":     [("LONG",["CRWD","PANW","ZS","FTNT","S","LDOS","SAIC","BAH"]),("SHORT",["JPM","BAC","NEE"])],
            "SUPERBOWL": [("LONG",["PEP","KO","TAP","NFLX","META","TTD","DASH","UBER"]),("WATCH",["WMT","TGT"])],
            "BTS":       [("LONG",["WMT","TGT","AMZN","AAPL","DELL","HPQ","UPS","FDX"]),("WATCH",["NKE","PVH"])],
        }

        # Score plano (máximo |score|) por símbolo — se calcula PRIMERO porque
        # ahora también se usa para resolver qué evento/dirección gana cuando
        # el mismo ticker aparece en más de un evento (ver bug de abajo).
        flat_scores = {}
        for ev_id, scores in SCORE_MAP.items():
            for sym, sc in scores.items():
                if sym not in flat_scores or abs(sc) > abs(flat_scores[sym]):
                    flat_scores[sym] = sc

        # Build dedup universe — antes esto se quedaba con el PRIMER evento que
        # apareciera en el diccionario (orden arbitrario de Python), lo que
        # hacía que p.ej. NVDA quedara atrapado como SHORT de TAIWAN (-65) y
        # jamás se evaluara como LONG de AI_BOOM (90, el score más fuerte de
        # toda la base). Ahora gana el evento con mayor |score| en SCORE_MAP.
        universe = {}
        for ev_id, secs in UNIVERSE.items():
            for dir_, tickers in secs:
                for sym in tickers:
                    candidato_abs = abs(SCORE_MAP.get(ev_id, {}).get(sym, 0))
                    actual = universe.get(sym)
                    if actual is None or candidato_abs > actual["_score_abs"]:
                        universe[sym] = {"sym": sym, "dir": dir_, "event": ev_id,
                                          "_score_abs": candidato_abs}
        for v in universe.values():
            v.pop("_score_abs", None)

        # Fetch price + calc RSI for all
        import concurrent.futures

        def fetch_ticker(item):
            sym, t = item
            try:
                import urllib.request as ur, json as js
                # 15m/5d en vez de 1d/30d — el RSI diario solo se mueve UNA VEZ
                # POR DÍA (al cerrar la vela), por eso HOT quedaba "como una
                # foto" toda la sesión. Con velas de 15min el RSI reacciona
                # de verdad al movimiento intradía.
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=15m&range=5d"
                req2 = ur.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with ur.urlopen(req2, timeout=8) as r:
                    data = js.loads(r.read())
                result = data["chart"]["result"][0]
                closes = [c for c in result.get("indicators",{}).get("quote",[{}])[0].get("close",[]) if c is not None]
                meta = result["meta"]
                # El % de cambio SIGUE siendo vs. el cierre del día anterior
                # (no vs. la vela de 15min anterior, que sería otra cosa) —
                # por eso esto no sale de `closes`, sale de los campos meta.
                prev = meta.get("regularMarketPreviousClose") or meta.get("chartPreviousClose") \
                       or (closes[-2] if len(closes)>=2 else None)
                price = meta.get("regularMarketPrice")
                if not price or not closes: return None
                chg = round((price-prev)/prev*100, 2) if prev else 0
                # RSI(14) sobre velas de 15min — se recalcula con cada vela nueva,
                # varias veces por hora, no una vez por día.
                rsi = None
                if len(closes) >= 15:
                    gains = losses = 0
                    for i in range(1,15):
                        d = closes[i]-closes[i-1]
                        if d>0: gains+=d
                        else: losses+=abs(d)
                    ag,al = gains/14, losses/14
                    for i in range(15, len(closes)):
                        d = closes[i]-closes[i-1]
                        ag = (ag*13+(d if d>0 else 0))/14
                        al = (al*13+(abs(d) if d<0 else 0))/14
                    rsi = round(100-(100/(1+ag/al)),1) if al else 100
                # Detección de sesión más confiable: marketState en este endpoint
                # (interval=1d) suele venir desactualizado o directamente mal.
                # Comparamos el horario real de sesión (currentTradingPeriod, en
                # timestamps UTC) contra el reloj actual, en vez de confiar en el string.
                import time as _time
                now_ts = _time.time()
                ctp = meta.get("currentTradingPeriod", {}) or {}
                def _in_period(p):
                    return bool(p) and p.get("start") and p.get("end") and p["start"] <= now_ts < p["end"]
                if _in_period(ctp.get("regular")):
                    session = "LIVE"
                elif _in_period(ctp.get("pre")):
                    session = "PRE"
                elif _in_period(ctp.get("post")):
                    session = "POST"
                else:
                    # fallback al string de Yahoo si no vino currentTradingPeriod
                    state = meta.get("marketState", "CLOSED")
                    session = {"REGULAR":"LIVE","PRE":"PRE","POST":"POST","POSTPOST":"POST"}.get(state, "CLOSED")
                t2 = dict(t)
                t2.update({"rsi":rsi,"price":price,"chg":chg,"session":session,
                           "market_state_raw": meta.get("marketState")})
                return t2
            except:
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            results = list(ex.map(fetch_ticker, universe.items()))

        valid = [r for r in results if r and r.get("rsi") is not None]

        # Score by RSI extremity
        for t in valid:
            if t["dir"] == "LONG":  t["rsiScore"] = 100 - t["rsi"]
            elif t["dir"] == "SHORT": t["rsiScore"] = t["rsi"]
            else: t["rsiScore"] = abs(t["rsi"] - 50)

        # Add score from SCORE_MAP
        for t in valid:
            t["score"] = flat_scores.get(t["sym"], 0)

        # Intensidad de noticias por evento — antes el ranking miraba
        # SOLO qué tan extremo estaba el RSI, sin importar si el evento que
        # lo justifica está frío o escalando de verdad ahora mismo. Se
        # calcula 1 vez POR EVENTO (no por ticker) y en paralelo, con cache
        # de 5min (ver get_event_intensity), para no multiplicar el costo
        # de RSS por los ~13 eventos del universo en cada request.
        eventos_presentes = list({t["event"] for t in valid if t.get("event")})
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(eventos_presentes))) as ex:
            intens_vals = list(ex.map(lambda ev: get_event_intensity(ev)["intensity"], eventos_presentes))
        intensidades = dict(zip(eventos_presentes, intens_vals))
        for t in valid:
            t["newsIntensity"] = intensidades.get(t.get("event"), 40)

        # Ranking combinado: 60% extremo de RSI (timing técnico) + 40%
        # intensidad de noticias del evento (contexto real) — antes era
        # 100% RSI, así que un evento completamente frío en las noticias
        # podía salir "hot" igual que uno con escalada real esta hora.
        RSI_WEIGHT, NEWS_WEIGHT = 0.6, 0.4
        for t in valid:
            t["hotScore"] = round(t["rsiScore"]*RSI_WEIGHT + t["newsIntensity"]*NEWS_WEIGHT, 1)

        top4 = sorted(valid, key=lambda x: x["hotScore"], reverse=True)[:5]

        # Enriquecer SOLO los finalistas con StockTwits + SEC Form 4 —
        # son fuentes externas con rate limit, no tiene sentido pegarle
        # a las 122 del universo completo, solo a los 5 que van a salir.
        def _enrich(t):
            st = fetch_stocktwits_sentiment(t["sym"])
            ins = fetch_insider_activity(t["sym"])
            t["stocktwits_bullish"] = st["bullish"]
            t["stocktwits_ratio"]   = st["ratio"]
            t["insider_buy"]        = ins["insider_buy"]
            t["insider_n_filings"]  = ins["n_filings"]
            return t

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            top4 = list(ex.map(_enrich, top4))

        return jsonify({"tickers": top4, "total_scanned": len(valid)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/log", methods=["POST"])
def trigger_log():
    log_signals()
    update_exits()
    return jsonify({"ok": True})

@app.route("/status")
def status():
    try:
        update_exits()
        log_signals()
        conn = get_db()
        cur  = conn.cursor(cursor_factory=RealDictCursor)

        # Overall accuracy
        cur.execute("SELECT COUNT(*) as total, SUM(CASE WHEN correct THEN 1 ELSE 0 END) as hits FROM signal_log WHERE correct IS NOT NULL")
        overall = cur.fetchone()

        # By event
        cur.execute("""
            SELECT event_id,
                   COUNT(*) as total,
                   SUM(CASE WHEN correct THEN 1 ELSE 0 END) as hits,
                   ROUND(AVG(chg_pct)::numeric,2) as avg_chg
            FROM signal_log WHERE correct IS NOT NULL
            GROUP BY event_id ORDER BY hits DESC
        """)
        by_event = cur.fetchall()

        # Last 20 signals with result
        cur.execute("""
            SELECT sym, event_id, score, dir, rsi,
                   price_entry, price_exit, chg_pct, correct, log_date
            FROM signal_log
            ORDER BY log_date DESC, ABS(score) DESC
            LIMIT 30
        """)
        recent = cur.fetchall()

        cur.close(); conn.close()

        total = overall["total"] or 0
        hits  = overall["hits"]  or 0
        pct   = round(hits/total*100,1) if total else 0

        # Build HTML
        rows_html = ""
        for r in recent:
            result_icon = "✅" if r["correct"] else ("❌" if r["correct"] is False else "⏳")
            chg_color   = "#4ade80" if (r["chg_pct"] or 0) > 0 else "#f87171"
            score_color = "#4ade80" if (r["score"] or 0) > 0 else "#f87171"
            rows_html += f"""
            <tr>
              <td>{r['log_date']}</td>
              <td style="font-weight:700;color:#fff">{r['sym']}</td>
              <td style="color:#7dd3fc">{r['event_id']}</td>
              <td style="color:{score_color}">{'+' if (r['score'] or 0)>0 else ''}{r['score']}</td>
              <td>{r['dir']}</td>
              <td style="color:#60a5fa">{r['rsi'] or '—'}</td>
              <td>${r['price_entry'] or '—'}</td>
              <td>${r['price_exit'] or '⏳'}</td>
              <td style="color:{chg_color}">{('+' if (r['chg_pct'] or 0)>0 else '') + str(r['chg_pct']) + '%' if r['chg_pct'] is not None else '⏳'}</td>
              <td style="font-size:16px">{result_icon}</td>
            </tr>"""

        event_rows = ""
        for e in by_event:
            acc = round(e['hits']/e['total']*100,1) if e['total'] else 0
            bar_color = "#4ade80" if acc >= 60 else "#f87171" if acc < 40 else "#F59E0B"
            event_rows += f"""
            <tr>
              <td style="color:#7dd3fc">{e['event_id']}</td>
              <td>{e['total']}</td>
              <td>{e['hits']}</td>
              <td style="color:{bar_color};font-weight:700">{acc}%</td>
              <td style="color:{'#4ade80' if (e['avg_chg'] or 0)>0 else '#f87171'}">{e['avg_chg'] or 0}%</td>
            </tr>"""

        acc_color = "#4ade80" if pct >= 60 else "#f87171" if pct < 40 else "#F59E0B"

        html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DEMAND SIGNAL · STATUS</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ background:#15406 0; color:#e2e8f0; font-family:'Inter',system-ui,sans-serif; padding:20px; }}
  h1 {{ font-size:14px; letter-spacing:3px; color:#7dd3fc; margin-bottom:4px; }}
  .sub {{ font-size:10px; color:#4a8aaa; letter-spacing:2px; margin-bottom:24px; }}
  .cards {{ display:flex; gap:12px; margin-bottom:24px; flex-wrap:wrap; }}
  .card {{ background:#112f4e; border:1px solid #1e527a; border-radius:8px; padding:16px 20px; min-width:140px; }}
  .card .val {{ font-size:28px; font-weight:800; font-family:monospace; }}
  .card .lbl {{ font-size:9px; color:#4a8aaa; letter-spacing:2px; margin-top:4px; }}
  h2 {{ font-size:10px; color:#4a8aaa; letter-spacing:3px; margin-bottom:10px; }}
  table {{ width:100%; border-collapse:collapse; font-size:11px; margin-bottom:28px; }}
  th {{ text-align:left; font-size:8px; color:#4a8aaa; letter-spacing:2px; padding:6px 8px; border-bottom:1px solid #1e527a; }}
  td {{ padding:7px 8px; border-bottom:1px solid #0f2a42; color:#94a3b8; }}
  tr:hover td {{ background:#0f2a42; }}
  a {{ color:#7dd3fc; text-decoration:none; font-size:10px; }}
  ::-webkit-scrollbar {{ width:2px; }} ::-webkit-scrollbar-thumb {{ background:#1e527a; }}
</style></head><body>
<h1>DEMAND SIGNAL · STATUS</h1>
<div class="sub">PREDICCIONES · RESULTADOS · APRENDIZAJE</div>
<div class="cards">
  <div class="card"><div class="val" style="color:{acc_color}">{pct}%</div><div class="lbl">ACCURACY GLOBAL</div></div>
  <div class="card"><div class="val" style="color:#fff">{total}</div><div class="lbl">PREDICCIONES TOTALES</div></div>
  <div class="card"><div class="val" style="color:#4ade80">{hits}</div><div class="lbl">ACIERTOS</div></div>
  <div class="card"><div class="val" style="color:#f87171">{total-hits}</div><div class="lbl">ERRORES</div></div>
</div>
<h2>ACCURACY POR EPISODIO</h2>
<table><thead><tr><th>EPISODIO</th><th>SEÑALES</th><th>ACIERTOS</th><th>ACCURACY</th><th>AVG CHG%</th></tr></thead>
<tbody>{event_rows if event_rows else '<tr><td colspan="5" style="color:#4a8aaa;padding:20px">Sin datos todavía — se registran señales diariamente</td></tr>'}</tbody></table>
<h2>SEÑALES RECIENTES</h2>
<table><thead><tr><th>FECHA</th><th>TICKER</th><th>EPISODIO</th><th>SCORE</th><th>DIR</th><th>RSI</th><th>ENTRADA</th><th>SALIDA</th><th>CHG%</th><th>OK</th></tr></thead>
<tbody>{rows_html if rows_html else '<tr><td colspan="10" style="color:#4a8aaa;padding:20px">Sin señales registradas aún · <a href="/api/log" onclick="fetch(this.href,{{method:\'POST\'}});return false">Registrar ahora</a></td></tr>'}</tbody></table>
<div style="font-size:9px;color:#1e527a;margin-top:12px">Auto-registra las top 10 señales del día · Evalúa resultado al día siguiente · <a href="/">← Dashboard</a></div>
</body></html>"""
        return html
    except Exception as e:
        return f"<pre style='color:red;background:#000;padding:20px'>Error: {e}</pre>", 500

import threading
import time as _time_top

def background_scheduler():
    """Corre log_signals()+update_exits() cada 15 min sin depender de un
    cron externo pegándole a /api/log — antes ese endpoint nunca se
    llamaba solo, así que el historial de performance por episodio
    quedaba vacío/desactualizado."""
    while True:
        try:
            log_signals()
            update_exits()
            print("[SCHEDULER] log_signals + update_exits OK")
        except Exception as e:
            print(f"[SCHEDULER ERROR] {e}")
        _time_top.sleep(900)

if __name__ == "__main__":
    init_db()
    threading.Thread(target=background_scheduler, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

# Init DB on startup
try:
    init_db()
except:
    pass
