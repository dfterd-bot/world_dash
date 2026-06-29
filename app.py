from flask import Flask, render_template, jsonify, request
import urllib.request
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime

app = Flask(__name__)

# ── RSS SOURCES ───────────────────────────────────────────────────────────────
RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/topNews",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
]

# ── SCORING MAP ───────────────────────────────────────────────────────────────
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
    "HEATWAVE":  "Ola de calor dispara consumo electrico por uso masivo de AC, beneficiando generadoras y distribuidoras de gas natural.",
    "HURRICANE": "Huracan destruye infraestructura generando demanda urgente de generadores, materiales de construccion y perjudicando aseguradoras.",
    "DROUGHT":   "Sequia dana cosechas disparando precios de commodities y demanda de fertilizantes para replantacion masiva.",
    "FLOOD":     "Inundaciones generan demanda de infraestructura hidrica y reconstruccion, golpeando seguros de propiedad.",
    "PANDEMIC":  "Brote pandemico dispara demanda de vacunas, diagnosticos y EPP, hundiendo aereas y hoteleria.",
    "FLU":       "Temporada de gripe severa impulsa ventas de antivirales, tests rapidos y trafico en farmacias.",
    "HORMUZ":    "Bloqueo en Hormuz restringe oferta de crudo, disparando precios y beneficiando productores mientras hunde aereas.",
    "TAIWAN":    "Escalada militar eleva gasto en defensa y activos refugio, generando panico en cadena de suministro de chips.",
    "NATO":      "Conflicto OTAN dispara contratos de defensa, demanda de energia y flujos hacia activos seguros.",
    "AI_BOOM":   "Boom de IA escala demanda de chips, energia para datacenters, refrigeracion y redes de fibra optica.",
    "CYBER":     "Ataque cibernetico genera demanda urgente de soluciones de seguridad, perjudicando entidades atacadas.",
    "SUPERBOWL": "Evento masivo impulsa consumo de bebidas, snacks, streaming, publicidad y servicios de delivery.",
    "BTS":       "Temporada escolar dispara gasto en electronica, ropa y logistica de entrega masiva.",
}

# ── RSS FETCH ─────────────────────────────────────────────────────────────────
KEYWORDS = {
    "HEATWAVE":  ["heat wave","heatwave","extreme heat","temperature record","electricity demand","power grid","AC demand"],
    "HURRICANE": ["hurricane","tropical storm","cyclone","storm surge","landfall","evacuation","gulf coast"],
    "DROUGHT":   ["drought","crop damage","harvest","rainfall deficit","water shortage","agriculture","corn","wheat","soybean"],
    "FLOOD":     ["flood","flooding","heavy rain","levee","dam","inundation","storm water"],
    "PANDEMIC":  ["pandemic","outbreak","WHO","pathogen","virus","epidemic","health emergency","infection"],
    "FLU":       ["flu","influenza","respiratory","antiviral","hospitalization","CDC","illness"],
    "HORMUZ":    ["hormuz","strait","tanker","oil supply","iran","gulf","crude","opec","pipeline"],
    "TAIWAN":    ["taiwan","strait","china military","pla","semiconductor","chip","tsmc","escalation"],
    "NATO":      ["nato","russia","ukraine","escalation","military","missile","nuclear","europe conflict"],
    "AI_BOOM":   ["artificial intelligence","AI investment","data center","nvidia","chips","openai","llm","gpu"],
    "CYBER":     ["cyberattack","ransomware","hack","breach","infrastructure attack","cybersecurity","malware"],
    "SUPERBOWL": ["super bowl","world cup","championship","nfl","fifa","advertising","viewership"],
    "BTS":       ["back to school","retail sales","consumer spending","holiday shopping","electronics demand"],
}

def fetch_rss_headlines(event_id):
    keywords = [k.lower() for k in KEYWORDS.get(event_id, [])]
    headlines = []
    for feed_url in RSS_FEEDS:
        try:
            req = urllib.request.Request(feed_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=6) as r:
                raw = r.read()
            root = ET.fromstring(raw)
            ns = {}
            items = root.findall(".//item")
            for item in items[:30]:
                title_el = item.find("title")
                desc_el  = item.find("description")
                link_el  = item.find("link")
                title = title_el.text if title_el is not None else ""
                desc  = desc_el.text  if desc_el  is not None else ""
                combined = (title + " " + desc).lower()
                if any(kw in combined for kw in keywords):
                    impact = "BULLISH"
                    neg_words = ["decline","fall","drop","cut","risk","warn","fear","crash","concern"]
                    pos_words = ["surge","rise","gain","jump","boost","record","high","demand"]
                    neg_count = sum(1 for w in neg_words if w in combined)
                    pos_count = sum(1 for w in pos_words if w in combined)
                    if neg_count > pos_count:
                        impact = "BEARISH"
                    elif neg_count == pos_count:
                        impact = "NEUTRAL"
                    headlines.append({
                        "title": title[:120],
                        "source": feed_url.split("/")[2].replace("www.","").replace("feeds.",""),
                        "impact": impact,
                        "summary": desc[:100].strip() if desc else title[:80]
                    })
                    if len(headlines) >= 6:
                        break
            if len(headlines) >= 6:
                break
        except:
            continue
    return headlines[:5]

# ── ROUTES ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/price/<symbol>")
def price(symbol):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=30d"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/score/<event_id>")
def score(event_id):
    try:
        sectors   = SECTOR_MAP.get(event_id, [])
        scores_raw = SCORE_MAP.get(event_id, {})
        intensity = INTENSITY_MAP.get(event_id, 40)
        demand    = DEMAND_SUMMARY.get(event_id, "")
        headlines = fetch_rss_headlines(event_id)

        # Build sectors with scores
        sectors_out = []
        for sec in sectors:
            tickers_out = []
            for sym in sec["tickers"]:
                sc = scores_raw.get(sym, 0)
                tickers_out.append({"sym": sym, "score": sc, "reason": ""})
            sectors_out.append({
                "name": sec["name"],
                "dir":  sec["dir"],
                "tickers": tickers_out
            })

        signal = "QUIET"
        if intensity >= 70: signal = "ACTIVE"
        elif intensity >= 45: signal = "ELEVATED"

        return jsonify({
            "eventIntensity": intensity,
            "signal":         signal,
            "lastSignal":     headlines[0]["title"] if headlines else "",
            "demandSummary":  demand,
            "headlines":      headlines,
            "sectors":        sectors_out,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
