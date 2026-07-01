from flask import Flask, render_template, jsonify, request
import urllib.request
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

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

@app.route("/api/score/<event_id>")
def score(event_id):
    try:
        sectors    = SECTOR_MAP.get(event_id, [])
        scores_raw = SCORE_MAP.get(event_id, {})
        intensity  = INTENSITY_MAP.get(event_id, 40)
        demand     = DEMAND_SUMMARY.get(event_id, "")
        headlines  = fetch_rss(event_id)
        sectors_out = []
        for sec in sectors:
            tickers_out = [{"sym":sym,"score":scores_raw.get(sym,0),"reason":""} for sym in sec["tickers"]]
            sectors_out.append({"name":sec["name"],"dir":sec["dir"],"tickers":tickers_out})
        signal = "ACTIVE" if intensity>=70 else "ELEVATED" if intensity>=45 else "QUIET"
        return jsonify({
            "eventIntensity": intensity, "signal": signal,
            "lastSignal": headlines[0]["title"] if headlines else "",
            "demandSummary": demand, "headlines": headlines, "sectors": sectors_out,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/hot")
def hot():
    """Returns top 4 tickers ranked by RSI extremity across all events"""
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

        # Build dedup universe
        universe = {}
        for ev_id, secs in UNIVERSE.items():
            for dir_, tickers in secs:
                for sym in tickers:
                    if sym not in universe:
                        universe[sym] = {"sym": sym, "dir": dir_, "event": ev_id}

        # Fetch price + calc RSI for all
        import concurrent.futures

        def fetch_ticker(item):
            sym, t = item
            try:
                import urllib.request as ur, json as js
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=30d"
                req2 = ur.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with ur.urlopen(req2, timeout=8) as r:
                    data = js.loads(r.read())
                result = data["chart"]["result"][0]
                closes = [c for c in result.get("indicators",{}).get("quote",[{}])[0].get("close",[]) if c is not None]
                meta = result["meta"]
                prev = closes[-2] if len(closes)>=2 else meta.get("regularMarketPreviousClose") or meta.get("chartPreviousClose")
                price = meta.get("regularMarketPrice")
                if not price or not closes: return None
                chg = round((price-prev)/prev*100, 2) if prev else 0
                # RSI(14)
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
                state = meta.get("marketState","CLOSED")
                session = {"REGULAR":"LIVE","PRE":"PRE","POST":"POST","POSTPOST":"POST"}.get(state,"CLOSED")
                t2 = dict(t)
                t2.update({"rsi":rsi,"price":price,"chg":chg,"session":session})
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

        top4 = sorted(valid, key=lambda x: x["rsiScore"], reverse=True)[:4]

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

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

# Init DB on startup
try:
    init_db()
except:
    pass
