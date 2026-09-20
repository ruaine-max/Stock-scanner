"""
AltınRock / Dividend Scout — Combined Python Backend (US + UK + APAC + Eurozone)
- Merges the standalone US "ALtrock2" backend, the UK "altinrock_uk"
  backend, the "altinrock_apac" (Australia/NZ/Japan/Hong Kong/
  Singapore/South Korea) backend, and the "altinrock" Eurozone/Swiss
  backend into ONE Flask app that serves ALL FOUR markets, each with its
  own ticker universe, cache, checked-ticker ledger, and scan job — so
  every market can be scanned independently and never clobber another's
  data.
- US market       : 800+ seed tickers, full US ticker list via NASDAQ
  Trader, SEC EDGAR filing verification, Twelvedata/Marketstack/Finnhub
  cross-check.
- UK market       : ~150 seed tickers, full LSE ticker list via bundled
  uk_all_tickers.json, Investegate/LSE/Companies House filing links.
- APAC market     : ~270 curated seed tickers across ASX/NZX/TSE/HKEX/
  SGX/KRX, a real full-market download for five of the six exchanges
  (NZX stays curated-only — no free full-directory feed exists for it),
  and per-stock currency/exchange/country metadata since APAC spans six
  currencies.
- Eurozone market : ~180 curated seed tickers across Germany/France/
  Netherlands/Belgium/Spain/Italy/Austria/Finland/Portugal/Ireland plus
  Switzerland, a real full-market download for the five countries
  Euronext runs (France/Netherlands/Belgium/Portugal/Ireland — a
  community-maintained CSV), and a "currency" field (EUR or CHF) per
  ticker since the pool spans two currencies.
- Every route that's market-specific lives at /api/<market>/... where
  <market> is "us", "uk", "apac", or "eurozone". Login/admin-key/sessions
  are shared across all markets (one account works for all four).
  /api/combined/stocks merges every market's cached tickers into one
  tagged list for the Combined tab.
- /api/add  : add individual tickers manually (UK auto-appends ".L" to a
  bare symbol with no dot; US leaves symbols as typed)
- /api/remove: remove a ticker

pip install yfinance flask flask-cors werkzeug
python server.py
"""
import math, json, datetime, threading, os, time, random, urllib.request, csv, io, secrets, re
import yfinance as yf
from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash

# ─── SINGLE-APP HOSTING ─────────────────────────────────────────────────────
# The React frontend (both the US and UK tabs, plus the Combined tab, all
# live in ONE built app) is built (`npm run build`) into a `dist` folder
# next to this file. Flask serves those static files directly, so the whole
# site — API + UI, both markets — is ONE deployable app on ONE host.
FRONTEND_DIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dist")

app = Flask(__name__, static_folder=FRONTEND_DIST, static_url_path="")

# In production (Render), set CORS_ORIGIN to your Netlify site URL, e.g.
# https://your-app-name.netlify.app — locking this down means random sites
# can't call your API from a browser. Locally it just falls back to "*".
CORS(app, origins=os.environ.get("CORS_ORIGIN", "*"))

@app.after_request
def add_no_cache_headers(response):
    """
    Every API response is explicitly marked as never-cacheable. Without this,
    a browser (or an intermediate proxy) could serve a stale GET /api/stocks
    response instead of hitting the server fresh — which would look exactly
    like "my data reset" even though the real server-side data is fine,
    and would explain a "wrong until I hit refresh" pattern.
    """
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response

# ─── ADMIN LOCK ────────────────────────────────────────────────────────────
# Set ADMIN_KEY as an environment variable on your host (Render dashboard →
# Environment). Scan/add/remove all require this key in an X-Admin-Key
# header. If ADMIN_KEY is unset (local dev), the lock is disabled and
# everything works exactly as before — nothing changes for local use.
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")

def require_admin(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if ADMIN_KEY:  # only enforce if a key has actually been configured
            supplied = request.headers.get("X-Admin-Key", "")
            if supplied != ADMIN_KEY:
                return jsonify({"error": "Unauthorized — admin key required"}), 401
        return fn(*args, **kwargs)
    return wrapper

# Where the cache files live. On Render, set DATA_DIR to your persistent
# disk's mount path (e.g. /var/data) in the dashboard — disk storage is
# separate from your code folder, and only paths under the mounted disk
# survive restarts/redeploys. Locally (no DATA_DIR set) this just falls
# back to the script's own folder, same as before.
BASE_DIR    = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
USERS_FILE  = os.path.join(BASE_DIR, "users.json")
STALE_HOURS = 24 * 7  # once a week (was 12h)

# ─── .env LOADING ───────────────────────────────────────────────────────────
# Minimal loader — no new dependency for two keys. Reads BASE_DIR/.env (NOT
# .env.production, which is the frontend's committed, non-secret file) and
# only fills in vars that aren't already set in the real environment, so a
# real deployment env var always wins over the local file.
def _load_dotenv(path=None):
    path = path or os.path.join(BASE_DIR, ".env")
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())
_load_dotenv()

# Independent data sources for cross-checking yfinance/Yahoo's dividend and
# split data — see verify_apis.py, which is the standalone script for
# actually testing these before any of this gets wired into calc_ttm_yield.
TWELVEDATA_API_KEY  = os.environ.get("TWELVEDATA_API_KEY", "")
MARKETSTACK_API_KEY = os.environ.get("MARKETSTACK_API_KEY", "")
FINNHUB_API_KEY     = os.environ.get("FINNHUB_API_KEY", "")
MISS_LIMIT_BEFORE_DROP = 5  # consecutive failed background-refresh attempts before a ticker is dropped
                            # rather than left showing an old, unverified number forever

# ─── USER ACCOUNTS & LOGIN ─────────────────────────────────────────────────
# Individual username/password accounts. There's no public signup — accounts
# only get created by you (the admin), via the admin-only endpoints below,
# after someone pays you directly (crypto, or however you arrange it) and
# messages you. "tier" (free/paid) is just a label for your own records —
# both tiers get identical access to the app.
#
# Passwords are hashed (never stored in plain text) using Werkzeug's
# generate_password_hash/check_password_hash — the same trusted library
# Flask itself depends on.

SESSION_HOURS = 24 * 30  # how long a login stays valid (30 days)
SESSIONS_FILE = os.path.join(BASE_DIR, "sessions.json")
_sessions_lock = threading.Lock()

# Sessions are persisted to disk rather than kept in a plain in-memory dict.
# Reason: hosts like PythonAnywhere can run more than one worker process for
# a single web app, each with its OWN separate memory — a session created by
# a login request handled by worker A would be invisible to worker B, making
# every subsequent request look like "not logged in" at random. Writing to
# disk means every worker reads the same, shared, up-to-date session data.

def load_sessions():
    if not os.path.exists(SESSIONS_FILE): return {}
    try:
        with open(SESSIONS_FILE) as f:
            sessions = json.load(f)
    except Exception:
        return {}
    # opportunistically drop expired sessions so this file doesn't grow forever
    now = datetime.datetime.now()
    cleaned = {}
    for tok, sess in sessions.items():
        try:
            if datetime.datetime.fromisoformat(sess["expires"]) > now:
                cleaned[tok] = sess
        except Exception:
            pass
    if len(cleaned) != len(sessions):
        save_sessions(cleaned)
    return cleaned

def save_sessions(sessions):
    try:
        tmp = SESSIONS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(sessions, f)
        os.replace(tmp, SESSIONS_FILE)
    except Exception as e:
        print(f"  Could not save sessions: {e}")

def load_users():
    if not os.path.exists(USERS_FILE): return {}
    try:
        with open(USERS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def save_users(users):
    try:
        with open(USERS_FILE, "w") as f:
            json.dump(users, f, indent=2)
    except Exception as e:
        print(f"  Could not save users: {e}")

def account_expired(username):
    """Check the user's own access_expires date (set via manage_users.py),
    separate from the login session's 30-day timeout. Returns True if their
    paid period has ended. No access_expires field = permanent access."""
    users = load_users()
    user = users.get(username)
    if not user:
        return True
    exp = user.get("access_expires")
    if not exp:
        return False  # no expiry set = permanent (e.g. comped free accounts)
    try:
        return datetime.datetime.now().date() > datetime.date.fromisoformat(exp)
    except Exception:
        return False

# ─── ANONYMOUS DEVICE TRIAL (no signup) ────────────────────────────────────
# When no accounts exist (USERS_FILE_ENABLED() is False), the app is fully
# open to anyone — but instead of unlimited-forever access, each browser/
# device gets a free trial window, tracked by a random ID the frontend
# generates once and stores in localStorage, sent as an X-Device-Id header
# on every request that goes through @require_login. After the trial ends,
# that device is blocked until you mark it paid by hand (see the
# /api/admin/devices endpoints below) once they've sent you a crypto
# payment and quoted you their device ID.
DEVICE_TRIALS_FILE = os.path.join(BASE_DIR, "device_trials.json")
DEVICE_TRIAL_DAYS  = int(os.environ.get("DEVICE_TRIAL_DAYS", "14"))
_device_trials_lock = threading.Lock()

def load_device_trials():
    if not os.path.exists(DEVICE_TRIALS_FILE): return {}
    try:
        with open(DEVICE_TRIALS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def save_device_trials(trials):
    try:
        tmp = DEVICE_TRIALS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(trials, f, indent=2)
        os.replace(tmp, DEVICE_TRIALS_FILE)
    except Exception as e:
        print(f"  Could not save device trials: {e}")

def check_device_trial(device_id):
    """Returns (allowed, days_left, paid). Registers a never-seen-before
    device ID as starting its trial right now."""
    with _device_trials_lock:
        trials = load_device_trials()
        entry = trials.get(device_id)
        now = datetime.datetime.now()
        if not entry:
            trials[device_id] = {"first_seen": now.isoformat(), "paid": False}
            save_device_trials(trials)
            return True, DEVICE_TRIAL_DAYS, False
        if entry.get("paid"):
            return True, None, True
        try:
            first_seen = datetime.datetime.fromisoformat(entry["first_seen"])
        except Exception:
            first_seen = now
        days_elapsed = (now - first_seen).days
        days_left = DEVICE_TRIAL_DAYS - days_elapsed
        if days_left > 0:
            return True, days_left, False
        return False, 0, False


def require_login(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not USERS_FILE_ENABLED():
            # No accounts configured — anonymous, no-signup mode. Gate by a
            # per-device free trial instead of a login wall.
            device_id = request.headers.get("X-Device-Id", "").strip()
            if not device_id:
                return jsonify({"error": "Missing device ID"}), 400
            allowed, days_left, paid = check_device_trial(device_id)
            if not allowed:
                return jsonify({
                    "error": "Your free trial has ended.",
                    "trialExpired": True,
                    "deviceId": device_id,
                }), 402
            request.device_days_left = days_left
            request.device_paid = paid
            return fn(*args, **kwargs)
        token = request.headers.get("X-Session-Token", "")
        with _sessions_lock:
            sessions = load_sessions()
            sess = sessions.get(token)
        if not sess:
            return jsonify({"error": "Not logged in"}), 401
        try:
            expires = datetime.datetime.fromisoformat(sess["expires"])
        except Exception:
            expires = datetime.datetime.min
        if datetime.datetime.now() > expires:
            with _sessions_lock:
                sessions = load_sessions()
                sessions.pop(token, None)
                save_sessions(sessions)
            return jsonify({"error": "Session expired, please log in again"}), 401
        if account_expired(sess["username"]):
            with _sessions_lock:
                sessions = load_sessions()
                sessions.pop(token, None)
                save_sessions(sessions)
            return jsonify({"error": "Your access has expired. Please renew.", "accessExpired": True}), 401
        request.username = sess["username"]
        return fn(*args, **kwargs)
    return wrapper

def get_optional_paid_user():
    """
    Like require_login, but never fails the request — used by endpoints that
    should stay open to anonymous/free visitors, while still unlocking full
    data for a logged-in user on the "paid" tier whose access hasn't expired.
    Returns the username string if paid+valid, otherwise None.
    """
    token = request.headers.get("X-Session-Token", "")
    if not token:
        return None
    with _sessions_lock:
        sessions = load_sessions()
        sess = sessions.get(token)
    if not sess:
        return None
    try:
        if datetime.datetime.now() > datetime.datetime.fromisoformat(sess["expires"]):
            return None
    except Exception:
        return None
    username = sess["username"]
    users = load_users()
    user = users.get(username)
    if not user or user.get("tier", "free") != "paid":
        return None
    if account_expired(username):
        return None
    return username

# ─── PAYWALL INFO (shared across both markets — one account, one price) ────
BTC_ADDRESS  = os.environ.get("BTC_ADDRESS", "")
USDC_ADDRESS = os.environ.get("USDC_ADDRESS", "")
CONTACT_INSTRUCTIONS = os.environ.get(
    "CONTACT_INSTRUCTIONS",
    "After sending payment, email your transaction ID to the address on this site to get unlocked."
)

# ─── MARKETS: US SEED UNIVERSE ──────────────────────────────────────────────
SEED_UNIVERSE_US = [
    # ── Consumer Staples ──────────────────────────────────────────────────────
    "KO","PEP","PG","CL","GIS","MKC","CLX","SJM","HSY","K","CPB","CAG","HRL",
    "MO","PM","BTI","UVV","STZ","BUD","TAP",
    # ── Healthcare ────────────────────────────────────────────────────────────
    "JNJ","ABBV","ABT","MDT","BDX","BMY","AMGN","PFE","MRK","UNH","CVS",
    "LLY","BSX","EW","ZBH","BAX","CAH","MCK","ABC","HSIC","PDCO",
    # ── Technology ────────────────────────────────────────────────────────────
    "MSFT","AAPL","TXN","AVGO","IBM","CSCO","QCOM","ADI","KLAC","LRCX",
    "MSI","GLW","JNPR","HPE","HPQ","NTAP","STX","WDC","AMAT","MU",
    # ── Financials ────────────────────────────────────────────────────────────
    "JPM","BAC","WFC","GS","MS","C","USB","PNC","TFC","COF","MTB","KEY",
    "RF","FITB","HBAN","CFG","CMA","ZION","PBCT","SNV",
    "AFL","CB","TRV","CINF","BEN","MET","PRU","AIG","HIG","ALL","LNC","UNM",
    "BLK","IVZ","AMG","TROW","EV","BK","STT","NTRS","FHN",
    # ── Consumer Discretionary ────────────────────────────────────────────────
    "MCD","WMT","HD","LOW","TGT","SBUX","COST","NKE","YUM","DRI",
    "DG","DLTR","BBY","ROST","TJX","KSS","M","JWN",
    # ── Industrials ───────────────────────────────────────────────────────────
    "ITW","MMM","EMR","GPC","SWK","CAT","ADP","DE","HON","RTX","GE","LMT",
    "NOC","GD","HII","TDG","FDX","UPS","CHRW","XPO","JBHT","R","URI",
    "PH","DOV","ROK","AME","ETN","IR","GWW","MSC","FAST","EXPD",
    # ── Energy ────────────────────────────────────────────────────────────────
    "CVX","XOM","COP","OKE","KMI","WMB","SLB","HAL","BKR","PSX","VLO","MPC",
    "PXD","EOG","DVN","FANG","APA","MRO","OXY","HES",
    # ── MLPs / Midstream ──────────────────────────────────────────────────────
    "EPD","ENB","ET","MPLX","MMP","PAA","TRGP","WES","DKL","CAPL",
    "SHLX","USAC","GLP","NGL","CEQP","PBFX","HESM","CQNWK",
    # ── Telecom ───────────────────────────────────────────────────────────────
    "T","VZ","TMUS","LUMN","USM","TDS","SHEN","CNSL",
    # ── Utilities ─────────────────────────────────────────────────────────────
    "NEE","SO","DUK","D","AEP","EXC","WEC","ES","XEL","CMS","LNT","EVRG",
    "NI","PNW","OGE","POR","AVA","IDA","SR","NWE","OTTR","ALE","UTL",
    "PPL","FE","ETR","AES","NRG","PNM","MDU","BKH","SWX","NFG","SPOK",
    # ── Equity REITs ──────────────────────────────────────────────────────────
    "O","NNN","VICI","GLPI","STAG","PLD","AMT","CCI","PSA","EXR",
    "IRM","WELL","VTR","OHI","LTC","WPC","IIPR","EQR","AVB","MPW",
    "SPG","KIM","REG","FRT","BRX","SITC","RPT","MAC","SKT","TCO",
    "DLR","EQIX","QTS","COR","CONE","SBAC","UNIT","AMH","INVH","UDR",
    "CPT","AIR","NHI","SNH","PEAK","HR","DOC","GMRE","NTST","EPRT",
    "PINE","ADC","BNL","SRC","FCPT","PECO","ROIC","KITE","CTO","ALEX",
    "UE","CBL","KRG","PDM","HIW","DEA","GMRE","CTRE","SBRA","NXRT",
    # ── mREITs ────────────────────────────────────────────────────────────────
    "NLY","AGNC","TWO","MITT","IVR","ARR","NYMT","MFA","EFC","EARN",
    "CHMI","PMT","BXMT","KREF","RC","ACRE","GPMT","TPVG","NMFC",
    # ── BDCs ──────────────────────────────────────────────────────────────────
    "MAIN","ARCC","HTGC","GAIN","GBDC","TPVG","PSEC","CSWC","OBDC","ORCC",
    "SLRC","PFLT","TCPC","FSK","OCSL","KCAP","GSBD","BCSF","MRCC","FDUS",
    "CGBD","SCM","OXSQ","TICC","HCAP","WHF","TCAP","CION","CCAP","GLAD",
    # ── Dividend ETFs ─────────────────────────────────────────────────────────
    "SCHD","VYM","DVY","HDV","DGRO","SDY","SPHD","SPYD","VIG","NOBL",
    "JEPI","JEPQ","DIVO","QYLD","RYLD","XYLD","KNG","IDV","PFF","NUSI",
    "FDVV","EDIV","DEM","DLS","DON","DTD","RDVY","REGL","SDOG","FVD",
    "PEY","KBWD","PFFD","PFXF","HYGV","LGLV","IUSV","USMV","SPLV","EFAV",
    # ── Real Estate / REIT ETFs ───────────────────────────────────────────────
    "VNQ","IYR","SCHH","RWR","USRT","REZ","BBRE","INDS","SRVR","HOMZ",
    # ── Bond/Preferred ETFs ───────────────────────────────────────────────────
    "PFF","PGX","PFFD","HYG","JNK","VCIT","LQD","MUB","EMB","BNDX",
    # ── Covered Call ETFs ─────────────────────────────────────────────────────
    "QYLD","RYLD","XYLD","NVDY","MSFO","AMZY","GOOGY","TSLY","APLY","CONY",
    # ── Closed-End Funds (high yield) ─────────────────────────────────────────
    "UTF","UTG","USA","RFI","RNP","DNI","AWF","FAV","PCN","PCI",
    "PDI","PTY","RCS","PHK","PFN","PHT","PCF","AOD","IHD","GDO",
    # ── International ADRs ────────────────────────────────────────────────────
    "RDS-A","RDS-B","BP","TOT","ENB","TRP","BCE","TD","BNS","RY",
    "CM","BMO","MFC","SLF","POW","GWO","FFH","AQN","FTS","H",
    "VALE","BBD","PBR","ITUB","ABEV","SID","GGBR","CIB","BSAC","BVN",
    "NGG","NVS","AZN","GSK","DEO","BTI","VOD","WPP","LGEN","HSBC",
    "UN","UL","PHG","RDS","RDSA","RDSB","ING","ABN","NN","PHIA",
]

# deduplicate while preserving order
seen = set()
_deduped = []
for t in SEED_UNIVERSE_US:
    if t not in seen:
        seen.add(t)
        _deduped.append(t)
SEED_UNIVERSE_US = _deduped

# ─── MARKETS: UK SEED UNIVERSE ──────────────────────────────────────────────
# ~150 well-known LSE-listed dividend payers, investment trusts and income
# ETFs — enough to give an instant, useful first load. All symbols use the
# ".L" suffix Yahoo Finance expects for LSE-listed instruments (e.g. Vodafone
# is "VOD.L", not "VOD" — "VOD" on Yahoo is an unrelated/defunct US ticker).
SEED_UNIVERSE_UK = [
    # ── Consumer Staples ──────────────────────────────────────────────────────
    "ULVR.L","DGE.L","RKT.L","BATS.L","IMB.L","TSCO.L","SBRY.L","ABF.L",
    "TATE.L","GNC.L",
    # ── Healthcare ────────────────────────────────────────────────────────────
    "AZN.L","GSK.L","HIK.L","SN..L","SPX.L","HSX.L",
    # ── Financials (banks, insurers, asset managers) ─────────────────────────
    "HSBA.L","BARC.L","LLOY.L","NWG.L","STAN.L","PRU.L","LGEN.L","AV..L",
    "PHNX.L","ADM.L","SDR.L","ABDN.L","JUP.L","ICP.L","III.L","HL.L","ASHM.L",
    # ── Energy & Mining ────────────────────────────────────────────────────────
    "SHEL.L","BP..L","RIO.L","BHP.L","GLEN.L","AAL.L","ANTO.L","FRES.L",
    "CNE.L","ENQ.L",
    # ── Telecom ───────────────────────────────────────────────────────────────
    "VOD.L","BT.A.L",
    # ── Utilities ─────────────────────────────────────────────────────────────
    "NG..L","SSE.L","SVT.L","UU..L","CNA.L","PNN.L",
    # ── Industrials ───────────────────────────────────────────────────────────
    "BA..L","RR..L","SMIN.L","IMI.L","WEIR.L","CRH.L","BME.L","HLMA.L",
    "BNZL.L","RTO.L","FERG.L","RS1.L","RSW.L","SPT.L","MCRO.L",
    "ITRK.L","QQ..L","SXS.L",
    # ── Real Estate / REITs ──────────────────────────────────────────────────
    "LAND.L","BLND.L","SGRO.L","UTG.L","WKP.L","SUPR.L","PHP.L",
    # ── Infrastructure / Renewables income funds ─────────────────────────────
    "HICL.L","INPP.L","BBGI.L","TRIG.L","GRID.L","FSFL.L","JLEN.L","UKW.L",
    # ── Consumer Discretionary / Retail ───────────────────────────────────────
    "NXT.L","KGF.L","WTB.L","JD..L","CPG.L","ITV.L","WPP.L","MKS.L","GRG.L",
    "BDEV.L","PSN.L","TW..L","BKG.L","HWDN.L","GAW.L","FEVR.L","SSPG.L",
    "WOSG.L","FRAS.L","SMDS.L","MNDI.L","BOY.L","ASC.L","SMWH.L","MAB1.L",
    "PSON.L","SAGA.L","DLG.L",
    # ── Other well-known FTSE names ───────────────────────────────────────────
    "EXPN.L","DPLM.L","ELM.L","BEZ.L","RMV.L","JII.L","TBCG.L","FGP.L",
    # ── Investment Trusts (income-focused) ────────────────────────────────────
    "CTY.L","MRCH.L","TMPL.L","MYI.L","HFEL.L","EDIN.L","FGT.L","CLDN.L",
    "BRWM.L","JMG.L","SCP.L","ATT.L","APAX.L","HGT.L",
    # ── Income ETFs ────────────────────────────────────────────────────────────
    "VUKE.L","ISF.L","IUKD.L","VHYL.L","VWRL.L","VMID.L",
]

seen_uk = set()
_deduped_uk = []
for t in SEED_UNIVERSE_UK:
    if t not in seen_uk:
        seen_uk.add(t)
        _deduped_uk.append(t)
SEED_UNIVERSE_UK = _deduped_uk

# ─── APAC EXCHANGE METADATA ──────────────────────────────────────────────
# Maps the Yahoo Finance ticker suffix to the exchange it lives on. Used for
# display (exchange badge, correct currency symbol) and to decide which
# tickers can be auto-discovered via a full-market download versus which
# rely on the curated seed list.
EXCHANGE_META = {
    "AX": {"exchange": "ASX",    "country": "Australia",     "currency": "AUD"},
    "NZ": {"exchange": "NZX",    "country": "New Zealand",   "currency": "NZD"},
    "T":  {"exchange": "TSE",    "country": "Japan",         "currency": "JPY"},
    "HK": {"exchange": "HKEX",   "country": "Hong Kong",     "currency": "HKD"},
    "SI": {"exchange": "SGX",    "country": "Singapore",     "currency": "SGD"},
    "KS": {"exchange": "KRX",    "country": "South Korea",   "currency": "KRW"},
    "KQ": {"exchange": "KOSDAQ", "country": "South Korea",   "currency": "KRW"},
}

def exchange_meta_for(ticker):
    """Look up exchange/country/currency from a ticker's Yahoo suffix
    (e.g. 'BHP.AX' -> ASX/Australia/AUD). Unknown/no suffix falls back to
    a generic 'Other'/USD guess (e.g. a US-listed ADR added manually)."""
    suffix = ticker.rsplit(".", 1)[-1] if "." in ticker else ""
    return EXCHANGE_META.get(suffix, {"exchange": "Other", "country": "", "currency": "USD"})

# ─── APAC SEED UNIVERSE (Australia, NZ, Japan, Hong Kong, Singapore, Korea) ──
# Curated list of well-known dividend payers across the Asia-Pacific region,
# using the ticker suffix Yahoo Finance/yfinance expects for each exchange
# (.AX, .NZ, .T, .HK, .SI, .KS). This seeds the cache instantly on first run.
#
# The ASX portion here is just a fast-start subset — fetch_ticker_list()
# below downloads ASX's full listed-companies directory (when reachable)
# for a genuinely complete Australian scan. NZX/Tokyo/Hong Kong/Singapore/
# Korea don't all have an equivalent free full-directory feed, so several of
# those exchanges are fully or partly represented by the curated names below.
SEED_UNIVERSE_APAC = [
    # ── ASX (Australia) — banks, miners, retail, healthcare, REITs, LICs ─────
    "CBA.AX","NAB.AX","WBC.AX","ANZ.AX","MQG.AX","BHP.AX","RIO.AX","FMG.AX",
    "WDS.AX","STO.AX","ORG.AX","AGL.AX","APA.AX","WES.AX","WOW.AX","COL.AX",
    "TLS.AX","TCL.AX","SCG.AX","GMG.AX","TWE.AX","ALL.AX","IAG.AX","SUN.AX",
    "QBE.AX","ASX.AX","S32.AX","AMC.AX","BXB.AX","SGP.AX","MGR.AX","DXS.AX",
    "GPT.AX","VCX.AX","CHC.AX","ARF.AX","WPR.AX","NSR.AX","HVN.AX","JBH.AX",
    "WOR.AX","ORI.AX","BSL.AX","ILU.AX","MIN.AX","LYC.AX","PLS.AX","IGO.AX",
    "WHC.AX","YAL.AX","NHC.AX","CTD.AX","FLT.AX","QAN.AX","SVW.AX","REH.AX",
    "GWA.AX","ABC.AX","CSR.AX","AWC.AX","BOQ.AX","BEN.AX","SUL.AX","RHC.AX",
    "SHL.AX","COH.AX","CSL.AX","RMD.AX","PME.AX","EDV.AX","DMP.AX","PMV.AX",
    "IPL.AX","GNC.AX","ELD.AX","NUF.AX","PPT.AX","MFG.AX","CPU.AX","IFL.AX",
    "NWL.AX","HUB.AX","GQG.AX","AUB.AX","CGF.AX","CQR.AX","RFF.AX","LLC.AX",
    "CIP.AX","ARG.AX","AFI.AX","MLT.AX","DUI.AX","BKI.AX","SOL.AX","TAH.AX",
    "EVN.AX","NST.AX","RRL.AX","SFR.AX","WGX.AX",
    # extra ASX names — ASX's free full-directory CSV was retired, so this
    # curated list is the primary source for Australia now, not just a
    # fast-start seed, hence the bigger list here than the other markets.
    "BPT.AX","KAR.AX","COE.AX","BOE.AX","PDN.AX","DYL.AX","AKE.AX","LTR.AX",
    "SYA.AX","VUL.AX","LIN.AX","CXO.AX","LKE.AX","AZS.AX","JRV.AX","RMS.AX",
    "CMM.AX","GOR.AX","DEG.AX","WAF.AX","SPR.AX","KCN.AX","BGL.AX","RED.AX",
    "SBM.AX","AWJ.AX","BRN.AX","AVZ.AX","GL1.AX","EMR.AX","AMA.AX","BAP.AX",
    "AD8.AX","ALU.AX","APX.AX","XRO.AX","WTC.AX","TNE.AX","NXT.AX","MP1.AX",
    "SDR.AX","IEL.AX","REA.AX","CAR.AX","SEK.AX","DHG.AX","NEC.AX","TPG.AX",
    "SGR.AX","SKC.AX","IVC.AX","CWY.AX","BIN.AX","VEA.AX","SUL.AX","PMV.AX",
    "LOV.AX","ACL.AX","AX1.AX","MYR.AX","UNI.AX","NCK.AX","JIN.AX","ADH.AX",
    "KGN.AX","TPW.AX","ANN.AX","EBO.AX","FPH.AX","RHP.AX","VRT.AX",
    "MSB.AX","CU6.AX","NAN.AX","IMU.AX","MVF.AX","PNV.AX","VNT.AX","AVH.AX",
    "TLX.AX","CGS.AX","SIG.AX","API.AX","EPX.AX","PRN.AX","ABP.AX","INA.AX",
    "GDI.AX","CDP.AX","CMW.AX","HDN.AX","URW.AX","BWP.AX","CQE.AX","ACF.AX",
    "AOF.AX","GOZ.AX","RIC.AX","AAC.AX","TGR.AX","SFC.AX","BGA.AX",
    "IPH.AX","CTT.AX","AMS.AX","CVL.AX","ADT.AX","MYX.AX","AHI.AX","CCX.AX",
    "GDG.AX","OFX.AX","AVA.AX","PLL.AX","AGY.AX","LPI.AX","HAS.AX",
    "MTX.AX","PPS.AX","PXX.AX","SIQ.AX","GTK.AX","NHF.AX","MPL.AX","ASG.AX",
    # ── NZX (New Zealand) ─────────────────────────────────────────────────────
    "FPH.NZ","AIA.NZ","SPK.NZ","MEL.NZ","CEN.NZ","MCY.NZ","IFT.NZ","FBU.NZ",
    "POT.NZ","RYM.NZ","SUM.NZ","KPG.NZ","GMT.NZ","PCT.NZ","VCT.NZ","CNU.NZ",
    "SKC.NZ","THL.NZ","MFT.NZ","EBO.NZ","NZX.NZ","HGH.NZ","TWR.NZ","ARB.NZ",
    "SCL.NZ",
    # ── TSE (Tokyo, Japan) ────────────────────────────────────────────────────
    "7203.T","8306.T","8316.T","8411.T","9432.T","9433.T","9434.T","8058.T",
    "8031.T","8001.T","8002.T","2914.T","5401.T","8801.T","8802.T","8830.T",
    "9022.T","9020.T","9021.T","9101.T","9104.T","9107.T","1605.T","5020.T",
    "4502.T","4503.T","4568.T","6301.T","7267.T","7201.T","6752.T","6503.T",
    "6702.T","8630.T","8725.T","8766.T","8604.T","7182.T","7181.T","6178.T",
    "9502.T","9503.T","9531.T","9532.T","2502.T","2503.T","2802.T","2269.T",
    "4911.T","8267.T","3382.T","8035.T","6367.T","6902.T","7269.T","7270.T",
    "5108.T",
    # ── HKEX (Hong Kong) ──────────────────────────────────────────────────────
    "0005.HK","0011.HK","0002.HK","0003.HK","0006.HK","0001.HK","0016.HK",
    "0012.HK","0083.HK","0688.HK","1109.HK","0017.HK","0019.HK","0087.HK",
    "0700.HK","0941.HK","0762.HK","0728.HK","0388.HK","2318.HK","1299.HK",
    "0939.HK","1398.HK","3988.HK","3968.HK","0027.HK","0066.HK","1038.HK",
    "0004.HK","0101.HK","0069.HK","0175.HK","1928.HK","0267.HK","0384.HK",
    "0836.HK","0857.HK","0386.HK","0883.HK",
    # ── SGX (Singapore) ───────────────────────────────────────────────────────
    "D05.SI","O39.SI","U11.SI","C38U.SI","A17U.SI","C09.SI","Z74.SI","S68.SI",
    "C6L.SI","F34.SI","C07.SI","N2IU.SI","ME8U.SI","M44U.SI","AJBU.SI",
    "J69U.SI","T82U.SI","BUOU.SI","K71U.SI","BN4.SI","U96.SI","U14.SI",
    "S58.SI","Y92.SI","G13.SI",
    # ── KRX (South Korea) ─────────────────────────────────────────────────────
    "005930.KS","000660.KS","005380.KS","000270.KS","005490.KS","051910.KS",
    "006400.KS","035420.KS","035720.KS","105560.KS","055550.KS","086790.KS",
    "316140.KS","003550.KS","034730.KS","015760.KS","017670.KS","030200.KS",
    "032830.KS","001450.KS","010950.KS","011200.KS",
]

seen_apac = set()
_deduped_apac = []
for t in SEED_UNIVERSE_APAC:
    if t not in seen_apac:
        seen_apac.add(t)
        _deduped_apac.append(t)
SEED_UNIVERSE_APAC = _deduped_apac

# ─── EUROZONE + SWITZERLAND SEED UNIVERSE ────────────────────────────────
# Curated list of well-known dividend payers across the eurozone plus
# Switzerland (included per the original request even though it isn't
# eurozone), using the Yahoo Finance exchange suffix for each market
# (exchange listings, ticker codes, and share classes do change over time,
# so treat this as a good starting point, not gospel):
#   .DE Germany (Xetra)         .PA France (Euronext Paris)
#   .AS Netherlands (Amsterdam)  .BR Belgium (Brussels)
#   .MI Italy (Milan)           .MC Spain (Madrid)
#   .VI Austria (Vienna)        .HE Finland (Helsinki)
#   .LS Portugal (Lisbon)       .IR Ireland (Euronext Dublin)
#   .SW Switzerland (SIX) — not eurozone, but included per the original
#       fork's request
#
# France/Netherlands/Belgium/Portugal/Ireland are also covered by REAL
# full-market discovery below (_fetch_eurozone_euronext_tickers) since
# Euronext publishes those five markets' listings freely. Germany/
# Switzerland/Austria/Spain/Italy/Finland have no equivalent free bulk
# feed, so those sections here are the ONLY source of candidates for those
# countries — this is where to add more names for deeper coverage there.
SEED_UNIVERSE_EUROZONE = [
    # ── Germany (DAX + MDAX/SDAX names) ───────────────────────────────────────
    "SAP.DE","SIE.DE","ALV.DE","DTE.DE","BAS.DE","BAYN.DE","BMW.DE","MBG.DE",
    "VOW3.DE","MUV2.DE","DBK.DE","DB1.DE","EOAN.DE","RWE.DE","IFX.DE","HEN3.DE",
    "HEI.DE","FRE.DE","FME.DE","MRK.DE","CON.DE","ADS.DE","BEI.DE","DHL.DE",
    "VNA.DE","PUM.DE","SY1.DE","QIA.DE","BNR.DE","1COV.DE","SHL.DE","ENR.DE",
    "MTX.DE","SRT3.DE","RHM.DE","CBK.DE","ZAL.DE","DTG.DE","SDF.DE",
    "SZG.DE","TLX.DE","WCH.DE","LEG.DE","TKA.DE","G1A.DE","SOW.DE","BOSS.DE",
    "GXI.DE","KGX.DE","DUE.DE","JUN3.DE","RAA.DE","AFX.DE","LHA.DE","ARL.DE",
    "PBB.DE","DWNI.DE","TEG.DE","GFT.DE","BC8.DE","FNTN.DE","SIX2.DE","AT1.DE",
    "EVK.DE","FPE3.DE",
    # ── France (CAC 40 + more) ────────────────────────────────────────────────
    "MC.PA","OR.PA","SAN.PA","AI.PA","BNP.PA","SU.PA","TTE.PA","DG.PA","EL.PA",
    "RMS.PA","SGO.PA","CS.PA","KER.PA","DSY.PA","ENGI.PA","ORA.PA","VIE.PA",
    "PUB.PA","CAP.PA","LR.PA","ML.PA","ACA.PA","GLE.PA","BN.PA","RI.PA",
    "WLN.PA","URW.PA","EDEN.PA","HO.PA","SAF.PA","AIR.PA","CA.PA","FR.PA",
    "STLAP.PA",
    # ── Netherlands (AEX + more) ──────────────────────────────────────────────
    "ASML.AS","UNA.AS","AD.AS","INGA.AS","PHIA.AS","DSFIR.AS","ASM.AS",
    "WKL.AS","REN.AS","HEIA.AS","AKZA.AS","KPN.AS","RAND.AS","NN.AS","AGN.AS",
    "ABN.AS","PRX.AS","AALB.AS","BESI.AS","FUR.AS","SBMO.AS","LIGHT.AS",
    "IMCD.AS","ARCAD.AS","FLOW.AS","BAMNB.AS",
    # ── Belgium (BEL 20 + more) ───────────────────────────────────────────────
    "ABI.BR","KBC.BR","UCB.BR","SOLB.BR","GBLB.BR","COFB.BR","AGS.BR","PROX.BR",
    "ELI.BR","MELE.BR","TNET.BR","WDP.BR","AED.BR","XIOR.BR","ONTEX.BR","BEKB.BR",
    # ── Spain (IBEX 35 + more) ────────────────────────────────────────────────
    "ITX.MC","SAN.MC","IBE.MC","BBVA.MC","TEF.MC","REP.MC","FER.MC","AMS.MC",
    "ELE.MC","NTGY.MC","CABK.MC","ACS.MC","ACX.MC","MAP.MC",
    "COL.MC","MRL.MC","AENA.MC","ANA.MC","SAB.MC","GRF.MC","IAG.MC","CLNX.MC",
    "PHM.MC","ENC.MC","VIS.MC","LOG.MC","ROVI.MC",
    # ── Italy (FTSE MIB + more) ───────────────────────────────────────────────
    "ENEL.MI","ENI.MI","ISP.MI","UCG.MI","G.MI","STM.MI","RACE.MI","TIT.MI",
    "PST.MI","SRG.MI","TRN.MI","MB.MI","CPR.MI","A2A.MI",
    "AZM.MI","BPE.MI","BAMI.MI","MONC.MI","PIRC.MI","IG.MI","HER.MI","IREN.MI",
    "DIA.MI","AMP.MI","FBK.MI","BGN.MI","NEXI.MI","PRY.MI","LDO.MI","SPM.MI",
    # ── Austria (ATX + more) ─────────────────────────────────────────────────
    "OMV.VI","EBS.VI","VIG.VI","VER.VI","ANDR.VI","RBI.VI","WIE.VI","POST.VI",
    "SBO.VI","IIA.VI","LNZ.VI","FACC.VI","SEM.VI",
    # ── Finland (OMX Helsinki, eurozone + more) ───────────────────────────────
    "NOKIA.HE","NESTE.HE","SAMPO.HE","KNEBV.HE","UPM.HE","FORTUM.HE",
    "STERV.HE","ORNBV.HE","VALMT.HE","WRT1V.HE","ELISA.HE","OUT1V.HE",
    "KESKOB.HE","METSB.HE","CGCBV.HE",
    # ── Portugal (PSI 20 + more) ──────────────────────────────────────────────
    "EDP.LS","GALP.LS","JMT.LS","NOS.LS","EDPR.LS","BCP.LS","SON.LS","SEM.LS",
    "COR.LS",
    # ── Ireland (Euronext Dublin, eurozone) ───────────────────────────────────
    "RYA.IR","KRZ.IR","BIRG.IR","A5G.IR","KRX.IR","GL9.IR","GVR.IR","PTSB.IR",
    # ── Switzerland (SMI / SIX + more) ────────────────────────────────────────
    "NESN.SW","ROG.SW","NOVN.SW","UHR.SW","ZURN.SW","ABBN.SW","CFR.SW",
    "SIKA.SW","LONN.SW","GIVN.SW","SREN.SW","UBSG.SW","SCMN.SW","HOLN.SW",
    "ALC.SW","GEBN.SW","SGSN.SW","KNIN.SW","BAER.SW","PGHN.SW","STMN.SW",
    "CLN.SW","LOGN.SW","SLHN.SW","SOON.SW","TEMN.SW","DKSH.SW","BALN.SW",
    "SCHP.SW","BUCN.SW","EMSN.SW","BELL.SW","BOSN.SW","VETN.SW","HELN.SW",
    "ADEN.SW","FHZN.SW","VZN.SW","SPSN.SW","MOBN.SW","PSPN.SW","AMS.SW",
    # ── European dividend / broad-market ETFs (verify tickers before relying) ─
    "EXSA.DE","EUNL.DE","IUSA.DE","EXS1.DE","IQQH.DE","EQQQ.DE",
]

seen_eurozone = set()
_deduped_eurozone = []
for t in SEED_UNIVERSE_EUROZONE:
    if t not in seen_eurozone:
        seen_eurozone.add(t)
        _deduped_eurozone.append(t)
SEED_UNIVERSE_EUROZONE = _deduped_eurozone

# ─── MARKETS REGISTRY ────────────────────────────────────────────────────────
# Everything that differs between the US and UK versions of this app funnels
# through this one dict, keyed by market code ("us" / "uk"). Every route
# takes a <market> URL segment and looks itself up here — that's what lets
# both markets be scanned/refreshed/cached completely independently while
# sharing all the same business logic (calc_ttm_yield, fetch_one, the
# screener loop, the override system, stale-refresh, diagnostics, etc.)
MARKET_META = {
    "us": {"label": "United States", "flag": "🇺🇸", "currency_default": "USD",
           "suffix": None, "seed": SEED_UNIVERSE_US},
    "uk": {"label": "United Kingdom", "flag": "🇬🇧", "currency_default": "GBp",
           "suffix": ".L",  "seed": SEED_UNIVERSE_UK},
    # APAC has six different currencies (one per exchange), so there is no
    # single "currency_default" the way US/UK have one — fetch_one() falls
    # back to exchange_meta_for(symbol)'s per-exchange currency instead of
    # this value for apac. It is set to USD purely as a last-resort default
    # (e.g. a ticker with no recognized suffix at all).
    "apac": {"label": "Asia-Pacific", "flag": "🌏", "currency_default": "USD",
             "suffix": None, "seed": SEED_UNIVERSE_APAC},
    # Eurozone spans EUR and CHF (Switzerland). Unlike APAC this needs no
    # per-exchange metadata table — yfinance reliably reports "EUR" or
    # "CHF" directly via info["currency"] for every ticker in this pool,
    # so fetch_one()'s ordinary info.get("currency") fallback already
    # does the right thing. "EUR" here is just the last-resort default.
    "eurozone": {"label": "Eurozone", "flag": "🇪🇺", "currency_default": "EUR",
                 "suffix": None, "seed": SEED_UNIVERSE_EUROZONE},
}

def valid_market(market):
    return market in MARKET_META

def market_or_400(market):
    """Every /api/<market>/... route calls this first. Returns an error
    response tuple if the market segment isn't "us" or "uk", else None."""
    if not valid_market(market):
        return jsonify({"error": f"Unknown market '{market}' — must be 'us', 'uk', 'apac', or 'eurozone'"}), 400
    return None

# Per-market data files. US keeps its original filenames (renamed with a
# "_us" suffix only where a UK file of the same base name would otherwise
# collide, e.g. checked_tickers.json/dividend_cache.json — all_tickers.json
# already gets a plain "_us" suffix for symmetry with uk_all_tickers.json).
MARKET_FILES = {
    "us": {
        "cache":            os.path.join(BASE_DIR, "dividend_cache_us.json"),
        "checked":          os.path.join(BASE_DIR, "checked_tickers_us.json"),
        "tickers":          os.path.join(BASE_DIR, "all_tickers_us.json"),
        "overrides":        os.path.join(BASE_DIR, "yield_overrides_us.json"),
        "refresh_progress": os.path.join(BASE_DIR, "refresh_progress_us.json"),
    },
    "uk": {
        "cache":            os.path.join(BASE_DIR, "dividend_cache_uk.json"),
        "checked":          os.path.join(BASE_DIR, "checked_tickers_uk.json"),
        "tickers":          os.path.join(BASE_DIR, "uk_all_tickers.json"),
        "overrides":        os.path.join(BASE_DIR, "yield_overrides_uk.json"),
        "refresh_progress": os.path.join(BASE_DIR, "refresh_progress_uk.json"),
    },
    # APAC keeps its own per-exchange ticker-list cache (all_tickers_apac.json
    # holds one sub-cache per exchange internally — see _load_apac_ticker_list_cache
    # / _save_apac_ticker_list_cache below) alongside the usual per-market cache/
    # checked/overrides/refresh-progress files, exactly like US and UK.
    "apac": {
        "cache":            os.path.join(BASE_DIR, "dividend_cache_apac.json"),
        "checked":          os.path.join(BASE_DIR, "checked_tickers_apac.json"),
        "tickers":          os.path.join(BASE_DIR, "all_tickers_apac.json"),
        "overrides":        os.path.join(BASE_DIR, "yield_overrides_apac.json"),
        "refresh_progress": os.path.join(BASE_DIR, "refresh_progress_apac.json"),
    },
    "eurozone": {
        "cache":            os.path.join(BASE_DIR, "dividend_cache_eurozone.json"),
        "checked":          os.path.join(BASE_DIR, "checked_tickers_eurozone.json"),
        "tickers":          os.path.join(BASE_DIR, "all_tickers_eurozone.json"),
        "overrides":        os.path.join(BASE_DIR, "yield_overrides_eurozone.json"),
        "refresh_progress": os.path.join(BASE_DIR, "refresh_progress_eurozone.json"),
    },
}

# ─── CACHE HELPERS ────────────────────────────────────────────────────────────

def load_cache(market="us"):
    """
    Reads the given market's cache file. If it's missing or corrupted (e.g.
    a browser upload to PythonAnywhere got interrupted mid-transfer, leaving
    a truncated/invalid JSON file), falls back to the last known-good backup
    instead of silently treating it as empty — which would otherwise trigger
    a full reseed and look like all your data vanished.
    """
    cache_file = MARKET_FILES[market]["cache"]
    cache_backup_file = cache_file + ".backup"
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                data = json.load(f)
            if isinstance(data, dict) and len(data) > 0:
                return data
            print(f"⚠ [{market}] Cache file is empty or not a valid dict — trying backup instead.")
        except Exception as e:
            print(f"⚠ [{market}] Could not read cache ({e}) — trying backup instead.")
    if os.path.exists(cache_backup_file):
        try:
            with open(cache_backup_file, "r") as f:
                data = json.load(f)
            if isinstance(data, dict) and len(data) > 0:
                print(f"✅ [{market}] Recovered {len(data)} tickers from backup.")
                # restore the primary file from the good backup so we're not
                # stuck reading from backup forever
                save_cache(data, market)
                return data
        except Exception as e:
            print(f"⚠ [{market}] Backup also unreadable ({e}).")
    return {}

def save_cache(cache, market="us"):
    cache_file = MARKET_FILES[market]["cache"]
    cache_backup_file = cache_file + ".backup"
    try:
        # keep a backup of whatever was already on disk BEFORE we overwrite it,
        # but only if it looks like a real, valid, non-trivial cache — so we
        # never let a backup regress to something tiny or broken
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r") as f:
                    existing = json.load(f)
                if isinstance(existing, dict) and len(existing) > 0:
                    with open(cache_backup_file, "w") as f:
                        json.dump(existing, f)
            except Exception:
                pass  # if the existing file is already corrupted, don't let it clobber a good backup
        tmp = cache_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(cache, f, indent=2)
        os.replace(tmp, cache_file)
    except Exception as e:
        print(f"⚠ [{market}] Could not save cache: {e}")

def is_stale(entry):
    try:
        fetched = datetime.datetime.fromisoformat(entry.get("_fetched_at","2000-01-01"))
        age = datetime.datetime.now() - fetched
        return age.total_seconds() > STALE_HOURS * 3600
    except Exception:
        return True

# ─── CHECKED-TICKER LEDGER ──────────────────────────────────────────────────
# Every ticker a scan has ever actually fetched (successfully) gets recorded
# here permanently — its yield, and when it was checked — regardless of
# whether it cleared any particular scan's minimum yield. Without this,
# every new scan restarts from the same point in the alphabet and re-checks
# (and re-burns rate-limit budget on) the exact same low-yield tickers every
# single time, which is why scans looked "stuck" even when working correctly.
# Tickers are only re-checked after CHECKED_STALE_DAYS, since yields do
# drift over time (price changes, dividend cuts/raises).

CHECKED_STALE_DAYS = 60

def load_checked(market="us"):
    checked_file = MARKET_FILES[market]["checked"]
    if not os.path.exists(checked_file): return {}
    try:
        with open(checked_file) as f:
            return json.load(f)
    except Exception:
        return {}

def save_checked(checked, market="us"):
    checked_file = MARKET_FILES[market]["checked"]
    try:
        tmp = checked_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(checked, f, indent=2)
        os.replace(tmp, checked_file)
    except Exception as e:
        print(f"⚠ [{market}] Could not save checked-ticker ledger: {e}")

def is_checked_recently(sym, checked):
    entry = checked.get(sym)
    if not entry: return False
    try:
        checked_at = datetime.datetime.fromisoformat(entry["checked_at"])
        return (datetime.datetime.now() - checked_at).days < CHECKED_STALE_DAYS
    except Exception:
        return False

# ─── DATA FETCHING ────────────────────────────────────────────────────────────

def safe_num(v, fallback=0):
    try:
        f = float(v)
        return fallback if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return fallback

def safe_str(v, fallback=""):
    if v is None: return fallback
    s = str(v).strip()
    return fallback if s.lower() in ("nan","none","null","") else s

def corroborate_split(hist, split_date, ratio, tolerance=0.5):
    """
    CORRECTION (this function was wrong before): it used to be checked
    against ticker_obj.history()'s default (split-ADJUSTED) closing prices.
    That can never corroborate a real split — adjustment is specifically
    designed to erase the exact price discontinuity a split causes, so a
    genuinely real split will always look "unconfirmed" against adjusted
    prices. That's exactly what happened to HERZ's real 2026-02-09 (and
    2026-02-06) 1-for-10 reverse split: it got wrongly rejected here, even
    though Marketstack's independent /splits data confirms the same split
    Yahoo reports. `hist` MUST be fetched with auto_adjust=False (raw,
    unadjusted closes) for this check to mean anything — see the
    ticker_obj.history(..., auto_adjust=False) call at this function's
    call site.
    """
    if hist is None or len(hist) < 2:
        return False  # can't verify it — don't trust it
    try:
        closes = hist["Close"]
        before = closes[closes.index < split_date]
        after = closes[closes.index >= split_date]
        if len(before) == 0 or len(after) == 0:
            return False
        observed = float(after.iloc[0]) / float(before.iloc[-1])
        expected = 1.0 / ratio if ratio > 0 else 1.0
        if expected <= 0:
            return False
        return abs(observed - expected) / expected <= tolerance
    except Exception:
        return False

# ─── MANUAL DATA OVERRIDES ──────────────────────────────────────────────────
# A small, explicit, CITED list of tickers where Yahoo Finance's raw dividend
# data is confirmed wrong against the company's own primary-source press
# release — not a general "fix" for bad data (that's not reliably detectable
# from the data alone), just a documented correction for specific cases that
# have actually been verified by hand. Add to this only with a source link.
#
# HERZ (revised after cross-checking against Marketstack's independent
# /dividends and /splits data — the first version of this override was
# itself wrong, see below):
#
# The real event, per Herzfeld's own press release, was ONE distribution of
# $0.6867/share (an 80% stock / 20% cash election), declared 2025-11-10,
# ex-date 2025-11-21, paid 2025-12-31:
# https://www.globenewswire.com/news-release/2025/11/10/3185015/0/en/Herzfeld-Credit-Income-Fund-Inc-Declares-Year-End-Distribution-in-Stock-and-Cash-Fund-Updates.html
#
# HERZ ALSO did a real 1-for-10 reverse split shortly after, ~2026-02-06/09
# — confirmed independently by BOTH Yahoo's split data AND Marketstack's
# /splits endpoint (access_key-verified, not scraped), so this is NOT the
# phantom-split case it first looked like. Because that $0.6867 distribution
# was paid on the OLD (pre-split) share basis, and today's price is on the
# NEW (post-split) basis, it needs to be rescaled onto today's basis to be
# comparable — the same logic calc_ttm_yield already applies to any other
# pre-split dividend: $0.6867 / 0.1 = $6.867 per share, counted ONCE (not
# twice — the 2025-11-21 and 2025-12-31 entries both showing the same
# amount are the ex-date and pay-date of that ONE distribution, not two
# separate payments).
#
# annual_div = $6.867 (the one special, split-adjusted) + 5 x $0.17
# (regular monthly, all paid AFTER the split so no adjustment needed) = $7.717
#
# Residual uncertainty we can't resolve from data alone: up to 80% of that
# distribution could have been paid in stock rather than cash (shareholder
# election) — a stock distribution isn't "yield" in the same sense a cash
# payment is, and none of our sources tell us which portion this specific
# shareholder base chose. Treat this number as the best-supported estimate,
# not a certainty.
MANUAL_TTM_DIVIDEND_OVERRIDES = {
    "HERZ": 7.717,
}

# ─── DYNAMIC OVERRIDES (admin-editable via API) ─────────────────────────────
# MANUAL_TTM_DIVIDEND_OVERRIDES above requires editing source code and a
# redeploy to change. These are stored on disk so an admin can set or clear
# them from the UI without touching Python. Same purpose — pin a specific
# ticker's annual dividend to a known-correct value when yfinance gets it
# wrong — just without the redeploy step. Both are checked at fetch time;
# the dynamic one wins if it has an entry for the symbol.
def load_overrides(market="us"):
    overrides_file = MARKET_FILES[market]["overrides"]
    if not os.path.exists(overrides_file): return {}
    try:
        with open(overrides_file) as f:
            return json.load(f)
    except Exception:
        return {}

def save_overrides(d, market="us"):
    overrides_file = MARKET_FILES[market]["overrides"]
    try:
        tmp = overrides_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(d, f, indent=2)
        os.replace(tmp, overrides_file)
    except Exception as e:
        print(f"  ⚠ [{market}] Could not save overrides: {e}")

def get_override(symbol, market="us"):
    """Returns the admin-set override annual_div for a symbol, or None.
    Order of precedence: dynamic file overrides (UI-set) > MANUAL_TTM_DIVIDEND_OVERRIDES (code-set)."""
    sym = (symbol or "").upper()
    if not sym: return None
    dyn = load_overrides(market).get(sym)
    if dyn is not None:
        return float(dyn), "ui"
    code = MANUAL_TTM_DIVIDEND_OVERRIDES.get(sym)
    if code is not None:
        return float(code), "code"
    return None

def flag_if_outlier(symbol, ttm_items, raw_sum):
    """
    Doesn't change what gets served — just prints a warning when one single
    dividend dominates a ticker's trailing-12-month sum in a way that looks
    like it could be the same kind of source-data error found in HERZ (see
    MANUAL_TTM_DIVIDEND_OVERRIDES above), so it's easy to spot candidates for
    a verified manual override instead of only finding them by accident.
    """
    try:
        amounts = [float(a) for _, a in ttm_items]
        if len(amounts) < 2 or raw_sum <= 0:
            return
        amounts_sorted = sorted(amounts)
        median = amounts_sorted[len(amounts_sorted)//2]
        biggest = amounts_sorted[-1]
        if median > 0 and biggest >= median * 15 and (biggest / raw_sum) >= 0.5:
            print(f"  ⚠ {symbol}: one distribution (${biggest:.4f}) is {biggest/median:.0f}x this "
                  f"ticker's own median and dominates its TTM sum — worth verifying against a "
                  f"primary source before trusting this yield.")
    except Exception:
        pass

def split_dividends_regular_vs_special(items):
    """
    Given a list of (date, amount) TTM dividend entries — already rescaled
    onto the current share basis — separates out any distribution(s) that
    look like a one-time/special/return-of-capital event rather than part
    of the ticker's regular recurring cadence. Without this, a single
    outsized event (a liquidating REIT's final capital return, a special
    stock-and-cash dividend like HERZ's) gets summed straight into "TTM
    yield" and produces a number that's technically the true trailing cash
    total but wildly misleading as "the yield" — e.g. ARI showing 68%
    because of a $3.75/share liquidation-related return of capital, when
    the recurring rate every other tracker quotes is 14%.

    Heuristic (same threshold flag_if_outlier already uses to flag this
    print-side): a distribution counts as "special" only if it's at least
    15x the sample's median AND makes up at least half of the total TTM
    sum. It's only actually excluded if doing so still leaves at least one
    "regular" payment behind to establish what normal looks like — without
    that baseline there's nothing to compare against, so nothing gets
    excluded and the full sum is used as-is (matches prior behavior, and
    is the safe default for e.g. a fund that only just started paying).

    Returns (regular_items, special_items) — special_items is empty when no
    confident split could be made.
    """
    if len(items) < 2:
        return items, []
    amounts = [float(a) for _, a in items]
    amounts_sorted = sorted(amounts)
    median = amounts_sorted[len(amounts_sorted) // 2]
    raw_sum = sum(amounts)
    if median <= 0 or raw_sum <= 0:
        return items, []
    regular, special = [], []
    for entry, amt in zip(items, amounts):
        if amt >= median * 15 and (amt / raw_sum) >= 0.5:
            special.append(entry)
        else:
            regular.append(entry)
    if not regular or not special:
        return items, []
    return regular, special

def calc_ttm_yield(ticker_obj, price, symbol=None, market="us"):
    """
    Sums actual dividends paid in the trailing 365 days, divided by current price.

    IMPORTANT: if a stock/fund split (including a reverse split) happened
    within that window, a dividend paid BEFORE the split is on a different
    share basis than the current price — summing them raw would badly
    distort the result. Each pre-split dividend gets rescaled to the
    current share basis before being summed, so a $0.69 old-share dividend
    before a genuine 1-for-10 reverse split correctly counts as $6.90 on
    today's share basis, not $0.69.

    But that rescaling is only trustworthy if the split itself is real —
    see corroborate_split()'s docstring for the HERZ case that motivated
    sanity-checking splits against actual price history before using them.

    Returns a dict, not a bare float:
      - yield:         TTM yield from RECURRING distributions only (a
                        confidently-identified one-time special is
                        excluded) — this is what gets used as "the" yield.
      - fullYield:      TTM yield including every distribution, special or
                        not — kept for transparency/suspect-detection, not
                        shown as the primary number.
      - specialAmount:  per-share amount of any excluded special (0 if none).
      - specialDates:   ISO dates of the excluded special distribution(s).
    """
    empty = {"yield": 0.0, "fullYield": 0.0, "specialAmount": 0.0, "specialDates": []}
    try:
        divs = ticker_obj.dividends
        if divs is None or len(divs) == 0: return empty
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)
        ttm = divs[divs.index >= cutoff]
        if len(ttm) == 0: return empty

        try:
            splits = ticker_obj.splits
        except Exception:
            splits = None

        if splits is not None and len(splits) > 0:
            splits_in_window = splits[splits.index >= cutoff]
        else:
            splits_in_window = None

        adjusted_items = []  # (date, split-adjusted amount), current share basis
        if splits_in_window is not None and len(splits_in_window) > 0:
            # Only fetch price history (an extra API call) when there's
            # actually a split to sanity-check — most tickers never hit
            # this path, so this doesn't add load to the common case.
            try:
                # auto_adjust=False is required here — the default (True)
                # returns split-adjusted closes, which erase the exact price
                # jump we need to see to confirm a split actually happened.
                hist = ticker_obj.history(period="1y", auto_adjust=False)
            except Exception:
                hist = None
            trusted_mask = [corroborate_split(hist, d, r) for d, r in splits_in_window.items()]
            trusted_splits = splits_in_window[trusted_mask]

            for div_date, div_amount in ttm.items():
                # cumulative ratio of every TRUSTED split that happened AFTER this
                # dividend was paid — that's what's needed to rescale it onto
                # today's share basis. Unverified/phantom splits are excluded.
                later_splits = trusted_splits[trusted_splits.index > div_date]
                ratio = float(later_splits.prod()) if len(later_splits) > 0 else 1.0
                if ratio <= 0: ratio = 1.0
                adjusted_items.append((div_date, float(div_amount) / ratio))
        else:
            adjusted_items = [(d, float(a)) for d, a in ttm.items()]

        full_annual_div = sum(a for _, a in adjusted_items)
        flag_if_outlier(symbol or "?", list(ttm.items()), float(ttm.sum()))

        regular_items, special_items = split_dividends_regular_vs_special(adjusted_items)
        recurring_annual_div = sum(a for _, a in regular_items)
        special_annual_div   = sum(a for _, a in special_items)
        if special_items:
            print(f"  ⚠ {symbol or '?'}: excluding {len(special_items)} one-time "
                  f"distribution(s) totaling ${special_annual_div:.4f}/share from TTM "
                  f"yield (recurring-only ${recurring_annual_div:.4f} vs full "
                  f"${full_annual_div:.4f}) — looks like a special/return-of-capital "
                  f"payment, not the regular cadence.")

        # Apply override (dynamic UI override beats the code-level override).
        # An override always wins outright — a human has already decided the
        # right annual figure, so it replaces both the recurring and full
        # numbers and there's no "special" left to report separately.
        ovr = get_override(symbol, market)
        if ovr is not None:
            recurring_annual_div = ovr[0]
            full_annual_div = ovr[0]
            special_annual_div = 0.0
            special_items = []

        recurring_yield = round((recurring_annual_div / price) * 100, 2) if price > 0 and recurring_annual_div > 0 else 0.0
        full_yield      = round((full_annual_div / price) * 100, 2) if price > 0 and full_annual_div > 0 else 0.0
        return {
            "yield":         recurring_yield,
            "fullYield":     full_yield,
            "specialAmount": round(special_annual_div, 4),
            "specialDates":  [d.date().isoformat() for d, _ in special_items],
        }
    except Exception:
        return empty

def fix_payout(raw):
    """
    yfinance simply doesn't report payoutRatio for ETFs (it's a
    company-earnings concept, not a fund concept) — every single ETF used
    to fall into the raw<=0 branch here and get a hardcoded 60 back,
    which the UI then displayed as if it were a real measured payout
    ratio. Nearly two-thirds of the entire cache (every ETF, plus any
    stock yfinance just didn't have the field for) was showing that same
    fabricated 60%.

    Now: no real data -> None, so the UI can honestly show "N/A" instead
    of a number that looks precise but was actually invented. The
    frontend's own scoring math already treats a missing payoutRatio as
    a neutral 60 assumption (see calcScore/getTraps in App.jsx, which use
    `s.payoutRatio ?? 60`) — that's the right place for an assumption
    like this to live, not baked into the data as if it were observed.
    """
    if raw <= 0: return None
    pct = raw * 100 if raw <= 1.5 else raw
    return min(int(round(pct)), 300)

def pick_primary_yield(ttm_yield, fwd_yield, symbol=None):
    """
    Decides which yield to use as primary for a ticker.

    Plain rule: use yfinance's forward yield (dividendRate / price) when
    available, fall back to our computed TTM when yfinance doesn't
    return it. That's the same number every other finance site displays
    by default (Yahoo, Morningstar, Seeking Alpha, your broker), so
    matching their primary number kills the "all my numbers look wrong"
    complaint.

    We DON'T try to be clever about which is "more correct" anymore.
    yfinance is the source of truth; TTM is a fallback and a secondary
    sanity check, not a competing authority. If a specific ticker
    needs an override (like HERZ's split-adjusted special distribution),
    MANUAL_TTM_DIVIDEND_OVERRIDES is the right place for that — not
    automatic rules here that have to guess right every time.
    """
    try:
        ttm = float(ttm_yield or 0)
        fwd = float(fwd_yield or 0)
    except Exception:
        return float(ttm_yield or 0), "ttm"

    if fwd > 0:
        return round(fwd, 2), "forward"
    if ttm > 0:
        return round(ttm, 2), "ttm"
    return 0.0, "ttm"

def detect_bt(info):
    n   = safe_str(info.get("longName")).lower()
    ind = safe_str(info.get("industry")).lower()
    sec = safe_str(info.get("sector")).lower()
    qt  = safe_str(info.get("quoteType")).lower()
    if qt == "etf":                                         return "etf"
    if "reit" in ind or "real estate" in sec:               return "reit"
    if "business development" in ind:                       return "bdc"
    if "midstream" in ind or "pipeline" in ind \
       or n.endswith(" lp") or " partners l" in n:         return "mlp"
    if "mortgage" in ind:                                   return "mreit"
    return "normal"

def consec_years(ticker_obj):
    try:
        hist = ticker_obj.dividends
        if hist is None or len(hist) == 0: return 0
        years = sorted(set(hist.index.year), reverse=True)
        c = 0
        for i, y in enumerate(years):
            if i == 0: c = 1
            elif years[i-1] - y == 1: c += 1
            else: break
        return c
    except Exception:
        return 0

def price_trend_1y(ticker_obj):
    try:
        h = ticker_obj.history(period="1y")
        if h is None or len(h) < 10: return 0
        now = float(h["Close"].iloc[-1])
        ago = float(h["Close"].iloc[0])
        if ago == 0: return 0
        v = ((now - ago) / ago) * 100
        return 0 if (math.isnan(v) or math.isinf(v)) else round(v, 1)
    except Exception:
        return 0

def is_rate_limit_error(e):
    """Detect Yahoo Finance's rate-limit/block error so callers can stop
    early instead of grinding through thousands of doomed requests."""
    msg = str(e).lower()
    return "too many requests" in msg or "rate limit" in msg or " 429" in msg

class RateLimitDetected(Exception):
    """Raised by fetch_one when Yahoo has rate-limited/blocked this IP,
    after retries were already exhausted — distinct from a normal
    'no data for this ticker' case, so callers can stop the whole scan
    instead of burning through thousands more doomed requests."""
    pass

def fetch_one(symbol, retries=2, market="us"):
    """
    Fetch one ticker's data from Yahoo Finance.
    Retries with backoff on failure — hosts like PythonAnywhere share IP
    addresses across many users, and Yahoo Finance rate-limits/blocks shared
    IPs more aggressively than a home connection. A short pause and retry
    often succeeds where the first attempt got a transient 404/timeout.
    """
    for attempt in range(retries + 1):
        try:
            t    = yf.Ticker(symbol)
            info = t.info or {}
            price = safe_num(
                info.get("currentPrice") or info.get("regularMarketPrice") or
                info.get("navPrice")     or info.get("previousClose"), 0)
            if price == 0:
                if attempt < retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                return None
            is_etf = safe_str(info.get("quoteType")).lower() == "etf"
            bt     = detect_bt(info)
            # Most LSE main-market equities are quoted by Yahoo in GBp/GBX
            # (pence), not GBP (pounds) — e.g. Vodafone trades around "70",
            # meaning 70 pence, not £70. Dividing raw dividend by raw price
            # still gives the correct yield% either way (same units on both
            # sides), but showing the price on its own needs to know which
            # unit it's actually in, so that's captured and passed through
            # rather than assumed. US tickers just get "USD" back here.
            apac_meta = exchange_meta_for(symbol) if market == "apac" else None
            currency = safe_str(
                info.get("currency"),
                apac_meta["currency"] if apac_meta else MARKET_META[market]["currency_default"],
            )
            ttm_result = calc_ttm_yield(t, price, symbol=symbol, market=market)
            dy             = ttm_result["yield"]
            full_ttm_yield = ttm_result["fullYield"]
            special_amount = ttm_result["specialAmount"]
            special_dates  = ttm_result["specialDates"]
            if dy <= 0 and full_ttm_yield > 0:
                # Every TTM distribution got excluded as "special" — shouldn't
                # happen (split_dividends_regular_vs_special requires at least
                # one regular payment to remain before it excludes anything),
                # but fall back to the full number rather than showing 0%.
                dy = full_ttm_yield
            if dy <= 0: return None
            payout = fix_payout(safe_num(info.get("payoutRatio"), 0))
            # Yahoo's own forward dividend rate — what every other finance
            # site shows by default. Used as the primary yield so the
            # numbers here match Yahoo/Morningstar/your broker. If yfinance
            # doesn't return `dividendRate` for this ticker (some funds,
            # ADRs, recently-listed tickers), pick_primary_yield falls
            # back to the TTM number we already computed above.
            forward_div = safe_num(info.get("dividendRate"), 0)
            forward_yield = round((forward_div / price) * 100, 2) if forward_div > 0 and price > 0 else 0
            # If there's a manual override for this ticker, calc_ttm_yield
            # already substituted the override annual_div, so `dy` IS the
            # override yield. Flag it so the UI can show "📌 override" and
            # the admin can see/edit it from the card.
            override_info = get_override(symbol, market)
            has_override  = override_info is not None
            if has_override:
                primary_yield = round(dy, 2)
                yield_method  = "override"
            else:
                primary_yield, yield_method = pick_primary_yield(dy, forward_yield, symbol)
            # Suspect detection: flag tickers where the only yield we have
            # is a TTM number we can't cross-check. Typical cause: yfinance
            # is missing `dividendRate` (new fund, ETF, foreign ADR) AND
            # its TTM series is unreliable (phantom distributions, bad
            # split adjustment, or a special distribution that rolled into
            # the sum). The admin sees a "Suspects" list and can override
            # the right number in one click instead of finding each one
            # by surprise.
            is_suspect = False
            suspect_reason = ""
            special_note = None
            if special_amount > 0:
                special_note = (
                    f"Excludes a one-time distribution of ${special_amount:.4f}/share "
                    f"on {', '.join(special_dates)} (return of capital / special "
                    f"dividend, not the regular recurring payout) — trailing-12-month "
                    f"total including it would be {full_ttm_yield}%."
                )
            # Whether a cross-check value is allowed to actually REPLACE
            # primary_yield. Only true when primary_yield came from our own
            # TTM fallback (forward_yield <= 0) — is there something worth
            # replacing. When Yahoo supplied a real forward dividendRate,
            # primary_yield IS that number by design (see pick_primary_
            # yield's docstring: "yfinance is the source of truth... We
            # DON'T try to be clever about which is more correct anymore").
            # A TTM/forward mismatch is very often just a recent dividend
            # change (TTM still includes the old rate) rather than a data
            # error — e.g. KREF cut its quarterly payout from $0.25 to
            # $0.10 in mid-2026, so TTM (11.3%) and forward (5.3%,
            # correct) disagreed by >50%, which used to let a cross-check
            # silently overwrite the correct 5.3% forward yield with a
            # corrupted SEC-sourced 45.9% (see _sec_xbrl_dividend's fix
            # for why SEC's number was wrong). Now a forward-sourced
            # primary_yield is never silently replaced — at most the
            # ticker gets flagged for a human to glance at.
            allow_cross_check_override = forward_yield <= 0
            if not has_override and primary_yield >= 5:
                if forward_yield <= 0:
                    if special_amount > 0:
                        # A dominant one-time distribution was already identified
                        # and excluded above (see calc_ttm_yield) — that's a
                        # confident explanation for why the number looked high,
                        # so there's no missing-forward-rate mystery left to flag.
                        # Deliberately skip the cross-check here: Twelvedata/
                        # Marketstack/SEC would each honestly re-sum the SAME
                        # special distribution from their own records and just
                        # reintroduce the inflated number this exclusion fixed.
                        is_suspect = False
                        suspect_reason = special_note
                    else:
                        is_suspect = True
                        suspect_reason = "yfinance missing forward rate — TTM only, no cross-check"
                else:
                    fwd = forward_yield
                    spread = abs(dy - fwd) / max(dy, 0.01)
                    if spread >= 0.5:  # 50%+ disagreement between TTM and FWD
                        is_suspect = True
                        suspect_reason = f"TTM ({dy:.1f}%) and FWD ({fwd:.1f}%) disagree by {int(spread*100)}% — using Yahoo's forward rate ({fwd:.1f}%); the gap is most often a recent dividend change, occasionally worth a manual glance."
            # ── Cross-source verification (the actual fix) ──
            # When yfinance looks suspect, cross-check with Twelvedata and
            # Marketstack. If they return a meaningfully different number,
            # use theirs instead — and clear the suspect flag, because the
            # row is now verified by multiple sources. This only spends
            # API budget on the ~5% of tickers that actually need it.
            dividend_source  = "yfinance"
            dividend_sources = {"yfinance": round((dy / 100) * price, 4) if dy > 0 and price > 0 else 0}
            if is_suspect and not has_override and (TWELVEDATA_API_KEY or MARKETSTACK_API_KEY):
                try:
                    verified_div, verified_source, all_sources = verify_dividend(
                        symbol, price,
                        (dy / 100) * price if dy > 0 and price > 0 else 0
                    )
                    dividend_sources = {k: round(v, 4) for k, v in all_sources.items()}
                    dividend_source  = verified_source
                    if verified_div > 0 and price > 0:
                        verified_yield = (verified_div / price) * 100
                        agrees = abs(verified_yield - primary_yield) < 0.5
                        # Sanity ceiling: confirmed live that DOMH's SEC XBRL
                        # entry returned $432,001.092 as a "per-share dividend"
                        # (a filer-side data error — almost certainly a total
                        # dollar amount mistagged as per-share, not fixable by
                        # better date-range logic) and it sailed straight
                        # through to a displayed yield of 15,940,999.7%. No
                        # real dividend yield gets anywhere near this range —
                        # reject an implausible cross-check value outright
                        # rather than ever adopting or displaying it.
                        PLAUSIBLE_YIELD_CEILING = 200.0
                        if verified_yield > PLAUSIBLE_YIELD_CEILING:
                            is_suspect = True
                            suspect_reason = (
                                f"{suspect_reason} Cross-check ({verified_source}) returned an "
                                f"implausible {verified_yield:,.0f}% — almost certainly a data "
                                f"error at the source, not adopted. Displayed yield unchanged."
                            ).strip()
                            dividend_source = "yfinance"
                        elif not allow_cross_check_override:
                            # primary_yield came from Yahoo's own forward rate —
                            # treat it as authoritative and never let a cross-
                            # checked TTM-style number silently replace it.
                            # Still update dividendSources/dividendSource above
                            # for transparency either way.
                            if agrees:
                                is_suspect = False
                                suspect_reason = ""
                            else:
                                suspect_reason = (
                                    f"{suspect_reason} Cross-check ({verified_source}) reports "
                                    f"{verified_yield:.1f}%, but the forward rate is being kept "
                                    f"as the displayed yield — verify by hand if this looks off."
                                )
                        elif agrees:
                            # Sources agree, false positive — just clear the flag
                            is_suspect    = False
                            suspect_reason = ""
                        else:
                            # Only adopt the verified value if it actually disagrees
                            # with yfinance AND the disagreement is material (>0.5pp),
                            # AND primary_yield came from our TTM fallback (checked
                            # above via allow_cross_check_override).
                            primary_yield = round(verified_yield, 2)
                            yield_method  = verified_source
                            is_suspect    = False
                            suspect_reason = f"auto-corrected via cross-check ({verified_source})"
                except Exception as e:
                    print(f"  verify_dividend error for {symbol}: {e}")
            row = {
                "ticker":               symbol,
                "name":                 safe_str(info.get("longName") or info.get("shortName"), symbol),
                "sector":               safe_str(info.get("sector"), "ETF" if is_etf else "Unknown"),
                "industry":             safe_str(info.get("industry"), ""),
                "price":                round(price, 2),
                "currency":             currency,
                "yield":                primary_yield,
                "ttmYield":             round(dy, 2),
                "fullTtmYield":         round(full_ttm_yield, 2) if full_ttm_yield > dy else None,
                "specialDistribution":  round(special_amount, 4) if special_amount > 0 else None,
                "specialDistributionDates": special_dates if special_dates else None,
                "forwardYield":         round(forward_yield, 2) if forward_yield > 0 else None,
                "forwardDiv":           round(forward_div, 4) if forward_div > 0 else None,
                "yieldMethod":          yield_method,
                "hasOverride":          has_override,
                "overrideSource":       override_info[1] if has_override else None,  # "ui" or "code"
                "isSuspect":            is_suspect,
                "suspectReason":        suspect_reason,
                "dividendSource":       dividend_source,
                "dividendSources":      dividend_sources,
                "payoutRatio":          payout,
                "priceTrend":           price_trend_1y(t),
                "consecutiveDividends": consec_years(t),
                "type":                 "etf" if is_etf else "stock",
                "businessType":         bt,
                "mktCap":               safe_num(info.get("marketCap"), 0),
                "week52High":           round(safe_num(info.get("fiftyTwoWeekHigh"), 0), 2),
                "week52Low":            round(safe_num(info.get("fiftyTwoWeekLow"),  0), 2),
                "description":          safe_str(info.get("longBusinessSummary"), "")[:200],
                "_fetched_at":          datetime.datetime.now().isoformat(),
            }
            # APAC-only fields: which exchange this listing trades on and
            # which country that is, so the UI can show an exchange badge
            # (ASX / NZX / TSE / HKEX / SGX / KRX) instead of a single flag.
            if apac_meta is not None:
                row["exchange"] = safe_str(info.get("exchange"), apac_meta["exchange"])
                row["country"]  = apac_meta["country"]
            for k, v in row.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    row[k] = 0
            return row
        except Exception as e:
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))  # backoff: 1.5s, then 3s
                continue
            print(f"  ✗ {symbol}: {e}")
            if is_rate_limit_error(e):
                raise RateLimitDetected(str(e))
            return None

# ─── FULL MARKET TICKER LIST ─────────────────────────────────────────────────
# NASDAQ Trader publishes the official listed-securities directory as plain
# pipe-delimited text files (this is the same feed brokerages use) — unlike
# api.nasdaq.com's screener endpoint, it isn't behind bot-protection, so it
# actually works reliably from a server. Covers ~11,000 US-listed stocks + ETFs.

TICKER_LIST_MAX_AGE_DAYS = 7  # re-download once a week
MIN_SANE_TICKER_COUNT = 3000  # a real download returns ~8-11k; anything far below this means the fetch failed
UK_MIN_SANE_TICKER_COUNT = 1000  # a real bundled UK file has ~4,400; far below this means the file is missing/corrupt

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL  = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

def _parse_symbol_file(text, symbol_col, test_issue_col, delimiter="|"):
    """Parse a pipe-delimited NASDAQ Trader symbol directory file into a set of tickers."""
    out = set()
    lines = text.strip().splitlines()
    if not lines:
        return out
    header = lines[0].split(delimiter)
    for line in lines[1:]:
        if line.startswith("File Creation Time"):
            continue
        cols = line.split(delimiter)
        if len(cols) <= max(symbol_col, test_issue_col):
            continue
        if cols[test_issue_col].strip().upper() == "Y":  # skip test issues
            continue
        sym = cols[symbol_col].strip().upper()
        if not sym: continue
        if len(sym) > 5: continue
        if sym.endswith(("W", "U", "R")): continue
        if any(c in sym for c in ["^", ".", "/", "-", "$"]): continue
        out.add(sym)
    return out

def fetch_ticker_list(market="us"):
    """Dispatch to the right market's full-universe ticker list loader."""
    if market == "uk":
        return _fetch_ticker_list_uk()
    if market == "apac":
        return _fetch_ticker_list_apac()
    if market == "eurozone":
        return _fetch_ticker_list_eurozone()
    return _fetch_ticker_list_us()

def _fetch_ticker_list_us():
    """
    Download full US stock list from NASDAQ Trader's symbol directory.
    Returns a list of ticker symbols (stocks + ETFs, no warrants/units/preferreds).
    Caches to disk for 7 days so we don't re-download every time.
    A failed/suspiciously-small download is NEVER cached, so the next scan retries
    instead of getting stuck on a broken result for a week.
    """
    ticker_list_file = MARKET_FILES["us"]["tickers"]
    if os.path.exists(ticker_list_file):
        try:
            with open(ticker_list_file) as f:
                data = json.load(f)
            age = datetime.datetime.now() - datetime.datetime.fromisoformat(data["fetched"])
            if age.days < TICKER_LIST_MAX_AGE_DAYS and len(data["tickers"]) >= MIN_SANE_TICKER_COUNT:
                print(f"  Ticker list: {len(data['tickers'])} tickers from cache (age {age.days}d)")
                return data["tickers"]
        except Exception:
            pass

    print("  Downloading full US ticker list from NASDAQ Trader…")
    all_tickers = set()

    # nasdaqlisted.txt: Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
    try:
        req = urllib.request.Request(NASDAQ_LISTED_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            text = resp.read().decode()
        before = len(all_tickers)
        all_tickers |= _parse_symbol_file(text, symbol_col=0, test_issue_col=3)
        print(f"    NASDAQ: → {len(all_tickers)-before} added")
    except Exception as e:
        print(f"    NASDAQ download failed: {e}")

    # otherlisted.txt: ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
    try:
        req = urllib.request.Request(OTHER_LISTED_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            text = resp.read().decode()
        before = len(all_tickers)
        all_tickers |= _parse_symbol_file(text, symbol_col=0, test_issue_col=6)
        print(f"    NYSE/AMEX/other: → {len(all_tickers)-before} added")
    except Exception as e:
        print(f"    NYSE/AMEX download failed: {e}")

    tickers = sorted(all_tickers)
    print(f"  Total unique tickers: {len(tickers)}")

    if len(tickers) < MIN_SANE_TICKER_COUNT:
        print(f"  ⚠ Download looks broken (only {len(tickers)} tickers, expected 8,000+). "
              f"NOT caching this — will retry fresh next scan instead of getting stuck.")
        # fall back to whatever we had cached before, even if stale, rather than nothing
        if os.path.exists(ticker_list_file):
            try:
                with open(ticker_list_file) as f:
                    old = json.load(f)
                if len(old["tickers"]) >= MIN_SANE_TICKER_COUNT:
                    print(f"  Falling back to previous cached list ({len(old['tickers'])} tickers).")
                    return old["tickers"]
            except Exception:
                pass
        return tickers  # better than nothing, but won't be cached

    try:
        with open(ticker_list_file, "w") as f:
            json.dump({"fetched": datetime.datetime.now().isoformat(), "tickers": tickers}, f)
    except Exception as e:
        print(f"  Could not save ticker list: {e}")

    return tickers


def _fetch_ticker_list_uk():
    """
    Load the bundled UK (LSE) ticker list — a static snapshot shipped as
    uk_all_tickers.json next to server.py (built from the LSE's own official
    "All Equity Instruments" spreadsheet). Unlike NASDAQ Trader's plain-text
    feed, the London Stock Exchange's own instrument list is a JS-rendered
    page with no stable plain-URL download, so it can't be re-scraped
    automatically — refreshing it means downloading the current xlsx and
    re-running the same TIDM extraction by hand.
    Falls back to just SEED_UNIVERSE_UK (still enough to run on) if the
    bundled file is missing.
    """
    ticker_list_file = MARKET_FILES["uk"]["tickers"]
    if os.path.exists(ticker_list_file):
        try:
            with open(ticker_list_file) as f:
                data = json.load(f)
            tickers = data.get("tickers", [])
            if len(tickers) >= UK_MIN_SANE_TICKER_COUNT:
                print(f"  Ticker list: {len(tickers)} LSE tickers from {os.path.basename(ticker_list_file)} "
                      f"(snapshot: {data.get('fetched', 'unknown date')})")
                return sorted(set(tickers))
            print(f"  ⚠ {ticker_list_file} only has {len(tickers)} tickers — looks incomplete.")
        except Exception as e:
            print(f"  ⚠ Could not read {ticker_list_file}: {e}")
    else:
        print(f"  ⚠ {ticker_list_file} not found.")

    print(f"  Falling back to the {len(SEED_UNIVERSE_UK)}-ticker seed list only — "
          f"put uk_all_tickers.json next to server.py for the full UK market pool.")
    return list(SEED_UNIVERSE_UK)


# ─── APAC FULL MARKET TICKER LIST (Australia + Asia-Pacific) ─────────────
# ASX publishes a free public CSV of every listed company — one of the few
# APAC exchanges with anything like what NASDAQ Trader's symbol directory
# provides for the US. KRX, HKEX and (optionally) TSE also have real
# full-market feeds; NZX and SGX either have none (NZX) or an unstable
# undocumented one (SGX), so those fall back to the curated seed names.
# Each exchange's download is cached and validated independently, so one
# exchange's feed breaking never takes the others down with it.

APAC_TICKER_LIST_FILE = MARKET_FILES["apac"]["tickers"]

ASX_LISTED_URL     = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
ASX_ISIN_XLS_URL   = "https://www.asx.com.au/programs/ISIN.xls"  # older, separate file; unconfirmed whether ASX still serves it
KRX_LISTED_URL  = "http://kind.krx.co.kr/corpgeneral/corpList.do"
HKEX_LISTED_URL = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"
TSE_LISTED_URL  = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
SGX_LISTED_URL  = "https://api.sgx.com/securities/v1.1?params=b,nc,n"

def _parse_asx_csv(text):
    """
    Parse ASX's public 'ASX Listed Companies' CSV into a set of Yahoo-style
    tickers (e.g. 'BHP' -> 'BHP.AX'). The file has a couple of title/blank
    lines before the real header row ('Company name,ASX code,GICS industry
    group'), so we skip everything until we find that header. If that
    header text isn't found at all (ASX has changed the file's wording
    before), falls back to a looser heuristic: any row whose 2nd column
    looks like a plausible 2-5 character ASX code.
    """
    out = set()
    lines = text.strip().splitlines()
    started = False
    for line in lines:
        if not started:
            if "asx code" in line.lower():
                started = True
            continue
        try:
            row = next(csv.reader([line]))
        except Exception:
            continue
        if len(row) < 2:
            continue
        code = row[1].strip().upper()
        if not code or len(code) > 5:
            continue
        if any(c in code for c in ["^", ".", "/", "$"]):
            continue
        out.add(f"{code}.AX")
    if out:
        return out

    # Fallback: expected header text wasn't found (format may have changed
    # again) — try every row and keep ones whose 2nd column is a plausible
    # bare ASX code, skipping the header row itself.
    for line in lines:
        try:
            row = next(csv.reader([line]))
        except Exception:
            continue
        if len(row) < 2:
            continue
        code = row[1].strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{2,5}", code):
            continue
        if code in ("CODE", "ASXCODE"):
            continue
        out.add(f"{code}.AX")
    return out


def _fetch_apac_bytes_with_local_override(url, override_filename, extra_headers=None, timeout=20):
    """
    Shared fetcher used by the exchanges most likely to sit behind bot
    protection (ASX, KRX). Checks for a manually-downloaded override file
    in BASE_DIR first — if a live fetch keeps getting blocked, open the
    URL in a normal browser, save the response, and drop it next to
    server.py under this filename; the next scan will use it automatically
    instead of hitting the network. Otherwise does a real HTTP GET with
    browser-like headers (some WAFs reject bare urllib requests outright).
    """
    override_path = os.path.join(BASE_DIR, override_filename)
    if os.path.exists(override_path):
        print(f"    Using manually-downloaded {override_filename} instead of a live fetch")
        with open(override_path, "rb") as f:
            return f.read()

    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "text/csv,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()

def _fetch_ticker_list_apac():
    """
    Build the full APAC scan pool by combining a real full-market download
    per exchange (where a free one exists) with the curated
    SEED_UNIVERSE_APAC names for exchanges that don't publish one:

      - ASX   (Australia)   — ASX's public listed-companies CSV.
      - KRX   (South Korea) — KRX/KIND's public corpList.do export
                               (KOSPI + KOSDAQ).
      - HKEX  (Hong Kong)   — HKEX's public ListOfSecurities.xlsx.
      - TSE   (Japan)       — JPX's public data_j.xls "listed issues" file.
                               Needs the optional `xlrd` package
                               (pip install xlrd) — without it, Japan falls
                               back to the curated list only.
      - SGX   (Singapore)   — SGX's public (undocumented) securities API.
                               This one is the least stable of the five; if
                               its response shape ever changes, it silently
                               falls back to the curated list instead of
                               breaking the scan.
      - NZX   (New Zealand) — NZX doesn't publish a free full-directory
                               feed at all, so NZX is entirely the curated
                               SEED_UNIVERSE_APAC names.

    Each exchange is fetched/cached/validated independently, so one
    exchange's feed breaking doesn't take the others down with it.
    """
    cache = _load_apac_ticker_list_cache()

    asx  = _get_apac_exchange_tickers(cache, "asx",  _fetch_asx_full,  MIN_SANE_ASX)
    krx  = _get_apac_exchange_tickers(cache, "krx",  _fetch_krx_full,  MIN_SANE_KRX)
    # NOTE: "hkex_v2" (not "hkex") — a padding bug in an earlier HKEX parser
    # got baked into some early cached ticker lists. Bumping the cache key
    # forces a fresh download instead of reusing a stale, buggy cache.
    hkex = _get_apac_exchange_tickers(cache, "hkex_v2", _fetch_hkex_full, MIN_SANE_HKEX)
    tse  = _get_apac_exchange_tickers(cache, "tse",  _fetch_tse_full,  MIN_SANE_TSE)
    sgx  = _get_apac_exchange_tickers(cache, "sgx",  _fetch_sgx_full,  MIN_SANE_SGX)

    _save_apac_ticker_list_cache(cache)

    full_download = asx | krx | hkex | tse | sgx
    curated_only   = {t for t in SEED_UNIVERSE_APAC if not any(
        t.endswith(f".{suf}") for suf in ("AX", "KS", "KQ", "HK", "T", "SI")
    )}  # NZX (and anything else with no full-download source) always comes from here
    curated_backfill = {t for t in SEED_UNIVERSE_APAC if t not in full_download}

    tickers = sorted(full_download | curated_only | curated_backfill)
    print(f"  Total APAC pool: {len(tickers)} tickers — "
          f"ASX {len(asx)}, KRX {len(krx)}, HKEX {len(hkex)}, TSE {len(tse)}, "
          f"SGX {len(sgx)}, + curated NZX/backfill {len(curated_only | curated_backfill)}")
    return tickers


# ─── APAC per-exchange full-market discovery ─────────────────────────────
# Every _fetch_*_full() below returns a set of Yahoo-style tickers on
# success, or an empty set on failure — never raises. _get_apac_exchange_
# tickers wraps them with disk caching + a minimum-sane-count sanity check,
# same pattern as the US/UK ticker-list loaders above.

MIN_SANE_ASX  = 1200   # real ASX download returns ~1,800-2,200
MIN_SANE_KRX  = 1500   # real KOSPI+KOSDAQ download returns ~2,400-2,800
MIN_SANE_HKEX = 1000   # real HKEX equities download returns ~2,000-2,600
MIN_SANE_TSE  = 2000   # real JPX download returns ~3,700-3,900
MIN_SANE_SGX  = 300    # real SGX securities download returns ~600-1,200

def _load_apac_ticker_list_cache():
    if os.path.exists(APAC_TICKER_LIST_FILE):
        try:
            with open(APAC_TICKER_LIST_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_apac_ticker_list_cache(cache):
    try:
        tmp = APAC_TICKER_LIST_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(cache, f)
        os.replace(tmp, APAC_TICKER_LIST_FILE)
    except Exception as e:
        print(f"  Could not save APAC ticker list cache: {e}")

def _get_apac_exchange_tickers(cache, key, fetch_fn, min_sane):
    """
    Shared cache/validate/fallback wrapper for one exchange: use
    fresh-enough cache if we have it -> else try a real download -> if
    that's too small to be believable, fall back to a stale cache (better
    than nothing) -> the caller unions in the curated names on top
    regardless, so a total failure here just means "curated only".
    """
    entry = cache.get(key)
    if entry:
        try:
            age = datetime.datetime.now() - datetime.datetime.fromisoformat(entry["fetched"])
            if age.days < TICKER_LIST_MAX_AGE_DAYS and len(entry["tickers"]) >= min_sane:
                print(f"  {key.upper()}: {len(entry['tickers'])} tickers from cache (age {age.days}d)")
                return set(entry["tickers"])
        except Exception:
            pass

    tickers = fetch_fn()
    if len(tickers) >= min_sane:
        print(f"    {key.upper()}: -> {len(tickers)} tickers")
        cache[key] = {"fetched": datetime.datetime.now().isoformat(), "tickers": sorted(tickers)}
        return tickers

    print(f"  ⚠ {key.upper()} download looks broken or unavailable "
          f"(only {len(tickers)} tickers, expected {min_sane}+). Not caching this result.")
    if entry:
        try:
            if len(entry["tickers"]) >= min_sane:
                print(f"  Falling back to previous cached {key.upper()} list ({len(entry['tickers'])} tickers).")
                return set(entry["tickers"])
        except Exception:
            pass
    return set()  # curated SEED_UNIVERSE_APAC names for this exchange still get included by the caller


def _fetch_asx_full():
    """
    ASX retired the free ASXListedCompanies.csv file (confirmed — it now
    404s; ASX replaced it with a JS-driven "Company directory" search page
    that has no plain data file behind it, so this isn't a bot-protection
    problem that better headers can fix). Two things are tried before
    giving up and letting ASX fall back to the curated SEED_UNIVERSE_APAC
    names:
      1. A manual override file (asx_listed_override.csv) — see
         _fetch_apac_bytes_with_local_override. If you find/export a
         current ASX listing CSV from somewhere (e.g. your broker, or a
         third-party aggregator), drop it here.
      2. ASX's older, separate ISIN.xls file, which is a different asset
         from the retired CSV and *might* still be live — this is
         unconfirmed, so it's attempted best-effort and just logged if it
         also fails. Needs the optional `xlrd` package, same as TSE.
    """
    try:
        data = _fetch_apac_bytes_with_local_override(
            ASX_LISTED_URL, "asx_listed_override.csv",
            extra_headers={"Referer": "https://www.asx.com.au/markets/trade-our-cash-market/directory"})
        text = data.decode("utf-8", errors="ignore")
        result = _parse_asx_csv(text)
        if result:
            return result
        snippet = text[:200].replace("\n", " ")
        print(f"    ASX: got {len(data)} bytes but found 0 tickers in it. "
              f"First 200 chars: {snippet!r}")
    except Exception as e:
        print(f"    ASX: ASXListedCompanies.csv failed ({e}) — ASX retired this file "
              f"(confirmed, not just bot protection), so this is expected.")

    # Fall back to ASX's older, separate ISIN.xls file — unconfirmed whether
    # it's still live, tried as a best-effort second attempt only.
    try:
        import xlrd
    except ImportError:
        print("    ASX: skipping ISIN.xls fallback attempt — `xlrd` not installed.")
        _print_asx_fallback_hint()
        return set()
    try:
        data = _fetch_apac_bytes_with_local_override(ASX_ISIN_XLS_URL, "asx_isin_override.xls")
        book = xlrd.open_workbook(file_contents=data)
        sheet = book.sheet_by_index(0)
        out = set()
        for r in range(sheet.nrows):
            for c in range(sheet.ncols):
                val = str(sheet.cell(r, c).value).strip().upper()
                if re.fullmatch(r"[A-Z0-9]{2,5}", val) and val not in ("CODE", "ISIN"):
                    out.add(f"{val}.AX")
        if out:
            print(f"    ASX: recovered {len(out)} tickers from the older ISIN.xls file")
        else:
            _print_asx_fallback_hint()
        return out
    except Exception as e:
        print(f"    ASX: ISIN.xls fallback also failed ({e})")
        _print_asx_fallback_hint()
        return set()


def _print_asx_fallback_hint():
    print(f"    ASX has no confirmed working free full-list source right now — "
          f"falling back to the curated ASX names in SEED_UNIVERSE_APAC for this "
          f"run. If you find a current ASX listing CSV/export somewhere, save it "
          f"as {os.path.join(BASE_DIR, 'asx_listed_override.csv')} and the next "
          f"scan will use it instead.")


def _fetch_krx_full():
    """
    KRX/KIND's corpgeneral/corpList.do?method=download endpoint returns an
    HTML table (despite being served as a download) with a company-name
    column and a 6-digit stock-code column. This is the same free endpoint
    widely used by Korean quant/finance tooling. Fetches KOSPI (-> .KS) and
    KOSDAQ (-> .KQ) separately since Yahoo uses different suffixes for each.
    Honors a manual override file per market if a live fetch keeps getting
    blocked (see _fetch_apac_bytes_with_local_override).
    """
    out = set()
    for market_type, suffix in (("stockMkt", "KS"), ("kosdaqMkt", "KQ")):
        try:
            url = (f"{KRX_LISTED_URL}?method=download&searchType=13"
                   f"&marketType={market_type}")
            raw = _fetch_apac_bytes_with_local_override(
                url, f"krx_listed_{market_type}_override.html",
                extra_headers={"Referer": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage"})
            try:
                text = raw.decode("euc-kr")
            except UnicodeDecodeError:
                text = raw.decode("utf-8", errors="ignore")
            rows = _extract_html_table_rows(text)
            found = 0
            for row in rows:
                # Don't assume the code is specifically in column index 1 —
                # KRX's table sometimes nests things in a way that shifts
                # columns (an extra leading checkbox/number column, etc).
                # Instead, take any cell in the row that's a bare 6-digit
                # number — Korean stock/fund codes are always exactly 6
                # digits, so this is a safe, position-independent match.
                for cell in row:
                    cell = cell.strip()
                    if cell.isdigit() and len(cell) == 6:
                        out.add(f"{cell}.{suffix}")
                        found += 1
            if found == 0:
                snippet = text[:200].replace("\n", " ")
                print(f"    KRX ({market_type}): got {len(raw)} bytes, extracted {len(rows)} table rows, "
                      f"but found 0 six-digit codes in any of them. "
                      f"First 200 chars: {snippet!r}")
                print(f"    Workaround: open {url} in your browser, save it, and put it at "
                      f"{os.path.join(BASE_DIR, f'krx_listed_{market_type}_override.html')} — "
                      f"the next scan will use that file instead of fetching it.")
        except Exception as e:
            print(f"    KRX ({market_type}) download failed: {e}")
    return out


def _fetch_hkex_full():
    """
    HKEX publishes a daily ListOfSecurities.xlsx with every listed
    security's stock code, name, and category. Parsed directly from the
    xlsx's underlying zip/XML (no openpyxl/pandas dependency needed) —
    only plain 'Equity' category rows are kept (skips bonds/warrants/CBBCs).
    """
    try:
        req = urllib.request.Request(HKEX_LISTED_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = resp.read()
        rows = _parse_xlsx_rows(data)
    except Exception as e:
        print(f"    HKEX download failed: {e}")
        return set()

    out = set()
    for row in rows:
        if len(row) < 3:
            continue
        code = row[0].strip()
        category = row[2].strip().lower() if len(row) > 2 else ""
        if not code.isdigit():
            continue
        if category and category != "equity":
            continue
        # The xlsx's own code column is already zero-padded to 5 chars
        # (e.g. "00001"), but Yahoo's actual HK tickers are 4-digit
        # minimum (e.g. "0001.HK", not "00001.HK") — going through int()
        # strips the source padding before re-padding to what Yahoo
        # expects, instead of just appending zeros on top of it.
        out.add(f"{int(code):04d}.HK")
    return out


def _fetch_tse_full():
    """
    JPX publishes a full "listed issues" workbook (data_j.xls, old binary
    .xls format) with every TSE-listed security's 4-digit code and name.
    Parsing old-format .xls needs the `xlrd` package — if it's not
    installed, this just returns nothing and TSE falls back to the
    curated seed names (pip install xlrd for full Japan coverage).
    """
    try:
        import xlrd
    except ImportError:
        print("    TSE: `xlrd` not installed — skipping full JPX download "
              "(pip install xlrd for full Japan coverage; using curated names for now).")
        return set()
    try:
        req = urllib.request.Request(TSE_LISTED_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = resp.read()
        book = xlrd.open_workbook(file_contents=data)
        sheet = book.sheet_by_index(0)
        header = [str(c.value).strip() for c in sheet.row(0)]
        # JPX's own header is in Japanese ("コード" = "code") — matched by
        # substring so this survives minor header wording changes; falls
        # back to column 0 if the header can't be matched at all.
        code_col = next((i for i, h in enumerate(header) if "コード" in h), 0)
        out = set()
        for r in range(1, sheet.nrows):
            raw = sheet.cell(r, code_col).value
            code = str(int(raw)) if isinstance(raw, float) else str(raw).strip()
            if code.isdigit() and len(code) == 4:
                out.add(f"{code}.T")
        return out
    except Exception as e:
        print(f"    TSE download failed: {e}")
        return set()


def _fetch_sgx_full():
    """
    SGX doesn't publish an official downloadable directory, but its
    website calls an unauthenticated public JSON endpoint
    (api.sgx.com/securities/v1.1) to list every tradeable security. This
    is the least stable source of the five — undocumented, could change
    shape without notice, and has been seen returning 403 to plain script
    requests (likely a WAF check on headers/origin rather than an API key)
    — so the field-name guesses below are validated defensively and this
    just returns nothing (falling back to the curated SGX names) if the
    response doesn't look right. Honors a manual override file if you open
    the URL in a browser, save the JSON, and drop it in as
    sgx_listed_override.json next to server.py.
    """
    try:
        raw = _fetch_apac_bytes_with_local_override(
            SGX_LISTED_URL, "sgx_listed_override.json",
            extra_headers={
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://www2.sgx.com",
                "Referer": "https://www2.sgx.com/securities/securities-prices",
            })
        payload = json.loads(raw.decode("utf-8", errors="ignore"))
        rows = payload.get("data", payload) if isinstance(payload, dict) else payload
        out = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            code = safe_str(row.get("b") or row.get("nc"))
            code = code.strip().upper()
            if code and len(code) <= 6 and code.replace(".", "").isalnum():
                out.add(f"{code}.SI")
        return out
    except Exception as e:
        print(f"    SGX download failed: {e}")
        return set()


def _extract_html_table_rows(html_text):
    """Minimal dependency-free HTML <table> -> rows-of-cell-text parser,
    used for KRX's corpList.do export. Good enough for a simple, single,
    non-nested data table; not a general-purpose HTML parser."""
    import html as html_module
    from html.parser import HTMLParser

    class _TableParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.rows = []
            self._row = None
            self._cell = None
        def handle_starttag(self, tag, attrs):
            if tag == "tr":
                self._row = []
            elif tag in ("td", "th"):
                self._cell = []
        def handle_endtag(self, tag):
            if tag == "tr" and self._row is not None:
                self.rows.append(self._row)
                self._row = None
            elif tag in ("td", "th") and self._cell is not None:
                if self._row is not None:
                    self._row.append(html_module.unescape("".join(self._cell)).strip())
                self._cell = None
        def handle_data(self, data):
            if self._cell is not None:
                self._cell.append(data)

    parser = _TableParser()
    parser.feed(html_text)
    return parser.rows


def _parse_xlsx_rows(xlsx_bytes):
    """
    Minimal dependency-free .xlsx -> rows-of-cell-text reader (no
    openpyxl/pandas). An .xlsx is just a zip of XML parts; this reads the
    first worksheet plus the shared-strings table and resolves each cell
    to plain text, in column order. Good enough for a flat data table like
    HKEX's ListOfSecurities.xlsx; not a general-purpose spreadsheet reader.
    """
    import zipfile
    import xml.etree.ElementTree as ET

    NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

    def _col_to_index(ref):
        letters = "".join(c for c in ref if c.isalpha())
        idx = 0
        for ch in letters:
            idx = idx * 26 + (ord(ch.upper()) - ord("A") + 1)
        return idx - 1

    with zipfile.ZipFile(io.BytesIO(xlsx_bytes)) as z:
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in root.findall(f"{NS}si"):
                text = "".join(t.text or "" for t in si.findall(f".//{NS}t"))
                shared.append(text)

        sheet_name = next((n for n in z.namelist()
                            if n.startswith("xl/worksheets/") and n.endswith(".xml")), None)
        if not sheet_name:
            return []
        root = ET.fromstring(z.read(sheet_name))
        rows_out = []
        for row in root.findall(f".//{NS}row"):
            cells = {}
            max_idx = -1
            for c in row.findall(f"{NS}c"):
                ref = c.get("r", "")
                idx = _col_to_index(ref) if ref else max_idx + 1
                t = c.get("t")
                v_el = c.find(f"{NS}v")
                if t == "s" and v_el is not None:
                    try:
                        value = shared[int(v_el.text)]
                    except Exception:
                        value = ""
                elif t == "inlineStr":
                    is_el = c.find(f"{NS}is")
                    value = "".join(x.text or "" for x in is_el.findall(f".//{NS}t")) if is_el is not None else ""
                else:
                    value = v_el.text if v_el is not None else ""
                cells[idx] = value or ""
                max_idx = max(max_idx, idx)
            rows_out.append([cells.get(i, "") for i in range(max_idx + 1)])
        return rows_out


# ─── EUROZONE FULL MARKET TICKER LIST ────────────────────────────────────
# Real full-market discovery for the 5 countries Euronext runs (France,
# Netherlands, Belgium, Portugal, Ireland): a community-maintained CSV on
# GitHub tracks every Euronext-listed instrument and which of Euronext's
# markets it trades on (github.com/derekbanas/Python4Finance). Its "Ticker"
# column always uses a .PA suffix regardless of the real listing exchange
# (a known quirk of that file), so this discards that suffix and rebuilds
# the correct Yahoo Finance suffix from the "Exchange" column instead —
# e.g. "HEIA.PA" + "Euronext Amsterdam" -> "HEIA.AS" (Heineken's real
# ticker). Only main/regulated markets are kept (Growth/Access/Expert tiers
# are excluded) since those smaller markets rarely have reliable dividend
# data on Yahoo Finance and would mostly just waste scan time.
#
# Germany (Xetra), Switzerland (SIX), Austria (Vienna), Spain (Madrid),
# Italy (Milan), and Finland (Helsinki) have no equivalent free bulk feed
# — those countries' candidates come ONLY from SEED_UNIVERSE_EUROZONE.

EURONEXT_CSV_URL = "https://raw.githubusercontent.com/derekbanas/Python4Finance/main/Euronext.csv"
EUROZONE_TICKER_LIST_FILE = MARKET_FILES["eurozone"]["tickers"]
MIN_SANE_EURONEXT_COUNT = 400  # a real download returns ~750-800 main-market names; far below this means the fetch/parse failed

# Exchange name (as it appears in the CSV) -> correct Yahoo Finance suffix.
# Only main/regulated markets are included on purpose (see note above).
EURONEXT_EXCHANGE_SUFFIX = {
    "Euronext Paris":     "PA",
    "Euronext Amsterdam": "AS",
    "Euronext Brussels":  "BR",
    "Euronext Lisbon":    "LS",
    "Euronext Dublin":    "IR",
}

def _parse_euronext_csv(text):
    """Parse the Euronext.csv content into a set of correctly-suffixed
    Yahoo Finance tickers, keeping only main-market listings."""
    out = set()
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        exch = (row.get("Exchange") or "").strip()
        suffix = EURONEXT_EXCHANGE_SUFFIX.get(exch)
        if not suffix:
            continue  # Oslo, Growth/Access/Expert tiers, etc. — skip
        raw_ticker = (row.get("Ticker") or "").strip().upper()
        if not raw_ticker:
            continue
        base = raw_ticker.split(".")[0]  # strip the CSV's always-".PA" suffix
        if not base:
            continue
        out.add(f"{base}.{suffix}")
    return out

def _fetch_eurozone_euronext_tickers():
    """
    Download + parse the Euronext main-market instrument list (see notes
    above). Caches to disk for 7 days (TICKER_LIST_MAX_AGE_DAYS, shared
    with the US/UK ticker-list loaders above). A failed/suspiciously-small
    download is NEVER cached, so the next scan retries instead of getting
    stuck on a broken result for a week — and falls back to a stale cached
    copy (or, failing that, an empty list) rather than crashing the scan.
    """
    if os.path.exists(EUROZONE_TICKER_LIST_FILE):
        try:
            with open(EUROZONE_TICKER_LIST_FILE) as f:
                data = json.load(f)
            age = datetime.datetime.now() - datetime.datetime.fromisoformat(data["fetched"])
            if age.days < TICKER_LIST_MAX_AGE_DAYS and len(data["tickers"]) >= MIN_SANE_EURONEXT_COUNT:
                print(f"  Euronext ticker list: {len(data['tickers'])} tickers from cache (age {age.days}d)")
                return data["tickers"]
        except Exception:
            pass

    print("  Downloading Euronext main-market ticker list…")
    try:
        req = urllib.request.Request(EURONEXT_CSV_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            text = resp.read().decode("utf-8", errors="replace")
        tickers = sorted(_parse_euronext_csv(text))
        print(f"    Euronext (Paris/Amsterdam/Brussels/Lisbon/Dublin main markets): {len(tickers)} tickers")
    except Exception as e:
        print(f"    Euronext download failed: {e}")
        tickers = []

    if len(tickers) < MIN_SANE_EURONEXT_COUNT:
        print(f"  ⚠ Euronext list looks broken (only {len(tickers)} tickers, expected 700+). "
              f"NOT caching this — will retry fresh next scan instead of getting stuck.")
        if os.path.exists(EUROZONE_TICKER_LIST_FILE):
            try:
                with open(EUROZONE_TICKER_LIST_FILE) as f:
                    old = json.load(f)
                if len(old["tickers"]) >= MIN_SANE_EURONEXT_COUNT:
                    print(f"  Falling back to previous cached list ({len(old['tickers'])} tickers).")
                    return old["tickers"]
            except Exception:
                pass
        return tickers  # better than nothing, but won't be cached

    try:
        with open(EUROZONE_TICKER_LIST_FILE, "w") as f:
            json.dump({"fetched": datetime.datetime.now().isoformat(), "tickers": tickers}, f)
    except Exception as e:
        print(f"  Could not save ticker list: {e}")

    return tickers


def _fetch_ticker_list_eurozone():
    """
    Full scan pool = real discovery across the 5 Euronext-run countries,
    UNION the curated SEED_UNIVERSE_EUROZONE (which also covers Germany/
    Switzerland/Austria/Spain/Italy/Finland, and acts as the fallback for
    the Euronext countries too if that download ever fails).
    """
    euronext = set(_fetch_eurozone_euronext_tickers())
    combined = sorted(euronext | set(SEED_UNIVERSE_EUROZONE))
    print(f"  Ticker list: {len(combined)} total ({len(euronext)} from Euronext discovery, "
          f"{len(SEED_UNIVERSE_EUROZONE)} curated)")
    return combined


# ─── SEC VERIFICATION (free, official, no account needed) ────────────────
# SEC EDGAR publishes every public company's filings for free, no API key,
# no account. This lets the app show the actual primary-source disclosures
# (dividend announcements, annual/quarterly reports) behind any ticker's
# numbers — the same legal documents companies are required to file — so
# a suspicious yield can be checked against the real source, not just
# taken on faith from Yahoo Finance.
#
# SEC asks that all requests identify a real contact — replace the email
# below with your own before deploying, or SEC may rate-limit/block you.
SEC_USER_AGENT = "smith.reevah@gmail.com"  # ← replace with your real contact email

TICKER_CIK_FILE = os.path.join(BASE_DIR, "sec_ticker_cik.json")
TICKER_CIK_MAX_AGE_DAYS = 30

def load_ticker_cik_map():
    """SEC publishes a free, complete ticker → CIK (company ID) mapping.
    Cached locally since it rarely changes and is a few MB."""
    if os.path.exists(TICKER_CIK_FILE):
        try:
            with open(TICKER_CIK_FILE) as f:
                data = json.load(f)
            age = datetime.datetime.now() - datetime.datetime.fromisoformat(data["fetched"])
            if age.days < TICKER_CIK_MAX_AGE_DAYS:
                return data["map"]
        except Exception:
            pass

    try:
        req = urllib.request.Request(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": SEC_USER_AGENT}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
        # raw is {"0": {"cik_str":..., "ticker":"AAPL", "title":"Apple Inc."}, "1": {...}, ...}
        mapping = {row["ticker"].upper(): {"cik": str(row["cik_str"]).zfill(10), "name": row["title"]}
                   for row in raw.values()}
        try:
            with open(TICKER_CIK_FILE, "w") as f:
                json.dump({"fetched": datetime.datetime.now().isoformat(), "map": mapping}, f)
        except Exception as e:
            print(f"  Could not save SEC ticker map: {e}")
        return mapping
    except Exception as e:
        print(f"  Could not fetch SEC ticker map: {e}")
        # fall back to a stale local copy rather than nothing, if one exists
        if os.path.exists(TICKER_CIK_FILE):
            try:
                with open(TICKER_CIK_FILE) as f:
                    return json.load(f)["map"]
            except Exception:
                pass
        return {}


# Filing types most likely to contain dividend/distribution info
RELEVANT_FORMS = {"8-K", "8-K/A", "10-K", "10-Q", "DEF 14A", "N-CSR", "N-CSRS", "SC TO-I"}

def fetch_sec_filings(ticker, limit=8):
    """
    Returns the company's recent relevant SEC filings — real, primary-source
    documents, not a re-summarized or scraped version. Each result links
    directly to the actual filing on sec.gov.
    """
    mapping = load_ticker_cik_map()
    entry = mapping.get(ticker.upper())
    if not entry:
        return None  # not a US-listed filer (e.g. some foreign ADRs, or ticker mismatch)

    cik = entry["cik"]
    try:
        req = urllib.request.Request(
            f"https://data.sec.gov/submissions/CIK{cik}.json",
            headers={"User-Agent": SEC_USER_AGENT}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        return {"ticker": ticker, "companyName": entry["name"], "cik": cik, "error": str(e), "filings": []}

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    filings = []
    for i in range(len(forms)):
        if forms[i] not in RELEVANT_FORMS:
            continue
        acc_nodash = accessions[i].replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodash}/{primary_docs[i]}"
        filings.append({"form": forms[i], "filingDate": dates[i], "url": url})
        if len(filings) >= limit:
            break

    return {
        "ticker": ticker,
        "companyName": entry["name"],
        "cik": cik,
        "secFilingsIndexUrl": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=&dateb=&owner=include&count=40",
        "filings": filings,
    }


# ─── UK FILING / DISCLOSURE LINKS (free, official, no account needed) ─────
# SEC EDGAR only covers US filers, so it can't verify UK-listed companies.
# There's no single free UK equivalent with an open bulk API the way SEC has
# one, so instead of fetching and re-hosting filing data, this builds direct
# links to the real primary sources so a suspicious yield/number can still
# be checked against the actual filings — just one click away:
#   • Investegate — free UK Regulatory News Service (RNS) archive.
#   • The company's own LSE instrument page — official price/company info.
#   • Companies House — the UK's free public register of company filings,
#     searched by company name since there's no reliable ticker → company-
#     number mapping.
# No network call is needed to build these — they're just URL templates —
# so this never fails or times out the way a live API fetch could.

def fetch_uk_filing_links(ticker):
    """
    Returns direct links to real UK primary-source disclosures for a ticker.
    `ticker` may be given with or without the ".L" suffix.
    """
    bare = ticker.upper().removesuffix(".L")
    if not bare:
        return None

    cache = load_cache("uk")
    entry = cache.get(ticker.upper()) or cache.get(f"{bare}.L")
    company_name = entry["name"] if entry else bare

    import urllib.parse
    name_q = urllib.parse.quote(company_name)

    links = [
        {"label": "RNS announcements (Investegate)",
         "url": f"https://www.investegate.co.uk/company/{bare}"},
        {"label": "Company page (London Stock Exchange)",
         "url": f"https://www.londonstockexchange.com/stock/{bare}/company-page"},
        {"label": "Search Companies House",
         "url": f"https://find-and-update.company-information.service.gov.uk/search/companies?q={name_q}"},
    ]
    return {"ticker": bare, "companyName": company_name, "links": links}


# ─── CROSS-SOURCE DIVIDEND VERIFICATION ─────────────────────────────────────
# yfinance is the primary source (free, covers 99% of US tickers) but its
# dividend data is occasionally wrong — phantom distributions, miscategorized
# special payouts, missing `dividendRate` for funds/ADRs. When that happens
# we cross-check with Twelvedata (cleaner data for US stocks) and Marketstack
# (good historical coverage), and use the median of all available sources as
# the truth. yfinance gets one vote; the paid sources each get one vote; the
# value closest to the median wins, which naturally filters out the bad one
# when only one of the three is wrong.
#
# Rate-limit awareness: free tiers are 800/day (Twelvedata) and 1000/month
# (Marketstack). The verification only fires inside fetch_one when yfinance's
# own data looks suspect (missing dividendRate, or TTM/FWD disagree >50%),
# so we never waste API budget on the ~95% of tickers yfinance already gets
# right.

def fetch_twelvedata_dividend(symbol):
    """
    Returns (annual_dividend_per_share, None) on success or (None, error_msg).
    Twelvedata's /quote endpoint returns a `dividend` field which is the
    annual rate as reported by the source — exactly what we need to
    cross-check yfinance's TTM. Free tier: 800 calls/day, 8/min.
    """
    if not TWELVEDATA_API_KEY:
        return None, "no api key"
    try:
        url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TWELVEDATA_API_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if isinstance(data, dict) and "dividend" in data and data["dividend"] is not None:
            try:
                val = float(data["dividend"])
                return (val if val > 0 else None), None
            except (TypeError, ValueError):
                return None, "dividend not numeric"
        if isinstance(data, dict) and data.get("status") == "error":
            return None, f"twelvedata error: {data.get('message','unknown')[:60]}"
        return None, "no dividend field"
    except Exception as e:
        return None, f"twelvedata: {str(e)[:60]}"

def fetch_marketstack_dividend(symbol):
    """
    Returns (annual_dividend_per_share, None) on success or (None, error_msg).
    Marketstack's /dividends endpoint returns historical payments; we sum
    the last 365 days to get TTM. Free tier: 1000 calls/month.
    """
    if not MARKETSTACK_API_KEY:
        return None, "no api key"
    try:
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
        url = f"https://api.marketstack.com/v1/dividends?symbols={symbol}&date_from={cutoff}&access_key={MARKETSTACK_API_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if isinstance(data, dict) and "data" in data and data["data"]:
            total = 0.0
            for d in data["data"]:
                amt = d.get("dividend")
                if amt is not None:
                    try: total += float(amt)
                    except (TypeError, ValueError): pass
            return (total if total > 0 else None), None
        if isinstance(data, dict) and "error" in data:
            return None, f"marketstack error: {data['error'].get('message','unknown')[:60]}"
        return None, "no data"
    except Exception as e:
        return None, f"marketstack: {str(e)[:60]}"

# ─── FINNHUB DIVIDEND PULL ───────────────────────────────────────────────
# Finnhub's free tier is generous: 60 calls/minute, no daily cap. The
# /stock/dividend endpoint returns historical dividend payments with
# payDate, amount, and currency. We sum the last 365 days to get TTM.
# Quality is good for US stocks; foreign coverage is uneven.
#
# Sign up at https://finnhub.io (free, just an email) → API token →
# FINNHUB_API_KEY=xxxxxxxx in your .env.

def fetch_finnhub_dividend(symbol):
    """
    Returns (annual_dividend_per_share, None) on success or (None, error_msg).
    Free tier: 60 calls/minute, no daily limit. Great for the bulk suspect
    cross-check where Twelvedata's 800/day would run out fast.
    """
    if not FINNHUB_API_KEY:
        return None, "no api key"
    try:
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
        today  = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        url = f"https://finnhub.io/api/v1/stock/dividend?symbol={symbol}&from={cutoff}&to={today}&token={FINNHUB_API_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if isinstance(data, dict) and "error" in data:
            return None, f"finnhub: {data.get('error','unknown')[:50]}"
        if isinstance(data, list) and data:
            total = 0.0
            for d in data:
                amt = d.get("amount")
                if amt is not None:
                    try: total += float(amt)
                    except (TypeError, ValueError): pass
            return (round(total, 4) if total > 0 else None), None
        return None, "no data"
    except Exception as e:
        return None, f"finnhub: {str(e)[:60]}"

# ─── SEC EDGAR DIVIDEND PULL (the legal primary source) ──────────────────
# SEC EDGAR's XBRL "companyconcept" API returns structured per-share
# dividend data — the same values companies are legally required to
# disclose in their 10-Q/10-K filings. Free, no API key, no rate-limit
# beyond SEC's 10 req/sec fair use. For any US-listed ticker, this is
# the gold standard: it tells us exactly what the board declared per
# share for every period in the company's history. We sum the last 365
# days of declarations to get TTM.
#
# Two tags cover virtually every US issuer:
#   us-gaap:CommonStockDividendsPerShareDeclared — most companies (cash + stock)
#   us-gaap:CommonStockDividendsPerShareCashPaid  — some prefer this
# We try declared first (more inclusive — captures stock divs too).

def _sec_xbrl_dividend(cik, tag):
    """Helper: hit the XBRL companyconcept endpoint for one tag, return
    (annual_div_per_share, None) on success or (None, error_msg).

    IMPORTANT — dedup overlapping periods: SEC XBRL facts for a dividend tag
    routinely include BOTH a single quarter's declaration AND a cumulative
    year-to-date (half-year, nine-month, full-year) figure for the SAME
    underlying payments, each as its own entry with its own 'end' date.
    Blindly summing every entry whose 'end' falls in the trailing 365 days
    double- or triple-counts the same dividend. Confirmed live for KREF: its
    Q2 2026 10-Q reports a $0.10 quarter-only entry (period 2026-04-01 to
    2026-06-30) AND a $0.35 year-to-date entry (period 2026-01-01 to
    2026-06-30, i.e. Q1+Q2 combined) — both with 'end'=2026-06-30 — on top
    of the standalone $0.25 Q1 entry. Summing all three counted Q1's $0.25
    twice and inflated the trailing total from the real $0.85 (four actual
    quarterly payments) to $3.45, which then got adopted as KREF's yield:
    45.9% instead of the correct ~5%.

    Fix: only sum entries whose period length looks like a single quarter
    (roughly 80-100 days), and de-duplicate identical (start, end) periods
    that appear more than once (e.g. restated in a later filing). If a
    company's filings never break dividends out quarterly (only annual
    figures exist), fall back to a single ~365-day entry instead of
    guessing from fragments of unknown, possibly-overlapping length.
    """
    try:
        url = f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/{tag.replace(':','/')}.json"
        req = urllib.request.Request(url, headers={"User-Agent": SEC_USER_AGENT})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if not isinstance(data, dict) or "units" not in data:
            return None, "no units"
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y-%m-%d")

        def _span_days(start, end):
            try:
                d1 = datetime.date.fromisoformat(start)
                d2 = datetime.date.fromisoformat(end)
                return (d2 - d1).days
            except Exception:
                return None

        # `end` is more reliable than `start` for "this declaration is for
        # this period" — a Q3 declaration has end = end of Q3, which is
        # always within 12 months of today.
        for unit_name, entries in data["units"].items():
            if not isinstance(entries, list): continue
            recent = [e for e in entries
                      if e.get("end","") >= cutoff and e.get("end","") != "" and e.get("start","") != ""]
            if not recent:
                continue

            # De-dup identical (start, end) periods first — a restated
            # filing can report the exact same period twice. Keep the max
            # of any conflicting values for a given period (doesn't matter
            # much which; what matters is counting each period only once).
            by_period = {}
            for e in recent:
                key = (e.get("start"), e.get("end"))
                try:
                    val = float(e.get("val", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if key not in by_period or val > by_period[key]:
                    by_period[key] = val

            quarterly = {k: v for k, v in by_period.items()
                         if (lambda s: s is not None and 75 <= s <= 100)(_span_days(*k))}
            if quarterly:
                total = sum(quarterly.values())
                if total > 0:
                    return round(total, 4), None

            # No quarter-length facts available (some filers only ever
            # report annual dividend totals) — fall back to a single
            # ~365-day period rather than summing periods of unknown,
            # possibly-overlapping length.
            annual = {k: v for k, v in by_period.items()
                      if (lambda s: s is not None and 350 <= s <= 380)(_span_days(*k))}
            if annual:
                total = max(annual.values())  # one period, not a sum
                if total > 0:
                    return round(total, 4), None
        return None, "no clean quarterly or annual period found"
    except urllib.error.HTTPError as e:
        return None, f"http {e.code}"
    except Exception as e:
        return None, str(e)[:60]

def fetch_sec_dividend(symbol):
    """
    Pulls trailing-12-month dividend per share from SEC EDGAR.
    Returns (annual_div_per_share, None) on success or (None, error_msg).
    Works for any US-listed company that files XBRL (covers ~95%+ of
    US stocks; ETFs/CFs that don't file standard XBRL will return None
    and we fall back to the other sources).
    """
    try:
        cik_map = load_ticker_cik_map()
        entry = cik_map.get(symbol.upper())
        if not entry: return None, "no CIK mapping (ticker not in SEC)"
        cik = entry["cik"]
        # Try the most common tags. First success wins.
        for tag in (
            "us-gaap:CommonStockDividendsPerShareDeclared",
            "us-gaap:CommonStockDividendsPerShareCashPaid",
        ):
            val, err = _sec_xbrl_dividend(cik, tag)
            if val is not None and val > 0:
                return val, None
        return None, "no SEC XBRL data for this ticker"
    except Exception as e:
        return None, f"sec: {str(e)[:60]}"


def verify_dividend(symbol, price, yf_annual_div):
    """
    Cross-checks a ticker's annual dividend with SEC EDGAR + Twelvedata +
    Marketstack. SEC is treated as the authoritative primary source when
    available (it's the legal disclosure); the other sources serve as
    corroboration or fallback. Returns (best_annual_div, source_label,
    sources_dict) where:
      - best_annual_div: the value to use as truth (per-share, annualized)
      - source_label:    which source provided it ("sec", "twelvedata",
                         "marketstack", "yfinance", "yfinance_only",
                         "sec_unavailable", or "no_data")
      - sources_dict:    every value we got, for UI transparency
                         {"sec": X, "yfinance": Y, "twelvedata": Z, "marketstack": W}

    Picking rule: when 2+ sources are available, the value closest to the
    median wins. yfinance gets a tiny tie-break penalty because in the
    "wrong" case (which is the whole reason this function exists), it's
    the bad one. When only yfinance returned data, we use that with no
    cross-check possible.
    """
    sources = {}
    if yf_annual_div and yf_annual_div > 0:
        sources["yfinance"] = float(yf_annual_div)

    # SEC EDGAR first — it's the authoritative primary source. If it
    # returns a value, we keep it as the leading vote but still consult
    # the other sources for the median pick.
    sec, _sec_err = fetch_sec_dividend(symbol)
    if sec and sec > 0:
        sources["sec"] = float(sec)

    td, _td_err = fetch_twelvedata_dividend(symbol)
    if td and td > 0:
        sources["twelvedata"] = float(td)
    ms, _ms_err = fetch_marketstack_dividend(symbol)
    if ms and ms > 0:
        sources["marketstack"] = float(ms)
    fh, _fh_err = fetch_finnhub_dividend(symbol)
    if fh and fh > 0:
        sources["finnhub"] = float(fh)

    if not sources:
        return 0.0, "no_data", sources
    if len(sources) == 1:
        only_name = next(iter(sources))
        return sources[only_name], only_name, sources

    # SEC's per-share figure is keyed by CIK (the ISSUER), not by the
    # specific security — a company with several preferred stock series
    # (or other share classes) under one CIK all resolve to the SAME CIK,
    # and the XBRL tag we pull is specifically the COMMON stock dividend.
    # For a preferred/other-class ticker that pulls in a completely
    # unrelated number. Confirmed live: AFGB/AFGC/AFGD/AFGE — four
    # different American Financial Group preferred series — all mapped to
    # AFG's one CIK and all returned the IDENTICAL $19.00 SEC figure,
    # which is AFG's common dividend and has nothing to do with any of
    # those preferred shares' actual, much smaller, fixed payouts. Same
    # pattern hit CMSA/CMSC/CMSD (CMS Energy), DUKB (Duke Energy), BPOPM
    # (Popular), KMPB (Kemper), AMJB (JPMorgan), AIZN (Assurant), and more.
    #
    # Guard: the other sources (twelvedata/marketstack/finnhub/yfinance)
    # are keyed by the actual ticker symbol, so they don't share this
    # failure mode. If SEC's number is wildly out of line (>2x) with
    # EVERY OTHER available source, treat it as unreliable for THIS
    # ticker and drop it from the pick — one CIK-collision-prone source
    # shouldn't get to unilaterally outvote every symbol-keyed source.
    # It's still returned in `sources` for transparency; it just doesn't
    # get to win the pick on its own.
    pick_pool = sources
    if "sec" in sources and len(sources) >= 2:
        others = {k: v for k, v in sources.items() if k != "sec"}
        sec_val = sources["sec"]
        if others and all(
            (max(sec_val, v) / max(min(sec_val, v), 0.01)) > 2.0
            for v in others.values()
        ):
            pick_pool = others
            if len(pick_pool) == 1:
                only_name = next(iter(pick_pool))
                return pick_pool[only_name], only_name, sources

    # 2+ sources: pick the value closest to the median.
    # NOTE: values[len(values)//2] is NOT a true median for an even-length
    # list — for exactly 2 sources it just picks the LARGER of the two
    # outright (index 1), which meant two disagreeing sources always had
    # the bigger number "win" regardless of which one was actually right.
    # Use a real median (average the two middle values when the count is
    # even) so a 2-source disagreement doesn't structurally favor whichever
    # value happens to be larger.
    values = sorted(pick_pool.values())
    n = len(values)
    median = values[n // 2] if n % 2 == 1 else (values[n // 2 - 1] + values[n // 2]) / 2.0
    best_name = None
    best_val  = None
    best_dist = float("inf")
    for name, val in pick_pool.items():
        dist = abs(val - median) / max(median, 0.01)
        if name == "yfinance": dist += 0.001  # tiny tie-break penalty
        if dist < best_dist:
            best_dist = dist
            best_name = name
            best_val  = val
    return best_val, best_name, sources


def run_screener(market="us"):
    """
    Get the full ticker list for this market, return the ones not already in
    cache AND not recently checked — this is the full pool of genuinely
    fresh territory to draw candidates from. No capping here; capping
    happens in background_scan once we know how many actually pass the
    yield filter.
    """
    cache = load_cache(market)
    checked = load_checked(market)
    all_tickers = fetch_ticker_list(market)

    new_tickers = [t for t in all_tickers if t not in cache and not is_checked_recently(t, checked)]
    skipped_known = len(all_tickers) - len(cache) - len(new_tickers)
    print(f"  [{market}] {len(all_tickers)} total tickers — {len(cache)} already cached — "
          f"{skipped_known} recently checked (skipping) — {len(new_tickers)} fresh to try")

    return new_tickers


def background_scan(min_yield, max_results, result_holder, market="us"):
    """
    Runs screener + fetch cycle in background thread.
    max_results = how many NEW qualifying tickers (yield >= min_yield) to add
    this session — NOT how many to attempt. Keeps trying untried tickers from
    the pool until either that many qualify, or the pool runs out.

    IMPORTANT: any real dividend payer found along the way gets saved to the
    cache regardless of whether it clears min_yield — the app already filters
    by yield client-side via presets, so there's no reason to throw away good
    data. And every ticker checked (payer or not) gets recorded in the
    checked-ticker ledger, so future scans never waste time re-confirming
    the same low-yield tickers — they'll skip straight to fresh territory.

    Progress is written to result_holder as it goes so /scan/status can show
    live "X added, Y tried" counts instead of going silent for a long scan.
    """
    print(f"\n🔍 [{market}] Screener starting (min_yield={min_yield}%, target={max_results} qualifying new tickers)…")
    pool = run_screener(market)
    print(f"  Pool of {len(pool)} untried tickers to draw from")

    checked = load_checked(market)
    added   = []
    tried   = 0
    final_total = len(load_cache(market))
    consecutive_rate_limits = 0
    RATE_LIMIT_STOP_THRESHOLD = 8  # this many in a row = Yahoo has blocked us, stop wasting time

    for sym in pool:
        if len(added) >= max_results:
            break
        tried += 1
        result_holder["tried"] = tried
        print(f"  [{market}] [{tried:>4} tried, {len(added):>3}/{max_results} added] {sym:<6}", end=" ", flush=True)
        try:
            d = fetch_one(sym, market=market)
            consecutive_rate_limits = 0  # any non-exception result means we're not currently blocked
            checked[sym] = {"yield": (d["yield"] if d else 0), "checked_at": datetime.datetime.now().isoformat()}
            if d:
                # Reload fresh from disk right before saving, and merge in only
                # this one ticker — NEVER hold one snapshot for the whole scan
                # and blindly re-save it, or a concurrent upload/other scan/
                # refresh happening at the same time gets silently wiped out.
                current = load_cache(market)
                current[sym] = d  # save every real payer, regardless of min_yield — nothing goes to waste
                save_cache(current, market)
                final_total = len(current)
                if d["yield"] >= min_yield:
                    added.append({k: v for k, v in d.items() if k != "_fetched_at"})
                    result_holder["added"] = list(added)  # live progress for polling
                    print(f"✓ {d['yield']}%")
                else:
                    print(f"— saved, but below {min_yield}% ({d['yield']}%)")
            else:
                print("— skipped (no dividend)")
        except RateLimitDetected:
            consecutive_rate_limits += 1
            print(f"— rate limited ({consecutive_rate_limits}/{RATE_LIMIT_STOP_THRESHOLD} in a row)")
            if consecutive_rate_limits >= RATE_LIMIT_STOP_THRESHOLD:
                print(f"\n🛑 Yahoo Finance is rate-limiting this server — stopping early instead of "
                      f"wasting time on {len(pool)-tried} more doomed requests.")
                result_holder["rateLimited"] = True
                break
        except Exception as e:
            # one bad ticker (or a transient disk/network hiccup) should never
            # take down the whole scan — log it and keep going
            consecutive_rate_limits = 0
            print(f"— error: {e}")
        if tried % 20 == 0:
            merged_checked = load_checked(market)
            merged_checked.update(checked)
            save_checked(merged_checked, market)  # merge, don't overwrite — a concurrent local_scan.py run may have added its own entries
        time.sleep(1.0 + random.random()*0.5)  # be gentle with Yahoo — shared IPs get blocked faster

    final_checked = load_checked(market)
    final_checked.update(checked)
    save_checked(final_checked, market)
    result_holder["done"]  = True
    result_holder["added"] = added
    result_holder["tried"] = tried
    result_holder["exhausted"] = len(added) < max_results  # ran out of pool before hitting target
    result_holder["total"] = final_total
    print(f"\n✅ [{market}] Screener done — {len(added)} tickers met your {min_yield}% bar. "
          f"Cache now has {final_total} tickers total, {len(final_checked)} ever checked.\n")

# scan job state — one PER MARKET, so US and UK can each run their own scan
# at the same time without interfering with each other.
def _fresh_scan_job():
    return {"running": False, "done": False, "added": [], "total": 0, "tried": 0, "exhausted": False, "started": None}
_scan_jobs  = {"us": _fresh_scan_job(), "uk": _fresh_scan_job(), "apac": _fresh_scan_job(), "eurozone": _fresh_scan_job()}
_scan_locks = {"us": threading.Lock(), "uk": threading.Lock(), "apac": threading.Lock(), "eurozone": threading.Lock()}

# ─── BACKGROUND REFRESH ───────────────────────────────────────────────────────
# Also one per market — refreshing stale US entries never blocks or shares
# state with refreshing stale UK entries.

_refresh_running = {"us": False, "uk": False, "apac": False, "eurozone": False}
_refresh_locks    = {"us": threading.Lock(), "uk": threading.Lock(), "apac": threading.Lock(), "eurozone": threading.Lock()}
# Progress state for the /api/<market>/refresh/stale endpoint. Updated live
# as the refresh runs so the admin UI can show "X of Y refreshed, ETA Z min."
def _fresh_refresh_progress():
    return {
        "running": False,
        "paused":  False,    # user clicked Pause — finish current ticker then stop
        "total":   0,        # how many stale entries when we started
        "done":    0,        # how many refreshed so far
        "failed":  0,        # how many errored / rate-limited
        "started": None,     # ISO timestamp
        "finished": None,    # ISO timestamp
        "lastError": None,   # last non-rate-limit error message
        "rateLimited": False, # set True if Yahoo blocked us mid-pass
    }
_refresh_progress = {"us": _fresh_refresh_progress(), "uk": _fresh_refresh_progress(), "apac": _fresh_refresh_progress(), "eurozone": _fresh_refresh_progress()}

# get_all() (GET /api/<market>/stocks) spawns a background_refresh() attempt
# on every request while that market's cache is non-empty — every page
# load, every poll. Each call to background_refresh() used to recompute
# "total"/"done" from scratch the moment it actually got to run (i.e.
# whenever the PREVIOUS pass had already finished or broken off, which
# happens constantly under Yahoo rate-limiting) — so the progress bar
# visibly restarted at 0 on every reload instead of showing continuous
# progress across a session. These track a multi-pass "campaign" (per
# market) so the counters accumulate across passes instead of resetting
# each time, and only start a genuinely new campaign after a real idle gap
# (finished with nothing stale left, or nothing has happened in a while)
# rather than on every single incoming request.
def load_refresh_campaign(market="us"):
    """Reads the persisted refresh-progress-bar state for this market from
    disk, so a restart of the server (closing the terminal, a crash, a
    computer restart) doesn't make the "X of Y refreshed" bar visibly jump
    back to 0 — it picks up counting from wherever it actually left off.
    Falls back to a fresh/empty campaign if the file is missing or corrupt
    (this is just UI progress state, never the ticker data itself, so
    there's nothing risky about starting over here on a bad read)."""
    path = MARKET_FILES[market]["refresh_progress"]
    if os.path.exists(path):
        try:
            with open(path) as f:
                d = json.load(f)
            started = datetime.datetime.fromisoformat(d["started"]) if d.get("started") else None
            return {"total": int(d.get("total", 0)), "done": int(d.get("done", 0)), "started": started}
        except Exception as e:
            print(f"⚠ [{market}] Could not read persisted refresh progress: {e}")
    return {"total": 0, "done": 0, "started": None}

def save_refresh_campaign(campaign, market="us"):
    path = MARKET_FILES[market]["refresh_progress"]
    try:
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({
                "total":   campaign["total"],
                "done":    campaign["done"],
                "started": campaign["started"].isoformat() if campaign["started"] else None,
            }, f)
        os.replace(tmp, path)
    except Exception as e:
        print(f"  [{market}] Could not save refresh progress: {e}")

_refresh_campaign = {
    "us": load_refresh_campaign("us"),
    "uk": load_refresh_campaign("uk"),
    "apac": load_refresh_campaign("apac"),
    "eurozone": load_refresh_campaign("eurozone"),
}
# Reflect whatever we just loaded from disk in the status endpoint's
# response immediately, so a page load right after restarting the server
# shows the real "X of Y so far" instead of a misleading 0/0 until the
# next refresh pass happens to kick off and repopulate it.
for _m in ("us", "uk", "apac", "eurozone"):
    _c = _refresh_campaign[_m]
    if _c["started"] is not None:
        _refresh_progress[_m]["total"] = _c["total"]
        _refresh_progress[_m]["done"] = _c["done"]
        _refresh_progress[_m]["started"] = _c["started"].isoformat()
del _m, _c
_REFRESH_CAMPAIGN_IDLE_RESET_MINUTES = 30
# Also don't even spawn a new attempt-thread more than once every few
# seconds — multiple browser tabs / rapid reloads shouldn't each fire off
# their own thread hammering Yahoo with the exact same stale list.
_last_refresh_trigger = {"us": None, "uk": None, "apac": None, "eurozone": None}
_REFRESH_TRIGGER_COOLDOWN_SECONDS = 15

def background_refresh(market="us"):
    """
    Refreshes stale (12+ hour old) cache entries in the background, for the
    given market only.

    IMPORTANT: this must NEVER hold a single in-memory snapshot of the whole
    cache across its run and blindly re-save it — if it did, any upload or
    scan that changes the file WHILE this loop is still running would get
    silently overwritten and reverted the next time this loop saves. Instead,
    every save reloads the CURRENT file from disk fresh and merges in only
    the one ticker just refreshed, so it can never stomp on someone else's
    more recent changes (a manual file upload, a concurrent scan, etc).
    """
    with _refresh_locks[market]:
        if _refresh_running[market]:
            return  # a refresh is already in progress for this market — don't stack another one
        _refresh_running[market] = True
    campaign = _refresh_campaign[market]
    progress = _refresh_progress[market]
    try:
        cache = load_cache(market)
        stale = [sym for sym, entry in cache.items() if is_stale(entry)]
        if not stale:
            print(f"✅ [{market}] All {len(cache)} entries are fresh.")
            progress.update({"running": False, "paused": False, "total": 0, "done": 0})
            campaign["total"] = 0
            campaign["done"] = 0
            campaign["started"] = None
            save_refresh_campaign(campaign, market)
            return

        now = datetime.datetime.now()
        campaign_stale = (
            campaign["started"] is None or
            campaign["done"] >= campaign["total"] or
            (now - campaign["started"]).total_seconds() > _REFRESH_CAMPAIGN_IDLE_RESET_MINUTES * 60
        )
        if campaign_stale:
            # Starting fresh (first run ever, previous campaign fully
            # finished, or it's been idle long enough that this counts as
            # a new session rather than a continuation).
            campaign["total"] = len(stale)
            campaign["done"] = 0
            campaign["started"] = now
        else:
            # Continuing an in-progress campaign — never shrink the total the
            # user has already seen; only grow it if more entries went stale
            # since the campaign started.
            campaign["total"] = max(campaign["total"], len(stale) + campaign["done"])
        save_refresh_campaign(campaign, market)

        print(f"\n🔄 [{market}] Background refresh: {len(stale)} stale entries "
              f"({campaign['done']}/{campaign['total']} done so far this campaign)…")
        progress.update({
            "running":     True,
            "paused":      False,
            "total":       campaign["total"],
            "done":        campaign["done"],
            "failed":      0,
            "started":     campaign["started"].isoformat(),
            "finished":    None,
            "lastError":   None,
            "rateLimited": False,
        })
        updated = 0
        for sym in stale:
            # Check pause flag before each ticker. We let the CURRENT
            # ticker finish (avoid cutting a fetch_one off mid-flight
            # and leaving partial data on disk), but stop before the
            # next one. Resume picks up at the next stale ticker.
            if progress.get("paused"):
                print(f"\n⏸ [{market}] Paused by user ({updated}/{len(stale)} refreshed). "
                      f"Click Resume to continue.")
                progress.update({
                    "running":  False,
                    "finished": datetime.datetime.now().isoformat(),
                })
                break
            print(f"  [{market}] Refreshing {sym}...", end=" ", flush=True)
            try:
                fresh = fetch_one(sym, market=market)
            except RateLimitDetected as e:
                # Yahoo has blocked/rate-limited this IP. Bailing out of the
                # whole function here (uncaught) used to silently kill this
                # thread after the very first stale ticker — since get_all()
                # spawns a fresh background_refresh() thread on every
                # /api/stocks request, the same still-mostly-stale list would
                # just get reloaded and die on the same first ticker again,
                # forever, and the cache would never actually make progress.
                # Stop this pass cleanly instead so _refresh_running resets
                # normally and the NEXT trigger (or this one, after a cool-
                # down) can pick up further down the stale list.
                print(f"— rate limited. Stopping this refresh pass early "
                      f"({updated} updated so far, {len(stale)} were stale); "
                      f"will resume on the next trigger.")
                progress.update({
                    "rateLimited": True,
                    "finished":    datetime.datetime.now().isoformat(),
                })
                break
            except Exception as e:
                # One ticker failing shouldn't take down the whole pass.
                progress["failed"] += 1
                progress["lastError"] = f"{sym}: {e}"
                print(f"error: {e}")
                continue
            # reload fresh from disk right before saving, so we merge our one
            # update into whatever is CURRENTLY there — never overwrite the
            # whole file with a stale in-memory copy
            current = load_cache(market)
            if fresh:
                fresh["_miss_count"] = 0
                current[sym] = fresh
                updated += 1
                campaign["done"] += 1
                print("✓")
            elif sym in current:
                # fetch_one came back empty — could be a transient blip (rate
                # limit, momentary Yahoo gap) or could mean this ticker
                # genuinely has no current TTM dividend data. We can't tell
                # the difference from one failed attempt, so previously this
                # just re-stamped _fetched_at and kept showing the OLD
                # number forever, marked "fresh" — meaning a ticker whose
                # data quietly went bad (or whose dividends rolled out of
                # the window) could display a wrong, unverified number
                # indefinitely with no way for anyone to tell it was stale.
                # Give it a few more refresh cycles to recover; if it keeps
                # failing, drop it rather than keep showing an unverified
                # number as if it were current.
                misses = current[sym].get("_miss_count", 0) + 1
                if misses >= MISS_LIMIT_BEFORE_DROP:
                    del current[sym]
                    print(f"dropped — {misses} consecutive failed refreshes, "
                          f"no longer trustworthy")
                else:
                    current[sym]["_miss_count"] = misses
                    current[sym]["_fetched_at"] = datetime.datetime.now().isoformat()
                    print(f"kept (miss {misses}/{MISS_LIMIT_BEFORE_DROP})")
            save_cache(current, market)
            save_refresh_campaign(campaign, market)
            progress["done"] = campaign["done"]
            progress["total"] = campaign["total"]
        else:
            print(f"\n✅ [{market}] Refresh done — {updated} updated.\n")
        progress.update({
            "running":  False,
            "finished": datetime.datetime.now().isoformat(),
        })
    finally:
        with _refresh_locks[market]:
            _refresh_running[market] = False
            progress["running"] = False
            if not progress.get("finished"):
                progress["finished"] = datetime.datetime.now().isoformat()

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/api/<market>/stocks")
@require_login
def get_all(market):
    err = market_or_400(market)
    if err: return err
    cache = load_cache(market)
    seed = MARKET_META[market]["seed"]
    if not cache:
        print(f"\n🆕 [{market}] First run — fetching {len(seed)} seed tickers…\n")
        for i, sym in enumerate(seed):
            print(f"  [{market}] [{i+1:>3}/{len(seed)}] {sym:<6}", end=" ", flush=True)
            try:
                d = fetch_one(sym, market=market)
                if d:
                    cache[sym] = d
                    save_cache(cache, market)  # save after every ticker so interruptions don't lose progress
                    print(f"✓  {d['yield']}%")
                else:
                    print("— skipped")
            except RateLimitDetected:
                print(f"— rate limited. Stopping seed early ({len(cache)} tickers saved so far); "
                      f"it will pick up the rest next time the cache is non-empty and gets refreshed.")
                break
            except Exception as e:
                print(f"— error: {e}")
        print(f"\n✅ [{market}] Seed complete — {len(cache)} tickers cached.\n")
    else:
        print(f"\n⚡ [{market}] Serving {len(cache)} tickers from cache instantly.")
        now = datetime.datetime.now()
        # Don't spawn a fresh refresh-attempt thread on literally every page
        # load / poll — a burst of requests (multiple tabs, quick reloads)
        # would otherwise each fire off their own thread hitting the same
        # rate-limited stale list. background_refresh()'s own lock already
        # prevents two passes running concurrently; this cooldown just avoids
        # the wasted thread churn and Yahoo hits in between.
        if _refresh_running[market] or (
            _last_refresh_trigger[market] is not None and
            (now - _last_refresh_trigger[market]).total_seconds() < _REFRESH_TRIGGER_COOLDOWN_SECONDS
        ):
            pass
        else:
            _last_refresh_trigger[market] = now
            threading.Thread(target=background_refresh, args=(market,), daemon=True).start()

    clean = [{k:v for k,v in e.items() if k not in ("_fetched_at","_miss_count")} for e in cache.values()]
    return jsonify(clean)


@app.route("/api/<market>/verify/<ticker>")
@require_login
def verify_ticker(market, ticker):
    """
    US: pulls real SEC filings for a ticker so its numbers can be checked
    against the actual primary source — free, official, no account needed.
    UK: returns direct links to Investegate/LSE/Companies House instead,
    since SEC EDGAR only covers US filers.
    """
    err = market_or_400(market)
    if err: return err
    if market == "uk":
        result = fetch_uk_filing_links(ticker.upper())
        if result is None:
            return jsonify({
                "ticker": ticker.upper(),
                "found": False,
                "message": "Couldn't build filing links for this ticker.",
            })
        result["found"] = True
        return jsonify(result)
    result = fetch_sec_filings(ticker.upper())
    if result is None:
        return jsonify({
            "ticker": ticker.upper(),
            "found": False,
            "message": "No SEC filer found for this ticker (may be a foreign-listed security, an ETF that doesn't file the same way, or a ticker mismatch).",
        })
    result["found"] = True
    return jsonify(result)


@app.route("/api/combined/stocks")
@require_login
def get_combined():
    """
    Merges every cached ticker from BOTH markets into one list, tagged with
    which market it came from — powers the Combined tab. Read-only: scanning,
    adding, and admin actions still happen per-market via /api/<market>/...
    """
    rows = []
    for market in ("us", "uk", "apac", "eurozone"):
        cache = load_cache(market)
        for e in cache.values():
            clean = {k: v for k, v in e.items() if k not in ("_fetched_at", "_miss_count")}
            clean["market"] = market.upper()
            rows.append(clean)
    return jsonify(rows)


@app.route("/api/paywall-info")
def paywall_info():
    return jsonify({
        "priceNote": "Contact for current pricing",
        "btcAddress": BTC_ADDRESS,
        "usdcAddress": USDC_ADDRESS,
        "instructions": CONTACT_INSTRUCTIONS,
    })


@app.route("/api/admin/check", methods=["POST"])
def admin_check():
    """Verify an admin key without doing anything destructive — lets the
    frontend show/hide admin controls."""
    if not ADMIN_KEY:
        return jsonify({"ok": True, "locked": False})  # no key configured = no lock (local dev)
    body = request.get_json() or {}
    ok = body.get("key", "") == ADMIN_KEY
    return jsonify({"ok": ok, "locked": True})


@app.route("/api/admin/devices")
@require_admin
def admin_list_devices():
    """List every device that's hit the free-trial gate, most recently
    seen first — use this to find the device ID someone quoted you after
    paying, and to see who's mid-trial vs. already expired."""
    trials = load_device_trials()
    now = datetime.datetime.now()
    rows = []
    for device_id, entry in trials.items():
        days_elapsed = None
        try:
            first_seen = datetime.datetime.fromisoformat(entry.get("first_seen", ""))
            days_elapsed = (now - first_seen).days
        except Exception:
            pass
        paid = bool(entry.get("paid"))
        days_left = None
        if not paid and days_elapsed is not None:
            days_left = max(0, DEVICE_TRIAL_DAYS - days_elapsed)
        rows.append({
            "deviceId": device_id,
            "firstSeen": entry.get("first_seen"),
            "paid": paid,
            "daysElapsed": days_elapsed,
            "daysLeft": days_left,
        })
    rows.sort(key=lambda r: r.get("firstSeen") or "", reverse=True)
    return jsonify(rows)


@app.route("/api/admin/devices/mark-paid", methods=["POST"])
@require_admin
def admin_mark_device_paid():
    """Call after someone emails/messages you proof of a crypto payment
    along with the device ID shown on their paywall screen."""
    body = request.get_json() or {}
    device_id = (body.get("deviceId") or "").strip()
    if not device_id:
        return jsonify({"error": "deviceId required"}), 400
    with _device_trials_lock:
        trials = load_device_trials()
        entry = trials.setdefault(device_id, {"first_seen": datetime.datetime.now().isoformat()})
        entry["paid"] = True
        save_device_trials(trials)
    return jsonify({"ok": True})


@app.route("/api/admin/devices/mark-unpaid", methods=["POST"])
@require_admin
def admin_mark_device_unpaid():
    """Undo mark-paid, e.g. if it was applied to the wrong device ID."""
    body = request.get_json() or {}
    device_id = (body.get("deviceId") or "").strip()
    if not device_id:
        return jsonify({"error": "deviceId required"}), 400
    with _device_trials_lock:
        trials = load_device_trials()
        if device_id in trials:
            trials[device_id]["paid"] = False
            save_device_trials(trials)
    return jsonify({"ok": True})


# ─── LOGIN / LOGOUT ─────────────────────────────────────────────────────────

@app.route("/api/login", methods=["POST"])
def login():
    """Log in with a username/password an admin created for you. Returns a
    session token to send back as X-Session-Token on every future request."""
    if not USERS_FILE_ENABLED():
        return jsonify({"ok": True, "locked": False})  # no accounts configured = open access (local dev)
    body = request.get_json() or {}
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    users = load_users()
    user = users.get(username)
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"ok": False, "error": "Incorrect username or password"}), 401
    if account_expired(username):
        return jsonify({"ok": False, "error": "Your access has expired. Please renew.", "accessExpired": True}), 401
    token = secrets.token_urlsafe(32)
    expires = (datetime.datetime.now() + datetime.timedelta(hours=SESSION_HOURS)).isoformat()
    with _sessions_lock:
        sessions = load_sessions()
        sessions[token] = {"username": username, "expires": expires}
        save_sessions(sessions)
    return jsonify({
        "ok": True, "token": token, "username": username,
        "tier": user.get("tier", "free"),
        "mustChangePassword": bool(user.get("must_change_password", False)),
    })


@app.route("/api/logout", methods=["POST"])
def logout():
    token = request.headers.get("X-Session-Token", "")
    with _sessions_lock:
        sessions = load_sessions()
        sessions.pop(token, None)
        save_sessions(sessions)
    return jsonify({"ok": True})


@app.route("/api/session/check", methods=["POST"])
def session_check():
    """Check whether the app requires login at all, and whether a given
    token (if any) is still valid — lets the frontend decide what to show
    without triggering a 401 in the console on first load."""
    if not USERS_FILE_ENABLED():
        return jsonify({"locked": False, "valid": True})
    token = request.headers.get("X-Session-Token", "")
    with _sessions_lock:
        sessions = load_sessions()
        sess = sessions.get(token)
    valid = False
    username = None
    must_change = False
    access_expired = False
    if sess:
        try:
            valid = datetime.datetime.now() <= datetime.datetime.fromisoformat(sess["expires"])
            if valid:
                username = sess["username"]
                if account_expired(username):
                    valid = False
                    access_expired = True
                else:
                    users = load_users()
                    must_change = bool(users.get(username, {}).get("must_change_password", False))
        except Exception:
            valid = False
    return jsonify({"locked": True, "valid": valid, "username": username, "mustChangePassword": must_change, "accessExpired": access_expired})


@app.route("/api/change-password", methods=["POST"])
@require_login
def change_password():
    """Let a logged-in user set their own password — used both for the
    forced first-login change and as a normal 'change my password' option.
    Requires their current password as confirmation."""
    body = request.get_json() or {}
    current  = body.get("currentPassword") or ""
    new_pw   = body.get("newPassword") or ""
    if len(new_pw) < 6:
        return jsonify({"error": "New password must be at least 6 characters"}), 400
    users = load_users()
    user = users.get(request.username)
    if not user:
        return jsonify({"error": "User not found"}), 404
    if not check_password_hash(user["password_hash"], current):
        return jsonify({"error": "Current password is incorrect"}), 401
    user["password_hash"] = generate_password_hash(new_pw)
    user["must_change_password"] = False
    save_users(users)
    return jsonify({"ok": True})


def USERS_FILE_ENABLED():
    """Login is only enforced once at least one user account exists — so
    setting this up doesn't lock you out of your own local dev instance
    before you've created any accounts."""
    return len(load_users()) > 0


# Account creation/removal is intentionally NOT exposed here as an HTTP
# endpoint — see manage_users.py. That keeps "who can create a login" off
# the public internet entirely: there's no URL an attacker could hit even
# if they somehow obtained your admin key. Only someone with direct access
# to your PythonAnywhere console (i.e. you) can create or remove accounts.


@app.route("/api/<market>/scan/start", methods=["POST"])
@require_admin
def scan_start(market):
    """Start a screener scan in the background for this market only — the
    other market's scan (if any) is completely unaffected."""
    err = market_or_400(market)
    if err: return err
    with _scan_locks[market]:
        if _scan_jobs[market]["running"]:
            return jsonify({"status":"already_running","message":"A scan is already in progress"}), 409
        body       = request.get_json() or {}
        min_yield  = float(body.get("minYield",  1.5))
        max_results= int(body.get("maxResults", 500))
        _scan_jobs[market] = {"running":True,"done":False,"added":[],"total":0,"tried":0,"exhausted":False,
                       "target":max_results,"started":datetime.datetime.now().isoformat()}
        threading.Thread(target=_run_scan_job, args=(min_yield, max_results, market), daemon=True).start()
    return jsonify({"status":"started","minYield":min_yield,"maxResults":max_results})

def _run_scan_job(min_yield, max_results, market):
    job = _scan_jobs[market]
    try:
        background_scan(min_yield, max_results, job, market=market)
    except Exception as e:
        import traceback
        print(f"\n❌ [{market}] Scan crashed: {e}")
        traceback.print_exc()
        job["error"] = str(e)
    finally:
        # ALWAYS release the lock, even on failure — otherwise every future
        # scan gets silently rejected forever, which is what caused scans to
        # appear to "do nothing" after the first one.
        with _scan_locks[market]:
            job["running"] = False
            job["done"] = True


@app.route("/api/<market>/scan/status")
def scan_status(market):
    """Poll this to check if this market's scan is done."""
    err = market_or_400(market)
    if err: return err
    job = _scan_jobs[market]
    return jsonify({
        "running":     job["running"],
        "done":        job["done"],
        "added":       len(job.get("added",[])),
        "tried":       job.get("tried", 0),
        "target":      job.get("target", 0),
        "exhausted":   job.get("exhausted", False),
        "rateLimited": job.get("rateLimited", False),
        "error":       job.get("error"),
        "total":       job.get("total", 0),
        "started":     job.get("started"),
    })


@app.route("/api/<market>/scan/reset", methods=["POST"])
@require_admin
def scan_reset(market):
    """Safety net: force-clear a stuck scan job if 'running' ever gets stuck true."""
    err = market_or_400(market)
    if err: return err
    with _scan_locks[market]:
        _scan_jobs[market] = {"running": False, "done": False, "added": [], "total": 0,
                     "tried": 0, "exhausted": False, "started": None}
    return jsonify({"status": "reset"})


@app.route("/api/<market>/scan/results")
def scan_results(market):
    """Get newly added stocks from this market's last scan."""
    err = market_or_400(market)
    if err: return err
    job = _scan_jobs[market]
    return jsonify({
        "done":    job["done"],
        "added":   job.get("added",[]),
        "total":   job.get("total", 0),
    })


@app.route("/api/<market>/add", methods=["POST"])
@require_admin
def add_tickers(market):
    err = market_or_400(market)
    if err: return err
    body    = request.get_json() or {}
    raw_symbols = [s.strip().upper() for s in (body.get("tickers") or []) if s.strip()]
    if not raw_symbols:
        return jsonify({"error": "No tickers provided"}), 400
    suffix = MARKET_META[market]["suffix"]
    if suffix:
        # UK convenience: if someone types "VOD" instead of "VOD.L", assume
        # they mean the LSE listing and append the suffix automatically —
        # but leave anything that already has a dot (".L", ".TO", ".PA",
        # etc, for a deliberately non-UK ticker) exactly as typed.
        symbols = [s if "." in s else f"{s}{suffix}" for s in raw_symbols]
    else:
        symbols = raw_symbols
    cache  = load_cache(market)
    added, skipped, already = [], [], []
    for sym in symbols:
        if sym in cache:
            already.append(sym)
            continue
        d = fetch_one(sym, market=market)
        if d:
            cache[sym] = d
            added.append(d)
        else:
            skipped.append(sym)
    if added: save_cache(cache, market)
    return jsonify({
        "added":   [{k:v for k,v in e.items() if k not in ("_fetched_at","_miss_count")} for e in added],
        "skipped": skipped,
        "already": already,
        "total_in_cache": len(cache),
    })


@app.route("/api/<market>/remove", methods=["POST"])
@require_admin
def remove_ticker(market):
    err = market_or_400(market)
    if err: return err
    body   = request.get_json() or {}
    symbol = (body.get("ticker") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "No ticker provided"}), 400
    cache = load_cache(market)
    if symbol in cache:
        del cache[symbol]
        save_cache(cache, market)
        return jsonify({"removed": symbol, "total_in_cache": len(cache)})
    return jsonify({"error": f"{symbol} not found"}), 404


@app.route("/api/<market>/refresh/<ticker>", methods=["POST"])
@require_admin
def refresh_ticker(market, ticker):
    """
    Re-fetch a single ticker's data from yfinance right now, ignoring the
    12-hour stale threshold. Use this when a specific card looks off
    (wrong yield, stale price, etc.) and you don't want to wait for the
    background refresh cycle to get to it.

    Admin-only because each call burns a yfinance request — on shared-IP
    hosts (PythonAnywhere, Render free tier) that budget is limited and
    shouldn't be spent by anyone with a login.
    """
    err = market_or_400(market)
    if err: return err
    symbol = (ticker or "").strip().upper()
    if not symbol:
        return jsonify({"error": "No ticker provided"}), 400
    try:
        d = fetch_one(symbol, market=market)
    except RateLimitDetected as e:
        return jsonify({"error": f"Rate-limited by Yahoo: {e}"}), 429
    except Exception as e:
        return jsonify({"error": f"Refresh failed: {e}"}), 500
    if not d:
        return jsonify({"error": f"{symbol} returned no data from yfinance"}), 404
    cache = load_cache(market)
    cache[symbol] = d
    save_cache(cache, market)
    return jsonify({
        "refreshed": {k:v for k,v in d.items() if k not in ("_fetched_at","_miss_count")},
        "total_in_cache": len(cache),
    })


@app.route("/api/<market>/override", methods=["POST", "DELETE"])
@require_admin
def set_override(market):
    """
    Set or clear a per-ticker yield override. When set, the override's
    annual dividend value is used as the source of truth for that
    ticker's yield, ignoring both yfinance's TTM and forward numbers.

    POST body: {ticker: "GOLI", annualDiv: 6.216, note: "..."}
    DELETE:    {ticker: "GOLI"}   → clears any override for that ticker

    Use case: yfinance has a ticker's distribution data wrong (split
    adjustment issues, missing special distributions, stale dividendRate
    for new income funds), and you need a specific number pinned. This
    saves you from editing the Python source and redeploying.
    """
    err = market_or_400(market)
    if err: return err
    body   = request.get_json() or {}
    symbol = (body.get("ticker") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "No ticker provided"}), 400
    overrides = load_overrides(market)
    if request.method == "DELETE":
        if symbol in overrides:
            del overrides[symbol]
            save_overrides(overrides, market)
        # Also force a re-fetch so the freshly-cleared row reflects yfinance
        # data on the next read instead of the now-deleted override.
        try:
            d = fetch_one(symbol, market=market)
            if d:
                cache = load_cache(market)
                cache[symbol] = d
                save_cache(cache, market)
        except Exception:
            pass
        return jsonify({"cleared": symbol})
    # POST
    try:
        annual_div = float(body.get("annualDiv") or 0)
    except Exception:
        return jsonify({"error": "annualDiv must be a number"}), 400
    if annual_div <= 0:
        return jsonify({"error": "annualDiv must be > 0"}), 400
    note = (body.get("note") or "").strip()[:200]
    overrides[symbol] = {"annualDiv": round(annual_div, 4), "note": note,
                         "setAt": datetime.datetime.now().isoformat()}
    save_overrides(overrides, market)
    # Force a re-fetch so the row reflects the new override immediately
    # instead of waiting for the next background refresh cycle.
    try:
        d = fetch_one(symbol, market=market)
        if d:
            cache = load_cache(market)
            cache[symbol] = d
            save_cache(cache, market)
    except Exception:
        pass
    return jsonify({"set": symbol, "annualDiv": annual_div, "note": note})


@app.route("/api/<market>/overrides", methods=["GET"])
@require_admin
def list_overrides(market):
    """Returns every currently-set override so the admin UI can populate
    its list. Combines UI-set and code-set ones for full visibility."""
    err = market_or_400(market)
    if err: return err
    dyn = load_overrides(market)
    code = {sym: {"annualDiv": v, "note": "Set in server.py source", "source": "code"}
            for sym, v in MANUAL_TTM_DIVIDEND_OVERRIDES.items()}
    # Dynamic wins on conflict (it's the more recent intent)
    for sym, v in dyn.items():
        v2 = dict(v); v2["source"] = "ui"
        code[sym] = v2
    return jsonify(code)


@app.route("/api/<market>/refresh/stale", methods=["POST"])
@require_admin
def refresh_stale(market):
    """
    Kick off a bulk refresh of every stale (>12h old) cache entry for this
    market right now, in the background. Use this when you've deployed new
    field handling (e.g. ttmYield / forwardYield / yieldMethod) and want the
    whole cache upgraded immediately instead of waiting for the natural
    background cycle to chip away at it over many days.

    Returns immediately with the count of stale entries it'll process.
    Poll /api/<market>/refresh/stale/status to see live progress (X of Y
    done, ETA, rate-limit flag).
    """
    err = market_or_400(market)
    if err: return err
    cache = load_cache(market)
    stale = [sym for sym, e in cache.items() if is_stale(e)]
    if not stale:
        return jsonify({"triggered": False, "reason": "No stale entries — cache is fully fresh.",
                        "staleCount": 0})
    with _refresh_locks[market]:
        if _refresh_running[market]:
            return jsonify({
                "triggered": False,
                "reason": "A refresh is already running.",
                "staleCount": len(stale),
                "progress": dict(_refresh_progress[market]),
            })
    # Clear any lingering pause from a prior session before starting fresh
    _refresh_progress[market]["paused"] = False
    # Spawn the existing background refresh in a thread; the same lock
    # and progress accounting is used so a second call can't double-fire.
    threading.Thread(target=background_refresh, args=(market,), daemon=True).start()
    return jsonify({"triggered": True, "staleCount": len(stale)})


@app.route("/api/<market>/refresh/stale/status", methods=["GET"])
@require_admin
def refresh_stale_status(market):
    """Live progress for the bulk refresh started by /api/<market>/refresh/stale.
    Returns the same shape whether or not a refresh is currently active
    (just with running=False) so the UI doesn't have to special-case it."""
    err = market_or_400(market)
    if err: return err
    return jsonify(dict(_refresh_progress[market]))


@app.route("/api/<market>/refresh/stale/pause", methods=["POST"])
@require_admin
def refresh_stale_pause(market):
    """Set the pause flag — the current ticker finishes, then the loop
    stops cleanly. Already-refreshed tickers stay marked fresh, so the
    next /api/<market>/refresh/stale resumes from exactly where we stopped."""
    err = market_or_400(market)
    if err: return err
    _refresh_progress[market]["paused"] = True
    return jsonify({"paused": True, "progress": dict(_refresh_progress[market])})


@app.route("/api/<market>/refresh/stale/resume", methods=["POST"])
@require_admin
def refresh_stale_resume(market):
    """
    Resume a paused refresh (or start fresh if none is running). If a
    refresh is already in flight, just clears the pause flag so the
    next iteration continues. If nothing's running, kicks off a new
    pass from the current stale list — same endpoint as the start button.
    """
    err = market_or_400(market)
    if err: return err
    if _refresh_running[market]:
        _refresh_progress[market]["paused"] = False
        return jsonify({"resumed": True, "alreadyRunning": True, "progress": dict(_refresh_progress[market])})
    cache = load_cache(market)
    stale = [sym for sym, e in cache.items() if is_stale(e)]
    if not stale:
        return jsonify({"resumed": False, "reason": "No stale entries to refresh.",
                        "staleCount": 0})
    with _refresh_locks[market]:
        if _refresh_running[market]:
            return jsonify({"resumed": False, "reason": "Already running.",
                            "staleCount": len(stale)})
    threading.Thread(target=background_refresh, args=(market,), daemon=True).start()
    return jsonify({"resumed": True, "staleCount": len(stale)})


@app.route("/api/<market>/diagnostics/suspects", methods=["GET"])
@require_admin
def list_suspects(market):
    """
    Returns every cached ticker that the most recent fetch flagged as
    having unreliable yfinance data — the "needs an override" pile.
    Each entry includes the reason so the admin can decide what the
    correct number should be (and either set an override or ignore it
    if the TTM is actually right).
    """
    err = market_or_400(market)
    if err: return err
    cache = load_cache(market)
    suspects = []
    for sym, row in cache.items():
        if not row.get("isSuspect"):
            continue
        suspects.append({
            "ticker":        sym,
            "name":          row.get("name", sym),
            "sector":        row.get("sector", ""),
            "price":         row.get("price", 0),
            "currency":      row.get("currency", MARKET_META[market]["currency_default"]),
            "yield":         row.get("yield", 0),
            "ttmYield":      row.get("ttmYield"),
            "forwardYield":  row.get("forwardYield"),
            "yieldMethod":   row.get("yieldMethod"),
            "hasOverride":   row.get("hasOverride", False),
            "suspectReason": row.get("suspectReason", ""),
            "dividendSource":row.get("dividendSource", "yfinance"),
            "dividendSources":row.get("dividendSources", {}),
            "fetchedAt":     row.get("_fetched_at", ""),
        })
    # Sort by highest displayed yield first — biggest discrepancies first
    suspects.sort(key=lambda x: -(x.get("yield") or 0))
    return jsonify({"total": len(suspects), "suspects": suspects})


@app.route("/api/<market>/verify/dividend/<ticker>", methods=["POST"])
@require_admin
def verify_dividend_endpoint(market, ticker):
    """
    On-demand cross-source verification for a single ticker. Pulls from
    yfinance + Twelvedata + Marketstack (whichever have keys configured),
    picks the median, updates the cache, and returns every source's value
    for transparency. Use this when you don't want to wait for the bulk
    refresh to get to a specific ticker. Works for both markets — Twelvedata
    and Marketstack cover LSE-listed tickers too, unlike the SEC-only
    filing-verification endpoint above.
    """
    err = market_or_400(market)
    if err: return err
    symbol = (ticker or "").strip().upper()
    if not symbol:
        return jsonify({"error": "No ticker provided"}), 400
    cache = load_cache(market)
    row = cache.get(symbol)
    if not row:
        return jsonify({"error": f"{symbol} not in cache — add it first"}), 404
    price = row.get("price", 0)
    ttm_yield = row.get("ttmYield", 0)
    yf_annual_div = (ttm_yield / 100) * price if ttm_yield and price else 0
    try:
        verified_div, source, all_sources = verify_dividend(symbol, price, yf_annual_div)
    except Exception as e:
        return jsonify({"error": f"Verification failed: {e}"}), 500
    # Adopt the verified value if it disagrees materially with the current row
    current_yield = row.get("yield", 0)
    changed = False
    if verified_div > 0 and price > 0:
        new_yield = (verified_div / price) * 100
        changed = abs(new_yield - current_yield) >= 0.5
        if changed:
            row["yield"] = round(new_yield, 2)
            row["yieldMethod"] = source
            row["dividendSource"] = source
            row["dividendSources"] = {k: round(v, 4) for k, v in all_sources.items()}
            row["isSuspect"] = False
            row["suspectReason"] = f"manually re-verified via cross-check ({source})"
            row["_fetched_at"] = datetime.datetime.now().isoformat()
            cache[symbol] = row
            save_cache(cache, market)
    return jsonify({
        "ticker":          symbol,
        "price":           price,
        "verifiedYield":   round((verified_div / price) * 100, 2) if verified_div > 0 and price > 0 else None,
        "verifiedDiv":     round(verified_div, 4) if verified_div > 0 else None,
        "source":          source,
        "sources":         {k: round(v, 4) for k, v in all_sources.items()},
        "changed":         changed,
        "previousYield":   current_yield,
    })


@app.route("/api/<market>/cache/info")
@require_login
def cache_info(market):
    err = market_or_400(market)
    if err: return err
    cache = load_cache(market)
    stale = [sym for sym, e in cache.items() if is_stale(e)]
    suspects = sum(1 for e in cache.values() if e.get("isSuspect") and not e.get("hasOverride"))
    return jsonify({
        "market":     market,
        "total":      len(cache),
        "stale":      len(stale),
        "suspects":   suspects,
        "stale_hours":STALE_HOURS,
        "cache_file": os.path.abspath(MARKET_FILES[market]["cache"]),
    })


@app.route("/api/health")
def health():
    us_cache = load_cache("us")
    uk_cache = load_cache("uk")
    apac_cache = load_cache("apac")
    eurozone_cache = load_cache("eurozone")
    return jsonify({
        "status": "ok",
        "cached": len(us_cache) + len(uk_cache) + len(apac_cache) + len(eurozone_cache),
        "us": {"cached": len(us_cache)},
        "uk": {"cached": len(uk_cache)},
        "apac": {"cached": len(apac_cache)},
        "eurozone": {"cached": len(eurozone_cache)},
    })


# ─── SERVE THE FRONTEND (single-app hosting) ───────────────────────────────
# Any request that isn't one of the /api/... routes above falls through to
# here, and gets the built React app's index.html — the app's own tab
# navigation (US / UK / Combined) is handled client-side from there. Also
# serves JS/CSS/images that Vite put in dist/ (favicon, assets/*.js,
# assets/*.css, etc.) directly.
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    if not os.path.isdir(FRONTEND_DIST):
        return jsonify({
            "error": "Frontend not built yet.",
            "hint": "Run `npm run build` in the frontend folder and copy the "
                     "resulting dist/ folder next to server.py, then restart."
        }), 500
    full_path = os.path.join(FRONTEND_DIST, path)
    if path and os.path.isfile(full_path):
        return app.send_static_file(path)
    return app.send_static_file("index.html")


# ─── START ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "─"*52)
    print("  💰 AltınRock / Dividend Scout — Combined Backend (US + UK + APAC + Eurozone)")
    print("─"*52)
    for market in ("us", "uk", "apac", "eurozone"):
        cache = load_cache(market)
        label = MARKET_META[market]["label"]
        if cache:
            stale = sum(1 for e in cache.values() if is_stale(e))
            print(f"  [{market.upper()}] {label:<15}: {len(cache)} tickers  ({stale} stale)")
        else:
            print(f"  [{market.upper()}] {label:<15}: empty — will seed {len(MARKET_META[market]['seed'])} tickers on first request")
        print(f"      File: {MARKET_FILES[market]['cache']}")
    port = int(os.environ.get("PORT", 8000))
    if os.path.isdir(FRONTEND_DIST):
        print(f"  Frontend: found dist/ — serving UI + API from one app")
    else:
        print(f"  Frontend: no dist/ found yet — run `npm run build` and copy it here")
    print(f"  App     : http://localhost:{port}" if "PORT" not in os.environ else f"  App     : listening on port {port}")
    print("─"*52 + "\n")
    app.run(host="0.0.0.0", port=port, debug=False)
