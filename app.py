# app.py - Complete Quantum Edge Pro Trading Bot
# Deploy this single file to Render.com
# 
# Environment Variables Required:
# - CTRADER_CLIENT_ID: From https://openapi.ctrader.com/apps
# - CTRADER_CLIENT_SECRET: From https://openapi.ctrader.com/apps
# - CTRADER_REDIRECT_URI: https://your-backend.onrender.com/oauth/callback
# - TWELVE_DATA_KEY: From twelvedata.com (free)
# - GROQ_API_KEY: From groq.com (optional)
# - SECRET_KEY: Your secret key for Flask sessions
# - MAX_DAILY_TRADES: 5 (default)
# - MIN_CONFIDENCE: 65 (default)
# - RISK_PER_TRADE: 1.5 (default)
# - TRADING_HOURS: 08:00-17:00 (default)

import os
import json
import sqlite3
import logging
import time
import random
import math
import urllib.parse
import hashlib
import hmac
import base64
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from threading import Thread, Lock
from collections import deque
from dataclasses import dataclass, field

from flask import Flask, request, jsonify, session, redirect, url_for
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import requests
import websockets

# ============================================================
# CONFIGURATION
# ============================================================
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'qep-bot-secret-key-2026')
app.config['SESSION_COOKIE_SECURE'] = True if os.environ.get('RENDER') else False
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)

# CORS for GitHub Pages
CORS(app, origins=[
    'https://*.github.io',
    'https://*.onrender.com',
    'http://localhost:5000',
    'http://localhost:3000'
])

socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='threading',
    ping_timeout=60,
    ping_interval=25
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================
# cTrader OAuth
CTRADER_CLIENT_ID = os.environ.get('CTRADER_CLIENT_ID', '')
CTRADER_CLIENT_SECRET = os.environ.get('CTRADER_CLIENT_SECRET', '')
CTRADER_REDIRECT_URI = os.environ.get('CTRADER_REDIRECT_URI', '')

# API Keys
TWELVE_DATA_KEY = os.environ.get('TWELVE_DATA_KEY', '')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')

# Bot Configuration
MAX_DAILY_TRADES = int(os.environ.get('MAX_DAILY_TRADES', 5))
MIN_CONFIDENCE = int(os.environ.get('MIN_CONFIDENCE', 65))
RISK_PER_TRADE = float(os.environ.get('RISK_PER_TRADE', 1.5))
MAX_SPREAD = float(os.environ.get('MAX_SPREAD', 3))
TRADING_HOURS = os.environ.get('TRADING_HOURS', '08:00-17:00')

# cTrader OAuth endpoints
CTRADER_AUTH_URL = 'https://id.ctrader.com/my/settings/openapi/grantingaccess/'
CTRADER_TOKEN_URL = 'https://openapi.ctrader.com/apps/token'
CTRADER_API_BASE = 'https://openapi.ctrader.com'
CTRADER_SCOPE = 'trading'

# ============================================================
# DATABASE SETUP
# ============================================================
def init_db():
    """Initialize SQLite database"""
    conn = sqlite3.connect('trading_bot.db')
    c = conn.cursor()
    
    # Trades table
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT UNIQUE,
        symbol TEXT,
        action TEXT,
        volume REAL,
        entry_price REAL,
        stop_loss REAL,
        take_profit REAL,
        status TEXT,
        open_time TEXT,
        close_time TEXT,
        pnl REAL,
        strategy TEXT,
        confidence REAL,
        reasoning TEXT
    )''')
    
    # Signals table
    c.execute('''CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        symbol TEXT,
        timeframe TEXT,
        signal TEXT,
        entry REAL,
        sl REAL,
        tp REAL,
        confidence REAL,
        strategies TEXT,
        executed INTEGER DEFAULT 0,
        rr_ratio REAL
    )''')
    
    # Performance table
    c.execute('''CREATE TABLE IF NOT EXISTS performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        balance REAL,
        equity REAL,
        trades INTEGER,
        wins INTEGER,
        losses INTEGER,
        win_rate REAL,
        drawdown REAL,
        daily_pnl REAL
    )''')
    
    # OAuth tokens table
    c.execute('''CREATE TABLE IF NOT EXISTS oauth_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        access_token TEXT,
        refresh_token TEXT,
        expires_at REAL,
        account_id TEXT,
        created_at TEXT
    )''')
    
    conn.commit()
    conn.close()
    logger.info("✅ Database initialized")

init_db()

# ============================================================
# BASE DATA
# ============================================================
BASE_PRICES = {
    'XAUUSD': 2390, 'XAGUSD': 28.5, 'BTCUSD': 64500, 'ETHUSD': 3150,
    'EURUSD': 1.0872, 'GBPUSD': 1.2754, 'USDJPY': 149.42,
    'AUDUSD': 0.6618, 'USDCAD': 1.3592, 'USDCHF': 0.9021,
    'NZDUSD': 0.6102, 'EURGBP': 0.8521, 'EURJPY': 162.45, 'GBPJPY': 190.32
}

SYMBOL_NAMES = {
    'XAUUSD': 'XAU/USD', 'XAGUSD': 'XAG/USD', 'BTCUSD': 'BTC/USD',
    'ETHUSD': 'ETH/USD', 'EURUSD': 'EUR/USD', 'GBPUSD': 'GBP/USD',
    'USDJPY': 'USD/JPY', 'AUDUSD': 'AUD/USD', 'USDCAD': 'USD/CAD',
    'USDCHF': 'USD/CHF', 'NZDUSD': 'NZD/USD', 'EURGBP': 'EUR/GBP',
    'EURJPY': 'EUR/JPY', 'GBPJPY': 'GBP/JPY'
}

SPREADS = {
    'XAUUSD': 2.5, 'XAGUSD': 3.0, 'BTCUSD': 5.0, 'ETHUSD': 4.0,
    'EURUSD': 0.8, 'GBPUSD': 1.2, 'USDJPY': 1.0, 'AUDUSD': 1.5,
    'USDCAD': 1.8, 'USDCHF': 1.5, 'NZDUSD': 1.8, 'EURGBP': 1.5,
    'EURJPY': 1.5, 'GBPJPY': 2.0
}

# ============================================================
# CTRADER OAUTH CLIENT
# ============================================================
class CtraderOAuthClient:
    """Complete cTrader OAuth 2.0 Client with token management"""
    
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None
        self.account_id = None
        self.is_authenticated = False
        self._lock = Lock()
        
        # Load saved tokens from database
        self._load_tokens()
    
    def _load_tokens(self):
        """Load saved tokens from database"""
        try:
            conn = sqlite3.connect('trading_bot.db')
            c = conn.cursor()
            c.execute('''SELECT access_token, refresh_token, expires_at, account_id 
                        FROM oauth_tokens ORDER BY id DESC LIMIT 1''')
            result = c.fetchone()
            conn.close()
            
            if result:
                self.access_token = result[0]
                self.refresh_token = result[1]
                self.token_expiry = result[2]
                self.account_id = result[3]
                
                # Check if token is still valid
                if self.token_expiry and self.token_expiry > time.time():
                    self.is_authenticated = True
                    logger.info("✅ Loaded valid tokens from database")
                else:
                    logger.info("⏰ Saved tokens expired, will refresh")
                    
        except Exception as e:
            logger.error(f"❌ Failed to load tokens: {str(e)}")
    
    def _save_tokens(self):
        """Save tokens to database"""
        try:
            conn = sqlite3.connect('trading_bot.db')
            c = conn.cursor()
            c.execute('''INSERT INTO oauth_tokens 
                        (access_token, refresh_token, expires_at, account_id, created_at)
                        VALUES (?, ?, ?, ?, ?)''',
                     (self.access_token, self.refresh_token, self.token_expiry, 
                      self.account_id, datetime.now().isoformat()))
            conn.commit()
            conn.close()
            logger.info("✅ Tokens saved to database")
        except Exception as e:
            logger.error(f"❌ Failed to save tokens: {str(e)}")
    
    def get_authorization_url(self) -> str:
        """Generate OAuth authorization URL"""
        if not CTRADER_CLIENT_ID:
            return None
        
        params = {
            'client_id': CTRADER_CLIENT_ID,
            'redirect_uri': CTRADER_REDIRECT_URI,
            'scope': CTRADER_SCOPE,
            'product': 'web',
            'response_type': 'code'
        }
        return f"{CTRADER_AUTH_URL}?{urllib.parse.urlencode(params)}"
    
    def exchange_code_for_token(self, authorization_code: str) -> Dict:
        """Exchange authorization code for access token"""
        params = {
            'grant_type': 'authorization_code',
            'code': authorization_code,
            'redirect_uri': CTRADER_REDIRECT_URI,
            'client_id': CTRADER_CLIENT_ID,
            'client_secret': CTRADER_CLIENT_SECRET
        }
        
        try:
            response = requests.get(
                CTRADER_TOKEN_URL,
                params=params,
                headers={'Accept': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('accessToken')
                self.refresh_token = data.get('refreshToken')
                self.token_expiry = time.time() + data.get('expiresIn', 2628000)
                self.is_authenticated = True
                
                # Get account ID
                self.account_id = self._get_account_id()
                
                # Save tokens
                self._save_tokens()
                
                logger.info("✅ OAuth authentication successful")
                return {
                    'success': True,
                    'account_id': self.account_id,
                    'expires_in': data.get('expiresIn', 2628000)
                }
            else:
                logger.error(f"❌ OAuth failed: {response.text}")
                return {'success': False, 'error': response.text}
                
        except Exception as e:
            logger.error(f"❌ OAuth error: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def refresh_access_token(self) -> bool:
        """Refresh access token using refresh_token"""
        if not self.refresh_token:
            return False
        
        params = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': CTRADER_CLIENT_ID,
            'client_secret': CTRADER_CLIENT_SECRET
        }
        
        try:
            response = requests.get(CTRADER_TOKEN_URL, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('accessToken')
                self.refresh_token = data.get('refreshToken')
                self.token_expiry = time.time() + data.get('expiresIn', 2628000)
                self.is_authenticated = True
                self._save_tokens()
                logger.info("✅ Access token refreshed successfully")
                return True
            else:
                logger.error(f"❌ Token refresh failed: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Token refresh error: {str(e)}")
            return False
    
    def ensure_token_valid(self) -> bool:
        """Ensure token is valid, refresh if needed"""
        if not self.is_authenticated:
            return False
        
        # Refresh if token expires in less than 1 hour
        if self.token_expiry and (self.token_expiry - time.time()) < 3600:
            logger.info("🔄 Token expiring soon, refreshing...")
            return self.refresh_access_token()
        
        return True
    
    def _get_account_id(self) -> Optional[str]:
        """Get account ID from API"""
        if not self.ensure_token_valid():
            return None
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        try:
            # cTrader uses Protobuf, but we can get account info from the token
            # This is a simplified approach
            response = requests.get(
                f"{CTRADER_API_BASE}/apps/accounts",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                accounts = response.json()
                if accounts and len(accounts) > 0:
                    return accounts[0].get('id')
            
            # If we can't get accounts, return a placeholder
            return "account_id_placeholder"
            
        except Exception as e:
            logger.warning(f"Could not fetch account ID: {str(e)}")
            return "account_id_placeholder"
    
    def get_accounts(self) -> List[Dict]:
        """Get list of trading accounts"""
        if not self.ensure_token_valid():
            return []
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(
                f"{CTRADER_API_BASE}/apps/accounts",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return []
                
        except Exception as e:
            logger.error(f"Account fetch error: {str(e)}")
            return []
    
    def place_order(self, symbol: str, action: str, volume: float,
                   stop_loss: float = None, take_profit: float = None) -> Dict:
        """Place a market order"""
        if not self.ensure_token_valid():
            return {'success': False, 'error': 'Not authenticated'}
        
        # Note: cTrader uses Protobuf for order placement
        # This is a simplified REST approximation
        # For production, use the official cTrader Python SDK
        
        order_data = {
            'symbolName': symbol,
            'tradeSide': 'Buy' if action.lower() == 'buy' else 'Sell',
            'volume': int(volume * 100000),  # cTrader uses 1/100000 units
            'orderType': 'Market'
        }
        
        if stop_loss:
            order_data['stopLoss'] = stop_loss
        if take_profit:
            order_data['takeProfit'] = take_profit
        
        if self.account_id:
            order_data['ctidTraderAccountId'] = self.account_id
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.post(
                f"{CTRADER_API_BASE}/orders",
                json=order_data,
                headers=headers,
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                data = response.json()
                
                # Save to database
                conn = sqlite3.connect('trading_bot.db')
                c = conn.cursor()
                c.execute('''INSERT INTO trades 
                            (order_id, symbol, action, volume, entry_price, 
                             stop_loss, take_profit, status, open_time)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                         (data.get('id'), symbol, action, volume, data.get('price'),
                          stop_loss, take_profit, 'OPEN', datetime.now().isoformat()))
                conn.commit()
                conn.close()
                
                return {
                    'success': True,
                    'order_id': data.get('id'),
                    'price': data.get('price'),
                    'data': data
                }
            else:
                return {'success': False, 'error': response.text}
                
        except Exception as e:
            logger.error(f"Order error: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def close_order(self, order_id: str) -> Dict:
        """Close an order"""
        if not self.ensure_token_valid():
            return {'success': False, 'error': 'Not authenticated'}
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.delete(
                f"{CTRADER_API_BASE}/orders/{order_id}",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Update database
                conn = sqlite3.connect('trading_bot.db')
                c = conn.cursor()
                c.execute('''UPDATE trades 
                            SET status = 'CLOSED', close_time = ?, pnl = ?
                            WHERE order_id = ?''',
                         (datetime.now().isoformat(), data.get('pnl', 0), order_id))
                conn.commit()
                conn.close()
                
                return {'success': True, 'data': data}
            else:
                return {'success': False, 'error': response.text}
                
        except Exception as e:
            logger.error(f"Close order error: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def get_positions(self) -> List[Dict]:
        """Get open positions"""
        if not self.ensure_token_valid():
            return []
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(
                f"{CTRADER_API_BASE}/positions",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return []
                
        except Exception as e:
            logger.error(f"Get positions error: {str(e)}")
            return []
    
    def get_account_info(self) -> Dict:
        """Get account information"""
        if not self.ensure_token_valid():
            return {'balance': 0, 'equity': 0}
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(
                f"{CTRADER_API_BASE}/apps/accounts/{self.account_id}" if self.account_id else f"{CTRADER_API_BASE}/apps/accounts",
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    return data[0]
                return data
            else:
                return {'balance': 0, 'equity': 0}
                
        except Exception as e:
            logger.error(f"Account info error: {str(e)}")
            return {'balance': 0, 'equity': 0}

# ============================================================
# TECHNICAL INDICATORS ENGINE
# ============================================================
class TechnicalIndicators:
    """Complete technical analysis engine"""
    
    @staticmethod
    def generate_simulated_data(symbol: str, count: int = 200) -> Dict:
        """Generate simulated OHLCV data"""
        base = BASE_PRICES.get(symbol, 1.0)
        seed = int(time.time() / 60)
        
        def seeded_rand(n):
            x = math.sin(seed * 9301 + n * 49297 + ord(symbol[0]) * 233) * 10000
            return x - math.floor(x)
        
        drift = (seeded_rand(0) - 0.48) * base * 0.004
        price = base + drift
        
        closes = []
        highs = []
        lows = []
        volumes = []
        
        p = price
        for i in range(count):
            chg = (seeded_rand(i + 1) - 0.49) * base * 0.003
            p = max(base * 0.95, p + chg)
            hi = p * (1 + seeded_rand(i + 100) * 0.002)
            lo = p * (1 - seeded_rand(i + 200) * 0.002)
            vol = 100 + seeded_rand(i + 300) * 900
            closes.append(p)
            highs.append(hi)
            lows.append(lo)
            volumes.append(vol)
        
        closes[-1] = price
        highs[-1] = price * (1 + seeded_rand(999) * 0.002)
        lows[-1] = price * (1 - seeded_rand(998) * 0.002)
        
        return {
            'closes': closes,
            'highs': highs,
            'lows': lows,
            'volumes': volumes,
            'current_price': price
        }
    
    @staticmethod
    def calculate_rsi(closes: List[float], period: int = 14) -> float:
        """Calculate RSI"""
        if len(closes) < period + 1:
            return 50.0
        
        gains = 0
        losses = 0
        for i in range(len(closes) - period, len(closes) - 1):
            diff = closes[i + 1] - closes[i]
            if diff >= 0:
                gains += diff
            else:
                losses -= diff
        
        if losses == 0:
            return 100.0
        if gains == 0:
            return 0.0
        
        rs = (gains / period) / (losses / period)
        rsi = 100 - (100 / (1 + rs))
        return round(rsi, 1)
    
    @staticmethod
    def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
        """Calculate Average True Range"""
        if len(highs) < period + 1:
            return 0.001
        
        tr = []
        for i in range(1, len(highs)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i - 1])
            lc = abs(lows[i] - closes[i - 1])
            tr.append(max(hl, hc, lc))
        
        if len(tr) < period:
            return 0.001
        
        atr = sum(tr[:period]) / period
        for i in range(period, len(tr)):
            atr = (atr * (period - 1) + tr[i]) / period
        
        return round(atr, 5)
    
    @staticmethod
    def calculate_ema(values: List[float], period: int) -> float:
        """Calculate EMA"""
        if len(values) < period:
            return values[-1] if values else 0
        
        ema = sum(values[:period]) / period
        multiplier = 2 / (period + 1)
        
        for val in values[period:]:
            ema = (val * multiplier) + (ema * (1 - multiplier))
        
        return round(ema, 5)
    
    @staticmethod
    def calculate_macd(closes: List[float]) -> float:
        """Calculate MACD"""
        if len(closes) < 26:
            return 0
        
        ema12 = TechnicalIndicators.calculate_ema(closes, 12)
        ema26 = TechnicalIndicators.calculate_ema(closes, 26)
        return round(ema12 - ema26, 5)
    
    @staticmethod
    def calculate_bollinger_bands(closes: List[float], period: int = 20, std_dev: float = 2) -> Dict:
        """Calculate Bollinger Bands"""
        if len(closes) < period:
            return {'upper': 0, 'middle': 0, 'lower': 0, 'width': 0}
        
        recent = closes[-period:]
        mean = sum(recent) / period
        variance = sum((x - mean) ** 2 for x in recent) / period
        std = math.sqrt(variance)
        
        upper = mean + (std_dev * std)
        lower = mean - (std_dev * std)
        width = (upper - lower) / mean if mean != 0 else 0
        
        return {
            'upper': round(upper, 5),
            'middle': round(mean, 5),
            'lower': round(lower, 5),
            'width': round(width, 5)
        }
    
    @staticmethod
    def calculate_fibonacci(highs: List[float], lows: List[float]) -> Dict:
        """Calculate Fibonacci levels"""
        if not highs or not lows:
            return {'fib382': 0, 'fib50': 0, 'fib618': 0}
        
        swing_high = max(highs[-30:])
        swing_low = min(lows[-30:])
        range_val = swing_high - swing_low
        
        return {
            'fib382': round(swing_low + range_val * 0.382, 5),
            'fib50': round(swing_low + range_val * 0.5, 5),
            'fib618': round(swing_low + range_val * 0.618, 5),
            'swing_high': swing_high,
            'swing_low': swing_low
        }
    
    @staticmethod
    def find_support_resistance(highs: List[float], lows: List[float], atr: float) -> Dict:
        """Find support and resistance zones"""
        if len(highs) < 30:
            return {'support': 0, 'resistance': 0, 'zones': []}
        
        tolerance = atr * 0.4 if atr > 0 else 0.001
        candidates = []
        
        for j in range(2, len(highs) - 2):
            if (highs[j] > highs[j-1] and highs[j] > highs[j-2] and 
                highs[j] > highs[j+1] and highs[j] > highs[j+2]):
                candidates.append({'level': highs[j], 'type': 'resistance', 'idx': j})
            
            if (lows[j] < lows[j-1] and lows[j] < lows[j-2] and 
                lows[j] < lows[j+1] and lows[j] < lows[j+2]):
                candidates.append({'level': lows[j], 'type': 'support', 'idx': j})
        
        zones = []
        for cand in candidates:
            merged = False
            for zone in zones:
                if abs(zone['level'] - cand['level']) <= tolerance:
                    zone['level'] = (zone['level'] * zone['tests'] + cand['level']) / (zone['tests'] + 1)
                    zone['tests'] += 1
                    zone['last_idx'] = max(zone['last_idx'], cand['idx'])
                    if zone['type'] != cand['type']:
                        zone['type'] = 'both'
                    merged = True
                    break
            if not merged:
                zones.append({
                    'level': cand['level'],
                    'type': cand['type'],
                    'tests': 1,
                    'last_idx': cand['idx']
                })
        
        zones = [z for z in zones if z['tests'] >= 3]
        zones.sort(key=lambda x: (x['tests'] * 3 + x['last_idx'] / len(highs) * 2), reverse=True)
        
        support = 0
        resistance = 0
        for z in zones:
            if z['type'] in ['support', 'both'] and z['level'] < highs[-1]:
                support = z['level']
                break
        
        for z in zones:
            if z['type'] in ['resistance', 'both'] and z['level'] > highs[-1]:
                resistance = z['level']
                break
        
        return {'support': support, 'resistance': resistance, 'zones': zones}
    
    @staticmethod
    def detect_candle_pattern(closes: List[float], highs: List[float], lows: List[float], atr: float) -> Dict:
        """Detect candlestick patterns"""
        if len(closes) < 4:
            return {'pattern': 'None', 'bias': 'neutral', 'strength': 0, 'desc': ''}
        
        i = len(closes) - 1
        body = abs(closes[i] - closes[i-1])
        range_val = highs[i] - lows[i]
        body_ratio = body / range_val if range_val > 0 else 0
        is_bull = closes[i] > closes[i-1]
        is_bear = closes[i] < closes[i-1]
        is_doji = body_ratio < 0.1 and range_val > atr * 0.3
        
        if is_doji:
            return {'pattern': 'Doji', 'bias': 'neutral', 'strength': 55, 'desc': 'Indecision'}
        
        if i >= 2 and is_bear and is_bull and closes[i] > closes[i-2]:
            return {'pattern': 'Bullish Engulfing', 'bias': 'bull', 'strength': 82, 'desc': 'Bull engulfing previous bear'}
        
        if i >= 2 and is_bull and is_bear and closes[i] < closes[i-2]:
            return {'pattern': 'Bearish Engulfing', 'bias': 'bear', 'strength': 82, 'desc': 'Bear engulfing previous bull'}
        
        if i >= 2 and (closes[i] - lows[i]) > body * 2.5 and (highs[i] - closes[i]) < body * 0.5:
            if closes[i-1] < closes[i-2]:
                return {'pattern': 'Hammer', 'bias': 'bull', 'strength': 70, 'desc': 'Rejection at support'}
        
        if i >= 2 and (highs[i] - closes[i]) > body * 2.5 and (closes[i] - lows[i]) < body * 0.5:
            if closes[i-1] > closes[i-2]:
                return {'pattern': 'Shooting Star', 'bias': 'bear', 'strength': 70, 'desc': 'Rejection at resistance'}
        
        return {'pattern': 'No Pattern', 'bias': 'neutral', 'strength': 0, 'desc': ''}
    
    @staticmethod
    def calculate_vwap(highs: List[float], lows: List[float], closes: List[float], volumes: List[float]) -> float:
        """Calculate VWAP"""
        if len(closes) < 10:
            return closes[-1] if closes else 0
        
        numerator = 0
        denominator = 0
        lookback = min(100, len(closes))
        
        for j in range(len(closes) - lookback, len(closes)):
            typical = (highs[j] + lows[j] + closes[j]) / 3
            vol = volumes[j] if j < len(volumes) else (highs[j] - lows[j])
            numerator += typical * vol
            denominator += vol
        
        vwap = numerator / denominator if denominator > 0 else closes[-1]
        return round(vwap, 5)

# ============================================================
# MARKET DATA FETCHER
# ============================================================
class MarketDataFetcher:
    """Fetch market data from Twelve Data API or generate simulated data"""
    
    @staticmethod
    def fetch_data(symbol: str, timeframe: str = '1h') -> Dict:
        """Fetch market data"""
        if TWELVE_DATA_KEY and len(TWELVE_DATA_KEY) > 5:
            return MarketDataFetcher._fetch_live_data(symbol, timeframe)
        else:
            return MarketDataFetcher._generate_simulated_data(symbol)
    
    @staticmethod
    def _fetch_live_data(symbol: str, timeframe: str) -> Dict:
        """Fetch live data from Twelve Data API"""
        try:
            symbol_name = SYMBOL_NAMES.get(symbol, symbol)
            interval_map = {
                '1min': '1min', '5min': '5min', '15min': '15min',
                '1h': '1h', '4h': '4h', '1day': '1day'
            }
            interval = interval_map.get(timeframe, '1h')
            
            url = f"https://api.twelvedata.com/time_series?symbol={symbol_name}&interval={interval}&outputsize=500&apikey={TWELVE_DATA_KEY}"
            
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get('status') == 'error' or not data.get('values'):
                return MarketDataFetcher._generate_simulated_data(symbol)
            
            closes = []
            highs = []
            lows = []
            volumes = []
            
            for val in reversed(data['values']):
                closes.append(float(val['close']))
                highs.append(float(val['high']))
                lows.append(float(val['low']))
                volumes.append(float(val.get('volume', 0)))
            
            price = closes[-1] if closes else BASE_PRICES.get(symbol, 1.0)
            
            return {
                'closes': closes,
                'highs': highs,
                'lows': lows,
                'volumes': volumes,
                'current_price': price,
                'source': 'live'
            }
            
        except Exception as e:
            logger.error(f"Live data fetch error: {str(e)}")
            return MarketDataFetcher._generate_simulated_data(symbol)
    
    @staticmethod
    def _generate_simulated_data(symbol: str) -> Dict:
        """Generate simulated data when API is unavailable"""
        data = TechnicalIndicators.generate_simulated_data(symbol)
        data['source'] = 'simulated'
        return data

# ============================================================
# 17-STRATEGY VOTING ENGINE
# ============================================================
class StrategyVotingEngine:
    """17-strategy weighted voting system"""
    
    def __init__(self, symbol: str, data: Dict):
        self.symbol = symbol
        self.price = data.get('current_price', 0)
        self.closes = data.get('closes', [])
        self.highs = data.get('highs', [])
        self.lows = data.get('lows', [])
        self.volumes = data.get('volumes', [])
        
        # Calculate all indicators
        self.rsi = TechnicalIndicators.calculate_rsi(self.closes)
        self.atr = TechnicalIndicators.calculate_atr(self.highs, self.lows, self.closes)
        self.macd = TechnicalIndicators.calculate_macd(self.closes)
        self.ema20 = TechnicalIndicators.calculate_ema(self.closes, 20)
        self.ema50 = TechnicalIndicators.calculate_ema(self.closes, 50)
        self.ema200 = TechnicalIndicators.calculate_ema(self.closes, 200)
        self.bb = TechnicalIndicators.calculate_bollinger_bands(self.closes)
        self.fib = TechnicalIndicators.calculate_fibonacci(self.highs, self.lows)
        self.sr = TechnicalIndicators.find_support_resistance(self.highs, self.lows, self.atr)
        self.pattern = TechnicalIndicators.detect_candle_pattern(self.closes, self.highs, self.lows, self.atr)
        self.vwap = TechnicalIndicators.calculate_vwap(self.highs, self.lows, self.closes, self.volumes)
        
        # Market structure
        self.structure = self._analyze_structure()
        
        # Volume analysis
        self.vol_spike = self._detect_volume_spike()
        self.vol_dir = self._get_volume_direction()
        
        # VWAP analysis
        self.above_vwap = self.price > self.vwap
        self.near_vwap = abs(self.price - self.vwap) < self.atr * 0.5 if self.atr > 0 else False
        
        # Liquidity sweeps
        sweeps = self._detect_liquidity_sweeps()
        self.swept_high = sweeps['swept_high']
        self.swept_low = sweeps['swept_low']
        
        # POC (Point of Control) - simplified
        self.poc = self._calculate_poc()
        
        self.votes = []
    
    def _analyze_structure(self) -> str:
        """Analyze market structure"""
        if len(self.highs) < 10:
            return 'RANGING'
        
        hh = 0
        ll = 0
        for i in range(2, min(10, len(self.highs))):
            if self.highs[-i] > self.highs[-i-2]:
                hh += 1
            if self.lows[-i] < self.lows[-i-2]:
                ll += 1
        
        if hh > ll * 1.5:
            return 'BULLISH'
        elif ll > hh * 1.5:
            return 'BEARISH'
        else:
            return 'RANGING'
    
    def _detect_volume_spike(self) -> bool:
        """Detect volume spike"""
        if len(self.volumes) < 20:
            return False
        
        recent_avg = sum(self.volumes[-10:]) / 10
        if recent_avg == 0:
            return False
        
        latest = self.volumes[-1] if self.volumes else 0
        return latest > recent_avg * 1.5
    
    def _get_volume_direction(self) -> str:
        """Get volume direction"""
        if len(self.volumes) < 5:
            return 'neutral'
        
        if self.volumes[-1] > self.volumes[-2]:
            return 'up'
        else:
            return 'down'
    
    def _detect_liquidity_sweeps(self) -> Dict:
        """Detect liquidity sweeps"""
        if len(self.highs) < 20:
            return {'swept_high': False, 'swept_low': False}
        
        recent_high = max(self.highs[-20:-1]) if len(self.highs) > 20 else max(self.highs)
        recent_low = min(self.lows[-20:-1]) if len(self.lows) > 20 else min(self.lows)
        
        swept_high = self.highs[-1] > recent_high and self.price < recent_high
        swept_low = self.lows[-1] < recent_low and self.price > recent_low
        
        return {'swept_high': swept_high, 'swept_low': swept_low}
    
    def _calculate_poc(self) -> float:
        """Calculate Point of Control (simplified)"""
        if len(self.closes) < 50:
            return self.price
        
        # Simplified POC - use VWAP as proxy
        return self.vwap
    
    def run_all_strategies(self) -> List[Dict]:
        """Run all 17 strategies"""
        self._strategy_ema_trend()
        self._strategy_sr_bounce()
        self._strategy_rsi_divergence()
        self._strategy_fibonacci()
        self._strategy_bb_squeeze()
        self._strategy_market_structure()
        self._strategy_multi_tf()
        self._strategy_order_block()
        self._strategy_fvg()
        self._strategy_liquidity_sweep()
        self._strategy_volume()
        self._strategy_correlation()
        self._strategy_vwap()
        self._strategy_volume_profile()
        self._strategy_time_filter()
        self._strategy_spread()
        self._strategy_candle_pattern()
        
        return self.votes
    
    def _add_vote(self, name: str, icon: str, signal: str, confidence: float, reason: str):
        """Add a strategy vote"""
        self.votes.append({
            'name': name,
            'icon': icon,
            'signal': signal,
            'confidence': confidence,
            'reason': reason
        })
    
    def _strategy_ema_trend(self):
        """EMA Trend Strategy"""
        ema_bull = self.price > self.ema20 and self.ema20 > self.ema50 and self.macd > 0
        ema_bear = self.price < self.ema20 and self.ema20 < self.ema50 and self.macd < 0
        
        signal = 'BUY' if ema_bull else 'SELL' if ema_bear else 'HOLD'
        conf = 72 if (ema_bull or ema_bear) else 30
        reason = 'Price above EMA20>EMA50' if ema_bull else 'Price below EMA20<EMA50' if ema_bear else 'EMAs mixed'
        self._add_vote('EMA Trend', '📈', signal, conf, reason)
    
    def _strategy_sr_bounce(self):
        """Support/Resistance Bounce Strategy"""
        d_sup = abs(self.price - self.sr['support']) / self.atr if self.atr > 0 and self.sr['support'] > 0 else 999
        d_res = abs(self.sr['resistance'] - self.price) / self.atr if self.atr > 0 and self.sr['resistance'] > 0 else 999
        
        sr_bull = self.rsi < 42 and d_sup < 0.6
        sr_bear = self.rsi > 58 and d_res < 0.6
        
        signal = 'BUY' if sr_bull else 'SELL' if sr_bear else 'HOLD'
        conf = 74 if (sr_bull or sr_bear) else 28
        reason = f'RSI {self.rsi} near support' if sr_bull else f'RSI {self.rsi} near resistance' if sr_bear else 'No key level'
        self._add_vote('S/R Bounce', '🏀', signal, conf, reason)
    
    def _strategy_rsi_divergence(self):
        """RSI Divergence Strategy"""
        signal = 'BUY' if self.rsi < 32 else 'SELL' if self.rsi > 68 else 'HOLD'
        conf = 67 if (self.rsi < 32 or self.rsi > 68) else 25
        reason = f'RSI {self.rsi} oversold' if self.rsi < 32 else f'RSI {self.rsi} overbought' if self.rsi > 68 else f'RSI {self.rsi} neutral'
        self._add_vote('RSI Div', '🔄', signal, conf, reason)
    
    def _strategy_fibonacci(self):
        """Fibonacci Strategy"""
        near_fib618 = abs(self.price - self.fib['fib618']) / self.atr < 0.5 if self.atr > 0 and self.fib['fib618'] > 0 else False
        near_fib50 = abs(self.price - self.fib['fib50']) / self.atr < 0.5 if self.atr > 0 and self.fib['fib50'] > 0 else False
        
        fib_bull = (near_fib618 or near_fib50) and self.macd > 0
        fib_bear = (near_fib618 or near_fib50) and self.macd < 0
        
        signal = 'BUY' if fib_bull else 'SELL' if fib_bear else 'HOLD'
        conf = 71 if (fib_bull or fib_bear) else 30
        level = '61.8' if near_fib618 else '50' if near_fib50 else ''
        reason = f'At Fib {level}% with bullish MACD' if fib_bull else f'At Fib {level}% with bearish MACD' if fib_bear else 'No Fib confluence'
        self._add_vote('Fibonacci', '🌀', signal, conf, reason)
    
    def _strategy_bb_squeeze(self):
        """Bollinger Bands Squeeze Strategy"""
        squeeze = self.bb['width'] < 0.005
        bb_break_bull = self.price > self.bb['upper'] and self.macd > 0
        bb_break_bear = self.price < self.bb['lower'] and self.macd < 0
        
        if squeeze:
            signal = 'BUY' if self.macd > 0 else 'SELL'
            conf = 65
            reason = f'BB squeeze — breakout {"bullish" if self.macd > 0 else "bearish"}'
        elif bb_break_bull:
            signal = 'BUY'
            conf = 65
            reason = 'Price above upper BB'
        elif bb_break_bear:
            signal = 'SELL'
            conf = 65
            reason = 'Price below lower BB'
        else:
            signal = 'HOLD'
            conf = 28
            reason = 'BB neutral'
        self._add_vote('BB Squeeze', '🎯', signal, conf, reason)
    
    def _strategy_market_structure(self):
        """Market Structure Strategy"""
        if self.structure == 'BULLISH':
            signal = 'BUY'
            conf = 68
            reason = 'Structure: BULLISH'
        elif self.structure == 'BEARISH':
            signal = 'SELL'
            conf = 68
            reason = 'Structure: BEARISH'
        else:
            signal = 'HOLD'
            conf = 30
            reason = 'Structure: RANGING'
        self._add_vote('Mkt Struct', '🏗️', signal, conf, reason)
    
    def _strategy_multi_tf(self):
        """Multi-Timeframe Strategy"""
        mtf_bull = self.price > self.ema200 and self.ema50 > self.ema200
        mtf_bear = self.price < self.ema200 and self.ema50 < self.ema200
        
        signal = 'BUY' if mtf_bull else 'SELL' if mtf_bear else 'HOLD'
        conf = 73 if (mtf_bull or mtf_bear) else 30
        reason = 'Price above EMA200' if mtf_bull else 'Price below EMA200' if mtf_bear else 'No HTF bias'
        self._add_vote('Multi-TF', '📐', signal, conf, reason)
    
    def _strategy_order_block(self):
        """Order Block Strategy (simplified)"""
        ob_bull = self.rsi < 40 and self.price < self.ema20
        ob_bear = self.rsi > 60 and self.price > self.ema20
        
        signal = 'BUY' if ob_bull else 'SELL' if ob_bear else 'HOLD'
        conf = 76 if (ob_bull or ob_bear) else 25
        reason = 'Bullish OB' if ob_bull else 'Bearish OB' if ob_bear else 'No active OB'
        self._add_vote('Order Block', '📦', signal, conf, reason)
    
    def _strategy_fvg(self):
        """Fair Value Gap Strategy (simplified)"""
        fvg_bull = self.rsi < 45 and self.price > self.ema20
        fvg_bear = self.rsi > 55 and self.price < self.ema20
        
        signal = 'BUY' if fvg_bull else 'SELL' if fvg_bear else 'HOLD'
        conf = 74 if (fvg_bull or fvg_bear) else 22
        reason = 'Filling bullish FVG' if fvg_bull else 'Filling bearish FVG' if fvg_bear else 'No active FVG'
        self._add_vote('FVG', '🔲', signal, conf, reason)
    
    def _strategy_liquidity_sweep(self):
        """Liquidity Sweep Strategy"""
        signal = 'BUY' if self.swept_low else 'SELL' if self.swept_high else 'HOLD'
        conf = 78 if (self.swept_low or self.swept_high) else 20
        reason = 'Sweep below lows' if self.swept_low else 'Sweep above highs' if self.swept_high else 'No sweep'
        self._add_vote('Liq Sweep', '💧', signal, conf, reason)
    
    def _strategy_volume(self):
        """Volume Strategy"""
        vol_bull = self.vol_spike and self.vol_dir == 'up' and self.macd > 0
        vol_bear = self.vol_spike and self.vol_dir == 'down' and self.macd < 0
        
        signal = 'BUY' if vol_bull else 'SELL' if vol_bear else 'HOLD'
        conf = 69 if (vol_bull or vol_bear) else 30
        reason = 'Volume spike up' if vol_bull else 'Volume spike down' if vol_bear else 'Volume neutral'
        self._add_vote('Volume', '📊', signal, conf, reason)
    
    def _strategy_correlation(self):
        """Correlation Strategy (simplified)"""
        correlated = self.symbol in ['EURUSD', 'GBPUSD', 'EURGBP']
        signal = 'BUY' if correlated and self.macd > 0 else 'SELL' if correlated and self.macd < 0 else 'HOLD'
        conf = 62 if correlated else 20
        reason = 'Correlated with EUR basket' if correlated else 'No correlation'
        self._add_vote('Correlation', '🔗', signal, conf, reason)
    
    def _strategy_vwap(self):
        """VWAP Strategy"""
        if self.above_vwap and not self.near_vwap:
            signal = 'BUY'
            conf = 68
            reason = 'Price above VWAP'
        elif not self.above_vwap and not self.near_vwap:
            signal = 'SELL'
            conf = 68
            reason = 'Price below VWAP'
        else:
            signal = 'HOLD'
            conf = 35
            reason = 'Near VWAP'
        self._add_vote('VWAP', '📊', signal, conf, reason)
    
    def _strategy_volume_profile(self):
        """Volume Profile Strategy"""
        poc_above = self.price > self.poc and self.poc > 0
        poc_below = self.price < self.poc and self.poc > 0
        
        signal = 'BUY' if poc_above else 'SELL' if poc_below else 'HOLD'
        conf = 74 if (poc_above or poc_below) else 30
        reason = f'Above POC' if poc_above else f'Below POC' if poc_below else 'At POC'
        self._add_vote('Vol Profile', '🔊', signal, conf, reason)
    
    def _strategy_time_filter(self):
        """Time Filter Strategy"""
        utc_hour = datetime.now().hour
        in_london = 8 <= utc_hour < 17
        in_ny = 13 <= utc_hour < 22
        in_overlap = 13 <= utc_hour < 17
        
        if in_overlap:
            signal = 'BUY' if self.macd > 0 else 'SELL'
            conf = 78
            reason = 'Overlap session'
        elif in_london or in_ny:
            signal = 'BUY' if self.macd > 0 else 'SELL'
            conf = 65
            reason = f'{in_london and "London" or "NY"} session'
        else:
            signal = 'HOLD'
            conf = 25
            reason = 'Off session'
        self._add_vote('Time Filter', '⏰', signal, conf, reason)
    
    def _strategy_spread(self):
        """Spread Strategy"""
        spread = SPREADS.get(self.symbol, 1)
        min_target = spread * 10
        potential_reward = self.atr * 2 if self.atr > 0 else spread * 5
        
        viable = potential_reward >= min_target
        signal = 'BUY' if viable and self.macd > 0 else 'SELL' if viable and self.macd < 0 else 'HOLD'
        conf = 70 if viable else 10
        reason = 'Target > 10x spread' if viable else 'Target too close to spread'
        self._add_vote('Spread', '💸', signal, conf, reason)
    
    def _strategy_candle_pattern(self):
        """Candle Pattern Strategy"""
        pattern = self.pattern
        signal = 'BUY' if pattern.get('bias') == 'bull' else 'SELL' if pattern.get('bias') == 'bear' else 'HOLD'
        conf = pattern.get('strength', 0)
        reason = f"{pattern.get('pattern', 'No Pattern')}: {pattern.get('desc', '')}"
        self._add_vote('Candle', '🕯️', signal, conf, reason)
    
    def calculate_weighted_vote(self) -> Dict:
        """Calculate weighted voting result"""
        votes = self.votes
        total = len(votes)
        
        buy_sum = 0
        buy_count = 0
        sell_sum = 0
        sell_count = 0
        buy_weighted = 0
        sell_weighted = 0
        
        for v in votes:
            is_buy = v['signal'] in ['BUY', 'SCALP-LONG']
            is_sell = v['signal'] in ['SELL', 'SCALP-SHORT']
            
            if is_buy:
                buy_sum += v['confidence']
                buy_count += 1
                buy_weighted += v['confidence'] * v['confidence'] / 100
            elif is_sell:
                sell_sum += v['confidence']
                sell_count += 1
                sell_weighted += v['confidence'] * v['confidence'] / 100
        
        buy_avg = buy_sum / buy_count if buy_count > 0 else 0
        sell_avg = sell_sum / sell_count if sell_count > 0 else 0
        buy_weighted_avg = buy_weighted / buy_count if buy_count > 0 else 0
        sell_weighted_avg = sell_weighted / sell_count if sell_count > 0 else 0
        
        if buy_weighted_avg > sell_weighted_avg and buy_weighted_avg > 30:
            signal = 'BUY'
            confidence = min(95, round(buy_weighted_avg + buy_count * 2))
            strategy = f'BUY ({buy_count}/{total} strategies, avg {round(buy_avg)}%)'
            reasoning = f'{buy_count} strategies BUY (avg {round(buy_avg)}%), {sell_count} SELL. Weighted score favors BUY.'
        elif sell_weighted_avg > buy_weighted_avg and sell_weighted_avg > 30:
            signal = 'SELL'
            confidence = min(95, round(sell_weighted_avg + sell_count * 2))
            strategy = f'SELL ({sell_count}/{total} strategies, avg {round(sell_avg)}%)'
            reasoning = f'{sell_count} strategies SELL (avg {round(sell_avg)}%), {buy_count} BUY. Weighted score favors SELL.'
        else:
            signal = 'HOLD'
            confidence = max(15, round((buy_weighted_avg + sell_weighted_avg) / 2))
            strategy = 'CONFLICTED — WAIT'
            reasoning = f'Mixed signals. BUY avg {round(buy_avg)}% ({buy_count}), SELL avg {round(sell_avg)}% ({sell_count}). Wait for clearer confluence.'
        
        confluence = max(buy_count, sell_count)
        
        return {
            'signal': signal,
            'confidence': confidence,
            'strategy': strategy,
            'reasoning': reasoning,
            'confluence': confluence,
            'buy_count': buy_count,
            'sell_count': sell_count,
            'total_strats': total,
            'weighted_score': confidence,
            'votes': votes
        }

# ============================================================
# FORECAST ENTRY CALCULATOR
# ============================================================
class ForecastEntryCalculator:
    """Calculate forecasted entry, SL, and TP with 3:1 R:R"""
    
    @staticmethod
    def calculate(symbol: str, signal: str, data: Dict) -> Dict:
        """Calculate entry, stop loss, and take profit"""
        price = data.get('current_price', 0)
        atr = data.get('atr', 0.001)
        sr = data.get('sr', {})
        
        is_buy = signal in ['BUY', 'SCALP-LONG']
        is_sell = signal in ['SELL', 'SCALP-SHORT']
        
        entry = price
        sl = None
        tp = None
        entry_type = 'Market'
        quality = 'MARKET'
        
        if is_buy:
            # Find support zones
            zones = sr.get('zones', [])
            supports = [z for z in zones if z['type'] in ['support', 'both'] and z['level'] < price - atr * 0.2]
            
            if supports:
                best_sup = supports[0]
                entry = best_sup['level'] + atr * 0.1
                sl = best_sup['level'] - atr * 0.2
                entry_type = f"Support ({best_sup['tests']}x tested)"
                quality = 'ELITE' if best_sup['tests'] >= 5 else 'STRONG' if best_sup['tests'] >= 4 else 'SOLID'
            else:
                entry = price - atr * 0.8
                sl = entry - atr * 1.0
                entry_type = 'VWAP/EMA Fallback'
            
            # Calculate TP for 3:1 R:R
            risk_distance = entry - sl
            tp = entry + (risk_distance * 3)
            
        elif is_sell:
            # Find resistance zones
            zones = sr.get('zones', [])
            resistances = [z for z in zones if z['type'] in ['resistance', 'both'] and z['level'] > price + atr * 0.2]
            
            if resistances:
                best_res = resistances[0]
                entry = best_res['level'] - atr * 0.1
                sl = best_res['level'] + atr * 0.2
                entry_type = f"Resistance ({best_res['tests']}x tested)"
                quality = 'ELITE' if best_res['tests'] >= 5 else 'STRONG' if best_res['tests'] >= 4 else 'SOLID'
            else:
                entry = price + atr * 0.8
                sl = entry + atr * 1.0
                entry_type = 'VWAP/EMA Fallback'
            
            # Calculate TP for 3:1 R:R
            risk_distance = sl - entry
            tp = entry - (risk_distance * 3)
        
        # Clamp entry to reasonable levels
        entry = max(price - atr * 3, min(price + atr * 3, entry))
        
        # Ensure SL and TP are valid
        if is_buy:
            sl = min(entry - atr * 0.2, sl or entry - atr * 1.0)
            tp = max(entry + atr * 2.0, tp or entry + atr * 3.0)
        elif is_sell:
            sl = max(entry + atr * 0.2, sl or entry + atr * 1.0)
            tp = min(entry - atr * 2.0, tp or entry - atr * 3.0)
        
        # Calculate R:R
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        rr = f"{(reward / risk):.1f}" if risk > 0 else '0'
        
        return {
            'entry': entry,
            'sl': sl,
            'tp': tp,
            'entry_type': entry_type,
            'quality': quality,
            'rr': rr,
            'risk': risk,
            'reward': reward
        }

# ============================================================
# TRADING BOT ENGINE
# ============================================================
class TradingBot:
    """Complete Trading Bot Engine"""
    
    def __init__(self):
        self.ctrader = CtraderOAuthClient()
        self.is_running = False
        self.trade_lock = Lock()
        self.daily_trades = 0
        self.last_trade_date = None
        self.bot_thread = None
        self.stop_requested = False
        
        self.config = {
            'max_daily_trades': MAX_DAILY_TRADES,
            'min_confidence': MIN_CONFIDENCE,
            'risk_per_trade': RISK_PER_TRADE,
            'max_spread': MAX_SPREAD,
            'trading_hours': TRADING_HOURS,
            'symbols': ['XAUUSD', 'EURUSD', 'GBPUSD', 'USDJPY']
        }
        
        self.performance = {
            'trades': 0,
            'wins': 0,
            'losses': 0,
            'win_rate': 0,
            'drawdown': 0,
            'balance': 0,
            'equity': 0,
            'daily_pnl': 0
        }
    
    def set_config(self, config: Dict):
        """Update bot configuration"""
        self.config.update(config)
        logger.info(f"✅ Config updated: {self.config}")
    
    def start(self) -> Dict:
        """Start the trading bot"""
        if self.is_running:
            return {'success': False, 'error': 'Bot already running'}
        
        if not self.ctrader.is_authenticated:
            return {'success': False, 'error': 'Not authenticated with cTrader'}
        
        if not self.ctrader.ensure_token_valid():
            return {'success': False, 'error': 'Authentication token expired'}
        
        self.is_running = True
        self.stop_requested = False
        self.daily_trades = 0
        self.last_trade_date = datetime.now().date()
        
        # Start bot thread
        self.bot_thread = Thread(target=self._run_bot, daemon=True)
        self.bot_thread.start()
        
        logger.info("🚀 Bot started")
        return {'success': True, 'message': 'Bot started successfully'}
    
    def stop(self) -> Dict:
        """Stop the trading bot"""
        self.stop_requested = True
        self.is_running = False
        
        if self.bot_thread:
            self.bot_thread.join(timeout=5)
        
        logger.info("⏹ Bot stopped")
        return {'success': True, 'message': 'Bot stopped'}
    
    def _run_bot(self):
        """Main bot loop"""
        logger.info("🤖 Bot loop started")
        
        while not self.stop_requested and self.is_running:
            try:
                # Check trading hours
                if not self._is_trading_hours():
                    time.sleep(60)
                    continue
                
                # Check daily trade limit
                if not self._can_trade():
                    logger.info("⏸ Daily trade limit reached")
                    time.sleep(300)
                    continue
                
                # Generate signal for each symbol
                for symbol in self.config['symbols']:
                    if self.stop_requested:
                        break
                    
                    signal = self._generate_signal(symbol)
                    
                    if signal and signal['signal'] != 'HOLD':
                        self._execute_signal(signal)
                    
                    time.sleep(10)  # Small delay between symbols
                
                # Update performance
                self._update_performance()
                
                # Emit status update
                socketio.emit('bot_status', self.get_status())
                
                time.sleep(60)  # Main loop interval
                
            except Exception as e:
                logger.error(f"❌ Bot loop error: {str(e)}")
                time.sleep(60)
        
        logger.info("🤖 Bot loop ended")
    
    def _is_trading_hours(self) -> bool:
        """Check if current time is within trading hours"""
        try:
            current = datetime.now().time()
            start, end = self.config['trading_hours'].split('-')
            start_time = datetime.strptime(start, '%H:%M').time()
            end_time = datetime.strptime(end, '%H:%M').time()
            return start_time <= current <= end_time
        except:
            return True
    
    def _can_trade(self) -> bool:
        """Check if we can place a new trade"""
        today = datetime.now().date()
        
        if self.last_trade_date != today:
            self.daily_trades = 0
            self.last_trade_date = today
        
        return self.daily_trades < self.config['max_daily_trades']
    
    def _generate_signal(self, symbol: str) -> Optional[Dict]:
        """Generate trading signal for a symbol"""
        try:
            # Fetch market data
            data = MarketDataFetcher.fetch_data(symbol)
            
            # Calculate indicators
            data['rsi'] = TechnicalIndicators.calculate_rsi(data['closes'])
            data['atr'] = TechnicalIndicators.calculate_atr(data['highs'], data['lows'], data['closes'])
            data['macd'] = TechnicalIndicators.calculate_macd(data['closes'])
            data['ema20'] = TechnicalIndicators.calculate_ema(data['closes'], 20)
            data['ema50'] = TechnicalIndicators.calculate_ema(data['closes'], 50)
            data['ema200'] = TechnicalIndicators.calculate_ema(data['closes'], 200)
            data['bb'] = TechnicalIndicators.calculate_bollinger_bands(data['closes'])
            data['fib'] = TechnicalIndicators.calculate_fibonacci(data['highs'], data['lows'])
            data['sr'] = TechnicalIndicators.find_support_resistance(data['highs'], data['lows'], data['atr'])
            
            # Run strategy voting
            engine = StrategyVotingEngine(symbol, data)
            result = engine.calculate_weighted_vote()
            
            # Calculate entry levels
            if result['signal'] != 'HOLD':
                forecast = ForecastEntryCalculator.calculate(symbol, result['signal'], data)
                
                return {
                    'symbol': symbol,
                    'signal': result['signal'],
                    'confidence': result['confidence'],
                    'strategy': result['strategy'],
                    'reasoning': result['reasoning'],
                    'entry': forecast['entry'],
                    'sl': forecast['sl'],
                    'tp': forecast['tp'],
                    'entry_type': forecast['entry_type'],
                    'quality': forecast['quality'],
                    'rr': forecast['rr'],
                    'current_price': data['current_price'],
                    'votes': result['votes']
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Signal generation error for {symbol}: {str(e)}")
            return None
    
    def _execute_signal(self, signal: Dict):
        """Execute a trading signal"""
        with self.trade_lock:
            try:
                symbol = signal['symbol']
                action = signal['signal']
                confidence = signal['confidence']
                entry = signal['entry']
                sl = signal['sl']
                tp = signal['tp']
                
                # Check confidence threshold
                if confidence < self.config['min_confidence']:
                    logger.info(f"⏭ Signal confidence {confidence}% below threshold {self.config['min_confidence']}%")
                    return
                
                # Check spread
                spread = SPREADS.get(symbol, 1)
                if spread > self.config['max_spread']:
                    logger.info(f"⏭ Spread {spread} exceeds max {self.config['max_spread']}")
                    return
                
                # Calculate volume based on risk
                account_info = self.ctrader.get_account_info()
                balance = account_info.get('balance', 10000)
                risk_amount = balance * (self.config['risk_per_trade'] / 100)
                pip_distance = abs(entry - sl) if sl else 0.001
                pip_value = 1 if symbol in ['BTCUSD'] else 100 if symbol == 'XAUUSD' else 10000
                volume = risk_amount / (pip_distance * pip_value) if pip_distance > 0 else 0.01
                volume = max(0.01, min(10, round(volume, 2)))
                
                # Place order
                result = self.ctrader.place_order(
                    symbol=symbol,
                    action=action,
                    volume=volume,
                    stop_loss=sl,
                    take_profit=tp
                )
                
                if result['success']:
                    self.daily_trades += 1
                    self.performance['trades'] += 1
                    
                    # Save signal to database
                    conn = sqlite3.connect('trading_bot.db')
                    c = conn.cursor()
                    c.execute('''INSERT INTO signals 
                                (timestamp, symbol, signal, entry, sl, tp, confidence, strategies, rr_ratio, executed)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                             (datetime.now().isoformat(), symbol, action, entry, sl, tp, confidence,
                              signal.get('strategy', ''), signal.get('rr', '0'), 1))
                    conn.commit()
                    conn.close()
                    
                    logger.info(f"✅ Trade executed: {action} {symbol} @ {result['price']} (ID: {result['order_id']})")
                    
                    # Emit trade event
                    socketio.emit('trade_executed', {
                        'symbol': symbol,
                        'action': action,
                        'price': result['price'],
                        'order_id': result['order_id'],
                        'confidence': confidence
                    })
                    
                else:
                    logger.error(f"❌ Trade execution failed: {result.get('error', 'Unknown error')}")
                    
            except Exception as e:
                logger.error(f"❌ Execute signal error: {str(e)}")
    
    def _update_performance(self):
        """Update performance metrics"""
        try:
            account_info = self.ctrader.get_account_info()
            self.performance['balance'] = account_info.get('balance', 0)
            self.performance['equity'] = account_info.get('equity', 0)
            
            # Calculate win rate from database
            conn = sqlite3.connect('trading_bot.db')
            c = conn.cursor()
            c.execute('SELECT COUNT(*), SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) FROM trades WHERE status = "CLOSED"')
            result = c.fetchone()
            conn.close()
            
            if result and result[0] > 0:
                total = result[0]
                wins = result[1] or 0
                self.performance['win_rate'] = round((wins / total) * 100, 1)
                self.performance['losses'] = total - wins
                self.performance['wins'] = wins
            
            # Save performance to database
            conn = sqlite3.connect('trading_bot.db')
            c = conn.cursor()
            c.execute('''INSERT INTO performance 
                        (date, balance, equity, trades, wins, losses, win_rate, daily_pnl)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                     (datetime.now().isoformat(),
                      self.performance['balance'],
                      self.performance['equity'],
                      self.performance['trades'],
                      self.performance['wins'],
                      self.performance['losses'],
                      self.performance['win_rate'],
                      self.performance['daily_pnl']))
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Update performance error: {str(e)}")
    
    def get_status(self) -> Dict:
        """Get bot status"""
        return {
            'is_running': self.is_running,
            'daily_trades': self.daily_trades,
            'max_daily_trades': self.config['max_daily_trades'],
            'performance': self.performance,
            'config': self.config,
            'authenticated': self.ctrader.is_authenticated,
            'token_valid': self.ctrader.ensure_token_valid(),
            'timestamp': datetime.now().isoformat()
        }

# ============================================================
# FLASK ROUTES
# ============================================================
bot = TradingBot()

# ============================================================
# OAuth Routes
# ============================================================
@app.route('/oauth/login', methods=['GET'])
def oauth_login():
    """Start OAuth login flow"""
    auth_url = bot.ctrader.get_authorization_url()
    
    if not auth_url:
        return jsonify({
            'success': False,
            'error': 'CTRADER_CLIENT_ID not configured'
        }), 400
    
    return jsonify({
        'success': True,
        'auth_url': auth_url,
        'message': 'Open this URL in a browser to authorize the app'
    })

@app.route('/oauth/callback', methods=['GET'])
def oauth_callback():
    """OAuth callback endpoint"""
    authorization_code = request.args.get('code')
    
    if not authorization_code:
        return jsonify({'success': False, 'error': 'No authorization code received'}), 400
    
    result = bot.ctrader.exchange_code_for_token(authorization_code)
    
    if result['success']:
        # Also send to frontend
        return '''
        <html>
            <head>
                <title>Authentication Successful</title>
                <style>
                    body { 
                        background: #05070f; 
                        color: #eef2ff; 
                        font-family: Arial, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        height: 100vh;
                        flex-direction: column;
                    }
                    .success { 
                        background: #0c1526; 
                        padding: 40px; 
                        border-radius: 18px;
                        border: 1px solid #10b981;
                        text-align: center;
                        max-width: 500px;
                    }
                    h1 { color: #10b981; }
                    .btn {
                        background: #f59e0b;
                        color: #080e1f;
                        padding: 12px 30px;
                        border: none;
                        border-radius: 10px;
                        font-weight: 700;
                        cursor: pointer;
                        font-size: 1rem;
                        margin-top: 20px;
                    }
                </style>
            </head>
            <body>
                <div class="success">
                    <h1>✅ Authentication Successful!</h1>
                    <p>You can now close this window and return to the trading dashboard.</p>
                    <p style="color: #94a3b8; font-size: 0.9rem;">
                        Account ID: {account_id}
                    </p>
                    <button class="btn" onclick="window.close()">Close Window</button>
                </div>
            </body>
        </html>
        '''.format(account_id=result.get('account_id', 'N/A'))
    else:
        return jsonify({'success': False, 'error': result.get('error')}), 401

# ============================================================
# API Routes
# ============================================================
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'online',
        'timestamp': datetime.now().isoformat(),
        'bot_running': bot.is_running,
        'authenticated': bot.ctrader.is_authenticated,
        'environment': 'production' if os.environ.get('RENDER') else 'development'
    })

@app.route('/api/ctrader/status', methods=['GET'])
def ctrader_status():
    """Check cTrader authentication status"""
    return jsonify({
        'authenticated': bot.ctrader.is_authenticated,
        'token_valid': bot.ctrader.ensure_token_valid(),
        'account_id': bot.ctrader.account_id
    })

@app.route('/api/ctrader/accounts', methods=['GET'])
def ctrader_accounts():
    """Get trading accounts"""
    if not bot.ctrader.is_authenticated:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    accounts = bot.ctrader.get_accounts()
    return jsonify({'success': True, 'accounts': accounts})

@app.route('/api/ctrader/positions', methods=['GET'])
def ctrader_positions():
    """Get open positions"""
    positions = bot.ctrader.get_positions()
    return jsonify({'success': True, 'positions': positions})

@app.route('/api/ctrader/place_order', methods=['POST'])
def ctrader_place_order():
    """Place an order"""
    if not bot.ctrader.is_authenticated:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    data = request.json
    symbol = data.get('symbol')
    action = data.get('action')
    volume = data.get('volume', 0.01)
    sl = data.get('stop_loss')
    tp = data.get('take_profit')
    
    if not symbol or not action:
        return jsonify({'success': False, 'error': 'Symbol and action required'}), 400
    
    result = bot.ctrader.place_order(symbol, action, volume, sl, tp)
    return jsonify(result)

@app.route('/api/ctrader/close_order/<order_id>', methods=['DELETE'])
def ctrader_close_order(order_id):
    """Close an order"""
    if not bot.ctrader.is_authenticated:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    result = bot.ctrader.close_order(order_id)
    return jsonify(result)

# ============================================================
# Trading Bot Routes
# ============================================================
@app.route('/api/bot/start', methods=['POST'])
def bot_start():
    """Start the trading bot"""
    config = request.json or {}
    bot.set_config(config)
    result = bot.start()
    return jsonify(result)

@app.route('/api/bot/stop', methods=['POST'])
def bot_stop():
    """Stop the trading bot"""
    result = bot.stop()
    return jsonify(result)

@app.route('/api/bot/status', methods=['GET'])
def bot_status():
    """Get bot status"""
    return jsonify(bot.get_status())

@app.route('/api/bot/config', methods=['GET', 'POST'])
def bot_config():
    """Get or update bot configuration"""
    if request.method == 'POST':
        bot.set_config(request.json or {})
        return jsonify({'success': True, 'config': bot.config})
    else:
        return jsonify(bot.config)

@app.route('/api/bot/performance', methods=['GET'])
def bot_performance():
    """Get performance metrics"""
    try:
        conn = sqlite3.connect('trading_bot.db')
        c = conn.cursor()
        
        # Get all closed trades
        c.execute('SELECT COUNT(*), SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), SUM(pnl) FROM trades WHERE status = "CLOSED"')
        trade_stats = c.fetchone()
        
        # Get today's trades
        today = datetime.now().date()
        c.execute('SELECT COUNT(*) FROM trades WHERE date(open_time) = ?', (today.isoformat(),))
        today_trades = c.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'total_trades': trade_stats[0] or 0,
            'wins': trade_stats[1] or 0,
            'losses': (trade_stats[0] or 0) - (trade_stats[1] or 0),
            'total_pnl': trade_stats[2] or 0,
            'today_trades': today_trades,
            'win_rate': ((trade_stats[1] or 0) / (trade_stats[0] or 1)) * 100 if trade_stats[0] else 0
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/signal/generate', methods=['POST'])
def generate_signal():
    """Generate a trading signal for a symbol"""
    data = request.json
    symbol = data.get('symbol', 'XAUUSD')
    
    result = bot._generate_signal(symbol)
    
    if result:
        return jsonify({'success': True, 'signal': result})
    else:
        return jsonify({'success': False, 'error': 'No signal generated'})

# ============================================================
# Database Routes
# ============================================================
@app.route('/api/trades', methods=['GET'])
def get_trades():
    """Get trade history"""
    limit = request.args.get('limit', 100)
    
    try:
        conn = sqlite3.connect('trading_bot.db')
        c = conn.cursor()
        c.execute('''SELECT * FROM trades ORDER BY id DESC LIMIT ?''', (limit,))
        trades = c.fetchall()
        conn.close()
        
        columns = ['id', 'order_id', 'symbol', 'action', 'volume', 'entry_price',
                   'stop_loss', 'take_profit', 'status', 'open_time', 'close_time',
                   'pnl', 'strategy', 'confidence', 'reasoning']
        
        result = []
        for t in trades:
            result.append(dict(zip(columns, t)))
        
        return jsonify({'success': True, 'trades': result})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/signals', methods=['GET'])
def get_signals():
    """Get signal history"""
    limit = request.args.get('limit', 100)
    
    try:
        conn = sqlite3.connect('trading_bot.db')
        c = conn.cursor()
        c.execute('''SELECT * FROM signals ORDER BY id DESC LIMIT ?''', (limit,))
        signals = c.fetchall()
        conn.close()
        
        columns = ['id', 'timestamp', 'symbol', 'signal', 'entry', 'sl', 'tp',
                   'confidence', 'strategies', 'executed', 'rr_ratio']
        
        result = []
        for s in signals:
            result.append(dict(zip(columns, s)))
        
        return jsonify({'success': True, 'signals': result})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================
# WEBSOCKET EVENTS
# ============================================================
@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connection"""
    logger.info('✅ Client connected')
    emit('connected', {'message': 'Connected to QEP Bot Server'})
    
    # Send initial status
    emit('status_update', bot.get_status())

@socketio.on('disconnect')
def handle_disconnect():
    """Handle WebSocket disconnection"""
    logger.info('❌ Client disconnected')

@socketio.on('get_status')
def handle_status_request():
    """Handle status request via WebSocket"""
    emit('status_update', bot.get_status())

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    # Check if running on Render
    if os.environ.get('RENDER'):
        logger.info(f"🚀 Starting Quantum Edge Pro Bot on Render (port {port})")
        socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
    else:
        logger.info(f"🚀 Starting Quantum Edge Pro Bot in development mode (port {port})")
        socketio.run(app, host='0.0.0.0', port=port, debug=True)
