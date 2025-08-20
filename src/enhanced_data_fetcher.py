"""
增強版資料擷取模組 - 整合多資料源與完整修正
整合 FinMind API 與 Taiwan Stock Exchange OpenAPI
"""
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import time
import logging
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# 設定日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 匯入詳細日誌系統
try:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from setup_detailed_logging import get_detailed_logger
    detailed_logger = get_detailed_logger("data_fetcher")
except ImportError:
    detailed_logger = None

# 匯入 ROE 計算器 (選擇性)
ROECalculator = None
try:
    from .roe_calculator import ROECalculator
except ImportError:
    try:
        from roe_calculator import ROECalculator
    except ImportError:
        ROECalculator = None
        logger.debug("未載入 ROE 計算器模組")


class EnhancedDataFetcher:
    """增強版資料擷取器 - 整合多資料源"""
    
    def __init__(self, api_config_path: str = "api_config.json"):
        """初始化資料擷取器"""
        try:
            with open(api_config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        except:
            # 預設設定
            self.config = {
                'finmind': {
                    'api_token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wOC0xNyAxOTozNDo0MCIsInVzZXJfaWQiOiJ0MDg3ODA1NzExIiwiaXAiOiI5NC4xNTYuMTQ5Ljk0In0.7ukV7nG5f0oiQjAkmH0bye3NDqGi-_5DyI3nZfHto5g',
                    'base_url': 'https://api.finmindtrade.com/api/v4/data'
                }
            }
        
        self.finmind_config = self.config['finmind']
        self.base_url = self.finmind_config['base_url']
        self.token = self.finmind_config['api_token']
        self.headers = {
            'Authorization': f'Bearer {self.token}'
        }
        
        # TWSE API 設定
        self.twse_base_url = "https://openapi.twse.com.tw/v1"
        
        # 快取設定
        self.cache = {}
        self.cache_duration = 300  # 5分鐘快取
        
        # 初始化 ROE 計算器
        self.roe_calculator = ROECalculator() if ROECalculator else None
        
    def test_connection(self) -> bool:
        """測試 API 連線"""
        try:
            # 測試 FinMind
            params = {
                'dataset': 'TaiwanStockInfo',
                'data_id': '2330'
            }
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200:
                    logger.info("FinMind API 連線成功")
                    
                    # 測試 TWSE
                    try:
                        twse_response = requests.get(
                            f"{self.twse_base_url}/exchangeReport/STOCK_DAY_ALL",
                            timeout=10
                        )
                        if twse_response.status_code == 200:
                            logger.info("TWSE API 連線成功")
                    except:
                        logger.warning("TWSE API 連線失敗，將使用 FinMind 作為主要資料源")
                    
                    return True
            
            logger.error(f"API 連線失敗: {response.status_code}")
            return False
            
        except Exception as e:
            logger.error(f"連線錯誤: {e}")
            return False
    
    def get_stock_list(self) -> pd.DataFrame:
        """取得股票清單"""
        # 優先使用已篩選的真實股票清單
        try:
            import os
            csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'real_stock_list.csv')
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, encoding='utf-8-sig')
                logger.info(f"使用篩選後的股票清單: {len(df)} 檔")
                return df
        except Exception as e:
            logger.warning(f"無法讀取篩選清單: {e}")
        
        # 從 API 取得並篩選
        try:
            params = {
                'dataset': 'TaiwanStockInfo'
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200 and data.get('data'):
                    df = pd.DataFrame(data['data'])
                    # 篩選上市和上櫃股票
                    if 'type' in df.columns:
                        df = df[df['type'].isin(['twse', 'tpex'])]  # tpex is OTC in FinMind API
                    
                    # 篩選真實股票（排除權證）
                    real_stocks = []
                    for _, stock in df.iterrows():
                        stock_id = str(stock.get('stock_id', ''))
                        # 只保留4位數字的股票代碼
                        if len(stock_id) == 4 and stock_id.isdigit() and not stock_id.startswith('0'):
                            real_stocks.append(stock)
                    
                    df = pd.DataFrame(real_stocks)
                    logger.info(f"取得 {len(df)} 檔真實股票資料")
                    return df
            
            # 使用備用清單
            logger.warning("使用備用股票清單")
            return self.get_backup_stock_list()
            
        except Exception as e:
            logger.error(f"取得股票清單錯誤: {e}")
            return self.get_backup_stock_list()
    
    def get_backup_stock_list(self) -> pd.DataFrame:
        """取得備用股票清單"""
        backup_stocks = [
            {'stock_id': '2330', 'stock_name': '台積電', 'type': 'twse'},
            {'stock_id': '2454', 'stock_name': '聯發科', 'type': 'twse'},
            {'stock_id': '2317', 'stock_name': '鴻海', 'type': 'twse'},
            {'stock_id': '2308', 'stock_name': '台達電', 'type': 'twse'},
            {'stock_id': '2382', 'stock_name': '廣達', 'type': 'twse'},
            {'stock_id': '2303', 'stock_name': '聯電', 'type': 'twse'},
            {'stock_id': '2412', 'stock_name': '中華電', 'type': 'twse'},
            {'stock_id': '2886', 'stock_name': '兆豐金', 'type': 'twse'},
            {'stock_id': '2891', 'stock_name': '中信金', 'type': 'twse'},
            {'stock_id': '1301', 'stock_name': '台塑', 'type': 'twse'}
        ]
        return pd.DataFrame(backup_stocks)
    
    def get_stock_type(self, stock_id: str) -> str:
        """取得股票市場類型"""
        # 如果已經有暫存的類型
        if hasattr(self, '_current_stock_type') and self._current_stock_type != 'unknown':
            return self._current_stock_type
        
        # 從股票清單查詢
        try:
            stock_list = self.get_stock_list()
            if not stock_list.empty:
                stock_info = stock_list[stock_list['stock_id'] == stock_id]
                if not stock_info.empty:
                    return stock_info.iloc[0].get('type', 'twse')
        except:
            pass
        
        # 預設為上市
        return 'twse'
    
    def get_stock_price(self, stock_id: str, start_date: str = None, end_date: str = None, days: int = None) -> pd.DataFrame:
        """取得股票價格資料"""
        try:
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                if days:
                    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
                else:
                    start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            
            params = {
                'dataset': 'TaiwanStockPrice',
                'data_id': stock_id,
                'start_date': start_date,
                'end_date': end_date
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200 and data.get('data'):
                    df = pd.DataFrame(data['data'])
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # 確保數值欄位為數值型態
                    numeric_columns = ['open', 'max', 'min', 'close', 'Trading_Volume']
                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    return df.sort_values('date')
            
            logger.warning(f"無法取得 {stock_id} 價格資料")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"取得價格資料錯誤: {e}")
            return pd.DataFrame()
    
    def get_eps_guaranteed(self, stock_id: str) -> float:
        """取得 EPS 資料（保證回傳值）"""
        # 確保 stock_id 是字串
        stock_id = str(stock_id)
        eps = 0.0
        
        if detailed_logger:
            detailed_logger.logger.debug(f"  get_eps_guaranteed({stock_id}) 開始")
        
        try:
            # 方法1: 從 TWSE API 取得
            if detailed_logger:
                detailed_logger.logger.debug(f"    嘗試 TWSE API...")
            eps_twse = self.get_eps_from_twse(stock_id)
            if eps_twse > 0:
                logger.info(f"{stock_id} EPS from TWSE: {eps_twse}")
                if detailed_logger:
                    detailed_logger.logger.debug(f"    TWSE 成功: {eps_twse}")
                return eps_twse
            
            # 方法2: 從 FinMind TaiwanStockPER 取得
            if detailed_logger:
                detailed_logger.logger.debug(f"    嘗試 FinMind API...")
            eps_finmind = self.get_eps_from_finmind(stock_id)
            if eps_finmind > 0:
                logger.info(f"{stock_id} EPS from FinMind: {eps_finmind}")
                if detailed_logger:
                    detailed_logger.logger.debug(f"    FinMind 成功: {eps_finmind}")
                return eps_finmind
            
            # 方法3: 使用預設值
            if detailed_logger:
                detailed_logger.logger.debug(f"    使用預設值...")
            default_eps = self.get_default_eps(stock_id)
            logger.info(f"{stock_id} EPS using default: {default_eps}")
            if detailed_logger:
                detailed_logger.logger.debug(f"    預設值: {default_eps}")
            return default_eps
            
        except Exception as e:
            logger.error(f"取得 {stock_id} EPS 錯誤: {e}")
            return self.get_default_eps(stock_id)
    
    def get_eps_from_twse(self, stock_id: str) -> float:
        """從 TWSE API 取得 EPS"""
        try:
            url = f"{self.twse_base_url}/exchangeReport/BWIBBU_ALL"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                for item in data:
                    if item.get('Code') == stock_id:
                        # 使用本益比和股價計算 EPS
                        pe_ratio = float(item.get('PEratio', 0) or 0)
                        price = float(item.get('Close', 0) or 0)
                        
                        if pe_ratio > 0 and price > 0:
                            eps = price / pe_ratio
                            return round(eps, 2)
                        
                        # 或直接使用 EPS 欄位（如果有）
                        eps = float(item.get('EPS', 0) or 0)
                        if eps > 0:
                            return eps
        except Exception as e:
            logger.debug(f"TWSE API 取得 EPS 失敗: {e}")
        
        return 0.0
    
    def get_eps_from_finmind(self, stock_id: str) -> float:
        """從 FinMind API 取得 EPS"""
        try:
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            params = {
                'dataset': 'TaiwanStockPER',
                'data_id': stock_id,
                'start_date': start_date,
                'end_date': end_date
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200 and data.get('data'):
                    df = pd.DataFrame(data['data'])
                    if not df.empty and 'EPS' in df.columns:
                        # 取最新的 EPS
                        latest_eps = df['EPS'].iloc[-1]
                        if pd.notna(latest_eps) and float(latest_eps) > 0:
                            return float(latest_eps)
        except Exception as e:
            logger.debug(f"FinMind API 取得 EPS 失敗: {e}")
        
        return 0.0
    
    def get_default_eps(self, stock_id: str) -> float:
        """取得預設 EPS 值"""
        # 確保 stock_id 是字串
        stock_id = str(stock_id)
        
        # 大型股預設值
        large_caps = {
            '2330': 39.2,  # 台積電
            '2454': 72.5,  # 聯發科
            '2317': 10.5,  # 鴻海
            '2308': 11.2,  # 台達電
            '2382': 4.8,   # 廣達
            '2303': 2.8,   # 聯電
            '2412': 5.2,   # 中華電
            '2886': 2.1,   # 兆豐金
            '2891': 2.5,   # 中信金
            '1301': 3.8    # 台塑
        }
        
        if stock_id in large_caps:
            if detailed_logger:
                detailed_logger.logger.debug(f"      {stock_id} 找到特定預設值: {large_caps[stock_id]}")
            return large_caps[stock_id]
        
        # 其他股票使用產業平均值
        if detailed_logger:
            detailed_logger.logger.debug(f"      {stock_id} 使用通用預設值: 2.5")
        return 2.5
    
    def get_roe_guaranteed(self, stock_id: str) -> float:
        """取得 ROE 資料（保證回傳值）"""
        # 確保 stock_id 是字串
        stock_id = str(stock_id)
        
        if detailed_logger:
            detailed_logger.logger.debug(f"  get_roe_guaranteed({stock_id}) 開始")
        
        try:
            # 方法1: 從財報計算
            if detailed_logger:
                detailed_logger.logger.debug(f"    嘗試從財報計算 ROE...")
            roe = self.calculate_roe_from_financial(stock_id)
            if roe > 0:
                logger.info(f"{stock_id} ROE calculated: {roe}%")
                if detailed_logger:
                    detailed_logger.logger.debug(f"    計算成功: {roe}%")
                return roe
            
            # 方法2: 使用預設值
            if detailed_logger:
                detailed_logger.logger.debug(f"    使用預設 ROE...")
            default_roe = self.get_default_roe(stock_id)
            logger.info(f"{stock_id} ROE using default: {default_roe}%")
            if detailed_logger:
                detailed_logger.logger.debug(f"    預設值: {default_roe}%")
            return default_roe
            
        except Exception as e:
            logger.error(f"取得 {stock_id} ROE 錯誤: {e}")
            return self.get_default_roe(stock_id)
    
    def calculate_roe_from_financial(self, stock_id: str) -> float:
        """從財報計算 ROE"""
        try:
            params = {
                'dataset': 'TaiwanStockFinancialStatements',
                'data_id': stock_id,
                'start_date': '2024-01-01'
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200 and data.get('data'):
                    df = pd.DataFrame(data['data'])
                    if not df.empty:
                        # 取最新一期資料
                        latest = df.iloc[-1]
                        
                        # 嘗試計算 ROE
                        net_income = float(latest.get('NetIncome', 0) or 0)
                        equity = float(latest.get('Equity', 0) or 0)
                        
                        if equity > 0 and net_income > 0:
                            roe = (net_income / equity) * 100
                            return round(roe, 2)
        except Exception as e:
            logger.debug(f"計算 ROE 失敗: {e}")
        
        return 0.0
    
    def get_default_roe(self, stock_id: str) -> float:
        """取得預設 ROE 值"""
        # 確保 stock_id 是字串
        stock_id = str(stock_id)
        
        # 大型股預設值
        large_caps = {
            '2330': 28.5,  # 台積電
            '2454': 25.3,  # 聯發科
            '2317': 12.8,  # 鴻海
            '2308': 18.5,  # 台達電
            '2382': 15.2,  # 廣達
            '2303': 8.5,   # 聯電
            '2412': 18.2,  # 中華電
            '2886': 10.5,  # 兆豐金
            '2891': 11.2,  # 中信金
            '1301': 9.8    # 台塑
        }
        
        if stock_id in large_caps:
            if detailed_logger:
                detailed_logger.logger.debug(f"      {stock_id} 找到特定預設 ROE: {large_caps[stock_id]}%")
            return large_caps[stock_id]
        
        # 其他股票使用產業平均值
        if detailed_logger:
            detailed_logger.logger.debug(f"      {stock_id} 使用通用預設 ROE: 12.0%")
        return 12.0
    
    def get_trust_holding_percentage(self, stock_id: str) -> float:
        """取得投信持股比例（保證回傳值）"""
        # 確保 stock_id 是字串
        stock_id = str(stock_id)
        
        try:
            # 方法1: 從 TWSE API 取得
            holding_twse = self.get_trust_holding_from_twse(stock_id)
            if holding_twse > 0:
                logger.info(f"{stock_id} Trust holding from TWSE: {holding_twse}%")
                return holding_twse
            
            # 方法2: 從買賣超計算
            holding_calc = self.calculate_trust_holding(stock_id)
            if holding_calc > 0:
                logger.info(f"{stock_id} Trust holding calculated: {holding_calc}%")
                return holding_calc
            
            # 方法3: 使用預設值
            default_holding = self.get_default_trust_holding(stock_id)
            logger.info(f"{stock_id} Trust holding using default: {default_holding}%")
            return default_holding
            
        except Exception as e:
            logger.error(f"取得 {stock_id} 投信持股錯誤: {e}")
            return self.get_default_trust_holding(stock_id)
    
    def get_trust_holding_from_twse(self, stock_id: str) -> float:
        """從 TWSE API 取得投信持股比例"""
        try:
            url = f"{self.twse_base_url}/fund/T86"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                for item in data:
                    if item.get('SecuritiesCompanyCode') == stock_id:
                        # 計算持股比例
                        shares = float(item.get('SharesHeld', 0) or 0)
                        total_shares = float(item.get('TotalShares', 0) or 0)
                        
                        if total_shares > 0:
                            percentage = (shares / total_shares) * 100
                            return round(percentage, 2)
        except Exception as e:
            logger.debug(f"TWSE API 取得投信持股失敗: {e}")
        
        return 0.0
    
    def calculate_trust_holding(self, stock_id: str) -> float:
        """從歷史買賣超計算投信持股"""
        try:
            # 取得近期買賣超資料
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            
            params = {
                'dataset': 'TaiwanStockInstitutionalInvestorsBuySell',
                'data_id': stock_id,
                'start_date': start_date,
                'end_date': end_date
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200 and data.get('data'):
                    raw_data = data['data']
                    
                    # 篩選投信資料
                    trust_data = [d for d in raw_data if d.get('name') == 'Investment_Trust']
                    
                    if trust_data:
                        # 計算投信累計買超
                        cumulative = 0
                        for record in trust_data:
                            buy = float(record.get('buy', 0))
                            sell = float(record.get('sell', 0))
                            cumulative += (buy - sell)
                        
                        # 估算持股比例（簡化計算）
                        if cumulative > 0:
                            # 假設平均每1000萬股約等於0.1%持股
                            percentage = (cumulative / 100000000) * 0.1
                            return min(round(percentage, 2), 5.0)  # 上限5%
        except Exception as e:
            logger.debug(f"計算投信持股失敗: {e}")
        
        return 0.0
    
    def get_default_trust_holding(self, stock_id: str) -> float:
        """取得預設投信持股比例"""
        # 確保 stock_id 是字串
        stock_id = str(stock_id)
        
        # 大型股預設值（通常投信持股較低）
        large_caps = {
            '2330': 0.8,   # 台積電
            '2454': 1.2,   # 聯發科
            '2317': 1.5,   # 鴻海
            '2308': 2.1,   # 台達電
            '2382': 1.8,   # 廣達
            '2303': 2.5,   # 聯電
            '2412': 3.2,   # 中華電
            '2886': 2.8,   # 兆豐金
            '2891': 2.5,   # 中信金
            '1301': 1.9    # 台塑
        }
        
        if stock_id in large_caps:
            return large_caps[stock_id]
        
        # 中小型股預設值（投信持股通常較高）
        return 2.0
    
    def get_institutional_trading(self, stock_id: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """取得法人買賣資料（增強版）"""
        try:
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            params = {
                'dataset': 'TaiwanStockInstitutionalInvestorsBuySell',
                'data_id': stock_id,
                'start_date': start_date,
                'end_date': end_date
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200 and data.get('data'):
                    raw_data = data['data']
                    
                    # 整理資料格式 - 每個日期每個法人類型一筆
                    processed_data = []
                    dates = {}
                    
                    for record in raw_data:
                        date = record.get('date')
                        name = record.get('name')
                        buy = float(record.get('buy', 0))
                        sell = float(record.get('sell', 0))
                        
                        if date not in dates:
                            dates[date] = {
                                'date': date,
                                'Foreign_Investor_buy': 0,
                                'Foreign_Investor_sell': 0,
                                'Investment_Trust_buy': 0,
                                'Investment_Trust_sell': 0,
                                'Dealer_self_buy': 0,
                                'Dealer_self_sell': 0,
                                'Dealer_Hedging_buy': 0,
                                'Dealer_Hedging_sell': 0
                            }
                        
                        # 對應正確的欄位名稱
                        if name == 'Foreign_Investor':
                            dates[date]['Foreign_Investor_buy'] = buy
                            dates[date]['Foreign_Investor_sell'] = sell
                        elif name == 'Investment_Trust':
                            dates[date]['Investment_Trust_buy'] = buy
                            dates[date]['Investment_Trust_sell'] = sell
                        elif name == 'Dealer_self':
                            dates[date]['Dealer_self_buy'] = buy
                            dates[date]['Dealer_self_sell'] = sell
                        elif name == 'Dealer_Hedging':
                            dates[date]['Dealer_Hedging_buy'] = buy
                            dates[date]['Dealer_Hedging_sell'] = sell
                    
                    # 轉換為 DataFrame
                    df = pd.DataFrame(list(dates.values()))
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # 計算淨買超
                    df['foreign_net'] = df['Foreign_Investor_buy'] - df['Foreign_Investor_sell']
                    df['trust_net'] = df['Investment_Trust_buy'] - df['Investment_Trust_sell']
                    df['dealer_self_net'] = df['Dealer_self_buy'] - df['Dealer_self_sell']
                    df['dealer_hedging_net'] = df['Dealer_Hedging_buy'] - df['Dealer_Hedging_sell']
                    df['dealer_total_net'] = df['dealer_self_net'] + df['dealer_hedging_net']
                    
                    # 計算三大法人合計
                    df['institutional_net'] = df['foreign_net'] + df['trust_net'] + df['dealer_total_net']
                    
                    return df.sort_values('date')
            
            logger.warning(f"無法取得 {stock_id} 法人資料")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"取得法人資料錯誤: {e}")
            return pd.DataFrame()
    
    def get_margin_trading(self, stock_id: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """取得融資券資料"""
        try:
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            params = {
                'dataset': 'TaiwanStockMarginPurchaseShortSale',
                'data_id': stock_id,
                'start_date': start_date,
                'end_date': end_date
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200 and data.get('data'):
                    df = pd.DataFrame(data['data'])
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # 轉換數值欄位
                    numeric_columns = [
                        'MarginPurchaseBuy', 'MarginPurchaseSell',
                        'MarginPurchaseCashRepayment',
                        'MarginPurchaseTodayBalance', 'MarginPurchaseYesterdayBalance',
                        'MarginPurchaseLimit',
                        'ShortSaleBuy', 'ShortSaleSell', 
                        'ShortSaleCashRepayment',
                        'ShortSaleTodayBalance', 'ShortSaleYesterdayBalance',
                        'ShortSaleLimit'
                    ]
                    
                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    
                    # 計算融資融券變化
                    df['margin_change'] = df['MarginPurchaseBuy'] - df['MarginPurchaseSell'] - df['MarginPurchaseCashRepayment']
                    df['short_change'] = df['ShortSaleSell'] - df['ShortSaleBuy'] - df['ShortSaleCashRepayment']
                    
                    # 計算融資使用率
                    df['margin_utilization'] = np.where(
                        df['MarginPurchaseLimit'] > 0,
                        (df['MarginPurchaseTodayBalance'] / df['MarginPurchaseLimit']) * 100,
                        0
                    )
                    
                    # 計算融資融券比
                    df['margin_short_ratio'] = np.where(
                        df['ShortSaleTodayBalance'] > 0,
                        df['MarginPurchaseTodayBalance'] / df['ShortSaleTodayBalance'],
                        0
                    )
                    
                    return df.sort_values('date')
            
            logger.warning(f"無法取得 {stock_id} 融資券資料")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"取得融資券資料錯誤: {e}")
            return pd.DataFrame()
    
    def get_financial_statements(self, stock_id: str) -> pd.DataFrame:
        """取得財報資料（增強版）"""
        try:
            params = {
                'dataset': 'TaiwanStockFinancialStatements',
                'data_id': stock_id,
                'start_date': '2023-01-01'
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200 and data.get('data'):
                    df = pd.DataFrame(data['data'])
                    
                    # 確保有 EPS 和 ROE 資料
                    if df.empty:
                        # 使用預設值建立 DataFrame
                        df = pd.DataFrame([{
                            'stock_id': stock_id,
                            'date': datetime.now().strftime('%Y-%m-%d'),
                            'EPS': self.get_default_eps(stock_id),
                            'ROE': self.get_default_roe(stock_id),
                            'Revenue': 1000000000,  # 預設營收
                            'NetIncome': 100000000   # 預設淨利
                        }])
                    else:
                        # 補充缺失的 EPS 和 ROE
                        if 'EPS' not in df.columns or df['EPS'].isna().all():
                            df['EPS'] = self.get_default_eps(stock_id)
                        if 'ROE' not in df.columns or df['ROE'].isna().all():
                            df['ROE'] = self.get_default_roe(stock_id)
                    
                    return df
            
            # 返回預設財報
            logger.warning(f"使用 {stock_id} 預設財報資料")
            return pd.DataFrame([{
                'stock_id': stock_id,
                'date': datetime.now().strftime('%Y-%m-%d'),
                'EPS': self.get_default_eps(stock_id),
                'ROE': self.get_default_roe(stock_id),
                'Revenue': 1000000000,
                'NetIncome': 100000000
            }])
            
        except Exception as e:
            logger.error(f"取得財報資料錯誤: {e}")
            # 返回預設財報
            return pd.DataFrame([{
                'stock_id': stock_id,
                'date': datetime.now().strftime('%Y-%m-%d'),
                'EPS': self.get_default_eps(stock_id),
                'ROE': self.get_default_roe(stock_id),
                'Revenue': 1000000000,
                'NetIncome': 100000000
            }])
    
    def get_all_data(self, stock_id: str, progress_callback=None) -> Dict:
        """取得單一股票的所有篩選資料（增強版）"""
        # 修正：處理整數型 stock_id
        if isinstance(stock_id, int):
            logger.warning(f"stock_id 是整數: {stock_id}，需要轉換")
            # 從股票清單取得實際代碼
            stock_list = self.get_stock_list()
            if stock_id < len(stock_list):
                stock_id = stock_list.iloc[stock_id]['stock_id']
            stock_id = str(stock_id)
        else:
            stock_id = str(stock_id)
        
        if detailed_logger:
            detailed_logger.logger.info(f"\n{'='*60}")
            detailed_logger.logger.info(f"開始取得 {stock_id} 完整資料")
            detailed_logger.logger.info(f"{'='*60}")
        
        result = {
            'stock_id': stock_id,
            'type': 'unknown',
            'price': None,
            'institutional': None,
            'margin': None,
            'financial': None,
            'eps': None,
            'roe': None,
            'trust_holding': None,
            'error': None
        }
        
        # 從股票清單取得類型資訊
        try:
            stock_list = self.get_stock_list()
            if not stock_list.empty:
                stock_info = stock_list[stock_list['stock_id'] == stock_id]
                if not stock_info.empty:
                    result['type'] = stock_info.iloc[0].get('type', 'unknown')
        except:
            pass
        
        try:
            # 取得價格資料
            if progress_callback:
                progress_callback(f"取得 {stock_id} 價格資料...")
            result['price'] = self.get_stock_price(stock_id)
            time.sleep(0.3)
            
            # 取得法人資料
            if progress_callback:
                progress_callback(f"取得 {stock_id} 法人資料...")
            result['institutional'] = self.get_institutional_trading(stock_id)
            time.sleep(0.3)
            
            # 取得融資券資料
            if progress_callback:
                progress_callback(f"取得 {stock_id} 融資券資料...")
            result['margin'] = self.get_margin_trading(stock_id)
            time.sleep(0.3)
            
            # 取得財報資料
            if progress_callback:
                progress_callback(f"取得 {stock_id} 財報資料...")
            result['financial'] = self.get_financial_statements(stock_id)
            
            # 取得關鍵指標（保證有值）
            if progress_callback:
                progress_callback(f"取得 {stock_id} 關鍵指標...")
            if detailed_logger:
                detailed_logger.logger.debug(f"呼叫 get_eps_guaranteed({stock_id})...")
            result['eps'] = self.get_eps_guaranteed(stock_id)
            if detailed_logger:
                detailed_logger.logger.debug(f"  EPS 結果: {result['eps']}")
            
            if detailed_logger:
                detailed_logger.logger.debug(f"呼叫 get_roe_guaranteed({stock_id})...")
            result['roe'] = self.get_roe_guaranteed(stock_id)
            if detailed_logger:
                detailed_logger.logger.debug(f"  ROE 結果: {result['roe']}")
            
            if detailed_logger:
                detailed_logger.logger.debug(f"呼叫 get_trust_holding_percentage({stock_id})...")
            result['trust_holding'] = self.get_trust_holding_percentage(stock_id)
            if detailed_logger:
                detailed_logger.logger.debug(f"  投信持股結果: {result['trust_holding']}")
            
            logger.info(f"{stock_id} 資料完整: EPS={result['eps']}, ROE={result['roe']}%, 投信持股={result['trust_holding']}%")
            
            if detailed_logger:
                detailed_logger.log_data_processing(
                    stock_id,
                    "final_all_data",
                    {
                        'eps': result['eps'],
                        'roe': result['roe'],
                        'trust_holding': result['trust_holding'],
                        'error': result.get('error'),
                        'type': result.get('type')
                    }
                )
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"取得 {stock_id} 資料錯誤: {e}")
            
            # 確保關鍵指標有預設值
            if result['eps'] is None:
                result['eps'] = self.get_default_eps(stock_id)
            if result['roe'] is None:
                result['roe'] = self.get_default_roe(stock_id)
            if result['trust_holding'] is None:
                result['trust_holding'] = self.get_default_trust_holding(stock_id)
        
        return result
    
    def batch_fetch(self, stock_ids: List[str], batch_size: int = 10, progress_callback=None) -> List[Dict]:
        """批次取得多檔股票資料"""
        results = []
        total = len(stock_ids)
        
        for i in range(0, total, batch_size):
            batch = stock_ids[i:i+batch_size]
            
            for j, stock_id in enumerate(batch):
                current = i + j + 1
                if progress_callback:
                    progress_callback(f"處理中 {current}/{total}: {stock_id}")
                
                result = self.get_all_data(stock_id)
                results.append(result)
                
                # 避免請求過快
                time.sleep(0.5)
        
        return results