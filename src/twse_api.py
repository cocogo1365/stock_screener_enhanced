"""
Taiwan Stock Exchange (TWSE) OpenAPI 模組
提供即時資料查詢 (不需要 token)
"""
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import time
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class TWSEAPI:
    """TWSE OpenAPI 整合類別"""
    
    def __init__(self):
        """初始化 TWSE API"""
        self.base_url = "https://openapi.twse.com.tw/v1"
        
        # API 端點
        self.endpoints = {
            'stock_day_all': '/exchangeReport/STOCK_DAY_ALL',          # 當日全部股票行情
            'bwibbu_all': '/exchangeReport/BWIBBU_ALL',                # 全部股票本益比、殖利率及股價淨值比
            'mi_index': '/exchangeReport/MI_INDEX',                     # 大盤指數
            't187ap_30': '/fund/T86',                                   # 投信持股
            'fmsrfk': '/exchangeReport/FMSRFK',                        # 外資及陸資買賣超日報
        }
        
        # 快取設定
        self.cache = {}
        self.cache_duration = 300  # 5分鐘
        
    def _get_cache_key(self, endpoint: str) -> str:
        """產生快取鍵值"""
        return f"{endpoint}_{datetime.now().strftime('%Y%m%d_%H')}"
    
    def _request(self, endpoint: str, use_cache: bool = True) -> Optional[List[Dict]]:
        """
        發送請求
        
        Args:
            endpoint: API 端點
            use_cache: 是否使用快取
            
        Returns:
            API 回應資料
        """
        # 檢查快取
        if use_cache:
            cache_key = self._get_cache_key(endpoint)
            if cache_key in self.cache:
                logger.info(f"使用快取: {endpoint}")
                return self.cache[cache_key]
        
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # 儲存快取
                if use_cache and data:
                    cache_key = self._get_cache_key(endpoint)
                    self.cache[cache_key] = data
                    # 清理舊快取
                    self._clean_cache()
                
                logger.info(f"TWSE API 成功: {endpoint}, 取得 {len(data)} 筆資料")
                return data
            else:
                logger.warning(f"TWSE API 失敗: {endpoint}, HTTP {response.status_code}")
                
        except requests.exceptions.Timeout:
            logger.error(f"TWSE API 逾時: {endpoint}")
        except Exception as e:
            logger.error(f"TWSE API 錯誤: {endpoint}, {e}")
        
        return None
    
    def _clean_cache(self):
        """清理過期快取"""
        current_key = datetime.now().strftime('%Y%m%d_%H')
        keys_to_remove = []
        
        for key in self.cache:
            if not key.endswith(current_key):
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.cache[key]
    
    def get_daily_quote(self, stock_id: str = None) -> pd.DataFrame:
        """
        取得當日行情
        
        Args:
            stock_id: 股票代號 (可選)
            
        Returns:
            當日行情資料
        """
        data = self._request(self.endpoints['stock_day_all'])
        
        if data:
            df = pd.DataFrame(data)
            
            # 轉換欄位名稱
            column_mapping = {
                'Code': 'stock_id',
                'Name': 'stock_name',
                'ClosingPrice': 'close',
                'Change': 'change',
                'OpeningPrice': 'open',
                'HighestPrice': 'high',
                'LowestPrice': 'low',
                'TradeVolume': 'volume',
                'TradeValue': 'amount',
                'Transaction': 'transaction'
            }
            
            df = df.rename(columns=column_mapping)
            
            # 確保數值欄位
            numeric_cols = ['close', 'change', 'open', 'high', 'low', 
                          'volume', 'amount', 'transaction']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), 
                                          errors='coerce')
            
            # 篩選特定股票
            if stock_id and 'stock_id' in df.columns:
                df = df[df['stock_id'] == stock_id]
            
            return df
        
        return pd.DataFrame()
    
    def get_per_pbr_all(self, stock_id: str = None) -> pd.DataFrame:
        """
        取得本益比、淨值比、殖利率
        
        Args:
            stock_id: 股票代號 (可選)
            
        Returns:
            本益比等資料
        """
        data = self._request(self.endpoints['bwibbu_all'])
        
        if data:
            df = pd.DataFrame(data)
            
            # 轉換欄位名稱
            column_mapping = {
                'Code': 'stock_id',
                'Name': 'stock_name',
                'PEratio': 'per',
                'DividendYield': 'dividend_yield',
                'PBratio': 'pbr'
            }
            
            df = df.rename(columns=column_mapping)
            
            # 確保數值欄位
            numeric_cols = ['per', 'dividend_yield', 'pbr']
            for col in numeric_cols:
                if col in df.columns:
                    # 處理空值和特殊字元
                    df[col] = df[col].astype(str).str.replace('-', '0')
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # 篩選特定股票
            if stock_id and 'stock_id' in df.columns:
                df = df[df['stock_id'] == stock_id]
            
            return df
        
        return pd.DataFrame()
    
    def get_market_index(self) -> pd.DataFrame:
        """取得大盤指數"""
        data = self._request(self.endpoints['mi_index'])
        
        if data:
            df = pd.DataFrame(data)
            return df
        
        return pd.DataFrame()
    
    def get_foreign_trading(self) -> pd.DataFrame:
        """取得外資買賣超"""
        data = self._request(self.endpoints['fmsrfk'])
        
        if data:
            df = pd.DataFrame(data)
            
            # 處理數值欄位
            numeric_cols = df.select_dtypes(include=['object']).columns
            for col in numeric_cols:
                if col != 'Name' and col != 'Code':
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), 
                                          errors='coerce')
            
            return df
        
        return pd.DataFrame()
    
    def test_connection(self) -> bool:
        """測試連線"""
        try:
            data = self._request(self.endpoints['stock_day_all'], use_cache=False)
            if data and len(data) > 0:
                logger.info("TWSE API 連線成功")
                return True
        except Exception as e:
            logger.error(f"TWSE API 連線失敗: {e}")
        
        return False
    
    def get_realtime_info(self, stock_id: str) -> Dict:
        """
        取得即時資訊整合
        
        Args:
            stock_id: 股票代號
            
        Returns:
            整合的即時資訊
        """
        info = {
            'stock_id': stock_id,
            'timestamp': datetime.now().isoformat(),
            'price': None,
            'per': None,
            'pbr': None,
            'dividend_yield': None,
            'volume': None
        }
        
        # 取得當日行情
        quote_df = self.get_daily_quote(stock_id)
        if not quote_df.empty:
            row = quote_df.iloc[0]
            info['price'] = row.get('close')
            info['volume'] = row.get('volume')
            info['change'] = row.get('change')
        
        # 取得本益比等
        per_df = self.get_per_pbr_all(stock_id)
        if not per_df.empty:
            row = per_df.iloc[0]
            info['per'] = row.get('per', 0)
            info['pbr'] = row.get('pbr', 0)
            info['dividend_yield'] = row.get('dividend_yield', 0)
        
        return info