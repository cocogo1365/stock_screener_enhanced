"""
FinMind API v3/v4 整合模組
實作 v3 (無需 token) 和 v4 (需要 token) 的分離邏輯
"""
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
import json

logger = logging.getLogger(__name__)


class FinMindAPI:
    """FinMind API 整合類別"""
    
    # v3 資料集 (不需要 token)
    V3_DATASETS = {
        'TaiwanStockInfo',           # 股票基本資訊
        'TaiwanStockPrice',           # 股價資料
        'TaiwanStockPricePrediction', # 股價預測
        'TaiwanStockStatistics',      # 股票統計
    }
    
    # v4 資料集 (需要 token)
    V4_DATASETS = {
        'TaiwanStockPER',                              # 本益比
        'TaiwanStockMonthRevenue',                     # 月營收
        'TaiwanStockFinancialStatements',              # 財報
        'TaiwanStockInstitutionalInvestorsBuySell',    # 法人買賣
        'TaiwanStockMarginPurchaseShortSale',          # 融資融券
        'TaiwanStockDividend',                         # 股利
        'TaiwanStockBalanceSheet',                     # 資產負債表
        'TaiwanStockCashFlowsStatement',               # 現金流量表
    }
    
    def __init__(self, token: str = None):
        """
        初始化 API
        Args:
            token: API token (可選，從環境變數或參數取得)
        """
        # 嘗試從環境變數取得 token
        self.token = token or os.environ.get('FINMIND_API_TOKEN', '')
        
        # API 基礎 URL
        self.base_url_v3 = 'https://api.finmindtrade.com/api/v3/data'
        self.base_url_v4 = 'https://api.finmindtrade.com/api/v4/data'
        
        # 重試設定
        self.max_retries = 3
        self.retry_delay = 1  # 秒
        
        # 請求限流
        self.last_request_time = 0
        self.min_request_interval = 0.2  # 200ms
        
    def _throttle(self):
        """請求限流"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, params: Dict, headers: Dict = None, 
                     timeout: int = 30) -> Optional[Dict]:
        """
        發送請求並處理重試
        """
        self._throttle()
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url,
                    params=params,
                    headers=headers or {},
                    timeout=timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # v3 和 v4 回應格式不同
                    if 'status' in data:
                        # v4 格式
                        if data.get('status') == 200:
                            return data
                        else:
                            error_msg = data.get('msg', 'Unknown error')
                            logger.warning(f"API error: {error_msg}")
                            
                            # 如果是 token 錯誤，不重試
                            if 'token' in error_msg.lower():
                                return None
                    else:
                        # v3 格式
                        return {'status': 200, 'data': data}
                
                elif response.status_code == 429:
                    # Rate limit
                    logger.warning(f"Rate limited, waiting {2 ** attempt} seconds")
                    time.sleep(2 ** attempt)
                    
                else:
                    logger.warning(f"HTTP {response.status_code}: {response.text[:200]}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout (attempt {attempt + 1}/{self.max_retries})")
            except requests.exceptions.ConnectionError:
                logger.warning(f"Connection error (attempt {attempt + 1}/{self.max_retries})")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay * (attempt + 1))
        
        return None
    
    def v3(self, dataset: str, **kwargs) -> pd.DataFrame:
        """
        呼叫 v3 API (不需要 token)
        
        Args:
            dataset: 資料集名稱
            **kwargs: 其他參數 (data_id, start_date, end_date 等)
            
        Returns:
            pd.DataFrame: 查詢結果
        """
        if dataset not in self.V3_DATASETS:
            logger.warning(f"{dataset} 可能需要 token，建議使用 v4()")
            # 嘗試使用 v4
            return self.v4(dataset, **kwargs)
        
        params = {'dataset': dataset}
        params.update(kwargs)
        
        # 移除空值參數
        params = {k: v for k, v in params.items() if v is not None}
        
        data = self._make_request(self.base_url_v3, params)
        
        if data and data.get('status') == 200:
            df = pd.DataFrame(data.get('data', []))
            if not df.empty:
                logger.info(f"v3 API 成功: {dataset}, 取得 {len(df)} 筆資料")
            return df
        
        logger.warning(f"v3 API 失敗: {dataset}")
        return pd.DataFrame()
    
    def v4(self, dataset: str, **kwargs) -> pd.DataFrame:
        """
        呼叫 v4 API (需要 token)
        
        Args:
            dataset: 資料集名稱
            **kwargs: 其他參數 (data_id, start_date, end_date 等)
            
        Returns:
            pd.DataFrame: 查詢結果
        """
        if not self.token:
            logger.error(f"v4 API 需要 token: {dataset}")
            logger.info("請設定環境變數 FINMIND_API_TOKEN 或在初始化時傳入 token")
            return pd.DataFrame()
        
        params = {'dataset': dataset}
        params.update(kwargs)
        
        # 移除空值參數
        params = {k: v for k, v in params.items() if v is not None}
        
        headers = {'Authorization': f'Bearer {self.token}'}
        
        data = self._make_request(self.base_url_v4, params, headers)
        
        if data and data.get('status') == 200:
            df = pd.DataFrame(data.get('data', []))
            if not df.empty:
                logger.info(f"v4 API 成功: {dataset}, 取得 {len(df)} 筆資料")
            return df
        
        logger.warning(f"v4 API 失敗: {dataset}")
        return pd.DataFrame()
    
    def auto(self, dataset: str, **kwargs) -> pd.DataFrame:
        """
        自動選擇 v3 或 v4 API
        
        Args:
            dataset: 資料集名稱
            **kwargs: 其他參數
            
        Returns:
            pd.DataFrame: 查詢結果
        """
        # 判斷使用 v3 還是 v4
        if dataset in self.V3_DATASETS:
            return self.v3(dataset, **kwargs)
        else:
            return self.v4(dataset, **kwargs)
    
    def test_connection(self) -> Tuple[bool, bool]:
        """
        測試 v3 和 v4 連線
        
        Returns:
            (v3_ok, v4_ok): 連線狀態
        """
        # 測試 v3
        v3_ok = False
        try:
            df = self.v3('TaiwanStockInfo', data_id='2330')
            v3_ok = not df.empty
            if v3_ok:
                logger.info("v3 API 連線成功")
        except Exception as e:
            logger.error(f"v3 API 連線失敗: {e}")
        
        # 測試 v4
        v4_ok = False
        if self.token:
            try:
                df = self.v4('TaiwanStockPER', data_id='2330', 
                           start_date=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
                v4_ok = not df.empty
                if v4_ok:
                    logger.info("v4 API 連線成功")
            except Exception as e:
                logger.error(f"v4 API 連線失敗: {e}")
        else:
            logger.warning("未設定 token，跳過 v4 測試")
        
        return v3_ok, v4_ok
    
    def get_stock_list(self) -> pd.DataFrame:
        """取得股票清單 (使用 v3，不需 token)"""
        df = self.v3('TaiwanStockInfo')
        
        if not df.empty:
            # 篩選上市櫃股票
            if 'type' in df.columns:
                df = df[df['type'].isin(['twse', 'otc'])]
            
            # 篩選一般股票 (4位數字代碼)
            if 'stock_id' in df.columns:
                df = df[df['stock_id'].str.match(r'^\d{4}$', na=False)]
            
            logger.info(f"取得 {len(df)} 檔股票")
        
        return df
    
    def get_price(self, stock_id: str, start_date: str = None, 
                  end_date: str = None) -> pd.DataFrame:
        """取得股價資料 (使用 v3，不需 token)"""
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        
        df = self.v3('TaiwanStockPrice', 
                    data_id=stock_id,
                    start_date=start_date,
                    end_date=end_date)
        
        if not df.empty:
            # 確保日期格式
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
            
            # 確保數值欄位
            numeric_cols = ['open', 'max', 'min', 'close', 'Trading_Volume']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def get_institutional_trading(self, stock_id: str, 
                                 start_date: str = None,
                                 end_date: str = None) -> pd.DataFrame:
        """取得法人買賣 (使用 v4，需要 token)"""
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        df = self.v4('TaiwanStockInstitutionalInvestorsBuySell',
                    data_id=stock_id,
                    start_date=start_date,
                    end_date=end_date)
        
        return df
    
    def get_margin_trading(self, stock_id: str,
                          start_date: str = None,
                          end_date: str = None) -> pd.DataFrame:
        """取得融資融券 (使用 v4，需要 token)"""
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        df = self.v4('TaiwanStockMarginPurchaseShortSale',
                    data_id=stock_id,
                    start_date=start_date,
                    end_date=end_date)
        
        if not df.empty:
            # 確保數值欄位
            numeric_cols = [
                'MarginPurchaseBuy', 'MarginPurchaseSell',
                'MarginPurchaseTodayBalance', 'MarginPurchaseLimit',
                'ShortSaleBuy', 'ShortSaleSell',
                'ShortSaleTodayBalance', 'ShortSaleLimit'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df
    
    def get_revenue(self, stock_id: str, 
                   start_date: str = None) -> pd.DataFrame:
        """取得月營收 (使用 v4，需要 token)"""
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        df = self.v4('TaiwanStockMonthRevenue',
                    data_id=stock_id,
                    start_date=start_date)
        
        if not df.empty and 'revenue' in df.columns:
            df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
        
        return df
    
    def get_financial_statements(self, stock_id: str) -> pd.DataFrame:
        """取得財報 (使用 v4，需要 token)"""
        df = self.v4('TaiwanStockFinancialStatements',
                    data_id=stock_id,
                    start_date='2023-01-01')
        
        return df
    
    def get_per_pbr(self, stock_id: str,
                   start_date: str = None,
                   end_date: str = None) -> pd.DataFrame:
        """取得本益比/淨值比 (使用 v4，需要 token)"""
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        df = self.v4('TaiwanStockPER',
                    data_id=stock_id,
                    start_date=start_date,
                    end_date=end_date)
        
        if not df.empty:
            # 確保數值欄位
            numeric_cols = ['PER', 'PBR', 'dividend_yield']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df