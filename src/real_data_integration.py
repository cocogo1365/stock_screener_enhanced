"""
完整的真實數據整合模組
替換所有模擬數據為真實 FinMind API 資料
"""
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import time
import logging
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class RealDataIntegration:
    """
    真實數據整合器
    使用 FinMind API 和其他資料源取得所有真實數據
    """
    
    def __init__(self, api_token: str = None):
        """初始化真實數據整合器"""
        # FinMind API 設定
        if api_token:
            self.api_token = api_token
        else:
            # 從設定檔讀取
            try:
                with open('api_config.json', 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.api_token = config['finmind']['api_token']
            except:
                self.api_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wOC0xNyAxOTozNDo0MCIsInVzZXJfaWQiOiJ0MDg3ODA1NzExIiwiaXAiOiI5NC4xNTYuMTQ5Ljk0In0.7ukV7nG5f0oiQjAkmH0bye3NDqGi-_5DyI3nZfHto5g'
        
        self.base_url = 'https://api.finmindtrade.com/api/v4/data'
        self.headers = {'Authorization': f'Bearer {self.api_token}'}
        
        # 快取設定
        self.cache = {}
        self.cache_ttl = 3600  # 1小時快取
        
        # 特殊股票清單
        self.warning_stocks = []
        self.disposition_stocks = []
        self.last_special_update = None
    
    # ========== 1. 投信持股相關 ==========
    
    def get_trust_holding_percentage(self, stock_id: str) -> float:
        """
        取得投信持股比例
        資料集：TaiwanStockShareholding (股權分散表)
        """
        try:
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            params = {
                'dataset': 'TaiwanStockShareholding',
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
                    
                    # 嘗試不同的欄位名稱
                    trust_pct_columns = [
                        'InvestmentTrustShareholdingPercent',
                        'InvestmentTrust',
                        '投信持股比例'
                    ]
                    
                    for col in trust_pct_columns:
                        if col in df.columns:
                            # 取最新的持股比例
                            trust_pct = pd.to_numeric(df[col].iloc[-1], errors='coerce')
                            if pd.notna(trust_pct):
                                return trust_pct
            
            # 如果無法取得，嘗試從三大法人買賣計算
            return self._estimate_trust_holding_from_trading(stock_id)
            
        except Exception as e:
            logger.error(f"取得投信持股比例錯誤 {stock_id}: {e}")
            return 0.0
    
    def get_trust_holding_change(self, stock_id: str, days: int = 5) -> float:
        """
        計算投信持股變化
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days+10)
            
            params = {
                'dataset': 'TaiwanStockShareholding',
                'data_id': stock_id,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
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
                    
                    if len(df) >= 2:
                        # 計算變化
                        for col in ['InvestmentTrustShareholdingPercent', 'InvestmentTrust']:
                            if col in df.columns:
                                latest = pd.to_numeric(df[col].iloc[-1], errors='coerce')
                                previous = pd.to_numeric(df[col].iloc[0], errors='coerce')
                                if pd.notna(latest) and pd.notna(previous):
                                    return latest - previous
            
            # 備用：從買賣超推估
            return self._estimate_holding_change_from_trading(stock_id, days)
            
        except Exception as e:
            logger.error(f"取得投信持股變化錯誤 {stock_id}: {e}")
            return 0.0
    
    def _estimate_trust_holding_from_trading(self, stock_id: str) -> float:
        """從買賣超資料推估持股比例"""
        try:
            # 取得總股本
            stock_info = self._get_stock_info(stock_id)
            total_shares = stock_info.get('shares_outstanding', 1000000)  # 預設值
            
            # 取得近期買賣超累計
            end_date = datetime.now()
            start_date = end_date - timedelta(days=90)
            
            params = {
                'dataset': 'TaiwanStockInstitutionalInvestorsBuySell',
                'data_id': stock_id,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
            }
            
            response = requests.get(self.base_url, params=params, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    df = pd.DataFrame(data['data'])
                    # 累計投信淨買超
                    if 'Investment_Trust_Buy' in df.columns:
                        net_buy = df['Investment_Trust_Buy'].sum() - df.get('Investment_Trust_Sell', 0).sum()
                        # 估算持股比例
                        holding_pct = (net_buy / total_shares) * 100
                        return max(0, min(holding_pct, 20))  # 限制在0-20%之間
            
            return 0.0
        except:
            return 0.0
    
    def _estimate_holding_change_from_trading(self, stock_id: str, days: int) -> float:
        """從買賣超資料推估持股變化"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            params = {
                'dataset': 'TaiwanStockInstitutionalInvestorsBuySell',
                'data_id': stock_id,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
            }
            
            response = requests.get(self.base_url, params=params, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    df = pd.DataFrame(data['data'])
                    if 'Investment_Trust_Buy' in df.columns:
                        # 計算期間淨買超
                        net_buy = df['Investment_Trust_Buy'].sum() - df.get('Investment_Trust_Sell', 0).sum()
                        # 轉換為持股比例變化（假設總股本10億股）
                        change_pct = (net_buy / 1000000000) * 100
                        return change_pct
            
            return 0.0
        except:
            return 0.0
    
    # ========== 2. 財務指標（EPS & ROE）==========
    
    def get_real_eps(self, stock_id: str) -> float:
        """
        取得真實 EPS 資料
        資料集：TaiwanStockFinancialStatements
        """
        try:
            # 取得最近的財報資料
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            
            params = {
                'dataset': 'TaiwanStockFinancialStatements',
                'data_id': stock_id,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
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
                    
                    # 尋找 EPS 相關欄位
                    eps_types = ['基本每股盈餘（元）', 'EPS', '每股盈餘']
                    
                    for eps_type in eps_types:
                        eps_data = df[df['type'] == eps_type]
                        if not eps_data.empty:
                            # 取最近四季加總
                            if len(eps_data) >= 4:
                                eps = eps_data['value'].iloc[-4:].sum()
                            else:
                                eps = eps_data['value'].iloc[-1]
                            return float(eps)
            
            # 備用方案：使用本益比資料推算
            return self._get_eps_from_per(stock_id)
            
        except Exception as e:
            logger.error(f"取得 EPS 錯誤 {stock_id}: {e}")
            return 0.0
    
    def get_real_roe(self, stock_id: str) -> float:
        """
        取得真實 ROE 資料
        """
        try:
            params = {
                'dataset': 'TaiwanStockFinancialStatements',
                'data_id': stock_id,
                'start_date': (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
                'end_date': datetime.now().strftime('%Y-%m-%d')
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    df = pd.DataFrame(data['data'])
                    
                    # ROE 相關欄位
                    roe_types = ['ROE(A)－稅後報酬率', 'ROE', '權益報酬率', 'ROE(%)']
                    
                    for roe_type in roe_types:
                        roe_data = df[df['type'] == roe_type]
                        if not roe_data.empty:
                            roe = pd.to_numeric(roe_data['value'].iloc[-1], errors='coerce')
                            if pd.notna(roe):
                                return float(roe)
                    
                    # 手動計算 ROE
                    return self._calculate_roe_manually(df)
            
            return 0.0
            
        except Exception as e:
            logger.error(f"取得 ROE 錯誤 {stock_id}: {e}")
            return 0.0
    
    def _calculate_roe_manually(self, financial_df: pd.DataFrame) -> float:
        """手動計算 ROE = 淨利 / 股東權益"""
        try:
            # 尋找淨利
            net_income = 0
            for income_type in ['本期淨利（淨損）', '淨利', '稅後淨利']:
                income_data = financial_df[financial_df['type'] == income_type]
                if not income_data.empty:
                    net_income = pd.to_numeric(income_data['value'].iloc[-1], errors='coerce')
                    break
            
            # 尋找股東權益
            equity = 0
            for equity_type in ['權益總額', '股東權益總額', '歸屬於母公司業主之權益']:
                equity_data = financial_df[financial_df['type'] == equity_type]
                if not equity_data.empty:
                    equity = pd.to_numeric(equity_data['value'].iloc[-1], errors='coerce')
                    break
            
            if equity > 0 and pd.notna(net_income):
                roe = (net_income / equity) * 100
                return float(roe)
            
            return 0.0
        except:
            return 0.0
    
    def _get_eps_from_per(self, stock_id: str) -> float:
        """從本益比資料推算 EPS"""
        try:
            params = {
                'dataset': 'TaiwanStockPER',
                'data_id': stock_id,
                'start_date': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                'end_date': datetime.now().strftime('%Y-%m-%d')
            }
            
            response = requests.get(self.base_url, params=params, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    df = pd.DataFrame(data['data'])
                    if 'EPS' in df.columns:
                        eps = pd.to_numeric(df['EPS'].iloc[-1], errors='coerce')
                        if pd.notna(eps):
                            return float(eps)
            
            return 0.0
        except:
            return 0.0
    
    # ========== 3. 殖利率 ==========
    
    def get_real_dividend_yield(self, stock_id: str, current_price: float = None) -> float:
        """
        計算真實殖利率
        資料集：TaiwanStockDividend + TaiwanStockDividendResult
        """
        try:
            # 如果沒有提供當前股價，從價格資料取得
            if current_price is None:
                current_price = self._get_current_price(stock_id)
            
            if current_price <= 0:
                return 0.0
            
            # 取得股利資料
            params = {
                'dataset': 'TaiwanStockDividend',
                'data_id': stock_id,
                'start_date': str(datetime.now().year - 1),
                'end_date': str(datetime.now().year)
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    df = pd.DataFrame(data['data'])
                    
                    # 計算年度現金股利
                    cash_dividend = 0
                    
                    # 嘗試不同的欄位名稱
                    dividend_columns = [
                        'CashEarningsDistribution',  # 現金盈餘分配
                        'CashDividend',  # 現金股利
                        'cash_dividend',  # 小寫版本
                        '現金股利'
                    ]
                    
                    for col in dividend_columns:
                        if col in df.columns:
                            cash_dividend = pd.to_numeric(df[col].sum(), errors='coerce')
                            if pd.notna(cash_dividend) and cash_dividend > 0:
                                break
                    
                    # 計算殖利率
                    if cash_dividend > 0:
                        yield_rate = (cash_dividend / current_price) * 100
                        return float(yield_rate)
            
            # 備用：從股利發放結果取得
            return self._get_yield_from_dividend_result(stock_id, current_price)
            
        except Exception as e:
            logger.error(f"計算殖利率錯誤 {stock_id}: {e}")
            return 0.0
    
    def _get_yield_from_dividend_result(self, stock_id: str, current_price: float) -> float:
        """從股利發放結果計算殖利率"""
        try:
            params = {
                'dataset': 'TaiwanStockDividendResult',
                'data_id': stock_id,
                'start_date': str(datetime.now().year - 1),
                'end_date': str(datetime.now().year)
            }
            
            response = requests.get(self.base_url, params=params, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    df = pd.DataFrame(data['data'])
                    if 'stock_and_cache_dividend' in df.columns:
                        dividend = pd.to_numeric(df['stock_and_cache_dividend'].iloc[-1], errors='coerce')
                        if pd.notna(dividend) and current_price > 0:
                            return (dividend / current_price) * 100
            
            return 0.0
        except:
            return 0.0
    
    def _get_current_price(self, stock_id: str) -> float:
        """取得當前股價"""
        try:
            params = {
                'dataset': 'TaiwanStockPrice',
                'data_id': stock_id,
                'start_date': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
                'end_date': datetime.now().strftime('%Y-%m-%d')
            }
            
            response = requests.get(self.base_url, params=params, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    df = pd.DataFrame(data['data'])
                    if not df.empty and 'close' in df.columns:
                        return float(df['close'].iloc[-1])
            
            return 0.0
        except:
            return 0.0
    
    # ========== 4. 警示股與處置股 ==========
    
    def get_warning_stocks(self) -> List[str]:
        """
        取得警示股清單
        資料來源：證交所
        """
        try:
            warning_stocks = []
            
            # 證交所警示股 API
            twse_url = "https://www.twse.com.tw/rwd/zh/announcement/notice"
            params = {'response': 'json'}
            
            response = requests.get(twse_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    for row in data['data']:
                        if len(row) > 0:
                            stock_code = str(row[0]).strip()
                            if stock_code.isdigit() and len(stock_code) == 4:
                                warning_stocks.append(stock_code)
            
            # 櫃買中心警示股
            otc_url = "https://www.tpex.org.tw/web/regular_emerging/supervision/notice/notice_result.php?l=zh-tw"
            # 這裡可以加入 OTC 的處理邏輯
            
            return warning_stocks
            
        except Exception as e:
            logger.error(f"取得警示股清單錯誤: {e}")
            return []
    
    def get_disposition_stocks(self) -> List[str]:
        """
        取得處置股清單
        """
        try:
            disposition_stocks = []
            
            # 證交所處置股票 API
            twse_url = "https://www.twse.com.tw/rwd/zh/announcement/punish"
            params = {'response': 'json'}
            
            response = requests.get(twse_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    for row in data['data']:
                        if len(row) > 0:
                            stock_code = str(row[0]).strip()
                            if stock_code.isdigit() and len(stock_code) == 4:
                                disposition_stocks.append(stock_code)
            
            return disposition_stocks
            
        except Exception as e:
            logger.error(f"取得處置股清單錯誤: {e}")
            return []
    
    def is_warning_or_disposition(self, stock_id: str) -> Tuple[bool, bool]:
        """
        檢查是否為警示股或處置股
        返回: (is_warning, is_disposition)
        """
        # 更新特殊股票清單（每小時更新一次）
        if (self.last_special_update is None or 
            (datetime.now() - self.last_special_update).seconds > 3600):
            self.warning_stocks = self.get_warning_stocks()
            self.disposition_stocks = self.get_disposition_stocks()
            self.last_special_update = datetime.now()
        
        is_warning = stock_id in self.warning_stocks
        is_disposition = stock_id in self.disposition_stocks
        
        return is_warning, is_disposition
    
    # ========== 5. 連續漲停判斷 ==========
    
    def check_consecutive_limit_up(self, price_df: pd.DataFrame, stock_id: str, days: int = 3) -> Tuple[bool, int]:
        """
        正確判斷連續漲停
        台股漲停板規則：一般股票10%，處置股可能是5%
        """
        try:
            if price_df is None or price_df.empty or len(price_df) < days + 1:
                return False, 0
            
            # 取得該股票的漲跌幅限制
            limit = self._get_price_limit(stock_id)
            limit_threshold = limit - 0.1  # 容許0.1%誤差
            
            consecutive_count = 0
            max_consecutive = 0
            
            for i in range(len(price_df) - 1):
                curr_idx = -(len(price_df) - i - 1)
                prev_idx = curr_idx - 1
                
                if prev_idx < -len(price_df):
                    break
                
                prev_close = price_df['close'].iloc[prev_idx]
                curr_close = price_df['close'].iloc[curr_idx]
                curr_high = price_df['max'].iloc[curr_idx] if 'max' in price_df.columns else price_df['high'].iloc[curr_idx]
                
                if prev_close > 0:
                    # 計算漲幅
                    change_pct = ((curr_close - prev_close) / prev_close) * 100
                    
                    # 判斷是否漲停
                    # 條件：1. 漲幅接近限制 2. 收盤價接近最高價
                    is_limit_up = (
                        change_pct >= limit_threshold and
                        abs(curr_close - curr_high) < prev_close * 0.001
                    )
                    
                    if is_limit_up:
                        consecutive_count += 1
                        max_consecutive = max(max_consecutive, consecutive_count)
                    else:
                        consecutive_count = 0
            
            # 檢查最近的連續漲停
            recent_consecutive = 0
            for i in range(min(days, len(price_df) - 1)):
                idx = -i - 1
                prev_idx = idx - 1
                
                if prev_idx < -len(price_df):
                    break
                
                prev_close = price_df['close'].iloc[prev_idx]
                curr_close = price_df['close'].iloc[idx]
                curr_high = price_df['max'].iloc[idx] if 'max' in price_df.columns else price_df['high'].iloc[idx]
                
                if prev_close > 0:
                    change_pct = ((curr_close - prev_close) / prev_close) * 100
                    
                    if change_pct >= limit_threshold and abs(curr_close - curr_high) < prev_close * 0.001:
                        recent_consecutive += 1
                    else:
                        break
            
            # 如果最近連續漲停天數達到門檻，返回 False（要排除）
            is_not_consecutive_limit = recent_consecutive < days
            
            return is_not_consecutive_limit, recent_consecutive
            
        except Exception as e:
            logger.error(f"檢查連續漲停錯誤: {e}")
            return True, 0  # 發生錯誤時預設為非連續漲停
    
    def _get_price_limit(self, stock_id: str) -> float:
        """
        取得股票的漲跌幅限制
        """
        # 檢查是否為處置股
        _, is_disposition = self.is_warning_or_disposition(stock_id)
        
        if is_disposition:
            return 5.0  # 處置股通常是5%
        else:
            return 10.0  # 一般股票10%
    
    # ========== 6. 輔助方法 ==========
    
    def _get_stock_info(self, stock_id: str) -> Dict:
        """取得股票基本資訊"""
        try:
            params = {
                'dataset': 'TaiwanStockInfo',
                'data_id': stock_id
            }
            
            response = requests.get(self.base_url, params=params, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    return data['data'][0] if data['data'] else {}
            
            return {}
        except:
            return {}
    
    # ========== 7. 完整資料取得方法 ==========
    
    def get_complete_real_data(self, stock_id: str, price_df: pd.DataFrame = None) -> Dict:
        """
        取得股票的完整真實數據
        """
        logger.info(f"開始取得 {stock_id} 的真實數據...")
        
        real_data = {
            'stock_id': stock_id,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # 1. 投信持股相關
            real_data['trust_holding_pct'] = self.get_trust_holding_percentage(stock_id)
            real_data['trust_holding_change'] = self.get_trust_holding_change(stock_id)
            time.sleep(0.3)  # 避免請求過快
            
            # 2. 財務指標
            real_data['eps'] = self.get_real_eps(stock_id)
            real_data['roe'] = self.get_real_roe(stock_id)
            time.sleep(0.3)
            
            # 3. 殖利率
            current_price = None
            if price_df is not None and not price_df.empty:
                current_price = price_df['close'].iloc[-1]
            real_data['dividend_yield'] = self.get_real_dividend_yield(stock_id, current_price)
            time.sleep(0.3)
            
            # 4. 警示股/處置股
            is_warning, is_disposition = self.is_warning_or_disposition(stock_id)
            real_data['is_warning'] = is_warning
            real_data['is_disposition'] = is_disposition
            
            # 5. 連續漲停
            if price_df is not None:
                is_not_limit, limit_days = self.check_consecutive_limit_up(price_df, stock_id)
                real_data['is_not_consecutive_limit'] = is_not_limit
                real_data['consecutive_limit_days'] = limit_days
            else:
                real_data['is_not_consecutive_limit'] = True
                real_data['consecutive_limit_days'] = 0
            
            logger.info(f"成功取得 {stock_id} 的真實數據")
            
        except Exception as e:
            logger.error(f"取得 {stock_id} 真實數據時發生錯誤: {e}")
        
        return real_data


# 測試程式
if __name__ == "__main__":
    # 初始化
    integrator = RealDataIntegration()
    
    # 測試台積電
    test_stock = '2330'
    print(f"\n測試股票: {test_stock}")
    print("="*60)
    
    # 取得完整真實數據
    real_data = integrator.get_complete_real_data(test_stock)
    
    print("\n真實數據結果:")
    print("-"*60)
    print(f"投信持股比例: {real_data.get('trust_holding_pct', 0):.2f}%")
    print(f"投信持股變化: {real_data.get('trust_holding_change', 0):.2f}%")
    print(f"EPS: {real_data.get('eps', 0):.2f}")
    print(f"ROE: {real_data.get('roe', 0):.2f}%")
    print(f"殖利率: {real_data.get('dividend_yield', 0):.2f}%")
    print(f"是否為警示股: {'是' if real_data.get('is_warning') else '否'}")
    print(f"是否為處置股: {'是' if real_data.get('is_disposition') else '否'}")
    print(f"連續漲停天數: {real_data.get('consecutive_limit_days', 0)}天")