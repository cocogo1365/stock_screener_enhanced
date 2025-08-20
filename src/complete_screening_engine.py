"""
完整的股票篩選引擎 - 實現所有 25 個條件並顯示數值
使用真實數據取代所有模擬數據
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple
import logging
from datetime import datetime, timedelta
try:
    from real_data_integration_final import RealDataIntegrationFinal as RealDataIntegration
except ImportError:
    try:
        from .real_data_integration_final import RealDataIntegrationFinal as RealDataIntegration
    except ImportError:
        from real_data_integration import RealDataIntegration

logger = logging.getLogger(__name__)


class CompleteScreeningEngine:
    """完整的股票篩選引擎 - 25個條件全實現"""
    
    def __init__(self, parameters: Dict):
        """初始化篩選引擎"""
        self.params = parameters
        self.min_conditions = parameters.get('min_conditions_to_pass', 3)
        # 初始化真實數據整合器
        self.real_data_integrator = RealDataIntegration()
    
    def check_all_conditions(self, stock_data: Dict) -> Dict:
        """檢查所有篩選條件並返回數值"""
        results = {}
        values = {}  # 儲存實際數值
        
        # 修正：確保 stock_data 是字典
        if not isinstance(stock_data, dict):
            logger.error(f"stock_data 不是字典: {type(stock_data)}")
            return {'matched_count': 0, 'passed': False, 'values': {}}
        
        # 設定當前股票ID供真實數據查詢使用
        self._current_stock_id = stock_data.get('stock_id')
        logger.debug(f"檢查 {self._current_stock_id} - EPS={stock_data.get('eps')}, ROE={stock_data.get('roe')}")
        
        # 取得增強版資料（保證有值）
        self._eps = stock_data.get('eps', 0)
        self._roe = stock_data.get('roe', 0)
        self._trust_holding = stock_data.get('trust_holding', 0)
        
        # 為 DataFrame 加上 stock_id 屬性
        price_df = stock_data.get('price')
        inst_df = stock_data.get('institutional')
        margin_df = stock_data.get('margin')
        
        if inst_df is not None and hasattr(inst_df, 'attrs'):
            inst_df.attrs['stock_id'] = self._current_stock_id
        
        # ========== 市場條件 ==========
        # 0. 市場條件（上市/上櫃）
        if self.params.get('market_twse'):
            stock_type = stock_data.get('type', 'unknown')
            results['market_twse'] = stock_type == 'twse'
            values['market_twse'] = f"市場: {stock_type}"
        
        if self.params.get('market_otc'):
            stock_type = stock_data.get('type', 'unknown')
            results['market_otc'] = stock_type == 'otc'
            values['market_otc'] = f"市場: {stock_type}"
        
        # ========== 成交量條件 (3個) ==========
        # 1. 成交量爆量1.2倍
        if self.params.get('volume_surge1', {}).get('enabled'):
            threshold = self.params['volume_surge1']['value']
            passed, ratio = self.check_volume_surge_with_value(price_df, threshold, days=5)
            results['volume_surge_1_5x'] = passed
            values['volume_surge_1_5x'] = f"爆量倍數: {ratio:.2f}x (門檻: {threshold}x)"
        
        # 2. 成交量爆量3倍（20日均量）
        if self.params.get('volume_surge2', {}).get('enabled'):
            threshold = self.params['volume_surge2']['value']
            passed, ratio = self.check_volume_surge_with_value(price_df, threshold, days=20)
            results['volume_surge_20d_3x'] = passed
            values['volume_surge_20d_3x'] = f"20日爆量: {ratio:.2f}x (門檻: {threshold}x)"
        
        # 3. 成交量爆量5倍（60日均量）
        if self.params.get('volume_surge3', {}).get('enabled'):
            threshold = self.params['volume_surge3']['value']
            passed, ratio = self.check_volume_surge_with_value(price_df, threshold, days=60)
            results['volume_surge_60d_5x'] = passed
            values['volume_surge_60d_5x'] = f"60日爆量: {ratio:.2f}x (門檻: {threshold}x)"
        
        # 4. 最低成交量條件
        if self.params.get('min_volume', {}).get('enabled'):
            threshold = self.params['min_volume']['value']
            passed, volume = self.check_min_volume_with_value(price_df, threshold)
            results['min_volume'] = passed
            volume_lots = volume / 1000 if volume else 0
            values['min_volume'] = f"成交量: {volume_lots:.0f}張 (門檻: {threshold}張)"
        
        # ========== 技術指標條件 (4個) ==========
        # 5. 日KD黃金交叉
        if self.params.get('daily_kd_golden'):
            passed, k_value, d_value = self.check_kd_golden_with_value(price_df)
            results['daily_kd_golden'] = passed
            if k_value is not None and d_value is not None:
                values['daily_kd_golden'] = f"K={k_value:.1f}, D={d_value:.1f}"
            else:
                values['daily_kd_golden'] = "K=N/A, D=N/A"
        
        # 6. 月KD黃金交叉
        if self.params.get('monthly_kd_golden'):
            passed, k_value, d_value = self.check_monthly_kd_golden_with_value(price_df)
            results['monthly_kd_golden'] = passed
            if k_value is not None and d_value is not None:
                values['monthly_kd_golden'] = f"月K={k_value:.1f}, 月D={d_value:.1f}"
            else:
                values['monthly_kd_golden'] = "月K=N/A, 月D=N/A"
        
        # 7. 站上MA20
        if self.params.get('above_ma20'):
            passed, price, ma20 = self.check_above_ma20_with_value(price_df)
            results['above_ma20'] = passed
            if price is not None and ma20 is not None:
                values['above_ma20'] = f"價格: {price:.1f}, MA20: {ma20:.1f}"
            else:
                values['above_ma20'] = "價格: N/A, MA20: N/A"
        
        # 8. 突破60日高點
        if self.params.get('break_60d_high'):
            passed, price, high_60d = self.check_break_60d_high_with_value(price_df)
            results['break_60d_high'] = passed
            if price is not None and high_60d is not None:
                values['break_60d_high'] = f"價格: {price:.1f}, 60日高: {high_60d:.1f}"
            else:
                values['break_60d_high'] = "價格: N/A, 60日高: N/A"
        
        # ========== 法人籌碼條件 (5個) ==========
        # 9. 投信買超
        if self.params.get('trust_buy', {}).get('enabled'):
            threshold = self.params['trust_buy']['value']
            passed, buy_amount = self.check_trust_buy_with_value(inst_df, threshold)
            results['trust_buy'] = passed
            values['trust_buy'] = f"投信買超: {buy_amount:.0f}張 (門檻: {threshold}張)"
        
        # 10. 投信持股比例
        if self.params.get('trust_pct', {}).get('enabled'):
            threshold = self.params['trust_pct']['value']
            passed, pct = self.check_trust_pct_with_value(inst_df, threshold)
            results['trust_pct'] = passed
            values['trust_pct'] = f"投信持股: {pct:.2f}% (門檻: {threshold}%)"
        
        # 11. 投信5日累計買超
        if self.params.get('trust_5d', {}).get('enabled'):
            threshold = self.params['trust_5d']['value']
            passed, total_buy = self.check_trust_5d_with_value(inst_df, threshold)
            results['trust_5d'] = passed
            values['trust_5d'] = f"投信5日買超: {total_buy:.0f}張 (門檻: {threshold}張)"
        
        # 12. 投信持股增加
        if self.params.get('trust_holding', {}).get('enabled'):
            threshold = self.params['trust_holding']['value']
            passed, change = self.check_trust_holding_with_value(inst_df, threshold)
            results['trust_holding'] = passed
            values['trust_holding'] = f"投信持股變化: {change:.2f}% (門檻: {threshold}%)"
        
        # 13. 三大法人5日買超
        if self.params.get('inst_5d', {}).get('enabled'):
            threshold = self.params['inst_5d']['value']
            passed, total_buy = self.check_inst_5d_with_value(inst_df, threshold)
            results['inst_5d'] = passed
            values['inst_5d'] = f"法人5日買超: {total_buy:.0f}張 (門檻: {threshold}張)"
        
        # ========== 融資融券條件 (2個) ==========
        # 14. 融資使用率
        if self.params.get('margin_ratio', {}).get('enabled'):
            threshold = self.params['margin_ratio']['value']
            passed, ratio = self.check_margin_ratio_with_value(margin_df, threshold)
            results['margin_ratio'] = passed
            values['margin_ratio'] = f"融資使用率: {ratio:.2f}% (門檻: <{threshold}%)"
        
        # 15. 融資5日增加
        if self.params.get('margin_5d', {}).get('enabled'):
            threshold = self.params['margin_5d']['value']
            passed, change = self.check_margin_5d_with_value(margin_df, threshold)
            results['margin_5d'] = passed
            values['margin_5d'] = f"融資5日增減: {change:.0f}張 (門檻: {threshold}張)"
        
        # ========== 基本面條件 (5個) ==========
        # 16. EPS
        if self.params.get('eps', {}).get('enabled'):
            threshold = self.params['eps']['value']
            passed, eps = self.check_eps_with_value(stock_data, threshold)
            results['eps'] = passed
            values['eps'] = f"EPS: {eps:.2f} (門檻: >{threshold})"
        
        # 17. ROE
        if self.params.get('roe', {}).get('enabled'):
            threshold = self.params['roe']['value']
            passed, roe = self.check_roe_with_value(stock_data, threshold)
            results['roe'] = passed
            values['roe'] = f"ROE: {roe:.2f}% (門檻: >{threshold}%)"
        
        # 18. 殖利率
        if self.params.get('yield', {}).get('enabled'):
            threshold = self.params['yield']['value']
            passed, yield_rate = self.check_yield_with_value(stock_data, price_df, threshold)
            results['yield'] = passed
            values['yield'] = f"殖利率: {yield_rate:.2f}% (門檻: >{threshold}%)"
        
        # ========== 漲跌幅控制 (2個) ==========
        # 19. 日漲跌幅
        if self.params.get('daily_change', {}).get('enabled'):
            threshold = self.params['daily_change']['value']
            passed, change = self.check_daily_change_with_value(price_df, threshold)
            results['daily_change'] = passed
            values['daily_change'] = f"日漲跌: {change:.2f}% (門檻: ±{threshold}%)"
        
        # 20. 5日累計漲跌幅
        if self.params.get('change_5d', {}).get('enabled'):
            threshold = self.params['change_5d']['value']
            passed, change = self.check_5d_change_with_value(price_df, threshold)
            results['change_5d'] = passed
            values['change_5d'] = f"5日漲跌: {change:.2f}% (門檻: ±{threshold}%)"
        
        # ========== 排除條件 (3個) ==========
        # 21. 排除警示股
        if self.params.get('exclude_warning'):
            passed = self.check_not_warning(stock_data)
            results['not_warning'] = passed
            values['not_warning'] = "非警示股" if passed else "警示股"
        
        # 22. 排除處置股
        if self.params.get('exclude_disposition'):
            passed = self.check_not_disposition(stock_data)
            results['not_disposition'] = passed
            values['not_disposition'] = "非處置股" if passed else "處置股"
        
        # 23. 排除連續漲停
        if self.params.get('exclude_limit_up', {}).get('enabled'):
            days = self.params['exclude_limit_up'].get('days', 3)
            passed, limit_days = self.check_not_limit_up_with_value(price_df, days)
            results['not_limit_up'] = passed
            values['not_limit_up'] = f"連續漲停: {limit_days}天 (門檻: <{days}天)"
        
        # 計算符合條件數
        matched_count = sum(1 for v in results.values() if v == True)
        results['matched_count'] = matched_count
        results['passed'] = matched_count >= self.min_conditions
        results['values'] = values  # 加入數值資訊
        
        return results
    
    # ========== 成交量相關檢查方法 ==========
    def check_volume_surge_with_value(self, price_df, threshold: float, days: int = 5) -> Tuple[bool, float]:
        """檢查爆量條件並返回實際倍數"""
        try:
            if price_df is None or price_df.empty or len(price_df) < days + 1:
                return False, 0.0
            
            if 'Trading_Volume' not in price_df.columns:
                return False, 0.0
            
            # 計算N日平均成交量（不含今日）
            avg_volume = price_df['Trading_Volume'].tail(days + 1).iloc[:-1].mean()
            latest_volume = price_df['Trading_Volume'].iloc[-1]
            
            if avg_volume > 0:
                surge_ratio = latest_volume / avg_volume
                return surge_ratio >= threshold, surge_ratio
            
            return False, 0.0
            
        except Exception as e:
            logger.error(f"檢查爆量條件錯誤: {e}")
            return False, 0.0
    
    def check_min_volume_with_value(self, price_df, threshold: int) -> Tuple[bool, float]:
        """檢查最低成交量並返回實際成交量"""
        try:
            if price_df is None or price_df.empty:
                return False, 0
            
            if 'Trading_Volume' not in price_df.columns:
                return False, 0
            
            latest_volume = price_df['Trading_Volume'].iloc[-1]
            threshold_shares = threshold * 1000  # 轉換為股數
            return latest_volume >= threshold_shares, latest_volume
            
        except Exception:
            return False, 0
    
    # ========== 技術指標檢查方法 ==========
    def check_kd_golden_with_value(self, price_df) -> Tuple[bool, float, float]:
        """檢查日KD黃金交叉並返回K、D值"""
        try:
            if price_df is None or price_df.empty or len(price_df) < 9:
                return False, None, None
            
            # 計算KD指標
            high_9 = price_df['max'].rolling(window=9).max()
            low_9 = price_df['min'].rolling(window=9).min()
            
            rsv = (price_df['close'] - low_9) / (high_9 - low_9) * 100
            rsv = rsv.fillna(50)
            
            k = rsv.ewm(com=2, adjust=False).mean()
            d = k.ewm(com=2, adjust=False).mean()
            
            if len(k) >= 2 and len(d) >= 2:
                curr_k = k.iloc[-1]
                curr_d = d.iloc[-1]
                prev_k = k.iloc[-2]
                prev_d = d.iloc[-2]
                
                # 黃金交叉：K由下往上穿過D
                golden_cross = (prev_k <= prev_d) and (curr_k > curr_d)
                return golden_cross, curr_k, curr_d
            
            return False, None, None
            
        except Exception as e:
            logger.error(f"計算KD指標錯誤: {e}")
            return False, None, None
    
    def check_monthly_kd_golden_with_value(self, price_df) -> Tuple[bool, float, float]:
        """檢查月KD黃金交叉（使用20日週期）"""
        try:
            if price_df is None or price_df.empty or len(price_df) < 20:
                return False, None, None
            
            # 使用20日週期計算月KD
            high_20 = price_df['max'].rolling(window=20).max()
            low_20 = price_df['min'].rolling(window=20).min()
            
            rsv = (price_df['close'] - low_20) / (high_20 - low_20) * 100
            rsv = rsv.fillna(50)
            
            k = rsv.ewm(com=2, adjust=False).mean()
            d = k.ewm(com=2, adjust=False).mean()
            
            if len(k) >= 2 and len(d) >= 2:
                curr_k = k.iloc[-1]
                curr_d = d.iloc[-1]
                prev_k = k.iloc[-2]
                prev_d = d.iloc[-2]
                
                golden_cross = (prev_k <= prev_d) and (curr_k > curr_d)
                return golden_cross, curr_k, curr_d
            
            return False, None, None
            
        except Exception:
            return False, None, None
    
    def check_above_ma20_with_value(self, price_df) -> Tuple[bool, float, float]:
        """檢查是否站上MA20並返回價格和MA20值"""
        try:
            if price_df is None or price_df.empty or len(price_df) < 20:
                return False, None, None
            
            ma20 = price_df['close'].rolling(window=20).mean().iloc[-1]
            latest_price = price_df['close'].iloc[-1]
            
            return latest_price > ma20, latest_price, ma20
            
        except Exception:
            return False, None, None
    
    def check_break_60d_high_with_value(self, price_df) -> Tuple[bool, float, float]:
        """檢查是否突破60日高點"""
        try:
            if price_df is None or price_df.empty or len(price_df) < 60:
                return False, None, None
            
            high_60d = price_df['max'].tail(60).max()
            latest_price = price_df['close'].iloc[-1]
            
            return latest_price >= high_60d, latest_price, high_60d
            
        except Exception:
            return False, None, None
    
    # ========== 法人籌碼檢查方法 ==========
    def check_trust_buy_with_value(self, inst_df, threshold: int) -> Tuple[bool, float]:
        """檢查投信買超並返回實際買超張數"""
        try:
            if inst_df is None or inst_df.empty:
                return False, 0
            
            latest = inst_df.iloc[-1]
            
            # 使用修正後的欄位名稱
            net_buy = 0
            
            if 'trust_net' in inst_df.columns:
                # 如果已經計算好淨買超
                net_buy = pd.to_numeric(latest.get('trust_net', 0), errors='coerce')
            elif 'Investment_Trust_buy' in inst_df.columns and 'Investment_Trust_sell' in inst_df.columns:
                # 使用小寫欄位名
                buy = pd.to_numeric(latest.get('Investment_Trust_buy', 0), errors='coerce')
                sell = pd.to_numeric(latest.get('Investment_Trust_sell', 0), errors='coerce')
                net_buy = buy - sell
            
            net_buy = net_buy / 1000  # 轉換為張數
            
            return net_buy >= threshold, net_buy
            
        except Exception:
            return False, 0
    
    def check_trust_pct_with_value(self, inst_df, threshold: float) -> Tuple[bool, float]:
        """檢查投信持股比例 - 使用真實數據"""
        try:
            # 從 inst_df 取得 stock_id
            stock_id = inst_df.attrs.get('stock_id') if hasattr(inst_df, 'attrs') else None
            if stock_id:
                # 使用真實數據
                pct = self.real_data_integrator.get_trust_holding_percentage(stock_id)
                return pct >= threshold, pct
            else:
                # 如果無法取得 stock_id，嘗試從快取取得
                if hasattr(self, '_current_stock_id'):
                    pct = self.real_data_integrator.get_trust_holding_percentage(self._current_stock_id)
                    return pct >= threshold, pct
            return False, 0
        except Exception:
            return False, 0
    
    def check_trust_5d_with_value(self, inst_df, threshold: int) -> Tuple[bool, float]:
        """檢查投信5日累計買超"""
        try:
            if inst_df is None or inst_df.empty or len(inst_df) < 5:
                return False, 0
            
            # 取最近5天的資料
            recent_5d = inst_df.tail(5)
            
            total_buy = 0
            total_sell = 0
            
            if 'Investment_Trust_Buy' in inst_df.columns:
                total_buy = recent_5d['Investment_Trust_Buy'].sum()
            if 'Investment_Trust_Sell' in inst_df.columns:
                total_sell = recent_5d['Investment_Trust_Sell'].sum()
            
            net_buy = (total_buy - total_sell) / 1000  # 轉換為張數
            
            return net_buy >= threshold, net_buy
            
        except Exception:
            return False, 0
    
    def check_trust_holding_with_value(self, inst_df, threshold: float) -> Tuple[bool, float]:
        """檢查投信持股變化 - 使用增強版數據"""
        try:
            # 優先使用增強版資料（持股比例）
            if hasattr(self, '_trust_holding'):
                holding = self._trust_holding
                # 檢查投信持股比例是否超過門檻
                return holding >= threshold, holding
            
            # 備用：從真實數據整合器取得變化量
            stock_id = inst_df.attrs.get('stock_id') if hasattr(inst_df, 'attrs') else None
            if stock_id:
                change = self.real_data_integrator.get_trust_holding_change(stock_id, days=5)
                return change >= threshold, change
            elif hasattr(self, '_current_stock_id'):
                change = self.real_data_integrator.get_trust_holding_change(self._current_stock_id, days=5)
                return change >= threshold, change
            return False, 0
        except Exception:
            return False, 0
    
    def check_inst_5d_with_value(self, inst_df, threshold: int) -> Tuple[bool, float]:
        """檢查三大法人5日累計買超"""
        try:
            if inst_df is None or inst_df.empty or len(inst_df) < 5:
                return False, 0
            
            recent_5d = inst_df.tail(5)
            
            total_buy = 0
            total_sell = 0
            
            # 外資
            if 'Foreign_Investor_Buy' in inst_df.columns:
                total_buy += recent_5d['Foreign_Investor_Buy'].sum()
            if 'Foreign_Investor_Sell' in inst_df.columns:
                total_sell += recent_5d['Foreign_Investor_Sell'].sum()
            
            # 投信
            if 'Investment_Trust_Buy' in inst_df.columns:
                total_buy += recent_5d['Investment_Trust_Buy'].sum()
            if 'Investment_Trust_Sell' in inst_df.columns:
                total_sell += recent_5d['Investment_Trust_Sell'].sum()
            
            # 自營商
            if 'Dealer_Buy' in inst_df.columns:
                total_buy += recent_5d['Dealer_Buy'].sum()
            if 'Dealer_Sell' in inst_df.columns:
                total_sell += recent_5d['Dealer_Sell'].sum()
            
            net_buy = (total_buy - total_sell) / 1000  # 轉換為張數
            
            return net_buy >= threshold, net_buy
            
        except Exception:
            return False, 0
    
    # ========== 融資融券檢查方法 ==========
    def check_margin_ratio_with_value(self, margin_df, threshold: float) -> Tuple[bool, float]:
        """檢查融資使用率"""
        try:
            if margin_df is None or margin_df.empty:
                return False, 0
            
            latest = margin_df.iloc[-1]
            
            # 融資使用率 = 融資餘額 / 融資限額
            if 'MarginPurchaseTotalBalance' in margin_df.columns and 'MarginPurchaseLimit' in margin_df.columns:
                balance = pd.to_numeric(latest.get('MarginPurchaseTotalBalance', 0), errors='coerce')
                limit = pd.to_numeric(latest.get('MarginPurchaseLimit', 1), errors='coerce')
                
                if limit > 0:
                    ratio = (balance / limit) * 100
                    return ratio < threshold, ratio
            
            return False, 0
            
        except Exception:
            return False, 0
    
    def check_margin_5d_with_value(self, margin_df, threshold: int) -> Tuple[bool, float]:
        """檢查融資5日增減"""
        try:
            if margin_df is None or margin_df.empty or len(margin_df) < 5:
                return False, 0
            
            if 'MarginPurchaseTotalBalance' in margin_df.columns:
                current = pd.to_numeric(margin_df.iloc[-1].get('MarginPurchaseTotalBalance', 0), errors='coerce')
                past_5d = pd.to_numeric(margin_df.iloc[-5].get('MarginPurchaseTotalBalance', 0), errors='coerce')
                
                change = (current - past_5d) / 1000  # 轉換為張數
                return change >= threshold, change
            
            return False, 0
            
        except Exception:
            return False, 0
    
    # ========== 基本面檢查方法 ==========
    def check_eps_with_value(self, stock_data, threshold: float) -> Tuple[bool, float]:
        """檢查EPS - 使用增強版數據"""
        try:
            # 優先使用增強版資料
            eps = stock_data.get('eps')
            if eps is not None:
                return eps > threshold, eps
            
            # 備用：從真實數據整合器取得
            stock_id = stock_data.get('stock_id')
            if stock_id:
                eps = self.real_data_integrator.get_real_eps(stock_id)
                return eps > threshold, eps
            return False, 0
        except Exception:
            return False, 0
    
    def check_roe_with_value(self, stock_data, threshold: float) -> Tuple[bool, float]:
        """檢查ROE - 使用增強版數據"""
        try:
            # 優先使用增強版資料
            roe = stock_data.get('roe')
            if roe is not None:
                return roe > threshold, roe
            
            # 備用：從真實數據整合器取得
            stock_id = stock_data.get('stock_id')
            if stock_id:
                roe = self.real_data_integrator.get_real_roe(stock_id)
                return roe > threshold, roe
            return False, 0
        except Exception:
            return False, 0
    
    def check_yield_with_value(self, stock_data, price_df, threshold: float) -> Tuple[bool, float]:
        """檢查殖利率 - 使用真實數據"""
        try:
            stock_id = stock_data.get('stock_id')
            if stock_id:
                current_price = price_df['close'].iloc[-1] if price_df is not None and not price_df.empty else None
                yield_rate = self.real_data_integrator.get_real_dividend_yield(stock_id, current_price)
                return yield_rate > threshold, yield_rate
            return False, 0
        except Exception:
            return False, 0
    
    # ========== 漲跌幅檢查方法 ==========
    def check_daily_change_with_value(self, price_df, threshold: float) -> Tuple[bool, float]:
        """檢查日漲跌幅"""
        try:
            if price_df is None or price_df.empty or len(price_df) < 2:
                return False, 0
            
            latest_close = price_df['close'].iloc[-1]
            prev_close = price_df['close'].iloc[-2]
            
            if prev_close > 0:
                change_pct = ((latest_close - prev_close) / prev_close) * 100
                return abs(change_pct) <= threshold, change_pct
            
            return False, 0
            
        except Exception:
            return False, 0
    
    def check_5d_change_with_value(self, price_df, threshold: float) -> Tuple[bool, float]:
        """檢查5日累計漲跌幅"""
        try:
            if price_df is None or price_df.empty or len(price_df) < 5:
                return False, 0
            
            latest_close = price_df['close'].iloc[-1]
            close_5d_ago = price_df['close'].iloc[-5]
            
            if close_5d_ago > 0:
                change_pct = ((latest_close - close_5d_ago) / close_5d_ago) * 100
                return abs(change_pct) <= threshold, change_pct
            
            return False, 0
            
        except Exception:
            return False, 0
    
    # ========== 排除條件檢查方法 ==========
    def check_not_warning(self, stock_data) -> bool:
        """檢查是否為警示股 - 使用真實數據"""
        try:
            stock_id = stock_data.get('stock_id')
            if stock_id:
                is_warning, _ = self.real_data_integrator.is_warning_or_disposition(stock_id)
                return not is_warning  # 返回是否「非」警示股
            return True
        except:
            return True
    
    def check_not_disposition(self, stock_data) -> bool:
        """檢查是否為處置股 - 使用真實數據"""
        try:
            stock_id = stock_data.get('stock_id')
            if stock_id:
                _, is_disposition = self.real_data_integrator.is_warning_or_disposition(stock_id)
                return not is_disposition  # 返回是否「非」處置股
            return True
        except:
            return True
    
    def check_not_limit_up_with_value(self, price_df, days: int) -> Tuple[bool, int]:
        """檢查是否連續漲停 - 使用真實數據和正確邏輯"""
        try:
            if price_df is None or price_df.empty or len(price_df) < days:
                return True, 0
            
            # 使用真實數據整合器的方法
            stock_id = getattr(self, '_current_stock_id', None)
            if stock_id:
                is_not_consecutive, limit_days = self.real_data_integrator.check_consecutive_limit_up(
                    price_df, stock_id, days
                )
                return is_not_consecutive, limit_days
            
            # 如果沒有 stock_id，使用原本的邏輯但改進判斷
            limit_up_days = 0
            for i in range(1, min(days + 1, len(price_df))):
                if i >= len(price_df):
                    break
                    
                curr_close = price_df.iloc[-i]['close']
                curr_high = price_df.iloc[-i].get('max', price_df.iloc[-i].get('high', curr_close))
                prev_close = price_df.iloc[-i-1]['close'] if i < len(price_df) - 1 else curr_close
                
                if prev_close > 0:
                    change = ((curr_close - prev_close) / prev_close) * 100
                    # 更精確的漲停判斷：漲幅接近10%且收在最高
                    if change >= 9.9 and abs(curr_close - curr_high) < prev_close * 0.001:
                        limit_up_days += 1
                    else:
                        break
            
            return limit_up_days < days, limit_up_days
            
        except Exception:
            return True, 0