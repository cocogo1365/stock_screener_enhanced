"""
技術指標計算模組
實作正確的技術指標計算公式
"""
import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class TechnicalCalculator:
    """技術指標計算器"""
    
    @staticmethod
    def calculate_kd(df: pd.DataFrame, period: int = 9) -> Tuple[pd.Series, pd.Series]:
        """
        計算KD指標
        
        正確公式:
        RSV = (收盤價 - 最近9日最低價) / (最近9日最高價 - 最近9日最低價) * 100
        K = 前日K值 * 2/3 + 今日RSV * 1/3
        D = 前日D值 * 2/3 + 今日K值 * 1/3
        """
        try:
            # 計算RSV
            high_n = df['high'].rolling(window=period, min_periods=1).max()
            low_n = df['low'].rolling(window=period, min_periods=1).min()
            
            rsv = (df['close'] - low_n) / (high_n - low_n) * 100
            rsv = rsv.fillna(50)  # 填充NaN值
            
            # 計算K值和D值 (使用正確的 alpha = 1/3)
            k = rsv.ewm(alpha=1/3, adjust=False).mean()
            d = k.ewm(alpha=1/3, adjust=False).mean()
            
            return k, d
            
        except Exception as e:
            logger.error(f"計算KD指標錯誤: {e}")
            return pd.Series(), pd.Series()
    
    @staticmethod
    def calculate_ma(df: pd.DataFrame, periods: list = [5, 20, 60]) -> Dict[str, pd.Series]:
        """計算移動平均線"""
        ma_dict = {}
        
        try:
            for period in periods:
                ma_dict[f'MA{period}'] = df['close'].rolling(window=period).mean()
            
            return ma_dict
            
        except Exception as e:
            logger.error(f"計算MA錯誤: {e}")
            return {}
    
    @staticmethod
    def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """計算RSI指標"""
        try:
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            return rsi
            
        except Exception as e:
            logger.error(f"計算RSI錯誤: {e}")
            return pd.Series()
    
    @staticmethod
    def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict:
        """計算MACD指標"""
        try:
            ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
            ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
            
            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=signal, adjust=False).mean()
            histogram = macd_line - signal_line
            
            return {
                'MACD': macd_line,
                'Signal': signal_line,
                'Histogram': histogram
            }
            
        except Exception as e:
            logger.error(f"計算MACD錯誤: {e}")
            return {}
    
    @staticmethod
    def calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> Dict:
        """計算布林通道"""
        try:
            ma = df['close'].rolling(window=period).mean()
            std = df['close'].rolling(window=period).std()
            
            upper = ma + (std * std_dev)
            lower = ma - (std * std_dev)
            
            return {
                'Upper': upper,
                'Middle': ma,
                'Lower': lower
            }
            
        except Exception as e:
            logger.error(f"計算布林通道錯誤: {e}")
            return {}
    
    @staticmethod
    def calculate_volume_surge(df: pd.DataFrame, period: int = 5) -> pd.Series:
        """計算爆量倍數"""
        try:
            # 確保成交量欄位存在
            volume_col = 'Trading_Volume' if 'Trading_Volume' in df.columns else 'volume'
            
            # 將成交量轉換為張 (1張 = 1000股)
            if volume_col == 'Trading_Volume':
                volume_in_lots = df[volume_col] / 1000
            else:
                volume_in_lots = df[volume_col]
            
            # 計算前N日平均量 (不包含當日)
            avg_volume = volume_in_lots.rolling(window=period).mean().shift(1)
            
            # 計算爆量倍數
            surge_ratio = volume_in_lots / avg_volume
            surge_ratio = surge_ratio.replace([np.inf, -np.inf], 0)
            
            return surge_ratio
            
        except Exception as e:
            logger.error(f"計算爆量倍數錯誤: {e}")
            return pd.Series()
    
    @staticmethod
    def check_golden_cross(k: pd.Series, d: pd.Series) -> bool:
        """檢查KD黃金交叉 (K值由下往上穿越D值，且K<80)"""
        try:
            if len(k) < 2 or len(d) < 2:
                return False
            
            # K線向上穿越D線，且K值不在超買區
            cross = (k.iloc[-1] > d.iloc[-1]) and (k.iloc[-2] <= d.iloc[-2])
            not_overbought = k.iloc[-1] < 80
            
            return cross and not_overbought
            
        except Exception:
            return False
    
    @staticmethod
    def check_death_cross(k: pd.Series, d: pd.Series) -> bool:
        """檢查KD死亡交叉"""
        try:
            if len(k) < 2 or len(d) < 2:
                return False
            
            # K線向下穿越D線
            return (k.iloc[-1] < d.iloc[-1]) and (k.iloc[-2] >= d.iloc[-2])
            
        except Exception:
            return False
    
    @staticmethod
    def calculate_change_pct(df: pd.DataFrame, days: int = 1) -> pd.Series:
        """計算漲跌幅"""
        try:
            return df['close'].pct_change(periods=days) * 100
            
        except Exception as e:
            logger.error(f"計算漲跌幅錯誤: {e}")
            return pd.Series()
    
    @staticmethod
    def find_support_resistance(df: pd.DataFrame, window: int = 20) -> Dict:
        """尋找支撐壓力位"""
        try:
            # 簡化的支撐壓力計算
            high_max = df['high'].rolling(window=window).max()
            low_min = df['low'].rolling(window=window).min()
            
            resistance = high_max.iloc[-1] if not high_max.empty else 0
            support = low_min.iloc[-1] if not low_min.empty else 0
            
            return {
                'resistance': resistance,
                'support': support,
                'current': df['close'].iloc[-1] if not df.empty else 0
            }
            
        except Exception as e:
            logger.error(f"計算支撐壓力錯誤: {e}")
            return {'resistance': 0, 'support': 0, 'current': 0}
    
    @staticmethod
    def check_new_high(df: pd.DataFrame, days: int = 60) -> bool:
        """檢查是否突破N日新高"""
        try:
            if len(df) < days + 1:
                return False
            
            current_price = df['close'].iloc[-1]
            # 前N日的最高價 (不包含今日)
            previous_high = df['close'].iloc[-days-1:-1].max()
            
            return current_price > previous_high
            
        except Exception as e:
            logger.error(f"檢查新高錯誤: {e}")
            return False
    
    @staticmethod
    def check_ma_support(df: pd.DataFrame, ma_period: int = 20) -> bool:
        """檢查股價是否站上均線"""
        try:
            if len(df) < ma_period:
                return False
            
            current_price = df['close'].iloc[-1]
            ma = df['close'].rolling(window=ma_period).mean().iloc[-1]
            
            # 站上均線 (允許2%誤差)
            return current_price >= ma * 0.98
            
        except Exception as e:
            logger.error(f"檢查均線支撐錯誤: {e}")
            return False
    
    @staticmethod
    def calculate_volume_ratio(df: pd.DataFrame) -> float:
        """計算量比 (今日成交量/5日均量)"""
        try:
            volume_col = 'Trading_Volume' if 'Trading_Volume' in df.columns else 'volume'
            
            # 轉換為張
            if volume_col == 'Trading_Volume':
                volumes = df[volume_col] / 1000
            else:
                volumes = df[volume_col]
            
            if len(volumes) < 5:
                return 0
            
            current_volume = volumes.iloc[-1]
            avg_volume_5d = volumes.iloc[-6:-1].mean()  # 前5日均量
            
            if avg_volume_5d > 0:
                return current_volume / avg_volume_5d
            
            return 0
            
        except Exception as e:
            logger.error(f"計算量比錯誤: {e}")
            return 0