"""
潛力評分系統
"""
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class ScoringSystem:
    """股票潛力評分系統"""
    
    def __init__(self):
        """初始化評分權重"""
        self.weights = {
            # 成交量相關 (30分)
            'volume_surge_5x': 20,      # 極度爆量
            'volume_surge_3x': 10,      # 中度爆量
            'volume_surge_1_5x': 5,     # 爆量
            'min_volume': 3,            # 基本成交量
            
            # 技術指標 (25分)
            'daily_kd_golden': 10,      # 日KD黃金交叉
            'monthly_kd_golden': 8,     # 月KD黃金交叉
            'above_ma20': 5,            # 站上月線
            'break_60d_high': 15,       # 突破60日新高
            
            # 法人籌碼 (30分)
            'trust_buy': 10,            # 投信買超
            'trust_pct': 5,             # 投信買超佔比
            'trust_5d': 10,             # 投信5日累計
            'inst_5d': 5,               # 法人5日淨買超
            
            # 基本面 (15分)
            'eps_positive': 5,          # EPS為正
            'roe_above': 5,             # ROE達標
            'yield_above': 5,           # 殖利率達標
            
            # 其他條件 (加分項)
            'daily_change_moderate': 3,  # 漲幅適中
            'change_5d_moderate': 3,     # 5日漲幅適中
            'margin_ratio': 2,           # 融資控制良好
            'margin_5d': 2,              # 融資5日控制
            'not_warning': 2,            # 非警示股
            'not_disposition': 2,        # 非處置股
            'not_limit_up': 3            # 非連續漲停
        }
        
        # 組合加分條件
        self.combo_bonus = {
            'perfect_volume': 10,        # 同時符合3個爆量條件
            'tech_fundamental': 15,      # 技術面+基本面都良好
            'institutional_support': 10  # 強勁法人支撐
        }
    
    def calculate_score(self, check_result: Dict) -> Dict:
        """計算潛力分數"""
        try:
            # 基礎分數
            base_score = 0
            matched_conditions = []
            
            # 計算各項分數
            for condition, passed in check_result.items():
                if condition in self.weights and passed:
                    base_score += self.weights[condition]
                    matched_conditions.append(condition)
            
            # 組合加分
            combo_score = self._calculate_combo_bonus(check_result)
            
            # 總分（最高100分）
            total_score = min(100, base_score + combo_score)
            
            # 評級
            grade = self._get_grade(total_score)
            
            # 關鍵信號
            key_signal = self._get_key_signal(matched_conditions)
            
            return {
                'total_score': total_score,
                'base_score': base_score,
                'combo_score': combo_score,
                'grade': grade,
                'key_signal': key_signal,
                'matched_conditions': matched_conditions
            }
            
        except Exception as e:
            logger.error(f"計算評分錯誤: {e}")
            return {
                'total_score': 0,
                'base_score': 0,
                'combo_score': 0,
                'grade': 'C',
                'key_signal': '評分錯誤',
                'matched_conditions': []
            }
    
    def _calculate_combo_bonus(self, check_result: Dict) -> int:
        """計算組合加分"""
        bonus = 0
        
        try:
            # 完美爆量組合
            if all(check_result.get(f'volume_surge_{x}', False) 
                   for x in ['1_5x', '3x', '5x']):
                bonus += self.combo_bonus['perfect_volume']
            
            # 技術面+基本面組合
            tech_conditions = ['daily_kd_golden', 'above_ma20', 'break_60d_high']
            fundamental_conditions = ['eps_positive', 'roe_above', 'yield_above']
            
            tech_count = sum(1 for c in tech_conditions if check_result.get(c, False))
            fund_count = sum(1 for c in fundamental_conditions if check_result.get(c, False))
            
            if tech_count >= 2 and fund_count >= 2:
                bonus += self.combo_bonus['tech_fundamental']
            
            # 強勁法人支撐
            inst_conditions = ['trust_buy', 'trust_5d', 'inst_5d']
            inst_count = sum(1 for c in inst_conditions if check_result.get(c, False))
            
            if inst_count >= 2:
                bonus += self.combo_bonus['institutional_support']
            
            return bonus
            
        except Exception as e:
            logger.error(f"計算組合加分錯誤: {e}")
            return 0
    
    def _get_grade(self, score: float) -> str:
        """取得評級"""
        if score >= 90:
            return 'A+'
        elif score >= 80:
            return 'A'
        elif score >= 70:
            return 'B+'
        elif score >= 60:
            return 'B'
        else:
            return 'C'
    
    def _get_key_signal(self, matched_conditions: list) -> str:
        """產生關鍵信號描述"""
        signals = []
        
        # 爆量信號
        if 'volume_surge_5x' in matched_conditions:
            signals.append("極度爆量")
        elif 'volume_surge_3x' in matched_conditions:
            signals.append("中度爆量")
        elif 'volume_surge_1_5x' in matched_conditions:
            signals.append("爆量")
        
        # 技術信號
        if 'daily_kd_golden' in matched_conditions:
            signals.append("KD黃金交叉")
        
        if 'break_60d_high' in matched_conditions:
            signals.append("突破新高")
        
        # 法人信號
        if 'trust_buy' in matched_conditions:
            signals.append("投信買超")
        
        if 'trust_5d' in matched_conditions:
            signals.append("投信連買")
        
        # 基本面信號
        if all(c in matched_conditions for c in ['eps_positive', 'roe_above']):
            signals.append("基本面優良")
        
        # 組合前3個最重要的信號
        if signals:
            return " + ".join(signals[:3])
        else:
            return "觀察中"