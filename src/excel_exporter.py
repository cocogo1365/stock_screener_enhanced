"""
Excel 報表輸出模組
"""
import pandas as pd
import xlsxwriter
from datetime import datetime
import os
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


class ExcelExporter:
    """Excel 報表輸出器"""
    
    def __init__(self, output_dir: str = "reports"):
        """初始化輸出器"""
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def export_screening_results(self, results: List[Dict], params: Dict = None) -> str:
        """輸出篩選結果到 Excel"""
        try:
            # 產生檔名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = os.path.join(self.output_dir, f'screening_{timestamp}.xlsx')
            
            # 建立 Excel writer
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                workbook = writer.book
                
                # 定義格式
                formats = self._create_formats(workbook)
                
                # 工作表1: 篩選結果
                self._write_results_sheet(writer, results, formats)
                
                # 工作表2: 統計分析
                self._write_statistics_sheet(writer, results, formats)
                
                # 工作表3: 參數設定
                if params:
                    self._write_params_sheet(writer, params, formats)
                
                # 工作表4: 產業分布
                self._write_industry_sheet(writer, results, formats)
            
            logger.info(f"Excel 報表已輸出: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"輸出 Excel 錯誤: {e}")
            raise
    
    def _create_formats(self, workbook) -> Dict:
        """建立 Excel 格式"""
        formats = {
            'header': workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'fg_color': '#4CAF50',
                'font_color': 'white',
                'border': 1
            }),
            'cell_center': workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'border': 1
            }),
            'cell_number': workbook.add_format({
                'align': 'right',
                'valign': 'vcenter',
                'border': 1,
                'num_format': '#,##0.00'
            }),
            'cell_percent': workbook.add_format({
                'align': 'right',
                'valign': 'vcenter',
                'border': 1,
                'num_format': '0.00%'
            }),
            'cell_positive': workbook.add_format({
                'align': 'right',
                'valign': 'vcenter',
                'border': 1,
                'font_color': 'red',
                'num_format': '+#,##0.00;-#,##0.00'
            }),
            'cell_negative': workbook.add_format({
                'align': 'right',
                'valign': 'vcenter',
                'border': 1,
                'font_color': 'green',
                'num_format': '+#,##0.00;-#,##0.00'
            }),
            'grade_a_plus': workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'bold': True,
                'font_color': 'red',
                'fg_color': '#FFE6E6',
                'border': 1
            }),
            'grade_a': workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'font_color': '#C00000',
                'fg_color': '#FFF2F2',
                'border': 1
            }),
            'grade_b_plus': workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'font_color': '#0070C0',
                'fg_color': '#E6F3FF',
                'border': 1
            }),
            'title': workbook.add_format({
                'bold': True,
                'font_size': 14,
                'align': 'center',
                'valign': 'vcenter'
            })
        }
        
        return formats
    
    def _write_results_sheet(self, writer, results: List[Dict], formats: Dict):
        """寫入篩選結果工作表"""
        # 準備資料
        data = []
        for i, result in enumerate(results, 1):
            stock_data = result.get('data', {})
            price_df = stock_data.get('price', pd.DataFrame())
            
            # 取得最新價格資料
            if not price_df.empty and isinstance(price_df, pd.DataFrame):
                latest_price = price_df.iloc[-1].get('close', 0)
                latest_volume = price_df.iloc[-1].get('Trading_Volume', 0)
                
                # 計算5日漲跌幅
                if len(price_df) >= 6:
                    price_5d_ago = price_df.iloc[-6].get('close', latest_price)
                    change_5d = ((latest_price - price_5d_ago) / price_5d_ago * 100) if price_5d_ago > 0 else 0
                else:
                    change_5d = 0
                
                # 計算日漲跌幅
                if len(price_df) >= 2:
                    prev_close = price_df.iloc[-2].get('close', latest_price)
                    change_1d = ((latest_price - prev_close) / prev_close * 100) if prev_close > 0 else 0
                else:
                    change_1d = 0
            else:
                latest_price = 0
                latest_volume = 0
                change_1d = 0
                change_5d = 0
            
            # 取得法人資料
            inst_data = stock_data.get('institutional', {})
            trust_buy = inst_data.get('投信買超', 0) if isinstance(inst_data, dict) else 0
            foreign_buy = inst_data.get('外資買超', 0) if isinstance(inst_data, dict) else 0
            
            # 取得融資券資料
            margin_data = stock_data.get('margin', {})
            margin_balance = margin_data.get('融資餘額', 0) if isinstance(margin_data, dict) else 0
            
            # 取得財務資料
            financial_data = stock_data.get('financial', {})
            eps = financial_data.get('EPS', 0) if isinstance(financial_data, dict) else 0
            roe = financial_data.get('ROE', 0) if isinstance(financial_data, dict) else 0
            dividend_yield = financial_data.get('殖利率', 0) if isinstance(financial_data, dict) else 0
            
            data.append({
                '排名': i,
                '股票代碼': result.get('stock_id', ''),
                '股票名稱': result.get('stock_name', ''),
                '潛力分數': result.get('score', 0),
                '評級': result.get('grade', 'C'),
                '收盤價': latest_price,
                '日漲跌%': change_1d,
                '5日漲跌%': change_5d,
                '成交量(張)': int(latest_volume / 1000) if latest_volume > 0 else 0,
                '投信買超': trust_buy,
                '外資買超': foreign_buy,
                '融資餘額': margin_balance,
                'EPS': eps,
                'ROE%': roe,
                '殖利率%': dividend_yield,
                '符合條件數': result.get('matched_conditions', 0),
                '關鍵信號': result.get('signal', '')
            })
        
        # 建立 DataFrame
        df = pd.DataFrame(data)
        
        # 寫入 Excel
        sheet_name = '篩選結果'
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=2)
        
        # 取得工作表
        worksheet = writer.sheets[sheet_name]
        workbook = writer.book
        
        # 寫入標題
        worksheet.merge_range('A1:Q1', '台股智能篩選結果報表', formats['title'])
        worksheet.write('A2', f'篩選時間: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', formats['cell_center'])
        
        # 設定欄寬
        column_widths = {
            'A': 8,   # 排名
            'B': 12,  # 股票代碼
            'C': 15,  # 股票名稱
            'D': 10,  # 潛力分數
            'E': 8,   # 評級
            'F': 10,  # 收盤價
            'G': 10,  # 日漲跌%
            'H': 10,  # 5日漲跌%
            'I': 12,  # 成交量
            'J': 12,  # 投信買超
            'K': 12,  # 外資買超
            'L': 12,  # 融資餘額
            'M': 10,  # EPS
            'N': 10,  # ROE%
            'O': 10,  # 殖利率%
            'P': 10,  # 符合條件數
            'Q': 20   # 關鍵信號
        }
        
        for col, width in column_widths.items():
            worksheet.set_column(f'{col}:{col}', width)
        
        # 凍結窗格
        worksheet.freeze_panes(3, 3)
        
        # 設定自動篩選
        worksheet.autofilter(2, 0, len(df) + 2, len(df.columns) - 1)
        
        # 條件格式化
        # 分數高亮
        worksheet.conditional_format(f'D4:D{len(df)+3}', {
            'type': 'data_bar',
            'bar_color': '#4CAF50'
        })
        
        # 漲跌幅顏色
        worksheet.conditional_format(f'G4:H{len(df)+3}', {
            'type': 'cell',
            'criteria': '>',
            'value': 0,
            'format': formats['cell_positive']
        })
        
        worksheet.conditional_format(f'G4:H{len(df)+3}', {
            'type': 'cell',
            'criteria': '<',
            'value': 0,
            'format': formats['cell_negative']
        })
    
    def _write_statistics_sheet(self, writer, results: List[Dict], formats: Dict):
        """寫入統計分析工作表"""
        # 準備統計資料
        stats_data = {
            '統計項目': [],
            '數值': []
        }
        
        # 基本統計
        stats_data['統計項目'].append('篩選股票總數')
        stats_data['數值'].append(len(results))
        
        # 分數統計
        scores = [r.get('score', 0) for r in results]
        if scores:
            stats_data['統計項目'].extend([
                '平均分數',
                '最高分數',
                '最低分數',
                '中位數分數'
            ])
            stats_data['數值'].extend([
                f"{sum(scores)/len(scores):.1f}",
                f"{max(scores):.1f}",
                f"{min(scores):.1f}",
                f"{sorted(scores)[len(scores)//2]:.1f}"
            ])
        
        # 評級分布
        grade_counts = {}
        for r in results:
            grade = r.get('grade', 'C')
            grade_counts[grade] = grade_counts.get(grade, 0) + 1
        
        for grade in ['A+', 'A', 'B+', 'B', 'C']:
            stats_data['統計項目'].append(f'{grade} 級股票數')
            stats_data['數值'].append(grade_counts.get(grade, 0))
        
        # 建立 DataFrame
        df_stats = pd.DataFrame(stats_data)
        
        # 寫入 Excel
        sheet_name = '統計分析'
        df_stats.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
        
        # 取得工作表
        worksheet = writer.sheets[sheet_name]
        
        # 寫入標題
        worksheet.merge_range('A1:B1', '篩選統計分析', formats['title'])
        
        # 設定欄寬
        worksheet.set_column('A:A', 20)
        worksheet.set_column('B:B', 15)
    
    def _write_params_sheet(self, writer, params: Dict, formats: Dict):
        """寫入參數設定工作表"""
        # 準備參數資料
        param_data = {
            '參數類別': [],
            '參數名稱': [],
            '設定值': [],
            '是否啟用': []
        }
        
        # 解析參數
        for category, settings in params.items():
            if isinstance(settings, dict):
                for param, value in settings.items():
                    if isinstance(value, dict):
                        param_data['參數類別'].append(category)
                        param_data['參數名稱'].append(param)
                        param_data['設定值'].append(value.get('value', ''))
                        param_data['是否啟用'].append('是' if value.get('enabled', False) else '否')
                    else:
                        param_data['參數類別'].append(category)
                        param_data['參數名稱'].append(param)
                        param_data['設定值'].append(value)
                        param_data['是否啟用'].append('是' if value else '否')
        
        # 建立 DataFrame
        df_params = pd.DataFrame(param_data)
        
        # 寫入 Excel
        sheet_name = '參數設定'
        df_params.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
        
        # 取得工作表
        worksheet = writer.sheets[sheet_name]
        
        # 寫入標題
        worksheet.merge_range('A1:D1', '篩選參數設定', formats['title'])
        
        # 設定欄寬
        worksheet.set_column('A:A', 15)
        worksheet.set_column('B:B', 20)
        worksheet.set_column('C:C', 15)
        worksheet.set_column('D:D', 10)
    
    def _write_industry_sheet(self, writer, results: List[Dict], formats: Dict):
        """寫入產業分布工作表"""
        # 簡化處理，實際應有產業分類資料
        industry_data = {
            '產業類別': ['電子', '金融', '傳產', '生技', '其他'],
            '股票數量': [
                len([r for r in results[:5]]),
                len([r for r in results[5:8]]),
                len([r for r in results[8:10]]),
                len([r for r in results[10:12]]),
                len([r for r in results[12:]])
            ]
        }
        
        # 建立 DataFrame
        df_industry = pd.DataFrame(industry_data)
        
        # 寫入 Excel
        sheet_name = '產業分布'
        df_industry.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
        
        # 取得工作表
        worksheet = writer.sheets[sheet_name]
        
        # 寫入標題
        worksheet.merge_range('A1:B1', '產業分布統計', formats['title'])
        
        # 設定欄寬
        worksheet.set_column('A:A', 15)
        worksheet.set_column('B:B', 12)