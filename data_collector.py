"""
A股数据收集器 v3
直接HTTP请求东方财富API，完全不依赖AKShare
- 多主机容错
- 自动重试 + 指数退避
- 详细调试日志
- 需求：市值倒数100名中，归母净利润增速前30
"""

import pandas as pd
from datetime import datetime, timedelta
import json
import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class AStockDataCollector:

    def __init__(self):
        self.data_dir = os.path.join(os.path.dirname(__file__), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.session = self._create_session()

    def _create_session(self):
        session = requests.Session()
        retry = Retry(total=5, backoff_factor=2,
                      status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        })
        return session

    # ─── 行情数据 ──────────────────────────────────────

    def get_all_stocks(self):
        """获取沪深A股实时行情，多主机容错"""
        print("  从东方财富获取沪深A股行情...")

        hosts = [
            "https://82.push2.eastmoney.com",
            "https://push2.eastmoney.com",
            "http://push2.eastmoney.com",
            "http://82.push2.eastmoney.com",
        ]

        for host in hosts:
            try:
                df = self._fetch_stock_list(host)
                if not df.empty and len(df) > 500:
                    print(f"  [OK] {host}: 获取到 {len(df)} 只A股")
                    return df
                print(f"  [WARN] {host}: 仅 {len(df)} 只，换下一个...")
            except Exception as e:
                print(f"  [FAIL] {host}: {e}")

        print("  所有主机均失败")
        return pd.DataFrame()

    def _fetch_stock_list(self, host):
        """从指定主机获取股票列表"""
        all_items = []
        page = 1
        page_size = 5000

        while True:
            url = f"{host}/api/qt/clist/get"
            params = {
                "pn": str(page),
                "pz": str(page_size),
                "po": "1",
                "np": "1",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": "2",
                "invt": "2",
                "wbp2u": "|0|0|0|web",
                "fid": "f3",
                "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
                "fields": "f2,f3,f5,f6,f8,f12,f14,f15,f16,f17,f18,f20,f21",
                "_": str(int(time.time() * 1000)),
            }

            resp = self.session.get(url, params=params, timeout=30)
            data = resp.json()

            if data.get('data') is None:
                print(f"    第{page}页: data=None")
                break

            items = data['data'].get('diff', [])
            total = data['data'].get('total', 0)
            print(f"    第{page}页: {len(items)} 条 (total={total})")

            if not items:
                break

            all_items.extend(items)

            if len(all_items) >= total:
                break
            page += 1
            time.sleep(0.5)

        if not all_items:
            return pd.DataFrame()

        records = []
        for item in all_items:
            code = str(item.get('f12', ''))
            name = str(item.get('f14', ''))
            if not code or code == '-' or not name or name == '-':
                continue
            records.append({
                '代码': code,
                '名称': name,
                '最新价': self._safe_float(item.get('f2')),
                '涨跌幅': self._safe_float(item.get('f3')),
                '成交量': self._safe_float(item.get('f5')),
                '成交额': self._safe_float(item.get('f6')),
                '最高': self._safe_float(item.get('f15')),
                '最低': self._safe_float(item.get('f16')),
                '今开': self._safe_float(item.get('f17')),
                '昨收': self._safe_float(item.get('f18')),
                '总市值': self._safe_float(item.get('f20')),
                '流通市值': self._safe_float(item.get('f21')),
                '换手率': self._safe_float(item.get('f8')),
            })

        df = pd.DataFrame(records)

        # 过滤 ST、退市、北交所、新三板
        df = df[
            ~df['名称'].str.contains('ST|退', na=False)
            & ~df['代码'].str.startswith('8', na=False)
            & ~df['代码'].str.startswith('4', na=False)
            & ~df['代码'].str.startswith('9', na=False)
        ]

        df = df.dropna(subset=['总市值'])
        df = df[df['总市值'] > 0]
        return df

    # ─── 业绩报表（净利润增速） ──────────────────────

    def get_profit_growth_bulk(self):
        """批量获取归母净利润同比增速 - 合并多报告期，优先最新"""
        print("  获取业绩报表数据（多报告期合并）...")

        api_urls = [
            "https://datacenter-web.eastmoney.com/api/data/v1/get",
            "https://datacenter.eastmoney.com/api/data/v1/get",
        ]
        report_dates = ['2025-12-31', '2025-09-30', '2025-06-30',
                        '2024-12-31', '2024-09-30']

        all_profit = pd.DataFrame()

        for api_url in api_urls:
            for report_date in report_dates:
                try:
                    df = self._fetch_profit_from(api_url, report_date)
                    if not df.empty:
                        if all_profit.empty:
                            all_profit = df
                        else:
                            # 只补充之前没有的股票，保证优先用最新报告期
                            existing_codes = set(all_profit['代码'].values)
                            new_rows = df[~df['代码'].isin(existing_codes)]
                            if not new_rows.empty:
                                all_profit = pd.concat(
                                    [all_profit, new_rows], ignore_index=True)
                                print(f"    补充 {len(new_rows)} 条新记录，"
                                      f"累计 {len(all_profit)} 条")
                except Exception as e:
                    print(f"    {report_date} @ "
                          f"{api_url.split('//')[1][:30]}: {e}")

            if not all_profit.empty:
                print(f"  累计获取 {len(all_profit)} 条业绩数据")
                return all_profit

            print(f"    该主机所有报告期均失败，换下一个...")

        print("  所有业绩数据源均失败")
        return pd.DataFrame()

    def get_financial_quality_data(self, stock_codes):
        """获取财务质量数据 - 包括扣非净利润、现金流等"""
        print(f"  获取 {len(stock_codes)} 只股票的财务质量数据...")

        quality_data = []

        # 按批次处理，避免请求过于频繁
        batch_size = 50
        for i in range(0, len(stock_codes), batch_size):
            batch_codes = stock_codes[i:i + batch_size]
            codes_str = ','.join([f"'{code}'" for code in batch_codes])

            api_url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            try:
                # 获取更多财务指标
                params = {
                    'reportName': 'RPT_DUP_FCI_PERFORMANCE_EVALUATION',
                    'columns': 'SECURITY_CODE,SECURITY_NAME_ABBR,PARENT_NETPROFIT,YOY_PROFIT,GROSS_INCOME,YOY_SALES,FROM_EQUITY_INVESTMENTS,FROM_FIXED_ASSETS,FROM_INTANGIBLE_ASSETS,CF_OPERA,CF_INVEST,CF_FINANCE,CASH_EQUIV_ADDITIONS',
                    'filter': f'(REPORT_TYPE="1")',
                    'pageNumber': '1',
                    'pageSize': '1000',
                    'sortColumns': 'UPDATE_DATE',
                    'sortTypes': '-1'
                }

                resp = self.session.get(api_url, params=params, timeout=30)
                data = resp.json()

                if data.get('success') and data.get('result'):
                    items = data['result'].get('data', [])

                    for item in items:
                        code = item.get('SECURITY_CODE')
                        if code in batch_codes:
                            quality_data.append({
                                '代码': code,
                                '营业收入': item.get('GROSS_INCOME'),
                                '营业收入同比增长率': item.get('YOY_SALES'),
                                '经营活动现金流': item.get('CF_OPERA'),
                                '投资活动现金流': item.get('CF_INVEST'),
                                '筹资活动现金流': item.get('CF_FINANCE'),
                                '现金及等价物净增加': item.get('CASH_EQUIV_ADDITIONS')
                            })

            except Exception as e:
                print(f"    获取财务质量数据失败: {e}")

        return pd.DataFrame(quality_data) if quality_data else pd.DataFrame()

    def get_valuation_data(self, stock_codes):
        """获取估值数据"""
        print(f"  获取 {len(stock_codes)} 只股票的估值数据...")

        valuation_data = []

        # 通过API获取市盈率、市净率等数据
        for code in stock_codes:
            try:
                # 根据股票代码确定市场
                market = '1' if code.startswith(('60', '688', '689')) else '0'
                secid = f"{market}.{code}"

                # 获取实时数据来计算估值指标
                url = f"https://push2.eastmoney.com/api/qt/ulist.np/get"
                params = {
                    'fltt': '2',
                    'invt': '2',
                    'fields': 'f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25',
                    'secids': secid
                }

                resp = self.session.get(url, params=params, timeout=30)
                data = resp.json()

                if data.get('data') and data['data'].get('diff'):
                    stock_info = data['data']['diff'][0]
                    valuation_data.append({
                        '代码': code,
                        '市盈率': self._safe_float(stock_info.get('f9')),  # 动态市盈率
                        '市净率': self._safe_float(stock_info.get('f8')),  # 市净率
                        '市销率': self._safe_float(stock_info.get('f22')),  # 市销率
                        '总市值': self._safe_float(stock_info.get('f20')),
                        '流通市值': self._safe_float(stock_info.get('f21'))
                    })
            except Exception as e:
                print(f"    获取 {code} 估值数据失败: {e}")

        return pd.DataFrame(valuation_data) if valuation_data else pd.DataFrame()

    def _fetch_profit_from(self, api_url, report_date):
        """从指定API获取某个报告期的业绩数据"""
        all_records = []
        page = 1

        while True:
            params = {
                'sortColumns': 'NOTICE_DATE,SECURITY_CODE',
                'sortTypes': '-1,-1',
                'pageSize': '500',
                'pageNumber': str(page),
                'reportName': 'RPT_LICO_FN_CPD',
                'columns': 'SECURITY_CODE,SECURITY_NAME_ABBR,'
                           'PARENT_NETPROFIT,SJLTZ',
                'filter': f"(REPORTDATE='{report_date}')",
                'source': 'WEB',
                'client': 'WEB',
            }

            resp = self.session.get(api_url, params=params, timeout=30)
            data = resp.json()

            if page == 1:
                print(f"    {report_date}: HTTP {resp.status_code}, "
                      f"success={data.get('success')}, "
                      f"code={data.get('code')}, "
                      f"has_result={data.get('result') is not None}")

            if not data.get('success') or data.get('result') is None:
                if page == 1:
                    msg = data.get('message', '')
                    print(f"    返回消息: {msg[:100]}")
                break

            items = data['result'].get('data', [])
            if not items:
                break

            for item in items:
                code = item.get('SECURITY_CODE', '')
                growth = item.get('SJLTZ')
                if code and growth is not None:
                    try:
                        all_records.append({
                            '代码': code,
                            '净利润同比增长率': float(growth),
                        })
                    except (ValueError, TypeError):
                        continue

            total_pages = data['result'].get('pages', 1)
            total_count = data['result'].get('count', 0)
            if page == 1:
                print(f"    总计 {total_count} 条, {total_pages} 页")

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)

        if all_records:
            print(f"    [OK] 获取到 {len(all_records)} 条业绩数据"
                  f"（报告期: {report_date}）")
            return pd.DataFrame(all_records)
        return pd.DataFrame()

    # ─── K线数据 ─────────────────────────────────────

    def get_stock_kline(self, stock_code, period="daily"):
        """获取K线数据 - 直接请求东方财富"""
        try:
            klt = {"daily": "101", "weekly": "102", "monthly": "103"
                   }.get(period, "101")

            if stock_code.startswith('6') or stock_code.startswith('9'):
                secid = f"1.{stock_code}"
            else:
                secid = f"0.{stock_code}"

            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            params = {
                'secid': secid,
                'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
                'fields1': 'f1,f2,f3,f4,f5,f6',
                'fields2': 'f51,f52,f53,f54,f55,f56,f57',
                'klt': klt,
                'fqt': '1',
                'beg': (datetime.now() - timedelta(days=730)).strftime("%Y%m%d"),
                'end': datetime.now().strftime("%Y%m%d"),
            }

            resp = self.session.get(url, params=params, timeout=30)
            klines = resp.json().get('data', {}).get('klines', [])

            records = []
            for line in klines:
                p = line.split(',')
                if len(p) >= 7:
                    records.append({
                        '日期': p[0], '开盘': float(p[1]), '收盘': float(p[2]),
                        '最高': float(p[3]), '最低': float(p[4]),
                        '成交量': float(p[5]), '成交额': float(p[6]),
                    })
            return pd.DataFrame(records) if records else pd.DataFrame()
        except Exception as e:
            print(f"获取 {stock_code} K线失败: {e}")
            return pd.DataFrame()

    # ─── 筛选主流程 ──────────────────────────────────

    def screen_stocks(self):
        """筛选：市值倒数100名中，综合质量评分前30"""
        print("=" * 50)
        print("开始筛选股票...")
        print("=" * 50)

        # 1. 行情
        print("\n1. 获取沪深A股行情数据...")
        all_stocks = self.get_all_stocks()
        if all_stocks.empty:
            print("行情数据获取失败！")
            return pd.DataFrame()
        print(f"  共 {len(all_stocks)} 只股票")

        # 2. 市值倒数1000（扩大筛选范围以进行更全面分析）
        print("\n2. 筛选市值最小的1000只股票...")
        all_stocks = all_stocks.sort_values('总市值', ascending=True)
        small_cap = all_stocks.head(1000).copy()
        print(f"  筛选出 {len(small_cap)} 只小市值股票")
        print(f"  市值范围: {small_cap['总市值'].min()/1e8:.2f}亿 "
              f"~ {small_cap['总市值'].max()/1e8:.2f}亿")

        # 3. 净利润增速
        print("\n3. 获取归母净利润增速数据...")
        profit_df = self.get_profit_growth_bulk()

        if profit_df.empty:
            print("  业绩数据获取失败，无法按净利润增速筛选")
            # 降级方案：直接返回市值最小的30只
            print("  降级方案：返回市值最小的30只股票")
            return small_cap.head(30)

        # 4. 合并基础数据
        print("\n4. 合并基础数据...")
        merged = small_cap.merge(profit_df, on='代码', how='inner')
        print(f"  匹配合并后有 {len(merged)} 只股票")

        if merged.empty:
            print("  没有匹配到任何数据")
            # 降级方案：左连接，保留所有小市值股票
            merged = small_cap.merge(profit_df, on='代码', how='left')
            merged['净利润同比增长率'] = merged['净利润同比增长率'].fillna(0)
            print(f"  降级方案：保留全部 {len(merged)} 只，增速缺失按0处理")

        # 5. 获取更多财务质量数据
        print("\n5. 获取财务质量与估值数据...")
        stock_codes = merged['代码'].tolist()

        # 获取财务质量数据
        quality_df = self.get_financial_quality_data(stock_codes)
        if not quality_df.empty:
            merged = merged.merge(quality_df, on='代码', how='left')
            print(f"  财务质量数据合并完成")

        # 获取估值数据
        valuation_df = self.get_valuation_data(stock_codes)
        if not valuation_df.empty:
            merged = merged.merge(valuation_df, on='代码', how='left')
            print(f"  估值数据合并完成")

        # 6. 计算综合评分
        print("\n6. 计算综合评分并筛选...")
        scored_stocks = self.calculate_comprehensive_score(merged)

        # 7. 风险评估
        print("\n7. 进行风险评估...")
        risk_indicators = self.risk_assessment(scored_stocks)

        # 输出风险较高的股票
        high_risk_stocks = [r for r in risk_indicators if r['风险等级'] == '高']
        if high_risk_stocks:
            print(f"  发现 {len(high_risk_stocks)} 只高风险股票:")
            for risk in high_risk_stocks[:5]:  # 只显示前5个
                print(f"    {risk['代码']}: {', '.join(risk['风险因素'])}")

        # 过滤高风险股票（可选）
        filtered_stocks = scored_stocks.copy()
        high_risk_codes = [r['代码'] for r in risk_indicators if r['风险等级'] == '高']
        if high_risk_codes:
            filtered_stocks = filtered_stocks[~filtered_stocks['代码'].isin(high_risk_codes)]
            print(f"  过滤高风险股票后剩余 {len(filtered_stocks)} 只")

        # 按综合评分排序，取前30
        top30 = filtered_stocks.nlargest(30, '综合评分')

        print(f"\n最终筛选出 {len(top30)} 只股票")
        print("\n前10只股票预览:")
        preview_cols = ['代码', '名称', '总市值', '净利润同比增长率', '综合评分']
        # 检查列是否存在
        available_cols = [col for col in preview_cols if col in top30.columns]
        if available_cols:
            preview = top30[available_cols].head(10).copy()
            if '总市值' in preview.columns:
                preview['总市值'] = (preview['总市值'] / 1e8).round(2)
            # 重命名列时也检查是否存在
            rename_mapping = {}
            if '总市值' in preview.columns:
                rename_mapping['总市值'] = '总市值(亿)'
            if '净利润同比增长率' in preview.columns:
                rename_mapping['净利润同比增长率'] = '净利润增速(%)'
            if '综合评分' in preview.columns:
                rename_mapping['综合评分'] = '综合评分'

            if rename_mapping:
                preview = preview.rename(columns=rename_mapping)
            print(preview.to_string(index=False))
        else:
            print("可用列:", list(top30.columns))
            # 显示前几行的所有数据作为备用
            print(top30.head(5))

        return top30

    def calculate_comprehensive_score(self, stocks_df):
        """计算综合评分"""
        if stocks_df.empty:
            return stocks_df

        scores = []
        detailed_scores = []  # 添加详细评分记录用于调试

        for idx, stock in stocks_df.iterrows():
            # 初始化各项得分
            growth_score = 0
            health_score = 0
            valuation_score = 0
            sustainability_score = 0

            # 1. 增长质量评分 (40分)
            profit_growth = stock.get('净利润同比增长率', 0)

            # 增长合理性评分 (避免过高增长)
            if 20 <= profit_growth <= 150:  # 适度增长更可持续
                growth_score = 40
            elif 150 < profit_growth <= 300:
                growth_score = 35
            elif profit_growth > 300:
                growth_score = 20  # 过高增长可能存在风险
            elif 0 < profit_growth < 20:
                growth_score = 30
            elif profit_growth <= 0:
                growth_score = max(0, 10 + profit_growth / 10)  # 负增长适当扣分
            else:
                growth_score = 0  # 默认0分

            # 2. 财务健康度评分 (30分)
            pb = stock.get('市净率', 5.0)  # 如果没有数据，默认5.0
            # 简化处理：如果PB过低(如小于0.1)或过高(大于10)则扣分
            if pb < 0.1:
                health_score = 5  # PB过低可能有财务问题
            elif 0.1 <= pb <= 3.0:
                health_score = 25  # 合理区间
            elif 3.0 < pb <= 5.0:
                health_score = 20  # 略高但可接受
            elif 5.0 < pb <= 10.0:
                health_score = 15  # 较高
            else:
                health_score = max(0, 30 - (pb - 10) * 2)  # 过高则扣分

            # 3. 估值合理性评分 (20分)
            pe = stock.get('市盈率', 100)  # 如果没有则默认100
            if pe <= 0:  # 亏损公司
                valuation_score = 0
            elif pe <= 15:
                valuation_score = 20
            elif pe <= 20:
                valuation_score = 18
            elif pe <= 25:
                valuation_score = 16
            elif pe <= 30:
                valuation_score = 14
            elif pe <= 40:
                valuation_score = 12
            elif pe <= 60:
                valuation_score = 8
            else:
                valuation_score = max(0, 8 - (pe - 60) / 10)

            # 4. 成长可持续性评分 (10分)
            revenue_growth = stock.get('营业收入同比增长率', 0)
            if 10 <= revenue_growth <= 100:
                sustainability_score = 10
            elif 0 <= revenue_growth < 10:
                sustainability_score = 7
            elif revenue_growth > 100:
                sustainability_score = 5  # 营收增长过高也可能是暂时的
            else:
                sustainability_score = max(0, 5 + revenue_growth / 10)

            # 计算总分 (权重可根据策略调整)
            total_score = (
                growth_score * 0.4 +      # 增长质量 40%
                health_score * 0.3 +      # 财务健康 30%
                valuation_score * 0.2 +   # 估值合理性 20%
                sustainability_score * 0.1 # 可持续性 10%
            )

            scores.append(total_score)

            # 记录详细评分用于调试
            detailed_scores.append({
                '代码': stock.get('代码', ''),
                '增长率': profit_growth,
                '增长得分': growth_score,
                'PB': pb,
                '健康得分': health_score,
                'PE': pe,
                '估值得分': valuation_score,
                '营收增长率': revenue_growth,
                '可持续得分': sustainability_score,
                '总分': total_score
            })

        stocks_df['综合评分'] = scores

        # 输出前几只股票的评分明细，便于分析
        print("\n前5只股票评分明细:")
        for detail in detailed_scores[:5]:
            print(f"  {detail['代码']}: 总分{detail['总分']:.1f} "
                  f"(增长{detail['增长得分']:.1f}, 健康{detail['健康得分']:.1f}, "
                  f"估值{detail['估值得分']:.1f}, 可持续{detail['可持续得分']:.1f})")

        return stocks_df.sort_values('综合评分', ascending=False)

    def risk_assessment(self, stock_data):
        """风险评估"""
        risk_indicators = []

        for idx, stock in stock_data.iterrows():
            risk_level = "低"
            risk_factors = []

            # 1. 增长率波动风险
            profit_growth = stock.get('净利润同比增长率', 0)
            if profit_growth > 500:
                risk_factors.append(f"增长率异常高({profit_growth:.2f}%),存在回归风险")
                risk_level = "高"
            elif 100 < profit_growth <= 500:
                risk_factors.append(f"增长率较高({profit_growth:.2f}%),关注可持续性")
                if risk_level != "高":
                    risk_level = "中"
            elif profit_growth < -50:
                risk_factors.append(f"净利润大幅下滑({profit_growth:.2f}%)")
                risk_level = "高"
            elif profit_growth < 0:
                risk_factors.append(f"净利润负增长({profit_growth:.2f}%)")
                if risk_level != "高":
                    risk_level = "中"

            # 2. 估值风险
            pe = stock.get('市盈率', 100)
            if pe > 100:
                risk_factors.append(f"市盈率过高({pe:.2f}倍)")
                risk_level = "高"
            elif pe < 0:  # 亏损
                risk_factors.append(f"市盈率为负({pe:.2f}),公司亏损")
                if risk_level != "高":
                    risk_level = "中"

            # 3. 规模风险
            market_cap = stock.get('总市值', 0)
            if market_cap < 500000000:  # 5亿以下
                risk_factors.append("市值过小(<5亿)，流动性风险")
                if risk_level == "低":
                    risk_level = "中"

            # 4. 财务风险 (如果获得更多信息)
            pb = stock.get('市净率', 10)
            if pb > 10:
                risk_factors.append(f"市净率过高({pb:.2f})")
                if risk_level != "高":
                    risk_level = "中"

            risk_indicators.append({
                '代码': stock.get('代码', ''),
                '风险等级': risk_level,
                '风险因素': risk_factors
            })

        return risk_indicators

    # ─── 保存结果 ────────────────────────────────────

    def save_screening_result(self, stocks_df):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        csv_path = os.path.join(self.data_dir, f'screening_result_{timestamp}.csv')
        # 保存包含综合评分的结果
        columns_to_save = [col for col in stocks_df.columns if col not in ['Unnamed: 0'] if not col.startswith('_')]
        stocks_df[columns_to_save].to_csv(csv_path, index=False, encoding='utf-8-sig')

        json_path = os.path.join(self.data_dir, 'latest_screening_result.json')
        # 将DataFrame转换为字典列表时处理特殊值
        records = []
        for _, row in stocks_df.iterrows():
            record = {}
            for col in stocks_df.columns:
                val = row[col]
                if pd.isna(val):
                    val = None
                elif isinstance(val, (pd.Int64Dtype, pd.Int32Dtype)):
                    val = int(val) if not pd.isna(val) else None
                elif isinstance(val, (int, float)):
                    if pd.isna(val):
                        val = None
                    else:
                        val = float(val) if isinstance(val, float) else int(val)
                record[col] = val
            records.append(record)

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        latest_csv = os.path.join(self.data_dir, 'latest_screening_result.csv')
        stocks_df[columns_to_save].to_csv(latest_csv, index=False, encoding='utf-8-sig')

        root_json = os.path.join(os.path.dirname(__file__), 'screening_result.json')
        with open(root_json, 'w', encoding='utf-8') as f:
            json.dump({
                'stocks': records,
                'timestamp': datetime.now().isoformat(),
                'count': len(stocks_df),
            }, f, ensure_ascii=False, indent=2)

        print(f"\n结果已保存:")
        print(f"  CSV: {csv_path}")
        print(f"  Root JSON: {root_json}")
        return csv_path, json_path

    def run_screening(self):
        print("\n" + "=" * 50)
        print(f"A股小市值股票筛选 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)

        top_stocks = self.screen_stocks()

        if top_stocks is not None and not top_stocks.empty:
            self.save_screening_result(top_stocks)
            return {
                'success': True,
                'count': len(top_stocks),
                'stocks': top_stocks.to_dict('records'),
                'timestamp': datetime.now().isoformat(),
            }
        return {
            'success': False,
            'error': '筛选失败，未获取到数据',
            'timestamp': datetime.now().isoformat(),
        }

    # ─── 工具方法 ────────────────────────────────────

    @staticmethod
    def _safe_float(val):
        if val is None or val == '-' or val == '':
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0


if __name__ == '__main__':
    collector = AStockDataCollector()
    result = collector.run_screening()
    if result['success']:
        print(f"\n筛选完成! 共 {result['count']} 只股票")
    else:
        print(f"\n筛选失败: {result['error']}")
