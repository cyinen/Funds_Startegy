import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import date
from typing import Optional

import os
import random
import time
import requests
import json

import warnings
import matplotlib
import platform
import os,sys
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import warnings

try:
    from pytdx.hq import TdxHq_API
    HAS_PYTDX = True
except ImportError:
    HAS_PYTDX = False
    print("⚠️ 未安装 pytdx，pip install pytdx")
matplotlib.use('Agg')
warnings.filterwarnings("ignore")


def setup_chinese_matplotlib():
    """跨平台中文字体配置 - 终极方案"""
    
    # 1️⃣ 优先尝试加载项目自带字体
    local_font = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                               'fonts', 'SourceHanSansSC-Regular.otf')
    if os.path.exists(local_font):
        fm.fontManager.addfont(local_font)
        font_name = fm.FontProperties(fname=local_font).get_name()
        plt.rcParams['font.sans-serif'] = [font_name, 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        return
    
    # 2️⃣ 按系统选择 + 自动检测可用性
    system = platform.system()
    candidates = {
        'Windows': ['Microsoft YaHei', 'SimHei'],
        'Darwin':  ['PingFang SC', 'Heiti SC', 'Arial Unicode MS'],
        'Linux':   ['WenQuanYi Micro Hei', 'Noto Sans CJK SC', 'Droid Sans Fallback'],
    }
    
    font_list = candidates.get(system, [])
    available = {f.name for f in fm.fontManager.ttflist}
    usable = [f for f in font_list if f in available]
    
    if not usable:
        # 3️⃣ 全部候选扫一遍
        all_candidates = sum(candidates.values(), [])
        usable = [f for f in all_candidates if f in available]
    
    usable.append('DejaVu Sans')
    plt.rcParams['font.sans-serif'] = usable
    plt.rcParams['axes.unicode_minus'] = False

setup_chinese_matplotlib()

# ========== 配置参数 ==========
CONFIG = {
    'initial_capital': 1000,
    'buy_threshold': -0.03,
    'sell_threshold': 0.07,
    'switch_threshold': 0.02,
    'stop_loss_threshold': -0.1,  # 硬止损线（回撤超过10%清仓）
    'cooldown_days': 15,           # [新增] 止损后的全局冷却期（交易日），期间禁止买入
    'ma_period': 120,
    'start_date': '2026-03-15',
    'end_date': None,
    'data_dir': './fund_data',
    'use_money_fund': True,
    'money_fund_yield_annual': 0.022,
}

FUND_POOL = {
    "515080": "招商中证红利ETF(场内）",
    "510880": "华泰柏瑞上证红利ETF(场内)",
    "515180": "易方达中证红利ETF(场内)",
    "513530": "华泰柏瑞中证港股通高股息投资ETF(QDII)(场内)",
    "563020": "易方达中证红利低波动ETF(场内)",
    "510720": "华夏上证国企红利ETF(场内)",
    "159209": "招商中证红利质量ETF(场内)",
}

MONEY_FUND = {
    "code": "511880",
    "name": "银华日利货币ETF",
}

BENCHMARK = {
    "code": "510300",
    "name": "沪深300ETF",
}

os.makedirs(CONFIG['data_dir'], exist_ok=True)


# ========== 通达信服务器列表 ==========
_TDX_SERVERS = [
    ('180.153.18.170', 7709),
    ('119.147.212.81', 7709),
    ('14.215.128.18', 7709),
    ('59.173.18.140', 7709),
    ('202.108.253.130', 7709),
    ('202.108.253.131', 7709),
    ('60.12.136.250', 7709),
    ('115.238.56.198', 7709),
    ('218.75.126.9', 7709),
    ('221.194.181.176', 7709),
]


def _get_market(fund_code: str) -> int:
    """0=深交所(0/1/3开头), 1=上交所(5/6/9开头)"""
    return 1 if fund_code[0] in ('5', '6', '9') else 0


# ========== pytdx 获取实时价格 ==========
def get_realtime_price(fund_code: str) -> Optional[dict]:
    """
    通过通达信获取 ETF 实时价格，自动轮询服务器。
    返回 dict 或 None。
    """
    if not HAS_PYTDX:
        return None

    market = _get_market(fund_code)
    servers = _TDX_SERVERS.copy()
    random.shuffle(servers)

    for host, port in servers:
        api = TdxHq_API()
        try:
            if not api.connect(host, port):
                continue

            data = api.get_security_quotes([(market, fund_code)])
            api.disconnect()

            if not data or len(data) == 0:
                continue

            q = data[0]
            price = q['price'] / 10
            last_close = q['last_close'] / 10

            if price <= 0:
                continue

            change_pct = round((price - last_close) / last_close * 100, 2) if last_close > 0 else 0.0

            return {
                'code':       fund_code,
                'price':      round(price, 4),
                'last_close': round(last_close, 4),
                'change_pct': change_pct,
                'time':       pd.Timestamp.now().strftime('%Y-%m-%d %H:%M'),
            }

        except Exception:
            try:
                api.disconnect()
            except Exception:
                pass
            continue

    print(f"  [{fund_code}] pytdx 所有服务器均失败")
    return None


# ========== 1. 数据获取（接口不变） ==========
def get_fund_k_history(fund_code: str, pz: int = 40000) -> pd.DataFrame:
    headers = {
        'User-Agent': 'EMProjJijin/6.2.8 (iPhone; iOS 13.6; Scale/2.00)',
        'GTOKEN': '98B423068C1F4DEF9842F82ADF08C5db',
        'clientInfo': 'ttjj-iPhone10,1-iOS-iOS13.6',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'fundmobapi.eastmoney.com',
        'Referer': 'https://mpservice.com/516939c37bdb4ba2b1138c50cf69a2e1/release/pages/FundHistoryNetWorth',
    }
    data = {
        'FCODE': fund_code,
        'appType': 'ttjj',
        'cToken': '1',
        'deviceid': '1',
        'pageIndex': '1',
        'pageSize': str(pz),
        'plat': 'Iphone',
        'product': 'EFund',
        'serverVersion': '6.2.8',
        'version': '6.2.8',
    }
    url = 'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNHisNetList'

    df = pd.DataFrame()
    try:
        resp = requests.get(url, headers=headers, data=data, timeout=10).json()
        if resp and resp.get('Datas'):
            rows = []
            for item in resp['Datas']:
                rows.append({
                    '日期':   item['FSRQ'],
                    '单位净值': item['DWJZ'],
                    '累计净值': item['LJJZ'],
                    '涨跌幅':  item['JZZZL'],
                })
            df = pd.DataFrame(rows)
            df['单位净值'] = pd.to_numeric(df['单位净值'], errors='coerce')
            df['累计净值'] = pd.to_numeric(df['累计净值'], errors='coerce')
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
            df = df.sort_values('日期').reset_index(drop=True)
    except Exception as e:
        print(f"获取 {fund_code} 历史净值失败: {e}")

    # ---- 判断是否需要补充当天数据 ----
    today = pd.Timestamp(date.today())

    if today.weekday() >= 5:
        return df
    if not df.empty and (df['日期'] == today).any():
        return df

    # ---- pytdx 获取实时价格补充 ----
    quote = get_realtime_price(fund_code)
    if quote:
        try:
            quote_date = pd.to_datetime(quote['time']).normalize()
            if quote_date == today:
                last_ljjz = None
                if not df.empty:
                    last_row = df.iloc[-1]
                    if pd.notna(last_row['累计净值']) and pd.notna(last_row['单位净值']):
                        last_ljjz = round(
                            last_row['累计净值'] + (quote['price'] - last_row['单位净值']), 4
                        )

                est_row = pd.DataFrame([{
                    '日期':    quote_date,
                    '单位净值': quote['price'],
                    '累计净值': last_ljjz,
                    '涨跌幅':  str(quote['change_pct']),
                }])
                df = pd.concat([df, est_row], ignore_index=True)
                df = df.sort_values('日期').reset_index(drop=True)
                print(f"[{fund_code}] 补充实时价格 {quote['price']}"
                      f"（{quote['change_pct']}%，{quote['time']}）")
        except Exception as e:
            print(f"[{fund_code}] 解析实时数据失败: {e}")

    return df

def fetch_all_fund_data():
    print("=" * 60)
    print(f"正在获取基金数据... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    fund_data = {}
    for code, name in FUND_POOL.items():
        df = get_fund_k_history(code)
        if len(df) > 0:
            df.to_csv(f"{CONFIG['data_dir']}/{code}_raw.csv", index=False, encoding='utf-8-sig')
            df_clean = df.rename(columns={'单位净值': 'close', '日期': 'date'})[['date', 'close']].dropna()
            fund_data[code] = df_clean
            print(f"✓ {code} ({name}): {len(df)} 条, "
                  f"{df_clean['date'].min().strftime('%Y-%m-%d')} ~ {df_clean['date'].max().strftime('%Y-%m-%d')}")
        else:
            print(f"✗ {code} ({name}): 获取失败")

    benchmark_df = get_fund_k_history(BENCHMARK['code'])
    if len(benchmark_df) > 0:
        benchmark_df.to_csv(f"{CONFIG['data_dir']}/{BENCHMARK['code']}_raw.csv", index=False, encoding='utf-8-sig')
        benchmark_df = benchmark_df.rename(columns={'单位净值': 'close', '日期': 'date'})[['date', 'close']].dropna()
        print(f"✓ {BENCHMARK['code']} ({BENCHMARK['name']}): {len(benchmark_df)} 条")

    return fund_data, benchmark_df


def prepare_merged_data(fund_data, benchmark_df):
    merged_df = None
    for code, df in fund_data.items():
        df_copy = df.rename(columns={'close': f'close_{code}'})
        if merged_df is None:
            merged_df = df_copy
        else:
            merged_df = pd.merge(merged_df, df_copy, on='date', how='outer')

    bench_copy = benchmark_df.rename(columns={'close': 'close_benchmark'})
    merged_df = pd.merge(merged_df, bench_copy, on='date', how='outer')
    merged_df = merged_df.sort_values('date').reset_index(drop=True)
    merged_df = merged_df.ffill().bfill()

    for code in fund_data.keys():
        col = f'close_{code}'
        merged_df[f'MA{CONFIG["ma_period"]}_{code}'] = merged_df[col].rolling(window=CONFIG['ma_period']).mean()
        merged_df[f'deviation_{code}'] = merged_df[col] / merged_df[f'MA{CONFIG["ma_period"]}_{code}'] - 1

    merged_df[f'MA{CONFIG["ma_period"]}_benchmark'] = (
        merged_df['close_benchmark'].rolling(window=CONFIG['ma_period']).mean()
    )

    merged_df = merged_df[merged_df['date'] >= CONFIG['start_date']].reset_index(drop=True)
    merged_df = merged_df.dropna().reset_index(drop=True)

    return merged_df


# ========== 2. 回测 ==========
def run_backtest(merged_df, fund_data):
    capital = CONFIG['initial_capital']
    position = {code: 0.0 for code in fund_data.keys()}
    money_position = 0.0
    holding_code = None          
    holding_cost = None          

    # [新增] 冷却期计数器
    cooldown_counter = 0

    portfolio_values = []
    trade_log = []
    daily_status = []

    if CONFIG['use_money_fund']:
        print(f"正在获取货币基金 {MONEY_FUND['code']} ({MONEY_FUND['name']}) 数据...")
        raw = get_fund_k_history(MONEY_FUND['code'])
        if len(raw) > 0:
            mdf = raw.rename(columns={'单位净值': 'close', '日期': 'date'})[['date', 'close']].dropna()
            mdf = mdf.sort_values('date').reset_index(drop=True)
            mdf = mdf.rename(columns={'close': 'close_money'})
            merged_df = pd.merge(merged_df, mdf[['date', 'close_money']], on='date', how='left')
            merged_df['close_money'] = merged_df['close_money'].ffill().bfill()
            print(f"✓ 货币基金初始净值: {merged_df['close_money'].iloc[0]:.4f}")
        else:
            print("✗ 货币基金数据获取失败，回退为现金模式")
            CONFIG['use_money_fund'] = False

    has_money_col = 'close_money' in merged_df.columns

    for idx, row in merged_df.iterrows():
        date = row['date']
        deviations = {code: row[f'deviation_{code}'] for code in fund_data.keys()}
        action_today = None

        # [新增] 每天一开始，先消耗当天的冷却时间
        if cooldown_counter > 0:
            cooldown_counter -= 1

        # --- 1. 卖出 / 止损判断 ---
        if holding_code is not None:
            current_dev = deviations[holding_code]
            current_price = row[f'close_{holding_code}']
            current_return = (current_price / holding_cost) - 1 if holding_cost else 0
            
            is_take_profit = current_dev > CONFIG['sell_threshold']
            is_stop_loss = current_return <= CONFIG['stop_loss_threshold']

            if is_take_profit or is_stop_loss:
                sell_price = current_price
                capital = position[holding_code] * sell_price
                old_code = holding_code                      

                action_type = '卖出(止盈)' if is_take_profit else '卖出(止损)'
                reason = f'偏离度{current_dev * 100:.2f}% > {CONFIG["sell_threshold"] * 100}%' if is_take_profit else f'回撤 {current_return * 100:.2f}% 触碰止损线'

                trade_log.append({
                    'date': date,
                    'action': action_type,
                    'code': old_code,
                    'name': FUND_POOL[old_code],
                    'price': sell_price,
                    'shares': position[old_code],
                    'deviation': current_dev,
                    'value': capital,
                    'reason': reason,
                })
                position[old_code] = 0
                holding_code = None
                holding_cost = None  
                action_today = action_type

                # [核心修正] 如果是触发了止损，立刻开启冷却期，期间禁止买入任何红利基金
                if is_stop_loss:
                    cooldown_counter = CONFIG['cooldown_days']

                if CONFIG['use_money_fund'] and has_money_col and pd.notna(row.get('close_money')):
                    money_position = capital / row['close_money']
                    trade_log.append({
                        'date': date,
                        'action': '买入货币',
                        'code': MONEY_FUND['code'],
                        'name': MONEY_FUND['name'],
                        'price': row['close_money'],
                        'shares': money_position,
                        'deviation': 0,
                        'value': capital,
                        'reason': '资金转入货币基金避险',
                    })
                    capital = 0
                    action_today += ' → 货币基金'

        # --- 2. 买入 / 换仓判断 ---
        # [核心修正] 如果在冷却期内，直接把候选池清空，强制观望
        if cooldown_counter > 0:
            candidates = {}
        else:
            candidates = {c: d for c, d in deviations.items() if d < CONFIG['buy_threshold']}

        if candidates:
            best_code = min(candidates, key=candidates.get)
            best_dev = candidates[best_code]

            if holding_code is None:
                if CONFIG['use_money_fund'] and money_position > 0 and has_money_col and pd.notna(row.get('close_money')):
                    capital = money_position * row['close_money']
                    trade_log.append({
                        'date': date,
                        'action': '赎回货币',
                        'code': MONEY_FUND['code'],
                        'name': MONEY_FUND['name'],
                        'price': row['close_money'],
                        'shares': money_position,
                        'deviation': 0,
                        'value': capital,
                        'reason': '赎回货币基金用于买入红利',
                    })
                    money_position = 0

                if capital > 0:
                    buy_price = row[f'close_{best_code}']
                    shares = capital / buy_price
                    position[best_code] = shares
                    holding_code = best_code
                    holding_cost = buy_price  
                    trade_log.append({
                        'date': date,
                        'action': '买入',
                        'code': best_code,
                        'name': FUND_POOL[best_code],
                        'price': buy_price,
                        'shares': shares,
                        'deviation': best_dev,
                        'value': capital,
                        'reason': f'偏离度{best_dev * 100:.2f}% < {CONFIG["buy_threshold"] * 100}%',
                    })
                    capital = 0
                    action_today = f'买入 {best_code}'

            elif best_code != holding_code and best_dev < deviations[holding_code] - CONFIG['switch_threshold']:
                old_code = holding_code
                sell_price = row[f'close_{old_code}']
                capital = position[old_code] * sell_price
                trade_log.append({
                    'date': date,
                    'action': '换仓卖',
                    'code': old_code,
                    'name': FUND_POOL[old_code],
                    'price': sell_price,
                    'shares': position[old_code],
                    'deviation': deviations[old_code],
                    'value': capital,
                    'reason': f'{best_code}偏离度更低，差{(deviations[old_code] - best_dev) * 100:.2f}%',
                })
                position[old_code] = 0

                buy_price = row[f'close_{best_code}']
                shares = capital / buy_price
                position[best_code] = shares
                holding_code = best_code
                holding_cost = buy_price  
                trade_log.append({
                    'date': date,
                    'action': '换仓买',
                    'code': best_code,
                    'name': FUND_POOL[best_code],
                    'price': buy_price,
                    'shares': shares,
                    'deviation': best_dev,
                    'value': capital,
                    'reason': f'偏离度最低{best_dev * 100:.2f}%',
                })
                capital = 0
                action_today = f'换仓 {old_code} → {best_code}'

        # --- 3. 空仓且有现金 → 转入货币基金 ---
        if holding_code is None and capital > 100:
            if CONFIG['use_money_fund'] and has_money_col and pd.notna(row.get('close_money')):
                money_position = capital / row['close_money']
                capital = 0

        pv = capital
        if holding_code is not None:
            pv += position[holding_code] * row[f'close_{holding_code}']
        if money_position > 0 and has_money_col and pd.notna(row.get('close_money')):
            pv += money_position * row['close_money']

        if holding_code:
            h_name = FUND_POOL[holding_code]
            h_code = holding_code
        elif money_position > 0:
            h_name = MONEY_FUND['name']
            h_code = MONEY_FUND['code']
        else:
            h_name = '现金'
            h_code = ''

        portfolio_values.append({
            'date': date,
            'portfolio_value': pv,
            'holding_code': h_code,
            'holding_name': h_name,
            'holding_cost': holding_cost,  
            'cooldown': cooldown_counter,  # [新增] 记录当前的冷却天数，方便排查
            'cash': capital,
            'money_shares': money_position,
            'action': action_today or '',
        })

        daily_record = {
            'date': date,
            'portfolio_value': pv,
            'holding': h_name,
            'benchmark_close': row['close_benchmark'],
            'money_close': row.get('close_money', None),
        }
        for code in fund_data.keys():
            daily_record[f'close_{code}'] = row[f'close_{code}']
            daily_record[f'MA120_{code}'] = row[f'MA{CONFIG["ma_period"]}_{code}']
            daily_record[f'deviation_{code}'] = deviations[code]
        daily_status.append(daily_record)

    return portfolio_values, trade_log, daily_status

# ========== 3. 保存结果 ==========
def save_results(portfolio_values, trade_log, daily_status, merged_df, fund_data):
    trade_df = None
    if trade_log:
        trade_df = pd.DataFrame(trade_log)
        trade_df['date'] = trade_df['date'].dt.strftime('%Y-%m-%d')
        trade_df.to_csv(f"{CONFIG['data_dir']}/trade_history.csv", index=False, encoding='utf-8-sig')
        trade_df.to_excel(f"{CONFIG['data_dir']}/trade_history.xlsx", index=False)
        print(f"✓ 交易记录已保存: trade_history.csv/xlsx ({len(trade_df)} 条)")

    portfolio_df = pd.DataFrame(portfolio_values)
    portfolio_df['date'] = portfolio_df['date'].dt.strftime('%Y-%m-%d')
    portfolio_df.to_csv(f"{CONFIG['data_dir']}/portfolio_daily.csv", index=False, encoding='utf-8-sig')
    print(f"✓ 每日净值已保存: portfolio_daily.csv ({len(portfolio_df)} 条)")

    daily_df = pd.DataFrame(daily_status)
    daily_df['date'] = daily_df['date'].dt.strftime('%Y-%m-%d')
    daily_df.to_csv(f"{CONFIG['data_dir']}/daily_status.csv", index=False, encoding='utf-8-sig')
    print(f"✓ 每日状态已保存: daily_status.csv ({len(daily_df)} 条)")

    latest = daily_status[-1]
    current_status = {
        'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'latest_date': latest['date'].strftime('%Y-%m-%d') if isinstance(latest['date'], pd.Timestamp) else latest['date'],
        'portfolio_value': round(latest['portfolio_value'], 2),
        'holding': latest['holding'],
        'funds': {},
    }
    for code in fund_data.keys():
        current_status['funds'][code] = {
            'name': FUND_POOL[code],
            'close': round(latest[f'close_{code}'], 4),
            'MA120': round(latest[f'MA120_{code}'], 4),
            'deviation': round(latest[f'deviation_{code}'] * 100, 2),
            'signal': get_signal(latest[f'deviation_{code}']),
        }
    with open(f"{CONFIG['data_dir']}/current_status.json", 'w', encoding='utf-8') as f:
        json.dump(current_status, f, ensure_ascii=False, indent=2)
    print(f"✓ 当前状态已保存: current_status.json")

    with open(f"{CONFIG['data_dir']}/config.json", 'w', encoding='utf-8') as f:
        json.dump(CONFIG, f, ensure_ascii=False, indent=2)

    return portfolio_df, trade_df


def get_signal(deviation):
    if deviation < CONFIG['buy_threshold']:
        return '买入'
    elif deviation > CONFIG['sell_threshold']:
        return '卖出'
    return '观望'


# ========== 4. 统计 ==========
def calculate_statistics(portfolio_values, daily_status, fund_data):
    portfolio_df = pd.DataFrame(portfolio_values)
    daily_df = pd.DataFrame(daily_status)

    initial = CONFIG['initial_capital']
    final_value = portfolio_df['portfolio_value'].iloc[-1]
    total_return = (final_value / initial - 1) * 100
    benchmark_return = (daily_df['benchmark_close'].iloc[-1] / daily_df['benchmark_close'].iloc[0] - 1) * 100

    days = (portfolio_df['date'].iloc[-1] - portfolio_df['date'].iloc[0]).days
    years = max(days / 365, 1 / 365)
    annual_return = ((final_value / initial) ** (1 / years) - 1) * 100

    cummax = portfolio_df['portfolio_value'].cummax()
    drawdown = (portfolio_df['portfolio_value'] - cummax) / cummax
    max_drawdown = drawdown.min() * 100

    fund_returns = {}
    for code in fund_data.keys():
        first = daily_df[f'close_{code}'].iloc[0]
        last = daily_df[f'close_{code}'].iloc[-1]
        fund_returns[code] = (last / first - 1) * 100

    return {
        'initial_capital': initial,
        'final_value': final_value,
        'total_return': total_return,
        'benchmark_return': benchmark_return,
        'excess_return': total_return - benchmark_return,
        'annual_return': annual_return,
        'max_drawdown': max_drawdown,
        'trading_days': len(portfolio_df),
        'years': years,
        'fund_returns': fund_returns,
    }


# ========== 5. 报告 ==========
def print_report(stats, trade_log, daily_status, fund_data):
    print("\n" + "=" * 70)
    print("【📊 回测结果报告】")
    print("=" * 70)

    d0 = daily_status[0]['date']
    d1 = daily_status[-1]['date']
    fmt = lambda d: d.strftime('%Y-%m-%d') if isinstance(d, pd.Timestamp) else d
    print(f"\n📅 回测期间: {fmt(d0)} ~ {fmt(d1)}")
    print(f"📆 交易天数: {stats['trading_days']} 天 ({stats['years']:.2f} 年)")

    print(f"\n💰 资金情况:")
    print(f"   初始资金: {stats['initial_capital']:>15,.0f} 元")
    print(f"   最终市值: {stats['final_value']:>15,.2f} 元")
    print(f"   绝对收益: {stats['final_value'] - stats['initial_capital']:>15,.2f} 元")

    print(f"\n📈 收益指标:")
    print(f"   策略总收益:   {stats['total_return']:>10.2f}%")
    print(f"   沪深300收益:  {stats['benchmark_return']:>10.2f}%")
    print(f"   超额收益:     {stats['excess_return']:>10.2f}%")
    print(f"   年化收益:     {stats['annual_return']:>10.2f}%")
    print(f"   最大回撤:     {stats['max_drawdown']:>10.2f}%")

    print(f"\n📊 各基金买入持有收益:")
    for code, ret in stats['fund_returns'].items():
        print(f"   {code} {FUND_POOL[code]:<12}: {ret:>8.2f}%")

    if trade_log:
        tdf = pd.DataFrame(trade_log)
        buy_count = len(tdf[tdf['action'].isin(['买入', '换仓买'])])
        sell_count = len(tdf[tdf['action'].isin(['卖出(止盈)', '卖出(止损)', '换仓卖'])])
        print(f"\n🔄 交易统计: 共 {len(tdf)} 笔 (红利买入 {buy_count}, 红利卖出 {sell_count})")


def print_current_status(daily_status, portfolio_values, fund_data):
    latest = daily_status[-1]
    latest_pv = portfolio_values[-1]

    print("\n" + "=" * 70)
    print("【🎯 当前状态与操作建议】")
    print("=" * 70)

    fmt = lambda d: d.strftime('%Y-%m-%d') if isinstance(d, pd.Timestamp) else d
    print(f"\n📅 数据日期: {fmt(latest['date'])}")
    print(f"💼 当前持仓: {latest_pv['holding_name']}")
    print(f"💰 账户市值: {latest['portfolio_value']:,.2f} 元")

    print(f"\n📊 各基金状态:")
    print("-" * 70)
    print(f"{'代码':<10} {'名称':<14} {'收盘价':<10} {'MA120':<10} {'偏离度':<10} {'信号':<10}")
    print("-" * 70)

    signals = []
    for code in fund_data.keys():
        close = latest[f'close_{code}']
        ma120 = latest[f'MA120_{code}']
        dev = latest[f'deviation_{code}']
        signal = get_signal(dev)
        icon = {"买入": "🟢 买入", "卖出": "🔴 卖出"}.get(signal, "⚪ 观望")
        print(f"{code:<10} {FUND_POOL[code]:<12} {close:<10.4f} {ma120:<10.4f} {dev * 100:>6.2f}%    {icon}")
        signals.append({'code': code, 'dev': dev, 'signal': signal})
    print("-" * 70)

    print("\n💡 操作建议:")
    current_holding = latest_pv['holding_code']
    holding_cost = latest_pv.get('holding_cost')
    buy_cands = [s for s in signals if s['signal'] == '买入']

    if current_holding and current_holding in FUND_POOL:
        current_dev = latest[f'deviation_{current_holding}']
        current_price = latest[f'close_{current_holding}']
        current_return = (current_price / holding_cost) - 1 if holding_cost else 0
        
        # [修改] 加入止损提示
        if current_return <= CONFIG['stop_loss_threshold']:
            print(f"   ⚠️  当前持仓 {current_holding}({FUND_POOL[current_holding]}) 回撤 {current_return * 100:.2f}% 已触发硬止损！")
            print(f"   👉 建议: 【卖出(止损)】→ 转入货币基金")


        elif current_dev > CONFIG['sell_threshold']:
            print(f"   ⚠️  当前持仓 {current_holding}({FUND_POOL[current_holding]}) 偏离度 {current_dev * 100:.2f}% 已超卖出线")
            print(f"   👉 建议: 【卖出(止盈)】→ 转入货币基金")


        elif buy_cands:
            best = min(buy_cands, key=lambda x: x['dev'])
            if best['code'] != current_holding and best['dev'] < current_dev - CONFIG['switch_threshold']:
                print(f"   ⚠️  更优基金 {best['code']}({FUND_POOL[best['code']]}) 偏离度 {best['dev'] * 100:.2f}%")
                print(f"   👉 建议: 【换仓】{current_holding} → {best['code']}")

            
            else:
                print(f"   ✅ 持仓 {current_holding} 偏离度 {current_dev * 100:.2f}%，当前盈亏 {current_return * 100:.2f}%")
                print(f"   👉 建议: 【持有】")

        else:
            print(f"   ✅ 持仓 {current_holding} 偏离度 {current_dev * 100:.2f}%，当前盈亏 {current_return * 100:.2f}%")
            print(f"   👉 建议: 【持有】")

    else:
        if buy_cands:
            best = min(buy_cands, key=lambda x: x['dev'])
            print(f"   🎯 买入机会: {best['code']}({FUND_POOL[best['code']]}) 偏离度 {best['dev'] * 100:.2f}%")
            print(f"   👉 建议: 【赎回货币基金 → 买入 {best['code']}】")

        else:
            print(f"   ⏳ 无买入信号")
            print(f"   👉 建议: 【观望】资金停留在货币基金")

    print("=" * 70)


def print_trade_history(trade_log):
    print("\n" + "=" * 70)
    print("【📝 完整交易记录】")
    print("=" * 70)
    if not trade_log:
        print("暂无交易记录")
        return
    tdf = pd.DataFrame(trade_log)
    print(f"\n{'序号':<4} {'日期':<12} {'操作':<10} {'代码':<8} {'名称':<14} {'价格':<10} {'份额':<14} {'偏离度':<8} {'金额':<12}")
    print("-" * 106)
    for i, r in tdf.iterrows():
        ds = r['date'].strftime('%Y-%m-%d') if isinstance(r['date'], pd.Timestamp) else r['date']
        print(f"{i + 1:<4} {ds:<12} {r['action']:<10} {r['code']:<8} {r['name']:<12} "
              f"{r['price']:<10.4f} {r['shares']:<14.2f} {r['deviation'] * 100:>6.2f}% {r['value']:>12,.2f}")
    print("-" * 106)
    print(f"共 {len(tdf)} 笔交易")


# ========== 6. 图表 ==========
def plot_charts(portfolio_values, daily_status, trade_log, fund_data):
    portfolio_df = pd.DataFrame(portfolio_values)
    daily_df = pd.DataFrame(daily_status)

    initial = CONFIG['initial_capital']
    portfolio_df['strategy_return'] = portfolio_df['portfolio_value'] / initial - 1
    daily_df['benchmark_return'] = daily_df['benchmark_close'] / daily_df['benchmark_close'].iloc[0] - 1
    for code in fund_data.keys():
        first_close = daily_df[f'close_{code}'].iloc[0]
        daily_df[f'return_{code}'] = daily_df[f'close_{code}'] / first_close - 1

    fig, axes = plt.subplots(3, 1, figsize=(15, 13))

    # 图1: 收益率对比
    ax1 = axes[0]
    fr = portfolio_df['strategy_return'].iloc[-1] * 100
    br = daily_df['benchmark_return'].iloc[-1] * 100
    ax1.plot(portfolio_df['date'], portfolio_df['strategy_return'] * 100,
             label=f'红利轮动策略 ({fr:.1f}%)', linewidth=2.5, color='red')
    ax1.plot(daily_df['date'], daily_df['benchmark_return'] * 100,
             label=f'沪深300ETF ({br:.1f}%)', linewidth=2, color='blue', alpha=0.8)
    colors_list = ['green', 'orange', 'purple', 'brown', 'cyan']
    for i, code in enumerate(fund_data.keys()):
        ret = daily_df[f'return_{code}'].iloc[-1] * 100
        ax1.plot(daily_df['date'], daily_df[f'return_{code}'] * 100,
                 label=f'{FUND_POOL[code]} ({ret:.1f}%)', linewidth=1.5,
                 alpha=0.6, linestyle='--', color=colors_list[i % len(colors_list)])
    ax1.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    ax1.fill_between(portfolio_df['date'], 0, portfolio_df['strategy_return'] * 100,
                     where=portfolio_df['strategy_return'] >= 0, alpha=0.1, color='red')
    ax1.fill_between(portfolio_df['date'], 0, portfolio_df['strategy_return'] * 100,
                     where=portfolio_df['strategy_return'] < 0, alpha=0.1, color='green')
    ax1.set_ylabel('累计收益率 (%)')
    ax1.set_title('📈 策略收益率对比', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.3)

    # 图2: 净值 + 交易标记
    ax2 = axes[1]
    ax2.plot(portfolio_df['date'], portfolio_df['portfolio_value'] / 10000, label='策略净值', linewidth=2, color='red')
    ax2.axhline(y=initial / 10000, color='gray', linestyle='--', linewidth=1)
    if trade_log:
        tdf = pd.DataFrame(trade_log)
        for _, t in tdf.iterrows():
            idx = portfolio_df[portfolio_df['date'] == t['date']].index
            if len(idx) > 0:
                val = portfolio_df.loc[idx[0], 'portfolio_value'] / 10000
                if t['action'] in ['买入', '换仓买']:
                    ax2.scatter(t['date'], val, color='green', marker='^', s=100, zorder=5)
                elif '卖出' in t['action'] or t['action'] == '换仓卖':
                    ax2.scatter(t['date'], val, color='red', marker='v', s=100, zorder=5)
    fv = portfolio_df['portfolio_value'].iloc[-1]
    cm = portfolio_df['portfolio_value'].cummax()
    md = ((portfolio_df['portfolio_value'] - cm) / cm).min() * 100
    ax2.set_ylabel('账户价值 (万元)')
    ax2.set_title(f'💰 净值 | {initial / 10000:.0f}万 → {fv / 10000:.2f}万 | 最大回撤 {md:.1f}%', fontsize=12)
    ax2.grid(True, alpha=0.3)
    from matplotlib.lines import Line2D
    ax2.legend(handles=[
        Line2D([0], [0], color='red', linewidth=2, label='策略净值'),
        Line2D([0], [0], marker='^', color='w', markerfacecolor='green', markersize=10, label='买入'),
        Line2D([0], [0], marker='v', color='w', markerfacecolor='red', markersize=10, label='卖出/止损'),
    ], loc='upper left')

    # 图3: 偏离度
    ax3 = axes[2]
    for i, code in enumerate(fund_data.keys()):
        ax3.plot(daily_df['date'], daily_df[f'deviation_{code}'] * 100,
                 label=FUND_POOL[code], linewidth=1.5, color=colors_list[i % len(colors_list)], alpha=0.8)
    ax3.axhline(y=CONFIG['buy_threshold'] * 100, color='green', linestyle='--', linewidth=2,
                label=f'买入线 ({CONFIG["buy_threshold"] * 100:.0f}%)')
    ax3.axhline(y=CONFIG['sell_threshold'] * 100, color='red', linestyle='--', linewidth=2,
                label=f'卖出线 ({CONFIG["sell_threshold"] * 100:.0f}%)')
    ax3.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    ax3.fill_between(daily_df['date'], CONFIG['buy_threshold'] * 100, -30, alpha=0.1, color='green')
    ax3.fill_between(daily_df['date'], CONFIG['sell_threshold'] * 100, 30, alpha=0.1, color='red')
    ax3.set_ylabel('偏离MA120 (%)')
    ax3.set_xlabel('日期')
    ax3.set_title('📊 各基金相对MA120偏离度', fontsize=14)
    ax3.legend(loc='upper right', fontsize=9)
    ax3.grid(True, alpha=0.3)
    ax3.set_ylim(-30, 30)

    plt.tight_layout()
    plt.savefig(f"{CONFIG['data_dir']}/strategy_chart.png", dpi=150, bbox_inches='tight')
    plt.show()
    print(f"✓ 图表已保存: {CONFIG['data_dir']}/strategy_chart.png")

# ========== 7. fix_Windows_encoding issues ==========
def fix_console_encoding():
    """
    修复Windows控制台Unicode编码问题
    当控制台使用GBK编码时，某些Unicode字符无法正常显示
    此函数将标准输出和标准错误流的编码设置为UTF-8
    """
    if sys.platform.startswith('win'):
        # Windows系统下，设置控制台编码为UTF-8
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer, 
            encoding='utf-8',
            errors='replace',  # 遇到无法编码的字符时用?替换
            line_buffering=True
        )
        sys.stderr = io.TextIOWrapper(
            sys.stderr.buffer, 
            encoding='utf-8',
            errors='replace',
            line_buffering=True
        )
        
        # 尝试设置控制台代码页为UTF-8
        try:
            import subprocess
            subprocess.run(['chcp', '65001'], shell=True, capture_output=True)
        except:
            pass
        
        return "已设置控制台编码为UTF-8"
    else:
        # 非Windows系统通常使用UTF-8，无需特殊处理
        return "非Windows系统，使用默认UTF-8编码"

# ========== 8. 主函数 ==========
def main():
    fix_console_encoding()
    
    print("\n" + "🚀 " * 15)
    print("     红利基金轮动策略回测系统（含硬止损）")
    print("🚀 " * 15)

    fund_data, benchmark_df = fetch_all_fund_data()
    if not fund_data:
        print("❌ 获取数据失败")
        return

    merged_df = prepare_merged_data(fund_data, benchmark_df)
    print(f"\n✓ 数据准备完成，有效交易日: {len(merged_df)} 天")

    portfolio_values, trade_log, daily_status = run_backtest(merged_df, fund_data)
    stats = calculate_statistics(portfolio_values, daily_status, fund_data)

    print("\n" + "-" * 60)
    print("正在保存数据...")
    save_results(portfolio_values, trade_log, daily_status, merged_df, fund_data)

    print_report(stats, trade_log, daily_status, fund_data)
    print_trade_history(trade_log)
    print_current_status(daily_status, portfolio_values, fund_data)
    plot_charts(portfolio_values, daily_status, trade_log, fund_data)

    print("\n" + "=" * 70)
    print(f"✅ 所有数据已保存至: {CONFIG['data_dir']}/")
    print("=" * 70)


if __name__ == "__main__":
    main()
