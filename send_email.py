import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.utils import formataddr
# ====== 配置区 ======
smtp_server = 'smtp.qq.com'
smtp_port = 465

sender = 'xx@qq.com'
password = 'xxxxx'   # ⚠️不是QQ密码

receiver = 'xx@qq.com'
# ===== 读取日志 =====
with open('log.txt', 'r', encoding='utf-8') as f:
    log = f.read()

# ===== 提取关键信息 =====
def extract(pattern):
    match = re.search(pattern, log)
    return match.group(1) if match else "N/A"

total_return = extract(r'策略总收益:\s+([\d\.%]+)')
annual_return = extract(r'年化收益:\s+([\d\.%]+)')
max_drawdown = extract(r'最大回撤:\s+([-\d\.%]+)')
holding = extract(r'当前持仓:\s+(.+)')
account_value = extract(r'账户市值:\s+([\d,\.]+)')
advice = extract(r'建议:\s+【(.+?)】')
profit = extract(r'当前盈亏\s+([\d\.%]+)')

# ===== HTML正文 =====
html = f"""
<html>
<body style="font-family:Arial;">
<h2>📊 红利轮动策略日报</h2>

<p><b>💰 市值：</b>{account_value} 元</p>
<p><b>📈 总收益：</b><span style="color:red;">{total_return}</span></p>
<p><b>📊 年化收益：</b>{annual_return}</p>
<p><b>⚠️ 最大回撤：</b>{max_drawdown}</p>

<hr>

<p><b>💼 当前持仓：</b>{holding}</p>

<p style="font-size:18px;">
<b>💡 操作建议：</b>
👉 <span style="color:blue;">{advice}</span>（当前盈亏 {profit}）
</p>

<hr>

<p>📎 详细数据请查看附件（log.txt + 图表）</p>

</body>
</html>
"""

# ===== 构造邮件 =====
msg = MIMEMultipart()
msg['From'] = formataddr(("策略系统", sender))
msg['To'] = formataddr(("用户", receiver))
msg['Subject'] = f"📊策略日报：{total_return}"

# 正文
msg.attach(MIMEText(html, 'html', 'utf-8'))

# ===== 附件1：log.txt =====
try:
    with open('log.txt', 'rb') as f:
        part = MIMEApplication(f.read())
        part.add_header('Content-Disposition', 'attachment', filename='log.txt')
        msg.attach(part)
except:
    print("⚠️ log.txt 未找到")

# ===== 附件2：图表 =====
try:
    with open('./fund_data/strategy_chart.png', 'rb') as f:
        img = MIMEApplication(f.read())
        img.add_header('Content-Disposition', 'attachment', filename='strategy_chart.png')
        msg.attach(img)
except:
    print("⚠️ 图表未找到")

# ===== 发送 =====
try:
    smtp = smtplib.SMTP_SSL('smtp.qq.com', 465)
    smtp.login(sender, password)
    smtp.sendmail(sender, [receiver], msg.as_string())
    smtp.quit()
    print("✅ 邮件发送成功（含附件）")
except Exception as e:
    print("❌ 发送失败:", e)
