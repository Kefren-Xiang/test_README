# thrift-daily-inspection

## 流程简介

- 一共是三个主要文件：dump_stats.py负责每日抓取最新数据(跨度范围 一个月 )，analyze_with_bailian.py负责将数据和prompt交给百炼api返回analysis_results_XXXX-XX-XX.json，call_attention负责根据json内容和实际数据情况发送飞书报警
- 其他文件说明：.env负责相关配置，company.csv负责选取调查公司范围（配置时请注明国家与公司名）dump_stats会从company.csv中找到需要调查的共公司范围，Terminology_explanation.csv是一个辅助文件，用于将原表格中部分不符合规范的术语转义，例如qc_num指的是去重量，verbal_template.csv是从日常巡检手册中的常用报告提炼而来，用于引导LLM回答问题。

## 操作手册

- 我在每个程序中留了一个可自定义变量target_date，可用于追踪选定的日期。
- 默认启动方式如下：
```bash
python dump_stats.py

python analyze_with_bailian.py

python call_attention.py
```
- 选定日期target_date指的是分析日期，例如要选取的数据范围是2025-10-30 ~ 2025-11-30，则target_date=2025-11-30，具体方式如下：
```bash
python dump_stats.py --target_date=YYYY-MM-DD

python analyze_with_bailian.py --target_date=YYYY-MM-DD

python call_attention.py --target_date=YYYY-MM-DD
```
- 注：不建议三个脚本分开太长时间运行，综合考虑存储和记录影响，分析结果以json形式分开储存（analysis_results_YYYY-MM-DD.json），但是数据脚本覆盖储存，因此日期间隔过久后建议重新按顺序运行三个脚本，避免日期读取混乱。

## dump_stats.py说明

- 目前一共选取了四张基本表——oss_ba.linda_company_day_static_v1，oss_ba.linda_sipline_data，oss_ba.linda_wf_tmp_dpd_company_day_v2，oss_ba.linda_wf_tmp_dpd_company_day_v2（具体sql语言见附一）
- 基本表涵盖内容包括：公司整体各指标，账龄分布情况，各条线路情况，preg情况。从日常巡检角度来看基本覆盖主要影响范围，后期如果有其他需要附加，可届时再添加内容。
- 当前dump_stats.py存在问题：目前我调用的全部为linda表，数据更新速度慢（每日9:30——10:00拿不到需求数据），调用其他表以我个人账号而言缺少权限。
- 其他内容：代码开头为配置位，可以按需修改账号。
```python
HIVE_HOST = "47.243.228.238"
HIVE_PORT = 10000
HIVE_USERNAME = "zhenghao.xiang"
HIVE_DATABASE = "default"

COMPANY_CSV_PATH = "company.csv"
OUTPUT_DIR = "exports"
```

## analyze_with_bailian.py说明

- 脚本大致运行逻辑如下，读取数据后，与verbal_template.csv中的固定话术一一对比，找到最符合条件的话术，并自行推断出最能说明有问题/没问题的对比日期。记录json示例如下：
```json
  {
    "company": "IVR_Akulaku",
    "analysis_date": "2025-11-11",
    "comparison_date": "2025-11-10",
    "id": "4",
    "report": "XL线路（sipline）接通率/G标率变化明显。"
  },
```

## call_attention.py说明

- 报警的示例内容可见附录所示。

## 其他说明

- .env目前配置如下，后期可按照需求自行修改。
```bash
ALARM_API_BASE=https://ares.airudder.com/
ALARM_LEVEL=3
ALARM_USERNAME=linda.xie
ALARM_SUPERVISOR=linda.xie
ALARM_WEBHOOK=NotifyOnlyUser
ALARM_RECEIVER_TYPE=user
ALARM_BODY_TAG=plain_text
ALARM_DEPARTMENT=
ALARM_PHONE=
ALARM_SUPERVISOR_PHONE=
```
- 各个.csv文件可自行修改维护。
- 目前各个代码已更新在云端服务器——flume001.aliyun-hk.airudderint.com，主机名172.20.207.75（未添加crontab自动执行脚本计划）

## 未来可能修改发展方向

- 目前模型可以来的话术模板verbal_template较少，可后期增加丰富程度。
- 目前模型可依赖的基本表需要扩充修改。
- 目前模型无法读取往期记忆，但是我在json中已标明各个文件日期，后续可修改为读取往期报告和真实数据得出结论。

## 附录一
- 各基本表抓取逻辑如下：
```python
# 1) 公司日表 oss_ba.linda_company_day_static_v1
MAIN_SQL_TEMPLATE = """
SELECT
    country,
    company,
    day,
    md_num,
    jt_num / round_num AS connection_rate,
    nog_jt_num,
    qc_jt_num,
    qc_jt_num / qc_num AS qc_connection_rate,
    qc_nogjt_num,
    qc_nogjt_num / qc_num AS qc_nog_connection_rate,
    A_num_dis,
    B_num_dis,
    C_num_dis,
    D_num_dis,
    E_num_dis,
    F_num_dis,
    G_num_dis,
    H_num_dis,
    I_num_dis,
    J_num_dis,
    K_num_dis,
    L_num_dis,
    M_num_dis,
    N_num_dis,
    O_num_dis,
    P_num_dis,
    Q_num_dis,

    A_num_dis / qc_num AS A_dis_rate,
    B_num_dis / qc_num AS B_dis_rate,
    C_num_dis / qc_num AS C_dis_rate,
    D_num_dis / qc_num AS D_dis_rate,
    E_num_dis / qc_num AS E_dis_rate,
    F_num_dis / qc_num AS F_dis_rate,
    G_num_dis / qc_num AS G_dis_rate,
    H_num_dis / qc_num AS H_dis_rate,
    I_num_dis / qc_num AS I_dis_rate,
    J_num_dis / qc_num AS J_dis_rate,
    K_num_dis / qc_num AS K_dis_rate,
    L_num_dis / qc_num AS L_dis_rate,
    M_num_dis / qc_num AS M_dis_rate,
    N_num_dis / qc_num AS N_dis_rate,
    O_num_dis / qc_num AS O_dis_rate,
    P_num_dis / qc_num AS P_dis_rate,
    Q_num_dis / qc_num AS Q_dis_rate

FROM oss_ba.linda_company_day_static_v1
WHERE company = '{company}'
  AND country = '{country}'
  AND day >= '{std}'
  AND day <= '{edy}'
ORDER BY company, day DESC
"""
```
```python
# 2) 线路表 oss_ba.linda_sipline_data
SIPLINE_SQL_TEMPLATE = """
SELECT
    local_day,
    vendor,
    SUM(bd_num)      AS total,
    SUM(jt_cnt)      AS jt_cnt,
    SUM(preg_cnt)    AS preg_cnt,
    SUM(nog_jt)      AS nog_jt,
    SUM(g_num)       AS g_num,
    SUM(g_num)    / SUM(bd_num) AS g_rate,
    SUM(preg_cnt) / SUM(bd_num) AS preg_rate,
    SUM(nog_jt)   / SUM(bd_num) AS nog_jt_rate,
    SUM(jt_cnt)   / SUM(bd_num) AS jt_rate
FROM oss_ba.linda_sipline_data
WHERE local_day >= '{std}'
  AND local_day <= '{edy}'
  AND company = '{company}'
  {country_filter}
GROUP BY local_day, vendor
ORDER BY local_day DESC, vendor
"""
```
```python
# 3) DPD 结构表（按日聚合） oss_ba.linda_wf_tmp_dpd_company_day_v2
DPD_SQL_TEMPLATE = """
SELECT
    country,
    company,
    day,
    SUM(md_num)            AS md_num,
    SUM(bd_num)            AS bd_num,
    SUM(qc_jt_num) / SUM(qc_num)   AS qc_connection_rate,
    SUM(qc_nogjt_num) / SUM(qc_num) AS qc_nog_connection_rate,
    SUM(nogjt_cnt11) / SUM(cnt11)   AS first_round_first_circle_nog_qc_connection_rate,
    SUM(jt_cnt11) / SUM(cnt11)      AS first_round_qc_connection_rate,
    SUM(A_num_dis) / SUM(qc_num)    AS A_dis_rate,
    SUM(B_num_dis) / SUM(qc_num)    AS B_dis_rate,
    SUM(E_num_dis) / SUM(qc_num)    AS E_dis_rate,
    SUM(F_num_dis) / SUM(qc_num)    AS F_dis_rate,
    SUM(G_num_dis) / SUM(qc_num)    AS G_dis_rate
FROM oss_ba.linda_wf_tmp_dpd_company_day_v2
WHERE company = '{company}'
  AND day >= '{std}'
  AND day <= '{edy}'
  AND CAST(dpd AS INT) >= {min_dpd}
  AND CAST(dpd AS INT) <= {max_dpd}
GROUP BY country, company, day
ORDER BY day DESC
"""
```
```python
# 4) “按 DPD 细分的全量表” oss_ba.linda_wf_tmp_dpd_company_day_v2
DPD_DETAIL_SQL_TEMPLATE = """
SELECT
    country,
    a.company,
    a.day,
    CASE
        WHEN a.dpd = '-1234' THEN 'ALL'
        WHEN a.dpd = '999999' THEN '-'
        ELSE a.dpd
    END AS dpd,

    md_num                                     AS `名单量 Total Phone Number`,
    md_num / NULLIF(
        SUM(CASE WHEN a.dpd = '-1234' THEN md_num END)
        OVER (
            PARTITION BY country, a.company, a.day, a.wf_template_id, a.wf_name
        ),
        0
    )                                          AS `名单占比 dpd_proportion`,
    qc_jt_num / qc_num                         AS `去重接通率 Unique  Connection Rate`,
    G_num_dis / qc_num                         AS G_dis_rate,

    qc_nogjt_num / qc_num                      AS `无G去重接通率 Unique NoG Connection Rate`,

    qc_jt_num                                  AS `去重接通数`,
    qc_num                                     AS `去重拨打数`,
    jt_num                                     AS `接通数 Connection Number`,

    nog_jt_num                                 AS `去G接通数 NoG Connection Number`,
    qc_num                                     AS `去重名单 Unique Phone Number`,
    qc_jt_num                                  AS `去重接通 Unique Connection Number`

FROM oss_ba.linda_wf_tmp_dpd_company_day_v2 a
WHERE a.company = '{company}'
  AND a.day >= '{std}'
  AND a.day <= '{edy}'
  AND a.wf_template_id = '-1234'
  AND a.wf_name = '-1234'
  {dpd_filter}
ORDER BY a.company, a.day DESC, dpd
"""
```
## 附录二
- prompt内容如下：
```python
# 任务说明
    tail = textwrap.dedent(
        f"""
        你需要基于以上四张表中的数据，完成以下任务：

        一、理解六种类型（id=1~6 的含义）：
        1. id=1：名单量变化明显
           - 重点是最近一段时间内的名单量 / 拨打次数是否出现明显放量或收缩。
        2. id=2：账龄占比发生变化，DPD 结构改变
           - 表现为某些 DPD 桶的名单占比、接通率有明显上升或下降。
        3. id=3：PreG 数据发生异常
           - 与 preG / preg 相关的字段（例如 preg_cnt / g_preg_cnt / preG 接通率等）出现明显异常。
        4. id=4：某条线路（sipline）接通率 / G 标率变化明显
           - 具体到某个线路供应商，在接通率或 G 标率上出现异常波动。
        5. id=5：接通率 / G 标率整体正常波动，或者明显呈现周规律，没有问题
           - 整体表现稳定、波动在正常区间，可以视为“正常波动或周规律”。
        6. id=6：其他
           - 不符合上述 1~5 任一类型，或者样本量过小难以判断时，归入“其他”。

        二、关于多个问题类型（id 数组）的判定：
        1. 一家公司在同一天可能同时存在多个问题类型，例如既有“名单量变化明显”（id=1）又有“账龄结构变化”（id=2）。
        2. 你需要结合四张表中的信息，判断该公司本次 analysis_date 最可能涉及哪些问题类型，可以选择 1~N 个，范围仍然限制在 {{1,2,3,4,5,6}}。
        3. id 数组中的各个元素都必须有清晰的数据依据，不要把所有类型都填上去，确实符合才写。
        4. id 数组的顺序很重要：你认为更重要的问题类型排在前面。

        三、关于 comparison_date（对照组日期）的选择：
        1. comparison_date 必须从表格中出现过的日期里选取（例如公司日表或 DPD 表中的某个 day）。
        2. 这个日期应该能帮助凸显本次 analysis_date 是否“有问题”或“没问题”，例如：
           - 选取最近一段时间里比较正常、接近平均水平的一天作为基准；
           - 或者选取某个典型异常日，用来凸显当前日期已经恢复正常。
        3. 如果你难以判断哪一天最合适，可以选择 analysis_date 之前最近的一天作为 comparison_date。

        四、关于 report 字段（非常重要）：一个公司可以有多个问题类型，但只能输出一个合成的简短结论。
        1. 你需要在六种类型中给出一个或多个最合适的 id（字符串形式："1" ~ "6"），以数组形式输出，数组长度必须 >= 1。
        2. 对于 id 数组中的每一个问题类型，如果该类型在 {{1,2,3,4,5}} 内：
           - 必须基于对应的 verbal_template 生成一条“单句结论”；
           - 这条单句的文字必须与 verbal_template 完全一致，只允许在 “XXX/XXXX” 等占位符位置替换成具体内容；
           - 模板中的其它中文文字、标点符号都不能增删或改动。
           示例：
             - id=1 → 单句一定是：“名单量变化明显”
             - id=2 → 单句形如：“账龄占比发生变化，DPD=3的占比上升，DPD=3的接通率下降”
             - id=4 → 单句形如：“Telkomsel线路（sipline）接通率/G标率变化明显。”
        3. 如果 id 数组中包含 "6"（其他）：
           - 你可以为 id=6 生成一条“其他问题”的单句结论；
           - 这条句子必须简短（不超过 40 个汉字），不使用换行。
        4. 最终的 report 字段是一个字符串，需要把所有单句结论合成为“一句话表达”，规则如下：
           - 按照 id 数组的顺序依次拼接各个问题类型的单句；
           - 第一条单句直接使用，不加连接词；
           - 从第二条单句开始，在前面加上类似“；同时，”或“；并且，”这样的连接词；
           - 最终在字符串末尾加一个句号“。”，如果最后一条单句已经带有句号，可以不重复添加；
           - 整体不要太长，尽量控制在 80 个汉字以内。
        5. 无论有多少个问题类型，report 最终都只能是一个字符串（一个字段），不允许输出数组或多段文本，也不能换行。

        五、输出 JSON 格式（必须严格遵守，不能包含任何多余文字，也不能使用 Markdown 代码块）：
        1. 你只输出一个 JSON 对象，不要输出其它内容。
        2. JSON 结构必须是：
           {{
             "company": "{company}",
             "analysis_date": "{analysis_date_str}",
             "comparison_date": "<你从数据中选择的一天，例如 '2025-11-10'>",
             "id": ["<类型id1>", "<类型id2（可选）>", "..."],
             "report": "<把所有问题类型对应模板句按上述规则合成的一句话>"
           }}
        3. 其中：
           - company 字段必须固定为 "{company}"；
           - analysis_date 字段必须固定为 "{analysis_date_str}"，不能自定义；
           - comparison_date 必须是 YYYY-MM-DD 格式的字符串；
           - id 必须是字符串数组，每个元素都在 "1"~"6" 之间，数组长度至少为 1；
           - report 必须满足第四部分的所有约束。
        4. 输出时不要添加任何注释、前后缀文字，也不要加 ```json 之类的标记，只能是纯 JSON。
        """
    ).strip()
```
## 附录三
- 报警文案内容如下：
```bash
公司：Kreditpintar
今日日期：2025-11-20
分析日期：2025-11-19
对比日期：2025-11-18
自动巡检结果认为该公司接通率 / G 标率情况需要额外关注。

分析日期核心数据：
名单量: 4468
接通率: 0.06668486792710396
G标率: 0.11830607214877885
接通量Top5账龄：
top1账龄：0，名单量：1325，占比：29.66%，去重接通率：0.3162
top2账龄：1，名单量：1261，占比：28.22%，去重接通率：0.2078
top3账龄：2，名单量：149，占比：3.33%，去重接通率：0.1409
top4账龄：13，名单量：124，占比：2.78%，去重接通率：0.1452
top5账龄：9，名单量：151，占比：3.38%，去重接通率：0.1192

各线路核心数据：
Telkomsel：总拨打量：9076，去G接通率：0.0216，G标率：0.0001
XL：总拨打量：3753，去G接通率：0.0133，G标率：0.0000
Indosat：总拨打量：2539，去G接通率：0.0240，G标率：0.2336
Hutchison：总拨打量：2485，去G接通率：0.0185，G标率：0.0040
Smartfren：总拨打量：908，去G接通率：0.0220，G标率：0.0000

对比日期核心数据：
名单量: 17209
接通率: 0.08620905338027414
G标率: 0.11867659030119786
接通量Top5账龄：
top1账龄：-2，名单量：13843，占比：80.44%，去重接通率：0.2997
top2账龄：1，名单量：1001，占比：5.82%，去重接通率：0.1908
top3账龄：0，名单量：438，占比：2.55%，去重接通率：0.3790
top4账龄：15，名单量：141，占比：0.82%，去重接通率：0.1418
top5账龄：13，名单量：133，占比：0.77%，去重接通率：0.1278

各线路核心数据：
Telkomsel：总拨打量：29352，去G接通率：0.0456，G标率：0.0002
XL：总拨打量：10846，去G接通率：0.0467，G标率：0.0004
Indosat：总拨打量：8398，去G接通率：0.0495，G标率：0.2458
Hutchison：总拨打量：7770，去G接通率：0.0435，G标率：0.0036
Smartfren：总拨打量：2627，去G接通率：0.0464，G标率：0.0008

模型分析报告如下：
名单量变化明显；同时，账龄占比发生变化，DPD=0的占比上升，DPD=0的接通率上升。
查看详情
✅ 2025-11-20T11:38:29+08:00 admin 认领了报警
✅ 1970-01-01T00:00:00+08:00 admin 关闭了报警: p3只通知，自动接警
卡片使用指南
点击加入服务群
```
