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
    - 注：不建议三个脚本分开太长时间运行，综合考虑存储和记录影响，分析结果以json形式分开储存（analysis_results_YYYY-MM-DD.json），但是数据脚本覆盖储存。

## dump_stats.py说明

    - 目前一共选取了四张基本表——oss_ba.linda_company_day_static_v1，oss_ba.linda_sipline_data，oss_ba.linda_wf_tmp_dpd_company_day_v2，oss_ba.linda_wf_tmp_dpd_company_day_v2（具体sql语言见附一）
    - 基本表涵盖内容包括：公司整体各指标，账龄分布情况，各条线路情况，preg情况。从日常巡检角度来看基本覆盖主要影响范围，后期如果有其他需要附加，可届时再添加内容。
    - 当前dump_stats.py存在问题：目前我调用的全部为linda表，数据更新速度慢（每日9:30——10:00拿不到需求数据），调用其他表以我个人账号而言缺少权限。



































```bash
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
```bash
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
```bash
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
```bash
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
