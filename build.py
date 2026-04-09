"""
Marro Retention Deep Dive — Auto-build script
Fetches data from Google Sheets API and generates the full HTML report.
Run via GitHub Actions daily or manually.
"""

import os
import sys
import json
import requests
from collections import Counter, defaultdict

# ============================================================
# CONFIG
# ============================================================
SPREADSHEET_ID = "17oI-NGxlePKRF2kuRdwSfK2l71oNGRVKnleWnLJGNZs"
SHEET_NAME = "Data Tidy"
# Columns H-Q, starting from row 3 (row 1 is merged header, row 2 is column headers)
RANGE = f"'{SHEET_NAME}'!H2:Q5000"
API_KEY = os.environ.get("GOOGLE_API_KEY")

if not API_KEY:
    print("ERROR: GOOGLE_API_KEY environment variable not set.")
    sys.exit(1)

# ============================================================
# FETCH DATA FROM GOOGLE SHEETS
# ============================================================
def fetch_sheet_data():
    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}"
        f"/values/{RANGE}?key={API_KEY}&valueRenderOption=FORMATTED_VALUE"
        f"&dateTimeRenderOption=FORMATTED_STRING"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    values = data.get("values", [])
    if not values:
        print("ERROR: No data returned from Google Sheets.")
        sys.exit(1)
    return values


def parse_rows(raw_values):
    """Parse raw sheet values into structured rows, skipping the header row."""
    # First row is column headers: CREATED WEEK, Created Date, #, Customer, Sales Rep, Pause Date, Days to Pause, Box 1 to Pause, Why?, SUB REASON
    rows = []
    for row in raw_values[1:]:  # Skip header
        padded = row + [''] * (10 - len(row))
        padded = padded[:10]
        if not padded[0] or not padded[3]:  # Skip empty rows
            continue
        rows.append({
            'created_week': padded[0].strip(),
            'created_date': padded[1].strip(),
            'row_num': padded[2].strip(),
            'customer': padded[3].strip(),
            'sales_rep': padded[4].strip(),
            'pause_date': padded[5].strip(),
            'days_to_pause': padded[6].strip(),
            'box1_to_pause': padded[7].strip(),
            'main_reason': padded[8].strip(),
            'sub_reason': padded[9].strip()
        })
    return rows


# ============================================================
# ANALYSIS HELPERS
# ============================================================
def parse_box1(val):
    val = val.strip().replace(',', '')
    if val == '':
        return None
    try:
        return int(val)
    except ValueError:
        return None


def is_pre_box(row):
    b1 = parse_box1(row['box1_to_pause'])
    return b1 is None or b1 <= 0


def box1_bucket(row):
    b1 = parse_box1(row['box1_to_pause'])
    if b1 is None:
        return "Pre-box"
    if b1 <= 0:
        return "0"
    if b1 >= 12:
        return "12+"
    return str(b1)


BUCKET_ORDER = ["Pre-box", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12+"]


# ============================================================
# FULL ANALYSIS
# ============================================================
def run_analysis(rows):
    total = len(rows)
    all_reasons = sorted(set(r['main_reason'] for r in rows))
    reps_list = sorted(set(r['sales_rep'] for r in rows))
    pre_box_total = len([r for r in rows if is_pre_box(r)])
    pre_box_pct = round(100 * pre_box_total / total, 1) if total > 0 else 0

    # Weekly data
    weekly = {}
    for w in sorted(set(int(r['created_week']) for r in rows)):
        w_rows = [r for r in rows if int(r['created_week']) == w]
        pb = [r for r in w_rows if is_pre_box(r)]
        weekly[w] = {
            'total': len(w_rows),
            'pre_box': len(pb),
            'pre_box_pct': round(100 * len(pb) / len(w_rows), 1) if w_rows else 0,
            'reasons': dict(Counter(r['main_reason'] for r in w_rows))
        }

    # Rep data
    reps_data = {}
    for rep in reps_list:
        rep_rows = [r for r in rows if r['sales_rep'] == rep]
        pb = [r for r in rep_rows if is_pre_box(r)]
        reasons = dict(Counter(r['main_reason'] for r in rep_rows))
        sub_reasons = dict(Counter(r['sub_reason'] if r['sub_reason'] else '(Blank)' for r in rep_rows))
        prebox_reasons = dict(Counter(r['main_reason'] for r in pb))
        prebox_sub_reasons = dict(Counter(r['sub_reason'] if r['sub_reason'] else '(Blank)' for r in pb))

        rep_weekly = {}
        for w in sorted(set(int(r['created_week']) for r in rep_rows)):
            wr = [r for r in rep_rows if int(r['created_week']) == w]
            wpb = [r for r in wr if is_pre_box(r)]
            rep_weekly[w] = {'total': len(wr), 'pre_box': len(wpb), 'pct': round(100 * len(wpb) / len(wr), 1) if wr else 0}

        days_vals = []
        box1_vals = []
        for r in rep_rows:
            d = r['days_to_pause'].replace(',', '')
            if d.lstrip('-').isdigit():
                days_vals.append(int(d))
            b = parse_box1(r['box1_to_pause'])
            if b is not None:
                box1_vals.append(b)

        reps_data[rep] = {
            'total': len(rep_rows),
            'pre_box': len(pb),
            'pre_box_pct': round(100 * len(pb) / len(rep_rows), 1) if rep_rows else 0,
            'avg_days_to_pause': round(sum(days_vals) / len(days_vals), 1) if days_vals else 0,
            'avg_box1_to_pause': round(sum(box1_vals) / len(box1_vals), 1) if box1_vals else 0,
            'reasons': reasons,
            'sub_reasons': sub_reasons,
            'weekly': rep_weekly,
            'prebox_reasons': prebox_reasons,
            'prebox_sub_reasons': prebox_sub_reasons,
        }

    # Reason data
    reason_data = {}
    for reason in all_reasons:
        r_rows = [r for r in rows if r['main_reason'] == reason]
        subs = dict(Counter(r['sub_reason'] if r['sub_reason'] else '(Blank)' for r in r_rows))
        reps = dict(Counter(r['sales_rep'] for r in r_rows))
        wt = {}
        for w in sorted(set(int(r['created_week']) for r in rows)):
            wc = len([r for r in r_rows if int(r['created_week']) == w])
            wt_total = weekly[w]['total']
            wt[w] = {'count': wc, 'pct': round(100 * wc / wt_total, 1) if wt_total else 0}
        reason_data[reason] = {
            'total': len(r_rows),
            'pct': round(100 * len(r_rows) / total, 1),
            'sub_reasons': subs,
            'reps': reps,
            'weekly_trend': wt
        }

    # Reason box1 data
    reason_box1_data = {}
    for reason in all_reasons:
        r_rows = [r for r in rows if r['main_reason'] == reason]
        subs = sorted(set(r['sub_reason'] if r['sub_reason'] else '(Blank)' for r in r_rows))
        dist = {b: {sub: 0 for sub in subs} for b in BUCKET_ORDER}
        for r in r_rows:
            b = box1_bucket(r)
            sub = r['sub_reason'] if r['sub_reason'] else '(Blank)'
            dist[b][sub] += 1
        reason_box1_data[reason] = {
            'sub_reasons': subs,
            'distribution': dist,
            'pre_box_count': len([r for r in r_rows if is_pre_box(r)])
        }

    # Global box1 fine buckets (with reason toggle)
    global_box1_fine = {}
    all_subs_g = sorted(set(r['sub_reason'] if r['sub_reason'] else '(Blank)' for r in rows))
    dist = {b: {sub: 0 for sub in all_subs_g} for b in BUCKET_ORDER}
    for r in rows:
        b = box1_bucket(r)
        sub = r['sub_reason'] if r['sub_reason'] else '(Blank)'
        dist[b][sub] += 1
    global_box1_fine['_all'] = {'sub_reasons': all_subs_g, 'distribution': dist}
    for reason in all_reasons:
        r_rows = [r for r in rows if r['main_reason'] == reason]
        subs = sorted(set(r['sub_reason'] if r['sub_reason'] else '(Blank)' for r in r_rows))
        dist = {b: {sub: 0 for sub in subs} for b in BUCKET_ORDER}
        for r in r_rows:
            b = box1_bucket(r)
            sub = r['sub_reason'] if r['sub_reason'] else '(Blank)'
            dist[b][sub] += 1
        global_box1_fine[reason] = {'sub_reasons': subs, 'distribution': dist}

    # Per-rep per-reason box1
    rep_reason_box1 = {}
    for rep in reps_list:
        rep_rows = [r for r in rows if r['sales_rep'] == rep]
        rep_reason_box1[rep] = {}
        all_subs_r = sorted(set(r['sub_reason'] if r['sub_reason'] else '(Blank)' for r in rep_rows))
        dist = {b: {sub: 0 for sub in all_subs_r} for b in BUCKET_ORDER}
        for r in rep_rows:
            b = box1_bucket(r)
            sub = r['sub_reason'] if r['sub_reason'] else '(Blank)'
            dist[b][sub] += 1
        rep_reason_box1[rep]['_all'] = {'sub_reasons': all_subs_r, 'distribution': dist}
        for reason in all_reasons:
            rr = [r for r in rep_rows if r['main_reason'] == reason]
            if not rr:
                continue
            subs = sorted(set(r['sub_reason'] if r['sub_reason'] else '(Blank)' for r in rr))
            dist = {b: {sub: 0 for sub in subs} for b in BUCKET_ORDER}
            for r in rr:
                b = box1_bucket(r)
                sub = r['sub_reason'] if r['sub_reason'] else '(Blank)'
                dist[b][sub] += 1
            rep_reason_box1[rep][reason] = {'sub_reasons': subs, 'distribution': dist}

    # Per-rep weekly pre-box pct
    rep_weekly_pct = {}
    for rep in reps_list:
        rep_rows = [r for r in rows if r['sales_rep'] == rep]
        w_data = {}
        for r in rep_rows:
            w = int(r['created_week'])
            if w not in w_data:
                w_data[w] = {'total': 0, 'pre_box': 0}
            w_data[w]['total'] += 1
            if is_pre_box(r):
                w_data[w]['pre_box'] += 1
        pct = {w: round(100 * d['pre_box'] / d['total'], 1) if d['total'] > 0 else 0 for w, d in w_data.items()}
        rep_weekly_pct[rep] = pct

    # Per-rep pre-box customer lists (sorted by reason then sub-reason)
    rep_prebox_customers = {}
    for rep in reps_list:
        pb_rows = [r for r in rows if r['sales_rep'] == rep and is_pre_box(r)]
        custs = [{
            'customer': r['customer'],
            'created_date': r['created_date'],
            'box1_to_pause': r['box1_to_pause'] if r['box1_to_pause'] else '-',
            'reason': r['main_reason'].replace('_', ' ').title(),
            'sub_reason': r['sub_reason'] if r['sub_reason'] else '-',
            'created_week': int(r['created_week'])
        } for r in pb_rows]
        custs.sort(key=lambda x: (x['reason'], x['sub_reason']))
        rep_prebox_customers[rep] = custs

    # Rep suggestions
    rep_suggestions = {}
    for rep, rd in reps_data.items():
        suggestions = []
        if rd['pre_box_pct'] > 25:
            suggestions.append({'severity': 'critical', 'title': 'Very High Pre-Box Pause Rate', 'detail': f"{rd['pre_box_pct']}% of customers pause before receiving their first box.", 'action': 'Review sales call recordings. Check if subscription model is clearly communicated.'})
        elif rd['pre_box_pct'] > 18:
            suggestions.append({'severity': 'warning', 'title': 'Elevated Pre-Box Pause Rate', 'detail': f"{rd['pre_box_pct']}% pre-box pause rate needs attention.", 'action': 'Monitor upcoming sales. Ensure subscription terms are clearly communicated.'})
        dnw = rd['reasons'].get('do_not_want_subscription', 0)
        dnw_pct = round(100 * dnw / rd['total'], 1) if rd['total'] > 0 else 0
        if dnw_pct > 30:
            suggestions.append({'severity': 'critical', 'title': 'High "Don\'t Want Subscription" Rate', 'detail': f"{dnw_pct}% of pauses cite not wanting a subscription.", 'action': 'Ensure subscription model is explained clearly BEFORE closing.'})
        price = rd['reasons'].get('price', 0)
        price_pct = round(100 * price / rd['total'], 1) if rd['total'] > 0 else 0
        if price_pct > 20:
            suggestions.append({'severity': 'warning', 'title': 'High Price-Related Pauses', 'detail': f"{price_pct}% of pauses are price-related.", 'action': 'Qualify customers on budget early. Train on value selling.'})
        didnt_try = rd['sub_reasons'].get("MY CAT DIDN'T EVEN GIVE MARRO A TRY", 0)
        if didnt_try > rd['total'] * 0.1:
            suggestions.append({'severity': 'warning', 'title': 'Cats Not Even Trying the Food', 'detail': f"{didnt_try} customers said their cat didn't even try Marro.", 'action': 'Share feeding transition tips at point of sale.'})
        didnt_know = rd['sub_reasons'].get("I DIDN'T REALISE MARRO IS A SUBSCRIPTION", 0)
        if didnt_know > 2:
            suggestions.append({'severity': 'critical', 'title': 'Customers Unaware of Subscription Model', 'detail': f"{didnt_know} customers didn't know Marro is a subscription.", 'action': 'URGENT: Explicitly state subscription during every sale.'})
        if rd['avg_days_to_pause'] < 7:
            suggestions.append({'severity': 'warning', 'title': 'Very Short Time to Pause', 'detail': f"Average {rd['avg_days_to_pause']} days from sale to pause.", 'action': 'Implement post-sale follow-up within 48 hours.'})
        if not suggestions:
            suggestions.append({'severity': 'good', 'title': 'Performing Within Acceptable Range', 'detail': 'Metrics are within team norms.', 'action': 'Continue monitoring. Share best practices from this rep.'})
        rep_suggestions[rep] = suggestions

    # Pre-box analysis text per rep
    rep_prebox_analysis = {}
    for rep in reps_list:
        rep_rows = [r for r in rows if r['sales_rep'] == rep]
        pb_rows = [r for r in rep_rows if is_pre_box(r)]
        if not pb_rows:
            rep_prebox_analysis[rep] = "No pre-box pausers recorded for this rep."
            continue
        pts = []
        total_pb = len(pb_rows)
        reasons = Counter(r['main_reason'] for r in pb_rows)
        top_r, top_c = reasons.most_common(1)[0]
        pts.append(f"<strong>{round(100*top_c/total_pb,1)}%</strong> of pre-box pauses ({top_c}/{total_pb}) cite <strong>\"{top_r.replace('_',' ').title()}\"</strong>.")
        if len(reasons) > 1:
            s2 = reasons.most_common(2)[1]
            pts.append(f"Second: \"{s2[0].replace('_',' ').title()}\" ({s2[1]} cases, {round(100*s2[1]/total_pb,1)}%).")
        subs = Counter(r['sub_reason'] if r['sub_reason'] else '(Blank)' for r in pb_rows)
        blank_pct = round(100 * subs.get('(Blank)', 0) / total_pb, 1)
        if blank_pct > 50:
            pts.append(f"⚠️ <strong>{blank_pct}%</strong> of pre-box records have no sub-reason.")
        notable = [(s, c) for s, c in subs.most_common() if s != '(Blank)' and c >= 2]
        if notable:
            pts.append(f"Most common sub-reason: \"{notable[0][0]}\" ({notable[0][1]} occurrences).")
        early = [r for r in pb_rows if int(r['created_week']) <= 7]
        late = [r for r in pb_rows if int(r['created_week']) >= 8]
        if len(early) > 2 and len(late) > 2:
            et = len([r for r in rep_rows if int(r['created_week']) <= 7])
            lt = len([r for r in rep_rows if int(r['created_week']) >= 8])
            er = round(100 * len(early) / et, 1) if et else 0
            lr = round(100 * len(late) / lt, 1) if lt else 0
            if lr > er * 1.3:
                pts.append(f"📈 Pre-box rate <strong>worsening</strong>: {er}% (Wks 1-7) → {lr}% (Wks 8+).")
            elif er > lr * 1.3:
                pts.append(f"📉 Positive: pre-box rate <strong>improved</strong> from {er}% to {lr}%.")
            else:
                pts.append(f"Pre-box rate stable: {er}% (Wks 1-7) vs {lr}% (Wks 8+).")
        trial = sum(1 for r in pb_rows if 'SEE IF' in (r['sub_reason'] or '').upper())
        if trial >= 2:
            pts.append(f"<strong>{trial} customers</strong> paused to \"see if cat likes Marro first\" — trial angle may be oversold.")
        rep_prebox_analysis[rep] = '<br>'.join(f'→ {p}' for p in pts)

    # Old-style box1 stacked (for compatibility)
    all_sub_reasons = sorted(set(r['sub_reason'] if r['sub_reason'] else '(Blank)' for r in rows))
    box1_buckets_old = ["Blank", "≤0", "1-5", "6-10", "11-20", "21-30", "31+"]
    box1_stacked = {}
    for r in rows:
        b1 = parse_box1(r['box1_to_pause'])
        if b1 is None: bucket = "Blank"
        elif b1 <= 0: bucket = "≤0"
        elif b1 <= 5: bucket = "1-5"
        elif b1 <= 10: bucket = "6-10"
        elif b1 <= 20: bucket = "11-20"
        elif b1 <= 30: bucket = "21-30"
        else: bucket = "31+"
        sub = r['sub_reason'] if r['sub_reason'] else '(Blank)'
        if bucket not in box1_stacked:
            box1_stacked[bucket] = {}
        box1_stacked[bucket][sub] = box1_stacked[bucket].get(sub, 0) + 1

    return {
        'total_records': total,
        'weekly': weekly,
        'reps': reps_data,
        'reason_data': reason_data,
        'all_reasons': all_reasons,
        'box1_stacked': box1_stacked,
        'box1_buckets': box1_buckets_old,
        'all_sub_reasons': all_sub_reasons,
        'pre_box_total': pre_box_total,
        'pre_box_pct': pre_box_pct,
        'reason_box1_data': reason_box1_data,
        'rep_prebox_customers': rep_prebox_customers,
        'rep_prebox_analysis': rep_prebox_analysis,
        'rep_suggestions': rep_suggestions,
        'bucket_order': BUCKET_ORDER,
        'rep_reason_box1': rep_reason_box1,
        'global_box1_fine': global_box1_fine,
        'rep_weekly_pct': rep_weekly_pct,
    }


# ============================================================
# HTML GENERATION (reads template from build_template.html)
# ============================================================
def generate_html(data):
    """Read the HTML template and inject the DATA JSON."""
    template_path = os.path.join(os.path.dirname(__file__), 'build_template.html')
    with open(template_path, 'r', encoding='utf-8') as f:
        template = f.read()

    # The template contains %%DATA_JSON%% placeholder
    data_json = json.dumps(data)
    html = template.replace('%%DATA_JSON%%', data_json)

    # Replace dynamic counts in the header
    reps_list = sorted(data['reps'].keys())
    html = html.replace('%%TOTAL_RECORDS%%', str(data['total_records']))
    html = html.replace('%%NUM_REPS%%', str(len(reps_list)))
    html = html.replace('%%PRE_BOX_TOTAL%%', str(data['pre_box_total']))
    html = html.replace('%%PRE_BOX_PCT%%', str(data['pre_box_pct']))

    # Build dynamic HTML sections
    html = html.replace('%%REP_TAB_BUTTONS%%', build_rep_tab_buttons(data))
    html = html.replace('%%REP_TAB_CONTENT%%', build_rep_tab_content(data))
    html = html.replace('%%REASON_BUTTONS%%', build_reason_buttons(data))
    html = html.replace('%%BOX1_TAB_TOGGLES%%', build_box1_tab_toggles(data))

    return html


def build_rep_tab_buttons(data):
    html = ""
    for rep in sorted(data['reps'].keys()):
        rd = data['reps'][rep]
        if rd['pre_box_pct'] > 25: badge = '<span class="badge badge-critical">High Risk</span>'
        elif rd['pre_box_pct'] > 18: badge = '<span class="badge badge-warning">Monitor</span>'
        else: badge = '<span class="badge badge-good">Acceptable</span>'
        html += f'<button class="sub-tab-btn" data-rep="{rep}">{rep.replace("_"," ")} {badge}</button>\n'
    return html


def build_reason_toggles(prefix):
    all_reasons = sorted(DATA_CACHE['reason_data'].keys(), key=lambda r: -DATA_CACHE['reason_data'][r]['total'])
    html = f'<button class="{prefix}-reason-btn toggle-btn active" data-reason="_all">All Reasons</button>\n'
    for r in all_reasons:
        count = DATA_CACHE['reason_data'][r]['total']
        html += f'<button class="{prefix}-reason-btn toggle-btn" data-reason="{r}">{r.replace("_"," ").title()} ({count})</button>\n'
    return html


DATA_CACHE = {}


def build_rep_tab_content(data):
    global DATA_CACHE
    DATA_CACHE = data
    all_reasons = sorted(data['reason_data'].keys(), key=lambda r: -data['reason_data'][r]['total'])
    html = ""
    for rep in sorted(data['reps'].keys()):
        rd = data['reps'][rep]
        suggs = data['rep_suggestions'][rep]
        prebox_custs = data['rep_prebox_customers'][rep]
        prebox_analysis = data['rep_prebox_analysis'][rep]

        if rd['pre_box_pct'] > 25: badge = '<span class="badge badge-critical">High Risk</span>'
        elif rd['pre_box_pct'] > 18: badge = '<span class="badge badge-warning">Monitor</span>'
        else: badge = '<span class="badge badge-good">Acceptable</span>'

        sugg_html = "".join(
            f'<div class="suggestion-card suggestion-{s["severity"]}"><div class="suggestion-header">{"🔴" if s["severity"]=="critical" else ("🟡" if s["severity"]=="warning" else "🟢")} {s["title"]}</div><div class="suggestion-detail">{s["detail"]}</div><div class="suggestion-action"><strong>Recommended Action:</strong> {s["action"]}</div></div>'
            for s in suggs)

        reason_rows = "".join(
            f"<tr><td>{r.replace('_',' ').title()}</td><td>{c}</td><td>{round(100*c/rd['total'],1)}%</td></tr>"
            for r, c in sorted(rd['reasons'].items(), key=lambda x: -x[1]))

        sub_rows = "".join(
            f"<tr><td>{s}</td><td>{c}</td><td>{round(100*c/rd['total'],1)}%</td></tr>"
            for s, c in sorted(rd['sub_reasons'].items(), key=lambda x: -x[1])[:10])

        prebox_table = "".join(
            f"<tr><td>{c['customer']}</td><td>{c['created_date']}</td><td style='color:#e74c3c;font-weight:bold;'>{c['box1_to_pause']}</td><td>{c['reason']}</td><td>{c['sub_reason']}</td></tr>"
            for c in prebox_custs)

        reason_toggle_html = build_reason_toggles(f'rep-{rep}')

        html += f"""
    <div class="rep-content" id="rep-{rep}" style="display:none;">
        <div class="rep-header-bar"><h2>{rep.replace('_',' ')}</h2>{badge}</div>
        <div class="metrics-grid">
            <div class="metric-card"><div class="metric-label">Total Pauses</div><div class="metric-value">{rd['total']}</div></div>
            <div class="metric-card {'warning' if rd['pre_box_pct']>25 else ('highlight' if rd['pre_box_pct']>18 else '')}"><div class="metric-label">Pre-Box Pauses</div><div class="metric-value">{rd['pre_box']} <small>({rd['pre_box_pct']}%)</small></div></div>
            <div class="metric-card"><div class="metric-label">Avg Days to Pause</div><div class="metric-value">{rd['avg_days_to_pause']}</div></div>
            <div class="metric-card"><div class="metric-label">Avg Box 1 to Pause</div><div class="metric-value">{rd['avg_box1_to_pause']} days</div></div>
        </div>
        <div class="insights-box"><h3>📋 Improvement Recommendations</h3>{sugg_html}</div>
        <div class="grid-2">
            <div class="chart-container"><div class="chart-title">Pause Reasons</div><table class="data-table"><thead><tr><th>Reason</th><th>Count</th><th>%</th></tr></thead><tbody>{reason_rows}</tbody></table></div>
            <div class="chart-container"><div class="chart-title">Top Sub-Reasons</div><table class="data-table"><thead><tr><th>Sub Reason</th><th>Count</th><th>%</th></tr></thead><tbody>{sub_rows}</tbody></table></div>
        </div>
        <div class="chart-container" style="margin-top:25px;">
            <div class="chart-title">⚠️ Pre-Box Pauser Details ({len(prebox_custs)} Customers)</div>
            <p style="color:#666;font-size:0.85rem;margin-bottom:15px;">Sorted by Reason → Sub-Reason</p>
            <table class="data-table"><thead><tr><th>Customer</th><th>Created Date</th><th>Box 1 to Pause</th><th>Reason</th><th>Sub-Reason</th></tr></thead><tbody>{prebox_table if prebox_table else '<tr><td colspan="5" style="text-align:center;color:#666;">No pre-box pausers</td></tr>'}</tbody></table>
        </div>
        <div class="insights-box" style="margin-top:15px;"><h3>🔍 Pre-Box Trend Analysis — {rep.replace('_',' ')}</h3><p style="line-height:1.8;font-size:0.9rem;">{prebox_analysis}</p></div>
        <div class="chart-container" style="margin-top:25px;">
            <div class="chart-title">📊 Box 1 to Pause Distribution (Stacked by Sub-Reason)</div>
            <div style="display:flex;flex-wrap:wrap;gap:2px;margin-bottom:15px;" id="rep-box1-toggles-{rep}">{reason_toggle_html}</div>
            <div style="position:relative;height:400px;"><canvas id="chart-rep-box1-{rep}"></canvas></div>
        </div>
        <div class="chart-container" style="margin-top:25px;">
            <div class="chart-title">📈 Weekly Trend</div>
            <div class="chart-wrapper"><canvas id="chart-rep-{rep}"></canvas></div>
        </div>
    </div>"""
    return html


def build_reason_buttons(data):
    all_reasons = sorted(data['reason_data'].keys(), key=lambda r: -data['reason_data'][r]['total'])
    return "".join(
        f'<button class="toggle-btn" data-reason="{r}">{r.replace("_"," ").title()} ({data["reason_data"][r]["total"]})</button>\n'
        for r in all_reasons)


def build_box1_tab_toggles(data):
    all_reasons = sorted(data['reason_data'].keys(), key=lambda r: -data['reason_data'][r]['total'])
    html = '<button class="box1tab-reason-btn toggle-btn active" data-reason="_all">All Reasons</button>\n'
    for r in all_reasons:
        html += f'<button class="box1tab-reason-btn toggle-btn" data-reason="{r}">{r.replace("_"," ").title()} ({data["reason_data"][r]["total"]})</button>\n'
    return html


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("Fetching data from Google Sheets...")
    raw = fetch_sheet_data()
    print(f"Fetched {len(raw)} rows (including header)")

    print("Parsing rows...")
    rows = parse_rows(raw)
    print(f"Parsed {len(rows)} data rows")

    print("Running analysis...")
    data = run_analysis(rows)
    print(f"Analysis complete: {data['total_records']} records, {len(data['reps'])} reps")

    print("Generating HTML...")
    html = generate_html(data)

    output_path = os.path.join(os.path.dirname(__file__), 'index.html')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Saved index.html ({len(html):,} characters)")
