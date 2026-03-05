"""
Sentinel Fraud Defense Platform
Tab 1: Live Fraud Queue — review and release flagged transactions (backed by Lakebase)
Tab 2: Fraud Investigator — conversational fraud investigation (backed by Genie)
"""

import os
import time
import json
import datetime
import psycopg2
import psycopg2.extras
from flask import Flask, render_template_string, request, jsonify, redirect, url_for, send_from_directory
from databricks.sdk import WorkspaceClient

app = Flask(__name__, static_folder="static")

GENIE_SPACE_ID = "01f118c4e4571dca8faac786a5561996"
LAKEBASE_INSTANCE = "fraud-triage-store"
LAKEBASE_DB = "fraud_triage"

w = WorkspaceClient()


# ─── Lakebase Connection ─────────────────────────────────────────────────────

def get_db_connection():
    db = w.database.get_database_instance(LAKEBASE_INSTANCE)
    cred = w.database.generate_database_credential(instance_names=[LAKEBASE_INSTANCE])
    current_user = w.current_user.me().user_name
    return psycopg2.connect(
        host=db.read_write_dns, port=5432, dbname=LAKEBASE_DB,
        user=current_user, password=cred.token, sslmode="require"
    )


# ─── Genie Helpers ───────────────────────────────────────────────────────────

def genie_ask(question, conversation_id=None):
    try:
        if conversation_id:
            waiter = w.genie.create_message(
                space_id=GENIE_SPACE_ID, conversation_id=conversation_id, content=question)
            # Response for create_message has conversation_id on response
            conversation_id = waiter.response.conversation_id
        else:
            waiter = w.genie.start_conversation(
                space_id=GENIE_SPACE_ID, content=question)
            conversation_id = waiter.response.conversation_id

        # Wait for Genie to finish (SDK handles polling internally)
        msg = waiter.result(timeout=datetime.timedelta(seconds=120))
        return _parse_genie(msg, conversation_id)
    except Exception as e:
        return {"conversation_id": conversation_id, "status": "error",
                "text": f"Genie error: {e}", "sql": None, "data": None}


def _parse_genie(msg, conversation_id):
    text_parts, sql_query, result_data = [], None, None
    attachments = getattr(msg, "attachments", None) or []

    for att in attachments:
        tc = getattr(att, "text", None)
        if tc:
            c = getattr(tc, "content", None)
            if c:
                text_parts.append(c)
        q = getattr(att, "query", None)
        if q:
            sql_query = getattr(q, "query", None)
            desc = getattr(q, "description", None)
            if desc:
                text_parts.append(desc)

    # Fetch actual query result data via statement execution API
    for att in attachments:
        q = getattr(att, "query", None)
        if q and not result_data:
            try:
                qr = w.genie.get_message_query_result(
                    space_id=GENIE_SPACE_ID, conversation_id=conversation_id,
                    message_id=msg.id)
                stmt_resp = getattr(qr, "statement_response", None)
                statement_id = getattr(stmt_resp, "statement_id", None) if stmt_resp else None

                if statement_id:
                    stmt = w.statement_execution.get_statement(statement_id)
                    if stmt.manifest and stmt.result:
                        columns = [c.name for c in stmt.manifest.schema.columns] if stmt.manifest.schema else []
                        data_array = getattr(stmt.result, "data_array", None) or []
                        result_data = {"columns": columns, "rows": [list(r) for r in data_array[:50]]}
            except Exception as e:
                text_parts.append(f"(Could not fetch results: {e})")

    return {"conversation_id": conversation_id, "status": "success",
            "text": "\n".join(text_parts) if text_parts else "Query completed.",
            "sql": sql_query, "data": result_data}


# ─── Main HTML ───────────────────────────────────────────────────────────────

MAIN_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sentinel - Fraud Defense Platform</title>
    <style>
        :root {
            --bg0: #070d18; --bg1: #0a1628; --bg2: #0f1f3d; --bg3: #132240; --bg4: #1a2d52;
            --border: #1e3a5f; --gold: #c9a44a; --gold-l: #f0d68a; --txt1: #e8ecf1; --txt2: #8a9bb5;
            --red: #e74c3c; --green: #2ecc71; --blue: #3498db; --purple: #9b59b6; --yellow: #f39c12;
        }
        * { margin:0; padding:0; box-sizing:border-box; }
        body { font-family:'Segoe UI',-apple-system,sans-serif; background:var(--bg0); color:var(--txt1); height:100vh; display:flex; flex-direction:column; }

        /* Header */
        .header { background:linear-gradient(135deg,var(--bg2),var(--bg1)); border-bottom:1px solid var(--border); padding:12px 28px; display:flex; align-items:center; gap:14px; }
        .header img { width:40px; height:40px; }
        .header-text h1 { font-family:Georgia,serif; font-size:20px; color:var(--gold-l); letter-spacing:2px; }
        .header-text p { font-size:10px; color:var(--txt2); letter-spacing:1px; }

        /* Tabs */
        .tabs { display:flex; margin-left:auto; gap:4px; }
        .tab { padding:8px 20px; border-radius:6px 6px 0 0; cursor:pointer; font-size:13px; font-weight:600; letter-spacing:0.5px;
               color:var(--txt2); border:1px solid transparent; border-bottom:none; transition:all 0.2s; }
        .tab:hover { color:var(--gold-l); }
        .tab.active { background:var(--bg1); color:var(--gold); border-color:var(--border); }

        /* Tab Content */
        .tab-content { display:none; flex:1; flex-direction:column; overflow:hidden; }
        .tab-content.active { display:flex; }

        /* ── QUEUE TAB ── */
        .queue-filters { padding:12px 28px; background:var(--bg2); display:flex; gap:12px; align-items:center; border-bottom:1px solid var(--border); }
        .queue-filters select { background:var(--bg4); color:var(--txt1); border:1px solid var(--border); padding:7px 10px; border-radius:6px; font-size:13px; }
        .queue-filters button { background:var(--blue); color:#fff; border:none; padding:7px 16px; border-radius:6px; cursor:pointer; font-size:13px; }
        .queue-filters .refresh-btn { background:none; border:1px solid var(--blue); color:var(--blue); text-decoration:none; padding:7px 16px; border-radius:6px; font-size:13px; }
        .queue-stats { display:flex; gap:24px; margin-left:auto; }
        .qs { text-align:center; }
        .qs .n { font-size:22px; font-weight:bold; }
        .qs .l { font-size:9px; text-transform:uppercase; color:var(--txt2); }
        .queue-scroll { flex:1; overflow:auto; padding:0 28px 20px; }
        table { width:100%%; border-collapse:collapse; margin-top:12px; }
        th { background:var(--bg2); padding:10px 12px; text-align:left; font-size:11px; text-transform:uppercase; color:var(--txt2); border-bottom:2px solid var(--border); position:sticky; top:0; z-index:1; }
        td { padding:10px 12px; border-bottom:1px solid rgba(30,58,95,0.4); font-size:13px; }
        tr:hover td { background:rgba(30,58,95,0.2); }
        .badge { padding:3px 9px; border-radius:10px; font-size:10px; font-weight:600; text-transform:uppercase; display:inline-block; }
        .b-red { background:rgba(231,76,60,0.2); color:var(--red); border:1px solid var(--red); }
        .b-yel { background:rgba(243,156,18,0.2); color:var(--yellow); border:1px solid var(--yellow); }
        .b-grn { background:rgba(46,204,113,0.2); color:var(--green); border:1px solid var(--green); }
        .b-pur { background:rgba(155,89,182,0.2); color:var(--purple); border:1px solid var(--purple); }
        .risk-bar { width:50px; height:6px; background:#222; border-radius:3px; overflow:hidden; display:inline-block; vertical-align:middle; margin-right:6px; }
        .risk-fill { height:100%%; border-radius:3px; }
        .btn { padding:5px 12px; border:none; border-radius:4px; cursor:pointer; font-size:11px; font-weight:600; }
        .btn-a { background:var(--green); color:#000; } .btn-b { background:var(--red); color:#fff; } .btn-e { background:var(--purple); color:#fff; }
        .actions { display:flex; gap:4px; }
        .expl { max-width:260px; font-size:11px; color:var(--txt2); line-height:1.4; }

        /* Modal */
        .modal-ov { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.7); z-index:100; }
        .modal { display:none; position:fixed; top:50%%; left:50%%; transform:translate(-50%%,-50%%); background:var(--bg2); padding:24px; border-radius:10px; z-index:101; width:440px; border:1px solid var(--border); }
        .modal h3 { margin-bottom:12px; font-size:16px; }
        .modal textarea { width:100%%; background:var(--bg4); color:var(--txt1); border:1px solid var(--border); padding:10px; border-radius:6px; min-height:70px; margin:8px 0; font-family:inherit; }
        .modal .btn { margin-right:8px; }

        /* ── GENIE TAB ── */
        .chat-area { flex:1; overflow-y:auto; padding:20px 28px; display:flex; flex-direction:column; gap:16px; }
        .msg { max-width:85%%; animation:fadeIn 0.3s ease; }
        @keyframes fadeIn { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:translateY(0)} }
        .msg.user { align-self:flex-end; }
        .msg.bot { align-self:flex-start; }
        .msg-label { font-size:9px; color:var(--txt2); margin-bottom:3px; text-transform:uppercase; letter-spacing:1px; }
        .msg.user .msg-label { text-align:right; }
        .msg-bubble { padding:12px 16px; border-radius:10px; line-height:1.6; font-size:14px; }
        .msg.user .msg-bubble { background:var(--bg4); border:1px solid var(--border); border-bottom-right-radius:3px; }
        .msg.bot .msg-bubble { background:var(--bg3); border:1px solid var(--border); border-bottom-left-radius:3px; }
        .sql-block { background:#0b1520; border:1px solid var(--border); border-radius:6px; margin-top:10px; overflow:hidden; }
        .sql-hdr { background:var(--bg4); padding:5px 10px; font-size:10px; color:var(--gold); font-weight:600; display:flex; justify-content:space-between; align-items:center; }
        .sql-hdr button { background:none; border:1px solid var(--border); color:var(--txt2); padding:2px 8px; border-radius:3px; cursor:pointer; font-size:9px; }
        .sql-hdr button:hover { color:var(--gold); border-color:var(--gold); }
        .sql-body { padding:10px; font-family:'Fira Code',monospace; font-size:12px; white-space:pre-wrap; overflow-x:auto; }
        .dt-wrap { margin-top:10px; border:1px solid var(--border); border-radius:6px; overflow:auto; max-height:360px; }
        .dt { width:100%%; border-collapse:collapse; font-size:12px; }
        .dt th { background:var(--bg4); padding:8px 12px; text-align:left; color:var(--gold); font-size:10px; text-transform:uppercase; letter-spacing:1px; position:sticky; top:0; border-bottom:2px solid var(--border); }
        .dt td { padding:7px 12px; border-bottom:1px solid rgba(30,58,95,0.4); }
        .dt tr:hover td { background:rgba(30,58,95,0.3); }
        .dt .num { text-align:right; font-variant-numeric:tabular-nums; }
        .input-area { background:var(--bg2); border-top:1px solid var(--border); padding:14px 28px; }
        .input-row { display:flex; gap:10px; }
        .input-row textarea { flex:1; background:var(--bg4); border:1px solid var(--border); color:var(--txt1); padding:10px 14px; border-radius:8px; font-size:14px; font-family:inherit; resize:none; height:44px; line-height:22px; }
        .input-row textarea:focus { outline:none; border-color:var(--gold); }
        .input-row textarea::placeholder { color:var(--txt2); }
        .input-row button { background:linear-gradient(135deg,var(--gold),#a8883a); color:var(--bg1); border:none; padding:10px 22px; border-radius:8px; font-weight:700; font-size:13px; cursor:pointer; letter-spacing:1px; white-space:nowrap; }
        .input-row button:disabled { opacity:0.4; cursor:not-allowed; }
        .suggs { display:flex; gap:6px; margin-top:8px; flex-wrap:wrap; }
        .sug { background:var(--bg3); border:1px solid var(--border); color:var(--txt2); padding:5px 12px; border-radius:14px; font-size:11px; cursor:pointer; }
        .sug:hover { border-color:var(--gold); color:var(--gold-l); }
        .typing span { width:7px;height:7px;background:var(--gold);border-radius:50%%;display:inline-block;animation:bounce 1.4s infinite; }
        .typing span:nth-child(2){animation-delay:.2s} .typing span:nth-child(3){animation-delay:.4s}
        @keyframes bounce{0%%,80%%,100%%{transform:translateY(0)}40%%{transform:translateY(-7px)}}
        .welcome { text-align:center; padding:50px 20px; color:var(--txt2); }
        .welcome img { width:70px; height:70px; margin-bottom:16px; opacity:0.8; }
        .welcome h2 { color:var(--gold-l); font-family:Georgia,serif; margin-bottom:6px; letter-spacing:2px; font-size:18px; }
        .welcome p { font-size:13px; max-width:460px; margin:0 auto; line-height:1.6; }
    </style>
</head>
<body>
    <div class="header">
        <img src="/static/logo.svg" alt="Sentinel">
        <div class="header-text"><h1>SENTINEL</h1><p>Fraud Defense Platform</p></div>
        <div class="tabs">
            <div class="tab active" onclick="switchTab('queue')">Fraud Queue</div>
            <div class="tab" onclick="switchTab('genie')">Investigator</div>
        </div>
    </div>

    <!-- ═══ QUEUE TAB ═══ -->
    <div class="tab-content active" id="tab-queue">
        <div class="queue-filters">
            <form method="GET" action="/" style="display:flex;gap:10px;align-items:center;">
                <input type="hidden" name="tab" value="queue">
                <select name="status">
                    <option value="">All Statuses</option>
                    <option value="RED_BLOCK" {{ 'selected' if filter_status=='RED_BLOCK' }}>RED - Blocked</option>
                    <option value="YELLOW_REVIEW" {{ 'selected' if filter_status=='YELLOW_REVIEW' }}>YELLOW - Pending</option>
                </select>
                <select name="sort">
                    <option value="risk_desc" {{ 'selected' if sort=='risk_desc' }}>Risk (High→Low)</option>
                    <option value="amount_desc" {{ 'selected' if sort=='amount_desc' }}>Amount (High→Low)</option>
                    <option value="time_desc" {{ 'selected' if sort=='time_desc' }}>Most Recent</option>
                </select>
                <button type="submit">Apply</button>
                <a href="/" class="refresh-btn">Refresh</a>
            </form>
            <div class="queue-stats">
                <div class="qs"><div class="n" style="color:var(--red)">{{ stats.red_count }}</div><div class="l">Blocked</div></div>
                <div class="qs"><div class="n" style="color:var(--yellow)">{{ stats.yellow_count }}</div><div class="l">Pending</div></div>
                <div class="qs"><div class="n" style="color:var(--green)">{{ stats.reviewed_count }}</div><div class="l">Reviewed</div></div>
                <div class="qs"><div class="n" style="color:var(--blue)">${{ "{:,.0f}".format(stats.total_exposure) }}</div><div class="l">Exposure</div></div>
            </div>
        </div>
        <div class="queue-scroll">
            <table>
                <thead><tr><th>Transaction</th><th>User</th><th>Amount</th><th>Type</th><th>Category</th><th>Risk</th><th>Status</th><th>Explanation</th><th>Actions</th></tr></thead>
                <tbody>
                {% for t in transactions %}
                <tr>
                    <td><strong>{{ t.transaction_id }}</strong></td>
                    <td>{{ t.user_id }}</td>
                    <td>${{ "{:,.2f}".format(t.amount) }}</td>
                    <td>{{ t.transaction_type or '' }}</td>
                    <td>{{ t.merchant_category or '' }}</td>
                    <td><div class="risk-bar"><div class="risk-fill" style="width:{{ t.risk_score }}%%;background:{{ '#e74c3c' if t.risk_score>=50 else '#f39c12' if t.risk_score>=20 else '#2ecc71' }}"></div></div>{{ t.risk_score }}</td>
                    <td><span class="badge {{ 'b-red' if t.triage_status=='RED_BLOCK' else 'b-yel' if t.triage_status=='YELLOW_REVIEW' else 'b-grn' }}">{{ t.triage_status }}</span></td>
                    <td class="expl">{{ t.risk_explanation or '' }}</td>
                    <td>
                        {% if not t.analyst_decision %}
                        <div class="actions">
                            <button class="btn btn-a" onclick="showModal('{{ t.transaction_id }}','APPROVED')">Release</button>
                            <button class="btn btn-b" onclick="showModal('{{ t.transaction_id }}','BLOCKED')">Block</button>
                            <button class="btn btn-e" onclick="showModal('{{ t.transaction_id }}','ESCALATED')">Escalate</button>
                        </div>
                        {% else %}
                        <span class="badge {{ 'b-grn' if t.analyst_decision=='APPROVED' else 'b-red' if t.analyst_decision=='BLOCKED' else 'b-pur' }}">{{ t.analyst_decision }}</span>
                        {% endif %}
                    </td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>
    </div>

    <!-- ═══ GENIE TAB ═══ -->
    <div class="tab-content" id="tab-genie">
        <div class="chat-area" id="chatArea">
            <div class="welcome" id="genieWelcome">
                <img src="/static/logo.svg" alt="Sentinel">
                <h2>Fraud Investigation Assistant</h2>
                <p>Ask questions about transactions, login anomalies, risk patterns, and fraud KPIs. Powered by Databricks Genie.</p>
            </div>
        </div>
        <div class="input-area">
            <div class="input-row">
                <textarea id="qInput" placeholder="Ask a fraud investigation question..." onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();sendQ();}"></textarea>
                <button id="sendBtn" onclick="sendQ()">INVESTIGATE</button>
            </div>
            <div class="suggs" id="suggs">
                <div class="sug" onclick="askS(this)">Show wire transfers over $10K with MFA changes</div>
                <div class="sug" onclick="askS(this)">What is the false positive ratio?</div>
                <div class="sug" onclick="askS(this)">List impossible travel events by velocity</div>
                <div class="sug" onclick="askS(this)">Fraud exposure by merchant category</div>
                <div class="sug" onclick="askS(this)">Show bot-detected transactions</div>
            </div>
        </div>
    </div>

    <!-- Modal -->
    <div class="modal-ov" id="modalOv" onclick="hideModal()"></div>
    <div class="modal" id="modal">
        <h3 id="mTitle">Confirm Decision</h3>
        <form method="POST" action="/decide">
            <input type="hidden" name="transaction_id" id="mTxn">
            <input type="hidden" name="decision" id="mDec">
            <div><div class="msg-label">Decision</div><div id="mDecShow" style="margin-bottom:8px"></div></div>
            <textarea name="notes" placeholder="Analyst notes (required for audit)..." required></textarea>
            <div><button type="submit" class="btn btn-a">Confirm</button><button type="button" class="btn" style="background:#555;color:#fff" onclick="hideModal()">Cancel</button></div>
        </form>
    </div>

    <script>
        let convId = null;

        function switchTab(tab) {
            document.querySelectorAll('.tab').forEach((t,i) => t.classList.toggle('active', (tab==='queue'?i===0:i===1)));
            document.getElementById('tab-queue').classList.toggle('active', tab==='queue');
            document.getElementById('tab-genie').classList.toggle('active', tab==='genie');
        }

        function showModal(txn, dec) {
            document.getElementById('mTxn').value=txn;
            document.getElementById('mDec').value=dec;
            document.getElementById('mDecShow').textContent={APPROVED:'RELEASE Transaction',BLOCKED:'BLOCK Transaction',ESCALATED:'ESCALATE to Senior Analyst'}[dec];
            document.getElementById('mTitle').textContent='Decision: '+txn;
            document.getElementById('modal').style.display='block';
            document.getElementById('modalOv').style.display='block';
        }
        function hideModal() { document.getElementById('modal').style.display='none'; document.getElementById('modalOv').style.display='none'; }

        function askS(el) { document.getElementById('qInput').value=el.textContent; sendQ(); }

        async function sendQ() {
            const inp=document.getElementById('qInput'), btn=document.getElementById('sendBtn'), q=inp.value.trim();
            if(!q) return;
            const w=document.getElementById('genieWelcome'); if(w) w.remove();
            addMsg(q,'user');
            inp.value=''; btn.disabled=true; btn.textContent='ANALYZING...';
            const tid=addTyping();
            try {
                const r=await fetch('/api/genie',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({question:q,conversation_id:convId})});
                const d=await r.json();
                rmTyping(tid);
                if(d.conversation_id) convId=d.conversation_id;
                addBot(d);
            } catch(e) { rmTyping(tid); addMsg('Error occurred. Try again.','bot'); }
            btn.disabled=false; btn.textContent='INVESTIGATE';
        }

        function addMsg(t,role) {
            const c=document.getElementById('chatArea'), d=document.createElement('div');
            d.className='msg '+role;
            d.innerHTML=`<div class="msg-label">${role==='user'?'You':'Sentinel AI'}</div><div class="msg-bubble">${esc(t)}</div>`;
            c.appendChild(d); c.scrollTop=c.scrollHeight;
        }

        function addBot(data) {
            const c=document.getElementById('chatArea'), d=document.createElement('div');
            d.className='msg bot';
            let h='<div class="msg-label">Sentinel AI</div>';
            h+=`<div class="msg-bubble">${esc(data.text||'')}</div>`;
            if(data.sql) h+=`<div class="sql-block"><div class="sql-hdr"><span>SQL QUERY</span><button onclick="copyS(this)">Copy</button></div><div class="sql-body">${esc(data.sql)}</div></div>`;
            if(data.data&&data.data.columns&&data.data.rows&&data.data.rows.length) {
                h+='<div class="dt-wrap"><table class="dt"><thead><tr>';
                data.data.columns.forEach(c=>h+=`<th>${esc(c)}</th>`);
                h+='</tr></thead><tbody>';
                data.data.rows.forEach(r=>{h+='<tr>';r.forEach(v=>{const s=v===null?'-':v;h+=`<td class="${!isNaN(s)&&s!==''?'num':''}">${esc(String(s))}</td>`;});h+='</tr>';});
                h+='</tbody></table></div>';
            }
            d.innerHTML=h; c.appendChild(d); c.scrollTop=c.scrollHeight;
        }

        function addTyping() {
            const c=document.getElementById('chatArea'),d=document.createElement('div'),id='t'+Date.now();
            d.id=id; d.className='msg bot';
            d.innerHTML='<div class="msg-label">Sentinel AI</div><div class="msg-bubble"><div class="typing"><span></span><span></span><span></span></div></div>';
            c.appendChild(d); c.scrollTop=c.scrollHeight; return id;
        }
        function rmTyping(id){const e=document.getElementById(id);if(e)e.remove();}
        function copyS(b){navigator.clipboard.writeText(b.closest('.sql-block').querySelector('.sql-body').textContent);b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500);}
        function esc(t){const d=document.createElement('div');d.textContent=t;return d.innerHTML;}
    </script>
</body>
</html>
"""


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    filter_status = request.args.get("status", "")
    sort = request.args.get("sort", "risk_desc")
    sort_map = {"risk_desc": "risk_score DESC", "amount_desc": "amount DESC", "time_desc": "flagged_at DESC"}
    order_by = sort_map.get(sort, "risk_score DESC")

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        where = ["triage_status IN ('RED_BLOCK','YELLOW_REVIEW')"]
        params = []
        if filter_status:
            where.append("triage_status = %s")
            params.append(filter_status)
        cur.execute(f"SELECT * FROM real_time_fraud_triage WHERE {' AND '.join(where)} ORDER BY analyst_decision NULLS FIRST, {order_by} LIMIT 100", params)
        transactions = cur.fetchall()
        cur.execute("""SELECT COALESCE(SUM(CASE WHEN triage_status='RED_BLOCK' THEN 1 ELSE 0 END),0) AS red_count,
            COALESCE(SUM(CASE WHEN triage_status='YELLOW_REVIEW' AND analyst_decision IS NULL THEN 1 ELSE 0 END),0) AS yellow_count,
            COALESCE(SUM(CASE WHEN analyst_decision IS NOT NULL THEN 1 ELSE 0 END),0) AS reviewed_count,
            COALESCE(SUM(amount),0) AS total_exposure FROM real_time_fraud_triage WHERE triage_status IN ('RED_BLOCK','YELLOW_REVIEW')""")
        stats = cur.fetchone()
        cur.close()
        conn.close()
    except Exception:
        transactions = []
        stats = {"red_count": 0, "yellow_count": 0, "reviewed_count": 0, "total_exposure": 0}

    return render_template_string(MAIN_HTML, transactions=transactions, stats=stats, filter_status=filter_status, sort=sort)


@app.route("/decide", methods=["POST"])
def decide():
    txn_id = request.form["transaction_id"]
    decision = request.form["decision"]
    notes = request.form.get("notes", "")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""UPDATE real_time_fraud_triage SET analyst_decision=%s, analyst_notes=%s, reviewed_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP,
        automated_action=CASE WHEN %s='APPROVED' THEN 'ALLOW' WHEN %s='BLOCKED' THEN 'BLOCK' ELSE automated_action END WHERE transaction_id=%s""",
        (decision, notes, decision, decision, txn_id))
    conn.commit()
    cur.close()
    conn.close()
    return redirect(url_for("index"))


@app.route("/api/genie", methods=["POST"])
def api_genie():
    data = request.get_json()
    result = genie_ask(data.get("question", ""), data.get("conversation_id"))
    return jsonify(result)


@app.route("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory("static", filename)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
