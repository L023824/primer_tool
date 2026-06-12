"""
File Primer — Flask backend
v2: SKILLS.md + EXAMPLES.md generators, enhanced CLAUDE.md
Pattern detection engine: column metadata + KPI keywords → relevant skill blocks
"""

import os, json, re, io, zipfile, logging
from datetime import datetime
from flask import Flask, request, jsonify, send_file
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(24))
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def get_conn():
    import psycopg2
    from flask import session as flask_session

    # Shared hosted mode (Posit Connect): credentials supplied per-user via login form
    # Local JupyterHub mode: falls back to .env environment variables
    if "db" in flask_session:
        creds = flask_session["db"]
    else:
        creds = {
            "host":     os.getenv("REDSHIFT_HOST",   "cwb-rs-cluster-prod.czywitd0zinp.us-east-2.redshift.amazonaws.com"),
            "port":     int(os.getenv("REDSHIFT_PORT", 5439)),
            "dbname":   os.getenv("REDSHIFT_DBNAME", "bia_db"),
            "user":     os.getenv("REDSHIFT_USER"),
            "password": os.getenv("REDSHIFT_PASSWORD"),
        }

    return psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["user"],
        password=creds["password"],
        connect_timeout=10,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# DOMAIN CONTEXT LIBRARY
# Pre-baked disease state, market context, key population, and drug performance
# content per TA / indication. Surfaced in Step 3 when user selects TA + indication.
# Fields are pre-filled but remain editable in the UI.
# ═══════════════════════════════════════════════════════════════════════════════

DOMAIN_CONTEXT = {
    "Oncology": {
        "CLL": {
            "disease_state": (
                "B-cell malignancy and the most common adult leukemia in Western countries. "
                "Follows an indolent course; treatment is triggered by symptoms, cytopenias, or rapid progression. "
                "BTK inhibitors have become standard of care across lines of therapy."
            ),
            "market_context": (
                "BTK inhibitor-dominated market. Ibrutinib (Imbruvica) established the class; "
                "acalabrutinib (Calquence) and zanubrutinib (Brukinsa) competitive in 1L+. "
                "Post-covalent BTKi segment growing as patients progress. "
                "BCL-2 inhibitor venetoclax (Venclexta) relevant in combination and sequential settings."
            ),
            "key_population": (
                "Adults with CLL/SLL. 1L defined as first systemic therapy. "
                "Post-BTKi segment = patients who progressed on or are intolerant of a prior covalent BTKi. "
                "Key diagnosis anchor: ICD-10 C91.1."
            ),
            "drug_performance": (
                "Lilly: Jaypirca (pirtobrutinib) — non-covalent BTKi; indicated for adults with CLL/SLL "
                "after ≥2 prior lines including a BTKi and BCL-2 inhibitor. "
                "Differentiated by activity in the post-covalent BTKi setting. "
                "Competitive context: covalent BTKi class (ibrutinib, acalabrutinib, zanubrutinib) "
                "dominates 1L/earlier lines; venetoclax combinations active in BTKi-intolerant patients."
            ),
        },
        "MCL": {
            "disease_state": (
                "Aggressive B-cell non-Hodgkin lymphoma with poor prognosis. "
                "BTK inhibitors are the backbone of relapsed/refractory therapy. "
                "High relapse rates after covalent BTKi treatment remain a significant clinical challenge."
            ),
            "market_context": (
                "Ibrutinib, acalabrutinib, and zanubrutinib established in R/R MCL. "
                "Post-BTKi MCL is an underserved segment with limited options prior to pirtobrutinib approval. "
                "Frontline chemoimmunotherapy (BR, R-CHOP, Nordic) followed by ASCT in eligible patients."
            ),
            "key_population": (
                "Adults with relapsed/refractory MCL. "
                "Post-BTKi segment = progressed on ≥1 prior BTKi. "
                "Key diagnosis anchor: ICD-10 C83.1."
            ),
            "drug_performance": (
                "Lilly: Jaypirca (pirtobrutinib) — indicated for R/R MCL after ≥2 prior lines including a BTKi. "
                "Positioned in a setting with high unmet need following covalent BTKi failure. "
                "Competitive context: brexucabtagene autoleucel (Tecartus) CAR-T active in post-BTKi; "
                "limited chemotherapy options in later lines."
            ),
        },
        "eBC": {
            "disease_state": (
                "Early breast cancer confined to the breast and/or regional lymph nodes; curative intent. "
                "HR+/HER2- is the dominant subtype in the adjuvant CDK4/6i setting. "
                "CDK4/6 inhibitors are now standard of care in the adjuvant setting for high-risk patients."
            ),
            "market_context": (
                "CDK4/6 inhibitor adjuvant market — abemaciclib (Verzenio) and ribociclib (Kisqali) approved. "
                "Palbociclib (Ibrance) does not have an adjuvant approval. "
                "Adjuvant endocrine backbone is aromatase inhibitor or tamoxifen. "
                "High-risk patient identification (nodal involvement, Ki-67) drives treatment eligibility."
            ),
            "key_population": (
                "High-risk HR+/HER2- early breast cancer patients post-surgery. "
                "Risk defined by nodal involvement and Ki-67 expression. "
                "Key diagnosis anchor: ICD-10 C50.x."
            ),
            "drug_performance": (
                "Lilly: Verzenio (abemaciclib) — CDK4/6 inhibitor approved in adjuvant HR+/HER2- eBC "
                "with high recurrence risk; monarchE trial supports 2-year treatment duration. "
                "First CDK4/6i with adjuvant approval at launch. "
                "Competitive context: Kisqali (ribociclib, Novartis) has since received adjuvant approval "
                "and competes directly in this setting."
            ),
        },
        "mBC": {
            "disease_state": (
                "Incurable HR+/HER2- advanced or metastatic breast cancer. "
                "Treatment goal is disease control and quality of life. "
                "CDK4/6 inhibitors combined with endocrine therapy are 1L standard of care. "
                "Endocrine resistance and ESR1 mutations are key later-line challenges."
            ),
            "market_context": (
                "CDK4/6 inhibitor market — palbociclib (Ibrance), ribociclib (Kisqali), "
                "abemaciclib (Verzenio) all approved in 1L+. "
                "PI3K/AKT pathway agents and antibody-drug conjugates (ADCs) active in later lines. "
                "Oral SERDs (elacestrant/Orserdu) established in ESR1-mutant later-line setting. "
                "Inluriyo entering a competitive later-line endocrine therapy landscape."
            ),
            "key_population": (
                "Adults with HR+/HER2- advanced or metastatic breast cancer. "
                "Prior CDK4/6i exposure increasingly common in later-line patients. "
                "ESR1 mutation status relevant for SERD positioning. "
                "Key diagnosis anchor: ICD-10 C50.x with metastatic staging."
            ),
            "drug_performance": (
                "Lilly: Verzenio (abemaciclib) — CDK4/6 inhibitor in 1L+ mBC in combination with endocrine therapy. "
                "Inluriyo (imlunestrant) — oral selective estrogen receptor degrader (SERD); "
                "indicated for ER+/HER2- advanced/metastatic BC after prior endocrine therapy. "
                "Differentiated by oral route vs. fulvestrant (injectable SERD) and activity in ESR1-mutant disease. "
                "Competitive context: Orserdu (elacestrant) established oral SERD; "
                "ADCs (Enhertu, Trodelvy) active in later lines regardless of ER status."
            ),
        },
    },

    "Immunology": {
        "Atopic Dermatitis": {
            "disease_state": (
                "Chronic inflammatory skin condition driven by Th2 pathway dysregulation, "
                "primarily IL-4 and IL-13 signaling. "
                "Characterized by pruritus, skin barrier disruption, and flares. "
                "Severity ranges from mild to severe; moderate-to-severe patients are the systemic therapy target."
            ),
            "market_context": (
                "IL-4/IL-13 pathway dominates biologics. "
                "Dupixent (dupilumab, Sanofi/Regeneron) is market leader with broad label. "
                "Ebglyss (lebrikizumab) and Adbry (tralokinumab) compete in the anti-IL-13 space. "
                "JAK inhibitors (Rinvoq/upadacitinib, Cibinqo/abrocitinib) active in moderate-severe segment. "
                "TCS/TCI use as background therapy is a key dependency metric for analytics."
            ),
            "key_population": (
                "Moderate-to-severe AD adults inadequately controlled on topical therapy. "
                "Prior biologic exposure (especially dupilumab) increasingly common in later-line patients. "
                "Key diagnosis anchor: ICD-10 L20.x."
            ),
            "drug_performance": (
                "Lilly: Ebglyss (lebrikizumab) — high-affinity anti-IL-13 monoclonal antibody; "
                "Q2W maintenance dosing after loading. "
                "Competitive context: Dupixent (dupilumab) targets IL-4Rα blocking both IL-4 and IL-13 — "
                "broader mechanism and established market leader. "
                "Adbry (tralokinumab) also anti-IL-13 but lower binding affinity differentiation. "
                "JAK inhibitors offer oral option but carry class-level safety labeling requirements."
            ),
        },
        "Psoriasis": {
            "disease_state": (
                "Chronic immune-mediated skin disease; plaque psoriasis is the most common form. "
                "IL-17 and IL-23 pathways are the primary targets for moderate-to-severe disease. "
                "High efficacy bar — PASI 90/100 clearance is now the expected benchmark."
            ),
            "market_context": (
                "IL-17 inhibitors (Cosentyx/secukinumab, Taltz/ixekizumab) and "
                "IL-23 inhibitors (Skyrizi/risankizumab, Tremfya/guselkumab) dominate the biologic market. "
                "Skyrizi (AbbVie) is the fastest-growing asset. "
                "Biosimilar pressure on older TNF inhibitors. "
                "Taltz competes across both PSO and PSA indications."
            ),
            "key_population": (
                "Adults with moderate-to-severe plaque psoriasis. "
                "Biologic-naive and biologic-experienced segments both relevant. "
                "Key diagnosis anchor: ICD-10 L40.0."
            ),
            "drug_performance": (
                "Lilly: Taltz (ixekizumab) — anti-IL-17A monoclonal antibody; "
                "approved for moderate-to-severe plaque psoriasis. "
                "Rapid onset of action; high PASI 90/100 response rates. "
                "Competitive context: Cosentyx (secukinumab, Novartis) established IL-17A competitor; "
                "Skyrizi (risankizumab, AbbVie) IL-23 inhibitor gaining share with strong durability data; "
                "Tremfya (guselkumab, J&J) IL-23 competitor."
            ),
        },
        "Psoriatic Arthritis": {
            "disease_state": (
                "Chronic inflammatory arthritis associated with psoriasis. "
                "Affects joints, entheses, and skin; heterogeneous presentation. "
                "IL-17 and IL-23 inhibitors, TNF inhibitors, and JAK inhibitors all active. "
                "Treat-to-target approach with minimal disease activity (MDA) as goal."
            ),
            "market_context": (
                "TNF inhibitors (Humira, Enbrel) remain widely used despite biosimilar entry. "
                "IL-17 inhibitors (Taltz, Cosentyx) active across joint and skin domains. "
                "IL-23 inhibitors (Skyrizi, Tremfya) approved in PSA. "
                "JAK inhibitors (Rinvoq, Xeljanz) offer oral option. "
                "Market moving toward agents with dual skin and joint efficacy."
            ),
            "key_population": (
                "Adults with active psoriatic arthritis. "
                "Biologic-naive and TNF-experienced segments both relevant. "
                "Patients with significant skin involvement benefit from dual-domain agents. "
                "Key diagnosis anchor: ICD-10 L40.5."
            ),
            "drug_performance": (
                "Lilly: Taltz (ixekizumab) — anti-IL-17A antibody approved for active PSA; "
                "demonstrates efficacy across joint, skin, and enthesitis domains. "
                "Competitive context: Cosentyx (secukinumab) direct IL-17A competitor; "
                "Skyrizi (risankizumab) and Tremfya (guselkumab) IL-23 inhibitors gaining in PSA; "
                "TNF biosimilars (adalimumab biosimilars) create pricing pressure in earlier lines."
            ),
        },
        "IBD": {
            "disease_state": (
                "Umbrella term for Crohn's disease (CD) and ulcerative colitis (UC). "
                "Chronic relapsing-remitting inflammation of the GI tract. "
                "Anti-TNF agents are established; IL-23, JAK inhibitors, and gut-selective biologics "
                "are increasingly used in moderate-to-severe and biologic-refractory patients."
            ),
            "market_context": (
                "Humira (adalimumab) biosimilar entry reshaping the market economics. "
                "Skyrizi (risankizumab, AbbVie) and Rinvoq (upadacitinib, AbbVie) strong in CD and UC. "
                "Entyvio (vedolizumab, Takeda) gut-selective with strong safety profile. "
                "Stelara (ustekinumab, J&J) IL-12/23 inhibitor losing exclusivity. "
                "High unmet need remains in biologic-refractory patients."
            ),
            "key_population": (
                "Adults with moderate-to-severe Crohn's disease or ulcerative colitis "
                "inadequately controlled on conventional or biologic therapy. "
                "Key diagnosis anchors: ICD-10 K50.x (Crohn's disease), K51.x (ulcerative colitis)."
            ),
            "drug_performance": (
                "Lilly: Omvoh (mirikizumab) — anti-IL-23p19 monoclonal antibody; "
                "approved for moderately to severely active ulcerative colitis. "
                "Differentiated by selective IL-23 blockade with favorable safety and efficacy profile. "
                "Competitive context: Skyrizi (risankizumab) IL-23 inhibitor approved in both UC and CD — "
                "direct competitor; Entyvio (vedolizumab) gut-selective biologic with strong real-world safety data; "
                "Rinvoq (upadacitinib) JAK inhibitor active across UC and CD."
            ),
        },
    },

    "Neuroscience": {
        "Alzheimer's Disease": {
            "disease_state": (
                "Progressive neurodegenerative disease; amyloid plaques and tau tangles are pathological hallmarks. "
                "Early symptomatic AD — mild cognitive impairment (MCI) to mild dementia — "
                "is the current treatment target for disease-modifying therapies. "
                "Amyloid confirmation by PET or CSF biomarker required for treatment eligibility."
            ),
            "market_context": (
                "Anti-amyloid antibody class emerging as the first disease-modifying category. "
                "Leqembi (lecanemab, Eisai/Biogen) received full FDA approval first; "
                "Kisunla (donanemab) second to market. "
                "Market still early-stage — patient identification, infusion infrastructure, "
                "and ARIA monitoring are significant access and adoption barriers. "
                "Payer coverage evolving; CMS coverage with evidence development pathway relevant."
            ),
            "key_population": (
                "Adults with early symptomatic Alzheimer's disease confirmed by amyloid biomarker "
                "(PET scan or CSF analysis). "
                "ApoE4 carrier status relevant for ARIA risk stratification and patient selection. "
                "Key diagnosis anchor: ICD-10 G30.x."
            ),
            "drug_performance": (
                "Lilly: Kisunla (donanemab) — anti-amyloid beta antibody targeting N3pG amyloid. "
                "Differentiated by potential treatment completion endpoint based on amyloid clearance — "
                "unique among the class. "
                "Competitive context: Leqembi (lecanemab) biweekly dosing vs. Kisunla monthly; "
                "ARIA profiles differ — direct head-to-head data not available; "
                "market education and diagnosis infrastructure are shared challenges for both assets."
            ),
        },
    },

    "Metabolic": {
        "Obesity": {
            "disease_state": (
                "Chronic disease defined by excess adiposity with metabolic and cardiovascular consequences. "
                "GLP-1 receptor agonists have redefined the treatment paradigm — "
                "weight loss of 15–25% is now achievable pharmacologically. "
                "Obesity is increasingly recognized as a disease requiring long-term management, "
                "not a lifestyle issue."
            ),
            "market_context": (
                "GLP-1/GIP class dominant — Wegovy (semaglutide, Novo Nordisk) and "
                "Zepbound (tirzepatide, Lilly) are leading injectable options. "
                "Supply constraints easing as manufacturing scales. "
                "Oral semaglutide (Rybelsus) and oral GLP-1s in pipeline. "
                "Significant payer and access pressure; step therapy and prior authorization requirements common. "
                "Compounding market created friction; Foundayo addresses specific access segments."
            ),
            "key_population": (
                "Adults with BMI ≥30, or BMI ≥27 with at least one weight-related comorbidity "
                "(T2D, hypertension, dyslipidemia, obstructive sleep apnea). "
                "Key diagnosis anchor: ICD-10 E66.x."
            ),
            "drug_performance": (
                "Lilly: Zepbound (tirzepatide) — dual GLP-1/GIP receptor agonist; "
                "superior weight loss vs. semaglutide demonstrated in SURMOUNT trials. "
                "Also approved for moderate-to-severe OSA in adults with obesity. "
                "Foundayo (tirzepatide) — compounding-resistant formulation positioned for "
                "specific payer and access segments. "
                "Competitive context: Wegovy (semaglutide 2.4mg, Novo Nordisk) established market leader "
                "by volume; CagriSema and oral options in Novo pipeline represent future competition."
            ),
        },
        "Diabetes": {
            "disease_state": (
                "Chronic metabolic disease of insulin resistance and progressive beta-cell dysfunction. "
                "GLP-1 receptor agonists are now the preferred add-on after metformin given "
                "CV and weight benefits. "
                "SGLT-2 inhibitors are relevant for patients with CKD or heart failure comorbidities. "
                "Market shifting toward combination cardiometabolic management."
            ),
            "market_context": (
                "GLP-1 class growing rapidly — Ozempic (semaglutide, Novo Nordisk) is the dominant injectable; "
                "Jardiance (empagliflozin, BI/Lilly) strong in CKD/HF segment. "
                "Mounjaro (tirzepatide) competing on superior HbA1c reduction and weight loss. "
                "Oral semaglutide (Rybelsus) and tirzepatide oral formulations in development. "
                "Class competition intensifying with pipeline GLP-1/GIP and triple agonists."
            ),
            "key_population": (
                "Adults with type 2 diabetes inadequately controlled on oral agents. "
                "GLP-1 naive and GLP-1 experienced segments both relevant. "
                "Cardiovascular risk, CKD, and obesity comorbidities drive treatment selection. "
                "Key diagnosis anchor: ICD-10 E11.x."
            ),
            "drug_performance": (
                "Lilly: Mounjaro (tirzepatide) — dual GLP-1/GIP agonist; "
                "superior HbA1c reduction vs. semaglutide demonstrated in SURPASS trials. "
                "Differentiated by meaningful weight loss benefit on top of glycemic control. "
                "Competitive context: Ozempic (semaglutide 1mg, Novo Nordisk) — established weekly injectable; "
                "Trulicity (dulaglutide, Lilly) earlier-generation GLP-1 still active in market; "
                "SGLT-2 class (Jardiance, Farxiga) preferred in CKD/HF regardless of GLP-1 use."
            ),
        },
        "Sleep Apnea": {
            "disease_state": (
                "Obstructive sleep apnea (OSA) caused by upper airway collapse during sleep. "
                "Historically managed with CPAP as the only effective intervention. "
                "GLP-1 receptor agonists have demonstrated meaningful AHI reduction in "
                "patients with obesity-related OSA, creating a new pharmacological treatment category."
            ),
            "market_context": (
                "Nascent pharmacological market — CPAP remains standard of care and is not displaced. "
                "Zepbound is the first and currently only FDA-approved pharmacological treatment "
                "for moderate-to-severe OSA in adults with obesity. "
                "Market development requires OSA patient identification through sleep medicine and "
                "primary care channels. "
                "Payer coverage for OSA indication still developing."
            ),
            "key_population": (
                "Adults with moderate-to-severe OSA (AHI ≥15 events/hour) and obesity (BMI ≥30). "
                "Patients who are CPAP-intolerant or inadequately controlled are key targets. "
                "Key diagnosis anchor: ICD-10 G47.33."
            ),
            "drug_performance": (
                "Lilly: Zepbound (tirzepatide) — first and only FDA-approved pharmacological treatment "
                "for moderate-to-severe OSA in adults with obesity. "
                "SURMOUNT-OSA trial demonstrated significant AHI reduction. "
                "Competitive context: No direct pharmacological competitors currently approved in OSA. "
                "CPAP device manufacturers (ResMed, Philips) represent the incumbent standard of care."
            ),
        },
    },
}


@app.route("/domain_context", methods=["GET"])
def domain_context():
    """
    Returns pre-baked domain context for a given TA + indication.
    Query params: ?ta=Oncology&indication=CLL
    Returns the four content fields or empty strings if not found.
    """
    ta         = request.args.get("ta", "").strip()
    indication = request.args.get("indication", "").strip()
    empty = {"disease_state": "", "market_context": "", "key_population": "", "drug_performance": ""}
    if not ta or not indication:
        return jsonify(empty)
    content = DOMAIN_CONTEXT.get(ta, {}).get(indication, empty)
    return jsonify(content)


# ═══════════════════════════════════════════════════════════════════════════════
# PATTERN LIBRARY
# Each pattern: id, name, description, col_triggers (regex list on col names),
#   type_triggers (regex list on col types), kpi_keywords (list of strings),
#   and a render(tables, cols_by_table, matched_tables) → str function.
#
# Scoring per pattern:
#   +1 for each col_trigger match found across any validated table
#   +1 for each kpi_keyword found in any KPI string (case-insensitive)
#   +1 if output_type matches pattern's preferred_output list
# Patterns with score >= 1 are included; sorted desc; capped at 6.
# ═══════════════════════════════════════════════════════════════════════════════

def _pattern_library():
    """
    Returns the full ordered pattern library.
    Each entry is a dict; render() receives:
        tables        — list of all "schema.table" strings
        cols_by_table — dict of schema.table → list of col dicts
        primary       — the first table name (schema.table)
        primary_cols  — col list for the first table
    """

    def col_names(cols):
        return [c["name"] for c in cols]

    def first_col_matching(cols, pattern):
        """Return first column name matching regex pattern, or placeholder."""
        rx = re.compile(pattern, re.IGNORECASE)
        for c in cols:
            if rx.search(c["name"]):
                return c["name"]
        return "<date_col>"

    def first_id_col(cols):
        return first_col_matching(cols, r"(patient_id|hcp_id|npi|provider_id|customer_id|member_id|person_id|subject_id)")

    def first_date_col(cols):
        return first_col_matching(cols, r"(_date|_dt|_time|index_|first_|start_|fill_date|rx_date|svc_date|service_date|disp_date)")

    def first_status_col(cols):
        return first_col_matching(cols, r"(_flag|_status|_segment|_tier|_type|_cat|_class|_label)")

    # ── 1. DEDUPLICATION ──────────────────────────────────────────────────────
    def render_dedup(tables, cols_by_table, primary, primary_cols):
        id_col = first_id_col(primary_cols)
        date_col = first_date_col(primary_cols)
        return f"""
**When to use:** Any table that has one row per event and you need one row per entity (patient, HCP, fill).  
Redshift-safe: `ROW_NUMBER()` is more reliable than `DISTINCT ON` for complex deduplication.

```sql
-- Deduplication: keep the latest record per {id_col}
-- Pattern: ROW_NUMBER() OVER PARTITION — most reliable on Redshift

DROP TABLE IF EXISTS temp_dedup_base;
CREATE TABLE temp_dedup_base AS
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY {id_col}
            ORDER BY {date_col} DESC      -- keep most recent record
        ) AS rn
    FROM {primary}
) t
WHERE rn = 1;

-- Verify: row count should equal DISTINCT {id_col} count
-- SELECT COUNT(*), COUNT(DISTINCT {id_col}) FROM temp_dedup_base;
```

**Fan-out check** — always run this after a join to detect unexpected row multiplication:
```sql
-- If these two counts differ, you have a fan-out
SELECT
    COUNT(*)                      AS total_rows,
    COUNT(DISTINCT {id_col})      AS distinct_entities
FROM temp_dedup_base;
```
"""

    # ── 2. DATE SPINE ─────────────────────────────────────────────────────────
    def render_date_spine(tables, cols_by_table, primary, primary_cols):
        date_col = first_date_col(primary_cols)
        id_col   = first_id_col(primary_cols)
        return f"""
**When to use:** Time-series analysis where gaps in dates would silently drop months from your output.  
Always join your metrics onto the spine rather than generating dates from the data itself.

```sql
-- Date spine: one row per month between two dates
-- Adjust granularity: DATE_TRUNC('week',...) or DATE_TRUNC('day',...) as needed

DROP TABLE IF EXISTS temp_date_spine;
CREATE TABLE temp_date_spine AS
SELECT
    DATEADD('month', seq.n, DATE_TRUNC('month', '2023-01-01'::DATE)) AS spine_month
FROM (
    SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS n
    FROM {primary}
    LIMIT 60          -- adjust: number of months in range
) seq
WHERE DATEADD('month', seq.n, DATE_TRUNC('month', '2023-01-01'::DATE))
      <= DATE_TRUNC('month', CURRENT_DATE);

-- Join your metrics onto the spine to fill gaps with zero
SELECT
    s.spine_month,
    COALESCE(m.metric_value, 0) AS metric_value
FROM temp_date_spine s
LEFT JOIN (
    SELECT
        DATE_TRUNC('month', {date_col}) AS month,
        COUNT(DISTINCT {id_col})        AS metric_value
    FROM {primary}
    GROUP BY 1
) m ON s.spine_month = m.month
ORDER BY s.spine_month;
```
"""

    # ── 3. ROLLING WINDOW ─────────────────────────────────────────────────────
    def render_rolling(tables, cols_by_table, primary, primary_cols):
        date_col = first_date_col(primary_cols)
        id_col   = first_id_col(primary_cols)
        val_col  = next(
            (c["name"] for c in primary_cols
             if re.search(r"(amount|count|qty|quantity|dose|units|value|revenue|spend)", c["name"], re.I)),
            "metric_value"
        )
        return f"""
**When to use:** Smoothing noisy monthly metrics, calculating rolling MAT (moving annual total),  
or computing inter-fill gap windows.

> ⚠️ Redshift window frame limitation: use `ROWS BETWEEN N PRECEDING AND CURRENT ROW` only —  
> `RANGE BETWEEN` with date intervals is not supported. Pre-aggregate to the desired grain first.

```sql
-- Rolling 3-month sum — pre-aggregate to monthly grain first
DROP TABLE IF EXISTS temp_monthly_base;
CREATE TABLE temp_monthly_base AS
SELECT
    DATE_TRUNC('month', {date_col})   AS month,
    {id_col},
    SUM({val_col})                    AS monthly_value
FROM {primary}
GROUP BY 1, 2;

-- Then apply rolling window on the pre-aggregated table
SELECT
    month,
    {id_col},
    monthly_value,
    SUM(monthly_value) OVER (
        PARTITION BY {id_col}
        ORDER BY month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW   -- 3-month rolling
    ) AS rolling_3m_sum,
    AVG(monthly_value) OVER (
        PARTITION BY {id_col}
        ORDER BY month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3m_avg
FROM temp_monthly_base
ORDER BY {id_col}, month;
```
"""

    # ── 4. YOY COMPARISON ────────────────────────────────────────────────────
    def render_yoy(tables, cols_by_table, primary, primary_cols):
        date_col = first_date_col(primary_cols)
        id_col   = first_id_col(primary_cols)
        return f"""
**When to use:** Period-over-period trending — quarterly business reviews, brand performance decks.

```sql
-- YoY: compare current year vs prior year
DROP TABLE IF EXISTS temp_yoy;
CREATE TABLE temp_yoy AS
SELECT
    {id_col},
    SUM(CASE WHEN EXTRACT(year FROM {date_col}) = EXTRACT(year FROM CURRENT_DATE)
             THEN 1 ELSE 0 END)                       AS cy_count,
    SUM(CASE WHEN EXTRACT(year FROM {date_col}) = EXTRACT(year FROM CURRENT_DATE) - 1
             THEN 1 ELSE 0 END)                       AS py_count
FROM {primary}
WHERE {date_col} >= DATEADD('year', -2, DATE_TRUNC('year', CURRENT_DATE))
GROUP BY 1;

SELECT
    {id_col},
    cy_count,
    py_count,
    cy_count - py_count                                              AS yoy_abs_change,
    ROUND(
        (cy_count - py_count) * 100.0 / NULLIF(py_count, 0),
    1)                                                               AS yoy_pct_change
FROM temp_yoy
ORDER BY yoy_abs_change DESC;
```
"""

    # ── 5. COHORT ANALYSIS ───────────────────────────────────────────────────
    def render_cohort(tables, cols_by_table, primary, primary_cols):
        id_col   = first_id_col(primary_cols)
        date_col = first_date_col(primary_cols)
        return f"""
**When to use:** Tracking retention, persistence, or engagement from a patient's/HCP's first event.  
Classic use case: first Rx date as cohort anchor, then measure refill behaviour by month offset.

```sql
-- Cohort: group by first event month, track activity at N months offset

DROP TABLE IF EXISTS temp_cohort_anchor;
CREATE TABLE temp_cohort_anchor AS
SELECT
    {id_col},
    MIN({date_col})                              AS first_event_date,
    DATE_TRUNC('month', MIN({date_col}))         AS cohort_month
FROM {primary}
GROUP BY 1;

-- Activity by months-since-first-event
SELECT
    ca.cohort_month,
    DATEDIFF('month', ca.first_event_date, e.{date_col})   AS months_since_start,
    COUNT(DISTINCT e.{id_col})                              AS active_entities
FROM {primary} e
INNER JOIN temp_cohort_anchor ca
    ON e.{id_col} = ca.{id_col}
WHERE e.{date_col} >= ca.first_event_date
GROUP BY 1, 2
ORDER BY 1, 2;
```
"""

    # ── 6. FUNNEL ANALYSIS ───────────────────────────────────────────────────
    def render_funnel(tables, cols_by_table, primary, primary_cols):
        id_col = first_id_col(primary_cols)
        # Try to find a second table for step 2
        second = tables[1] if len(tables) > 1 else primary
        return f"""
**When to use:** Patient journey funnels (identified → diagnosed → prescribed → filled → refilled),  
HCP targeting funnels (universe → segmented → called → written).  
Multiply step flags to enforce sequential ordering.

```sql
-- Funnel: sequential step tracking with drop-off rates

DROP TABLE IF EXISTS temp_funnel_steps;
CREATE TABLE temp_funnel_steps AS
SELECT
    t1.{id_col},
    1                                                          AS step_1_in_universe,
    CASE WHEN t2.{id_col} IS NOT NULL THEN 1 ELSE 0 END       AS step_2_qualified,
    -- Add further steps: join additional tables and flag 1/0
    -- CASE WHEN t3.{id_col} IS NOT NULL THEN 1 ELSE 0 END    AS step_3_actioned,
    NULL::INT                                                  AS step_3_placeholder
FROM {primary} t1
LEFT JOIN {second} t2
    ON t1.{id_col} = t2.{id_col};

-- Summary with conversion rates
SELECT
    COUNT(*)                                                   AS step_1_universe,
    SUM(step_2_qualified)                                      AS step_2_qualified,
    ROUND(SUM(step_2_qualified) * 100.0 / NULLIF(COUNT(*), 0), 1)  AS step_1_to_2_pct
    -- extend for each step
FROM temp_funnel_steps;
```
"""

    # ── 7. PRIORITY CASE WATERFALL (mutual exclusivity) ──────────────────────
    def render_waterfall(tables, cols_by_table, primary, primary_cols):
        id_col = first_id_col(primary_cols)
        status_col = first_status_col(primary_cols)
        return f"""
**When to use:** Assigning patients or HCPs to mutually exclusive segments where an entity  
could qualify for multiple buckets — share of market segments, TUA classification,  
patient eligibility buckets. Priority order in the CASE determines the segment assigned.

> BI&A standard: use a priority-based CASE waterfall, not multiple overlapping flags.  
> Document the priority order here so it can be audited.

```sql
-- Priority CASE waterfall: assign exactly one segment per {id_col}
-- Order: most restrictive / highest priority first

DROP TABLE IF EXISTS temp_segmented;
CREATE TABLE temp_segmented AS
SELECT
    {id_col},
    {status_col},
    CASE
        WHEN <condition_tier_1>  THEN 'Segment A'    -- highest priority
        WHEN <condition_tier_2>  THEN 'Segment B'
        WHEN <condition_tier_3>  THEN 'Segment C'
        ELSE                          'Other'         -- catch-all last
    END AS segment,

    -- Audit flags — keep these alongside segment for QA
    CASE WHEN <condition_tier_1> THEN 1 ELSE 0 END   AS flag_tier_1,
    CASE WHEN <condition_tier_2> THEN 1 ELSE 0 END   AS flag_tier_2,
    CASE WHEN <condition_tier_3> THEN 1 ELSE 0 END   AS flag_tier_3
FROM {primary};

-- Verify mutual exclusivity: every row should have exactly one segment
SELECT segment, COUNT(*) AS n FROM temp_segmented GROUP BY 1 ORDER BY 2 DESC;
```
"""

    # ── 8. CUMULATIVE EVER-FLAG ───────────────────────────────────────────────
    def render_ever_flag(tables, cols_by_table, primary, primary_cols):
        id_col   = first_id_col(primary_cols)
        date_col = first_date_col(primary_cols)
        return f"""
**When to use:** Trialist / User / Adopter (TUA) classification — once an entity crosses a  
threshold it stays classified at that level. Also: ever-prescribed, ever-diagnosed flags.  
Lilly BI&A standard for cumulative goal-based tracking views.

> Gotcha: if the source table has one row per event, cumulate *before* joining to other tables  
> to avoid fan-out inflating counts (lb=97/98 style issues).

```sql
-- Cumulative ever-flag: classify each {id_col} by highest milestone reached

DROP TABLE IF EXISTS temp_event_counts;
CREATE TABLE temp_event_counts AS
SELECT
    {id_col},
    MIN({date_col})          AS first_event_date,
    COUNT(DISTINCT {date_col}::DATE)  AS distinct_event_days,    -- use days to avoid same-day duplication
    COUNT(*)                 AS total_events
FROM {primary}
GROUP BY 1;

DROP TABLE IF EXISTS temp_ever_classified;
CREATE TABLE temp_ever_classified AS
SELECT
    {id_col},
    first_event_date,
    distinct_event_days,
    -- Adjust thresholds to match business definitions
    CASE
        WHEN distinct_event_days >= 4 THEN 'Adopter'
        WHEN distinct_event_days >= 2 THEN 'User'
        WHEN distinct_event_days >= 1 THEN 'Trialist'
        ELSE                               'None'
    END AS classification,
    -- Cumulative flags — useful for tracking view joins
    CASE WHEN distinct_event_days >= 1 THEN 1 ELSE 0 END AS is_trialist,
    CASE WHEN distinct_event_days >= 2 THEN 1 ELSE 0 END AS is_user,
    CASE WHEN distinct_event_days >= 4 THEN 1 ELSE 0 END AS is_adopter
FROM temp_event_counts;
```
"""

    # ── 9. INTER-FILL GAP CLASSIFICATION ─────────────────────────────────────
    def render_fill_gap(tables, cols_by_table, primary, primary_cols):
        id_col   = first_id_col(primary_cols)
        date_col = first_date_col(primary_cols)
        return f"""
**When to use:** Therapy persistence, gap analysis, days-on-therapy (DOT) calculations.  
Classifies each fill interval as continuous, short gap, or lapse.  
Used for patient persistence metrics in Ebglyss / Jaypirca type analyses.

```sql
-- Inter-fill gap: calculate days between consecutive fills per patient

DROP TABLE IF EXISTS temp_fill_gaps;
CREATE TABLE temp_fill_gaps AS
SELECT
    {id_col},
    {date_col}                                                AS fill_date,
    LAG({date_col}) OVER (
        PARTITION BY {id_col}
        ORDER BY {date_col}
    )                                                         AS prior_fill_date,
    DATEDIFF('day',
        LAG({date_col}) OVER (
            PARTITION BY {id_col}
            ORDER BY {date_col}
        ),
        {date_col}
    )                                                         AS days_since_prior_fill,
    ROW_NUMBER() OVER (
        PARTITION BY {id_col}
        ORDER BY {date_col}
    )                                                         AS fill_number
FROM {primary};

-- Classify gaps — adjust thresholds to match therapy / days-supply assumptions
SELECT
    {id_col},
    fill_date,
    prior_fill_date,
    days_since_prior_fill,
    fill_number,
    CASE
        WHEN fill_number = 1              THEN 'Index Fill'
        WHEN days_since_prior_fill <= 45  THEN 'Continuous'
        WHEN days_since_prior_fill <= 90  THEN 'Short Gap'
        ELSE                                   'Lapse / Restart'
    END AS persistence_status
FROM temp_fill_gaps
ORDER BY {id_col}, fill_date;
```
"""

    # ── 10. WINDOW TRUNCATION AUDIT ──────────────────────────────────────────
    def render_window_truncation(tables, cols_by_table, primary, primary_cols):
        date_col = first_date_col(primary_cols)
        return f"""
**When to use:** Any time you see unexpected metric *improvements* in a period-over-period  
refresh — especially at the most recent month. This is almost always a window truncation  
artifact: the latest period has incomplete data, so counts appear lower/better than prior periods.

> BI&A standard: always check for truncation before attributing trend changes to real behaviour.

```sql
-- Window truncation audit: check if the most recent period is incomplete

-- Step 1: compare row density across months
SELECT
    DATE_TRUNC('month', {date_col})   AS month,
    COUNT(*)                           AS row_count,
    COUNT(*) * 1.0 / MAX(COUNT(*)) OVER ()   AS pct_of_peak_month
FROM {primary}
GROUP BY 1
ORDER BY 1 DESC
LIMIT 6;   -- inspect the 6 most recent months

-- Interpretation:
-- If the most recent month is < 80% of the prior month, it is likely truncated.
-- Do NOT report the most recent month as a completed period.
-- Flag this in stakeholder communications as: "Most recent month subject to lag."

-- Step 2: find data freshness (max date in table)
SELECT MAX({date_col}) AS latest_record_date FROM {primary};
```
"""

    # ── 11. FULL OUTER JOIN MONTH-WISE UPDATE ────────────────────────────────
    def render_full_outer(tables, cols_by_table, primary, primary_cols):
        id_col   = first_id_col(primary_cols)
        date_col = first_date_col(primary_cols)
        second   = tables[1] if len(tables) > 1 else primary
        return f"""
**When to use:** Combining actuals with goals/targets when either side may have months the other  
doesn't — common in goal-based tracking views where goal tables don't always have rows for  
every month or every HCP. Also: SoM cohort flag updates across monthly snapshots.

> Gotcha on UNION type casting: if actuals use `INTEGER` and goals use `BIGINT`,  
> the UNION will fail. Cast both sides explicitly.

```sql
-- Full outer join: merge actuals and goals preserving all months from both sides

DROP TABLE IF EXISTS temp_actuals_vs_goals;
CREATE TABLE temp_actuals_vs_goals AS
SELECT
    COALESCE(a.month, g.month)         AS month,
    COALESCE(a.{id_col}, g.{id_col})   AS {id_col},
    a.actual_value,
    g.goal_value,
    COALESCE(a.actual_value, 0)        AS actual_filled,
    COALESCE(g.goal_value, 0)          AS goal_filled,
    ROUND(
        COALESCE(a.actual_value, 0) * 100.0
        / NULLIF(COALESCE(g.goal_value, 0), 0),
    1)                                 AS pct_to_goal
FROM (
    SELECT
        DATE_TRUNC('month', {date_col}) AS month,
        {id_col},
        COUNT(DISTINCT {id_col})        AS actual_value
    FROM {primary}
    GROUP BY 1, 2
) a
FULL OUTER JOIN (
    SELECT
        month::DATE,
        {id_col}::VARCHAR,            -- cast to match actuals — adjust type
        goal_value::INT
    FROM {second}
) g
ON  a.month      = g.month
AND a.{id_col}   = g.{id_col};
```
"""

    # ── 12. TOP-N RANKING ────────────────────────────────────────────────────
    def render_topn(tables, cols_by_table, primary, primary_cols):
        id_col   = first_id_col(primary_cols)
        val_col  = next(
            (c["name"] for c in primary_cols
             if re.search(r"(amount|count|qty|quantity|units|value|revenue|spend|trx|rxs|scripts)", c["name"], re.I)),
            "metric_value"
        )
        status_col = first_status_col(primary_cols)
        return f"""
**When to use:** HCP opportunity ranking, patient cost stratification, territory leaderboards.  
Use `DENSE_RANK()` when ties should share the same rank position;  
use `ROW_NUMBER()` when you need exactly N rows with no ties.

```sql
-- Top-N ranking: rank {id_col} by {val_col}, with optional group-level ranking

DROP TABLE IF EXISTS temp_ranked;
CREATE TABLE temp_ranked AS
SELECT
    {id_col},
    {val_col},
    {status_col},
    -- Overall rank
    ROW_NUMBER()  OVER (ORDER BY {val_col} DESC)                    AS overall_rank,
    DENSE_RANK()  OVER (ORDER BY {val_col} DESC)                    AS overall_rank_dense,
    -- Rank within segment (e.g. per territory or decile group)
    ROW_NUMBER()  OVER (PARTITION BY {status_col}
                        ORDER BY {val_col} DESC)                    AS rank_within_segment
FROM {primary};

-- Pull top 10 overall
SELECT * FROM temp_ranked WHERE overall_rank <= 10 ORDER BY overall_rank;

-- Pull top 3 per segment
SELECT * FROM temp_ranked WHERE rank_within_segment <= 3 ORDER BY {status_col}, rank_within_segment;
```
"""

    # ── PATTERN REGISTRY ─────────────────────────────────────────────────────
    return [
        {
            "id": "deduplication",
            "name": "Deduplication",
            "desc": "Keep one row per entity; detect fan-out after joins",
            "col_triggers": [r"patient_id", r"hcp_id", r"npi", r"provider_id", r"member_id", r"customer_id"],
            "type_triggers": [],
            "kpi_keywords": ["unique", "distinct", "dedupe", "one per", "per patient", "per hcp"],
            "preferred_output": ["SQL Pipeline", "Ad hoc"],
            "render": render_dedup,
        },
        {
            "id": "date_spine",
            "name": "Date spine",
            "desc": "Continuous date sequence for gap-free time-series",
            "col_triggers": [r"_date$", r"_dt$", r"fill_date", r"rx_date", r"svc_date", r"service_date", r"disp_date"],
            "type_triggers": [r"date", r"timestamp"],
            "kpi_keywords": ["trend", "monthly", "weekly", "time series", "over time", "mat", "rolling"],
            "preferred_output": ["SQL Pipeline", "Dashboard"],
            "render": render_date_spine,
        },
        {
            "id": "rolling_window",
            "name": "Rolling window",
            "desc": "Rolling sums / averages; MAT; inter-period smoothing",
            "col_triggers": [r"_date$", r"_dt$", r"amount", r"quantity", r"units", r"trx", r"rxs"],
            "type_triggers": [r"numeric", r"integer", r"bigint", r"float"],
            "kpi_keywords": ["rolling", "mat", "moving", "3 month", "6 month", "12 month", "smoothed"],
            "preferred_output": ["SQL Pipeline", "Dashboard"],
            "render": render_rolling,
        },
        {
            "id": "yoy",
            "name": "YoY comparison",
            "desc": "Year-over-year and period-over-period metrics",
            "col_triggers": [r"_date$", r"_dt$", r"year", r"period"],
            "type_triggers": [],
            "kpi_keywords": ["yoy", "year over year", "prior year", "growth", "change vs", "vs prior", "qoq", "mom"],
            "preferred_output": ["SQL Pipeline", "Dashboard", "Report"],
            "render": render_yoy,
        },
        {
            "id": "cohort",
            "name": "Cohort analysis",
            "desc": "Group by first event date; track retention / persistence by offset",
            "col_triggers": [r"first_", r"index_", r"cohort", r"index_date", r"start_date"],
            "type_triggers": [],
            "kpi_keywords": ["cohort", "retention", "persistence", "days on therapy", "dot", "time to", "first fill", "index"],
            "preferred_output": ["SQL Pipeline", "Dashboard"],
            "render": render_cohort,
        },
        {
            "id": "funnel",
            "name": "Funnel analysis",
            "desc": "Sequential step tracking with conversion rates",
            "col_triggers": [r"patient_id", r"hcp_id", r"npi", r"step", r"stage", r"status"],
            "type_triggers": [],
            "kpi_keywords": ["funnel", "conversion", "journey", "step", "pipeline", "opportunity", "identified", "trialist", "writer"],
            "preferred_output": ["SQL Pipeline", "Dashboard"],
            "render": render_funnel,
        },
        {
            "id": "waterfall",
            "name": "Priority CASE waterfall",
            "desc": "Mutually exclusive segmentation; SoM buckets; TUA classification",
            "col_triggers": [r"_flag$", r"_status$", r"_segment$", r"_tier$", r"_type$", r"_cat$", r"_class$"],
            "type_triggers": [],
            "kpi_keywords": ["segment", "mutually exclusive", "share of market", "som", "tua", "trialist", "user", "adopter", "bucket", "classify"],
            "preferred_output": ["SQL Pipeline", "Ad hoc"],
            "render": render_waterfall,
        },
        {
            "id": "ever_flag",
            "name": "Cumulative ever-flag (TUA)",
            "desc": "Trialist / User / Adopter classification; once-ever milestone tracking",
            "col_triggers": [r"patient_id", r"hcp_id", r"npi", r"_date$"],
            "type_triggers": [],
            "kpi_keywords": ["trialist", "user", "adopter", "tua", "ever", "cumulative", "net new", "first time", "new writer"],
            "preferred_output": ["SQL Pipeline"],
            "render": render_ever_flag,
        },
        {
            "id": "fill_gap",
            "name": "Inter-fill gap classification",
            "desc": "Days between fills; therapy persistence; lapse detection",
            "col_triggers": [r"fill_date", r"rx_date", r"disp_date", r"fill_", r"refill", r"dispens"],
            "type_triggers": [],
            "kpi_keywords": ["persistence", "gap", "lapse", "days on therapy", "dot", "refill", "fill", "discontinuation", "adherence"],
            "preferred_output": ["SQL Pipeline", "Ad hoc"],
            "render": render_fill_gap,
        },
        {
            "id": "window_truncation",
            "name": "Window truncation audit",
            "desc": "Detect data lag in most recent period before reporting trends",
            "col_triggers": [r"_date$", r"_dt$", r"month", r"period"],
            "type_triggers": [],
            "kpi_keywords": ["trend", "refresh", "latest", "recent", "lag", "incomplete", "partial month", "truncat"],
            "preferred_output": ["SQL Pipeline", "Dashboard", "Report"],
            "render": render_window_truncation,
        },
        {
            "id": "full_outer",
            "name": "Full outer join — actuals vs goals",
            "desc": "Merge actuals and targets preserving all months from both sides",
            "col_triggers": [r"goal", r"target", r"quota", r"budget", r"plan"],
            "type_triggers": [],
            "kpi_keywords": ["goal", "target", "vs goal", "pct to goal", "attainment", "quota", "plan vs actual"],
            "preferred_output": ["SQL Pipeline", "Dashboard"],
            "render": render_full_outer,
        },
        {
            "id": "topn",
            "name": "Top-N ranking",
            "desc": "Rank entities by metric; overall and within group",
            "col_triggers": [r"amount", r"quantity", r"units", r"trx", r"rxs", r"scripts", r"spend", r"revenue"],
            "type_triggers": [r"numeric", r"integer", r"bigint"],
            "kpi_keywords": ["top", "rank", "decile", "highest", "lowest", "leaderboard", "opportunity", "priority"],
            "preferred_output": ["SQL Pipeline", "Dashboard", "Report"],
            "render": render_topn,
        },
    ]


# ── Pattern detection engine ──────────────────────────────────────────────────

def _detect_patterns(validated_tables, kpis, output_type):
    """
    Score each pattern against the validated table metadata + KPI strings.
    Returns list of (pattern_dict, score, matched_info) sorted by score desc.
    Includes patterns with score >= 1; capped at 6.
    """
    library = _pattern_library()

    # Flatten all column names and types across all validated tables
    all_cols = []
    cols_by_table = {}
    for tname, meta in validated_tables.items():
        if meta.get("ok"):
            cols_by_table[tname] = meta.get("columns", [])
            all_cols.extend(meta.get("columns", []))

    kpi_str = " ".join(
        (k.get("name","") + " " + k.get("definition","")).lower() if isinstance(k, dict)
        else str(k).lower()
        for k in kpis if k
    )
    output_lower = output_type.lower()

    scored = []
    for pattern in library:
        score = 0
        reasons = []

        # Col name triggers
        for trigger in pattern.get("col_triggers", []):
            rx = re.compile(trigger, re.IGNORECASE)
            matched = [c["name"] for c in all_cols if rx.search(c["name"])]
            if matched:
                score += 1
                reasons.append(f"col: {matched[0]}")
                break   # one col trigger match is enough for +1

        # Col type triggers
        for trigger in pattern.get("type_triggers", []):
            rx = re.compile(trigger, re.IGNORECASE)
            matched = [c["name"] for c in all_cols if rx.search(c.get("type", ""))]
            if matched:
                score += 1
                reasons.append(f"type: {trigger}")
                break

        # KPI keyword triggers
        for kw in pattern.get("kpi_keywords", []):
            if kw.lower() in kpi_str:
                score += 1
                reasons.append(f"kpi: '{kw}'")
                break

        # Output type preference — removed (all projects get full scaffold)

        if score >= 1:
            scored.append((pattern, score, reasons))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:6]


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

# ── Credential / session routes (Posit Connect multi-user support) ────────────

@app.route("/status")
def status():
    from flask import session as flask_session
    # Connected if: session credentials present (Posit mode)
    # OR env vars are set (local JupyterHub mode)
    env_present = all([
        os.getenv("REDSHIFT_HOST"),
        os.getenv("REDSHIFT_USER"),
        os.getenv("REDSHIFT_PASSWORD"),
    ])
    return jsonify({"connected": ("db" in flask_session) or bool(env_present)})


@app.route("/connect", methods=["POST"])
def connect():
    from flask import session as flask_session
    import psycopg2
    data = request.get_json(force=True)

    # Host, port, dbname are shared config — hardcoded defaults with env var override
    # Only username and password are user-specific
    host   = os.getenv("REDSHIFT_HOST",   "cwb-rs-cluster-prod.czywitd0zinp.us-east-2.redshift.amazonaws.com")
    port   = int(os.getenv("REDSHIFT_PORT", 5439))
    dbname = os.getenv("REDSHIFT_DBNAME", "bia_db")
    user   = data.get("user", "").strip()
    password = data.get("password", "")

    if not user or not password:
        return jsonify({"status": "error", "message": "Username and password are required."}), 400

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=10,
        )
        conn.close()
        flask_session["db"] = {
            "host":     host,
            "port":     port,
            "dbname":   dbname,
            "user":     user,
            "password": password,
        }
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route("/disconnect", methods=["POST"])
def disconnect():
    from flask import session as flask_session
    flask_session.pop("db", None)
    return jsonify({"status": "ok"})

@app.route("/")
def index():
    html_path = os.path.join(os.path.dirname(__file__), "fp_ui.html")
    with open(html_path) as f:
        return f.read()


@app.route("/load_session", methods=["POST"])
def load_session():
    """
    Accepts a session.json upload (multipart or raw JSON body).
    Returns the parsed session object with a change_hints array
    flagging fields the user likely wants to review on refresh.
    """
    try:
        if request.content_type and "multipart" in request.content_type:
            f = request.files.get("session")
            if not f:
                return jsonify({"error": "No file uploaded"}), 400
            raw = f.read().decode("utf-8")
        else:
            raw = request.get_data(as_text=True)
        session = json.loads(raw)
    except Exception as e:
        return jsonify({"error": f"Could not parse session.json: {str(e)}"}), 400

    # Infer change hints — fields commonly updated on refresh
    hints = []

    kpis = session.get("kpis", [])
    kpi_names = [k.get("name","") for k in kpis if isinstance(k, dict) and k.get("name","").strip()]
    if kpi_names:
        hints.append({
            "field": "kpis",
            "label": "KPI time windows",
            "detail": f"{len(kpi_names)} KPI(s) loaded — confirm time windows and caveats are still current.",
            "step": 4,
        })

    validated = session.get("validated_tables", {})
    ok_tables = [t for t, m in validated.items() if m.get("ok")]
    if ok_tables:
        hints.append({
            "field": "validated_tables",
            "label": "Table metadata",
            "detail": f"{len(ok_tables)} table(s) previously validated — re-validate if schema has changed.",
            "step": 3,
        })

    domains = session.get("domains", [])
    blank_context = [i+1 for i, d in enumerate(domains)
                     if not (
                         (isinstance(d.get("market_context"), dict) and any(d["market_context"].values())) or
                         (isinstance(d.get("market_context"), str) and d.get("market_context","").strip()) or
                         (isinstance(d.get("key_population"), dict) and any(d["key_population"].values())) or
                         (isinstance(d.get("key_population"), str) and d.get("key_population","").strip())
                     )]
    if blank_context:
        hints.append({
            "field": "domains",
            "label": "Domain context gaps",
            "detail": f"Domain block(s) {blank_context} have no key population or market context — worth filling in for richer CLAUDE.md.",
            "step": 2,
        })

    if not session.get("stakeholder_notes","").strip():
        hints.append({
            "field": "stakeholder_notes",
            "label": "Stakeholder notes",
            "detail": "No stakeholder notes captured — add them if this refresh has a new audience or communication context.",
            "step": 5,
        })

    return jsonify({"session": session, "hints": hints})


@app.route("/validate", methods=["POST"])
def validate():
    """
    Accepts: { "tables": ["schema.table", ...] }
    Returns column metadata + approx row counts per table.
    """
    body = request.get_json(force=True)
    raw_tables = body.get("tables", [])

    tables = []
    for t in raw_tables:
        t = t.strip()
        if t and "." in t:
            tables.append(t)
    tables = list(dict.fromkeys(tables))

    if not tables:
        return jsonify({"error": "No valid schema.table entries provided"}), 400

    results = {}
    try:
        conn = get_conn()
    except Exception as e:
        return jsonify({"error": f"Redshift connection failed: {str(e)}"}), 503

    with conn:
        for full_name in tables:
            schema, table = full_name.split(".", 1)
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT
                            a.attname                                        AS name,
                            pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
                            NOT a.attnotnull                                 AS nullable,
                            a.attisdistkey                                   AS distkey,
                            a.attsortkeyord                                  AS sortkey
                        FROM pg_catalog.pg_attribute a
                        JOIN pg_catalog.pg_class c     ON c.oid = a.attrelid
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = %s
                          AND c.relname = %s
                          AND a.attnum > 0
                          AND NOT a.attisdropped
                        ORDER BY a.attnum
                    """, (schema, table))
                    cols = [dict(r) for r in cur.fetchall()]

                    if not cols:
                        results[full_name] = {"ok": False, "error": "Table not found or no access"}
                        continue

                    cur.execute("""
                        SELECT COALESCE(reltuples::bigint, -1) AS approx_rows
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = %s AND c.relname = %s
                    """, (schema, table))
                    row = cur.fetchone()
                    approx = row["approx_rows"] if row else -1

                results[full_name] = {
                    "ok": True,
                    "row_count": approx,
                    "columns": [
                        {
                            "name": c["name"],
                            "type": c["type"],
                            "nullable": bool(c["nullable"]),
                            "distkey": bool(c["distkey"]),
                            "sortkey": int(c["sortkey"]),
                        }
                        for c in cols
                    ],
                }
            except Exception as e:
                log.exception("Error fetching metadata for %s", full_name)
                results[full_name] = {"ok": False, "error": str(e)}

    conn.close()
    return jsonify({"results": results})


@app.route("/generate", methods=["POST"])
def generate():
    """
    Full session → zip of scaffold files.
    """
    session = request.get_json(force=True)
    validated_tables = session.get("validated_tables", {})
    visual_output    = session.get("visual_output", False)
    # back-compat: old sessions used output_type string
    if not isinstance(visual_output, bool):
        visual_output = str(visual_output).lower() in ("true","1")
    _legacy_otype = session.get("output_type", "")
    if not visual_output and _legacy_otype in ("Dashboard", "Report"):
        visual_output = True
    output_type      = "Visual" if visual_output else "Data"
    kpis             = session.get("kpis", [])
    stakeholder_notes = session.get("stakeholder_notes", "")
    project_name     = session.get("project_name", "Unnamed Project")

    # Detect relevant patterns once — shared by SKILLS + EXAMPLES
    detected = _detect_patterns(validated_tables, kpis, output_type)

    files = {}
    files["CLAUDE.md"]          = _gen_claude_md(session, validated_tables, detected)
    files["schema_reference.md"] = _gen_schema_reference(validated_tables, session)
    files["kpi_definitions.md"] = _gen_kpi_definitions(kpis, session, validated_tables)
    files["EXAMPLES.md"]        = _gen_examples_md(session, validated_tables, detected)
    files["README.md"]          = _gen_readme(session)

    if visual_output:
        files["styling_guide.md"] = _gen_styling_guide(session)

    if stakeholder_notes.strip():
        files["stakeholder_notes.md"] = _gen_stakeholder_notes(session)

    files["session.json"] = json.dumps(session, indent=2, default=str)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname, content in files.items():
            zf.writestr(fname, content)
    buf.seek(0)

    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", project_name.lower())
    zip_name = f"file_primer_{safe_name}_{datetime.now().strftime('%Y%m%d')}.zip"

    return send_file(buf, mimetype="application/zip",
                     as_attachment=True, download_name=zip_name)


# ═══════════════════════════════════════════════════════════════════════════════
# FILE GENERATORS
# ═══════════════════════════════════════════════════════════════════════════════

def _gen_claude_md(s, validated_tables, detected):
    visual_output = s.get("visual_output", False) or s.get("output_type","") in ("Dashboard","Report")
    output_type   = "Visual" if visual_output else "Data"
    project_name = s.get("project_name", "Unnamed Project")
    description  = s.get("description", "")
    owner        = s.get("owner", "")
    audience     = s.get("audience", [])
    kpis         = s.get("kpis", [])
    today        = datetime.now().strftime("%B %d, %Y")

    # Domain context — multi-domain array (new); fall back to flat fields (legacy sessions)
    raw_domains = s.get("domains", [])
    if not raw_domains:
        raw_domains = [{
            "therapeutic_area": s.get("therapeutic_area", ""),
            "brand":            s.get("brand", ""),
            "indication":       s.get("indication", []),
            "indication_other": s.get("indication_other", ""),
            "data_sources":     s.get("data_sources", []),
            "data_sources_other": s.get("data_sources_other", ""),
            "key_population":   s.get("key_population", ""),
            "known_exclusions": s.get("known_exclusions", ""),
            "market_context":   "",
        }]
    domains     = [d for d in raw_domains if d.get("therapeutic_area") or d.get("brand") or d.get("market_context")]
    vocab       = s.get("vocab", [])
    all_data_sources = []
    for d in raw_domains:
        for ds in (d.get("data_sources") or []):
            if ds not in all_data_sources:
                all_data_sources.append(ds)
    data_sources = all_data_sources

    table_list   = "\n".join(f"- `{t}`" for t in validated_tables) or "- *(no tables validated)*"
    def _kname(k): return k.get("name","") if isinstance(k, dict) else str(k)
    kpi_block    = ("\n## KPIs in scope\n" + "\n".join(f"- {_kname(k)}" for k in kpis if _kname(k).strip())) if any(_kname(k).strip() for k in kpis) else ""
    audience_str = ", ".join(audience) if audience else "not specified"

    # Schema-inferred identifier and date columns
    all_cols = []
    for meta in validated_tables.values():
        if meta.get("ok"):
            all_cols.extend(meta.get("columns", []))
    id_col   = next((c["name"] for c in all_cols if re.search(r"(patient_id|hcp_id|npi|provider_id|member_id)", c["name"], re.I)), "entity_id")
    date_col = next((c["name"] for c in all_cols if re.search(r"(_date|_dt|fill_date|rx_date|svc_date)", c["name"], re.I)), "event_date")

    # File inventory
    file_rows = [
        ("schema_reference.md", "Column-level metadata for all validated tables"),
        ("kpi_definitions.md",  "Business definitions and SQL logic for each KPI"),
        ("EXAMPLES.md",         "Query stubs for each KPI — the primary SQL reference for this project"),
        ("README.md",           "Project context, data sources, known caveats"),
    ]
    if visual_output:
        file_rows.append(("styling_guide.md", "Lilly brand colors, chart conventions, typography"))
    if s.get("stakeholder_notes", "").strip():
        file_rows.append(("stakeholder_notes.md", "Stakeholder context and communication notes"))
    file_rows.append(("session.json", "Input state snapshot — use for refresh path"))
    file_table = "\n".join(f"| `{f}` | {d} |" for f, d in file_rows)

    # Multi-domain context section
    domain_blocks = []
    for idx, d in enumerate(domains):
        ta          = d.get("therapeutic_area", "")
        brand_d     = d.get("brand", "")
        indications       = d.get("indication", [])
        indication_other  = d.get("indication_other", "").strip()
        key_pop_dict      = d.get("key_population", {})
        exclusions        = d.get("known_exclusions", "")
        ds_list           = d.get("data_sources", [])
        ds_other          = d.get("data_sources_other", "").strip()
        market_ctx_dict   = d.get("market_context", {})
        disease_dict      = d.get("disease_state", {})
        drug_perf_dict    = d.get("drug_performance", {})
        additional_ctx    = d.get("additional_context", "").strip()

        # Back-compat: handle old flat string format from pre-update sessions
        def as_dict(val):
            if isinstance(val, dict): return val
            if isinstance(val, str) and val.strip(): return {"_custom": val}
            return {}

        key_pop_dict    = as_dict(key_pop_dict)
        market_ctx_dict = as_dict(market_ctx_dict)
        disease_dict    = as_dict(disease_dict)
        drug_perf_dict  = as_dict(drug_perf_dict)

        # Expand "Other" entries with free text if provided
        def expand_other(items, other_text):
            return [f"Other ({other_text})" if x == "Other" and other_text else x for x in items]

        indications_display = expand_other(indications, indication_other)
        ds_list_display     = expand_other(ds_list, ds_other)

        # Active indications for per-indication subsections (exclude Other)
        active_inds = [x for x in indications if x != "Other"]

        label   = " / ".join(filter(None, [ta, brand_d])) or f"Domain {idx+1}"
        heading = f"### {label}" if len(domains) > 1 else "## Domain context"

        parts = [heading, ""]
        if ta:                   parts.append(f"**Therapeutic area**: {ta}")
        if brand_d:              parts.append(f"**Brand / drug**: {brand_d}")
        if indications_display:  parts.append(f"**Indication(s) in scope**: {', '.join(indications_display)}")
        if ds_list_display:      parts.append(f"**Data sources**: {', '.join(ds_list_display)}")
        if exclusions:           parts.append(f"**Known exclusions**: {exclusions}")

        # Per-indication nested subsections
        if active_inds:
            for ind in active_inds:
                disease   = disease_dict.get(ind, "").strip()
                key_pop   = key_pop_dict.get(ind, "").strip()
                mkt_ctx   = market_ctx_dict.get(ind, "").strip()
                drug_perf = drug_perf_dict.get(ind, "").strip()

                if any([disease, key_pop, mkt_ctx, drug_perf]):
                    parts.append(f"\n#### {ind}")
                    if disease:   parts.append(f"\n**Disease state:**\n{disease}")
                    if key_pop:   parts.append(f"\n**Key population**: {key_pop}")
                    if mkt_ctx:   parts.append(f"\n**Market context:**\n{mkt_ctx}")
                    if drug_perf: parts.append(f"\n**Drug performance context:**\n{drug_perf}")
        else:
            # No indication selected — render any _custom freetext flat
            disease   = disease_dict.get("_custom", "").strip()
            key_pop   = key_pop_dict.get("_custom", "").strip()
            mkt_ctx   = market_ctx_dict.get("_custom", "").strip()
            drug_perf = drug_perf_dict.get("_custom", "").strip()
            if disease:   parts.append(f"\n**Disease state:**\n{disease}")
            if key_pop:   parts.append(f"\n**Key population**: {key_pop}")
            if mkt_ctx:   parts.append(f"\n**Market context:**\n{mkt_ctx}")
            if drug_perf: parts.append(f"\n**Drug performance context:**\n{drug_perf}")

        if additional_ctx:
            parts.append(f"\n**Additional context:**\n{additional_ctx}")

        domain_blocks.append("\n".join(parts))

    domain_section = ""
    if domain_blocks:
        header = "\n---\n\n## Domain context\n\n"
        domain_section = header + "\n\n---\n\n".join(domain_blocks) + "\n"

    # Business vocabulary table
    active_vocab = [v for v in vocab if isinstance(v, dict) and v.get("term","").strip()]
    vocab_section = ""
    if active_vocab:
        vocab_rows = "\n".join(f"| **{v['term']}** | {v.get('definition','')} |" for v in active_vocab)
        vocab_section = f"""
---

## Business vocabulary

> These are project-specific terms. Use exactly these definitions — do not interpret them differently.

| Term | Definition |
|------|-----------|
{vocab_rows}
"""

    # Data source context — what each source means
    ds_notes = {
        "LAAD":          "IQVIA Longitudinal Access and Adjudication Data — retail/mail Rx claims",
        "ELAAD":         "Extended LAAD — broader claim coverage including specialty pharmacy",
        "Flatiron":      "Real-world oncology EHR data — structured clinical + outcomes",
        "IQVIA MMIT":    "IQVIA MMIT — formulary and payer access data",
        "MarketScan":    "IBM MarketScan — commercial claims (medical + Rx)",
        "MiBA":          "MiBA — Lilly internal brand analytics platform",
        "Guardant 360":  "Guardant 360 — liquid biopsy / genomic testing data",
        "IQVIA Claims":  "IQVIA Xponent — prescriber-level Rx data",
        "Symphony Health": "Symphony Health PHAST — prescription tracking",
        "APLD":          "Anonymous Patient-Level Data — longitudinal patient claims",
    }
    ds_context = ""
    if data_sources:
        ds_lines = []
        for ds in data_sources:
            note = ds_notes.get(ds, "")
            ds_lines.append(f"- **{ds}**{': ' + note if note else ''}")
        ds_context = "\n### Data source context\n" + "\n".join(ds_lines) + "\n"

    return f"""---
name: {re.sub(r'[^a-z0-9-]', '-', project_name.lower())}-analyst
description: "Primary context file for {project_name}. Auto-loaded by Claude Code at session start."
---

# {project_name} — Claude context
*Generated by File Primer on {today}*

---

## Role & project
**Project**: {project_name}
**Visual deliverable**: {"Yes — styling_guide.md included" if visual_output else "No"}
**Owner / DRI**: {owner or "—"}
**Audience**: {audience_str}

{description}
{kpi_block}
{domain_section}
---

## Tools & stack

| Tool | Purpose |
|------|---------|
| **Amazon Redshift** | Primary data warehouse — all SQL runs here |
| **SQL** | Core language for extraction, transformation, analysis |
| **Tableau** | Visual analytics and interactive dashboards |
| **JupyterHub** | Exploratory analysis and pipeline development |
| **Claude Code** | AI assistant via terminal / MCP |

---

## Data environment

### Platform
- **Database**: Amazon Redshift (PostgreSQL-compatible)
- **SQL dialect**: Redshift SQL — always use Redshift-specific syntax
{ds_context}
### Tables in scope
{table_list}

### Redshift syntax rules
- Use `LISTAGG` — not `STRING_AGG`
- Use `DATEADD`, `DATEDIFF`, `DATE_TRUNC` for all date operations
- Use `NVL()` or `COALESCE()` for null handling
- Window frame: `ROWS BETWEEN N PRECEDING AND CURRENT ROW` only — `RANGE BETWEEN` with date intervals not supported
- Use `APPROXIMATE COUNT(DISTINCT col)` for large cardinality estimates
- No `TOP N` — use `LIMIT`
- No `CROSS JOIN LATERAL` — use subqueries or temp tables

---

## SQL conventions (Lilly BI&A standard)

- **Persistent temp tables over CTEs** for multi-step logic — one logical step per table
- Naming: `temp_[project]_[step]` (e.g. `temp_{re.sub(r'[^a-z0-9]', '_', project_name.lower())[:20]}_base`)
- Always drop before creating: `DROP TABLE IF EXISTS temp_...;`
- Cast both sides of a `UNION` to matching types explicitly
- Use `PERCENTILE_CONT` carefully — limited window contexts in Redshift
- **Never `SELECT *`** in production — always specify columns

### Standard temp table pattern
```sql
DROP TABLE IF EXISTS temp_{re.sub(r'[^a-z0-9]', '_', project_name.lower())[:15]}_step1;
CREATE TABLE temp_{re.sub(r'[^a-z0-9]', '_', project_name.lower())[:15]}_step1 AS
SELECT
    {id_col},
    {date_col},
    -- ... specify all needed columns
FROM {next(iter(validated_tables), "schema.table_name")}
WHERE <filter_conditions>;

-- Always verify row counts before proceeding
-- SELECT COUNT(*), COUNT(DISTINCT {id_col}) FROM temp_...;
```
{vocab_section}
---

## Terms I use → what I mean

| Term | What it means in this project |
|------|-------------------------------|
| "Rolling N months" | `ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW` window frame |
| "Dedupe" | `ROW_NUMBER() OVER (PARTITION BY {id_col} ORDER BY {date_col} DESC)` keep rn=1 |
| "Cohort" | Group by first event date (`MIN({date_col})`) |
| "Funnel" | Sequential step flags multiplied to enforce ordering |
| "Flag / Tag" | `CASE WHEN` returning 1/0 or a label string |
| "TUA" | Trialist / User / Adopter — cumulative ever-flag classification |
| "SoM" | Share of Market — mutually exclusive segment via priority CASE waterfall |
| "Fan-out" | Row multiplication from a join — detect with COUNT(*) vs COUNT(DISTINCT {id_col}) |
| "Window truncation" | Most recent period has incomplete data — check before attributing trend changes |
| "YoY" | Same calendar period, current year vs prior year |
| "Date spine" | Continuous month/week sequence for gap-free time-series joins |
| "Index date" | First qualifying event date — anchor for cohort and DOT calculations |

---

## Relevant patterns for this project

---

## What to avoid
- Do **not** use CTEs for multi-step logic — use persistent temp tables
- Do **not** use MySQL syntax (`LIMIT x, y`, backtick identifiers)
- Do **not** use `STRING_AGG` — use `LISTAGG`
- Do **not** use `TOP N` — use `LIMIT`
- Do **not** `SELECT *` in production SQL
- Do **not** rename or invent column names — use exactly what `schema_reference.md` specifies
- Do **not** report the most recent month as complete without a window truncation check
- Do **not** join tables without checking for fan-out first
        {chr(10).join(f"- Do **not** include: {d.get('known_exclusions','')}" for d in domains if d.get('known_exclusions','').strip())}

---

## How I want Claude to respond

### SQL
- Write clean SQL with 4-space indentation
- Add `-- comment` for non-obvious logic
- Give a 1–2 sentence explanation before the query: what it does, why this approach
- If multiple approaches exist, show the recommended one and briefly note the alternative
- Flag Redshift limitations relevant to the query
- Use exact column names from `schema_reference.md` — never invent them

### Standard query format
```sql
-- Brief description: what this query does and why this approach

DROP TABLE IF EXISTS temp_{re.sub(r'[^a-z0-9]', '_', project_name.lower())[:15]}_step;
CREATE TABLE temp_{re.sub(r'[^a-z0-9]', '_', project_name.lower())[:15]}_step AS
SELECT
    {id_col},                                         -- entity key
    DATE_TRUNC('month', {date_col})  AS month,        -- reporting period
    COUNT(DISTINCT {id_col})         AS metric_value  -- the measure
FROM {next(iter(validated_tables), "schema.table_name")}
WHERE {date_col} >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))
  AND <additional_filters>
GROUP BY 1, 2
ORDER BY 1, 2;

-- Always validate before proceeding to next step
-- SELECT COUNT(*), COUNT(DISTINCT {id_col}) FROM temp_...;
```

### Problem solving
- Translate the business question into SQL logic first, then write code
- Break complex problems into named steps before writing the first temp table
- Validate intermediate row counts at each step
- If something looks wrong in the output, check for fan-out and window truncation before concluding it's a data issue

---

## File inventory

| File | Purpose |
|------|---------|
{file_table}

---

*Generated by File Primer — edit as tables, KPIs, or workflow change.*
"""
    project_name = s.get("project_name", "Unnamed Project")
    description  = s.get("description", "")
    owner        = s.get("owner", "")
    audience     = s.get("audience", [])
    kpis         = s.get("kpis", [])
    today        = datetime.now().strftime("%B %d, %Y")

    table_list   = "\n".join(f"- `{t}`" for t in validated_tables) or "- *(no tables validated)*"
    def _kname(k): return k.get("name","") if isinstance(k, dict) else str(k)
    kpi_block    = ("\n## KPIs in scope\n" + "\n".join(f"- {_kname(k)}" for k in kpis if _kname(k).strip())) if any(_kname(k).strip() for k in kpis) else ""
    audience_str = ", ".join(audience) if audience else "not specified"

    # Schema-inferred identifier columns for the "terms I use" table
    all_cols = []
    for meta in validated_tables.values():
        if meta.get("ok"):
            all_cols.extend(meta.get("columns", []))
    id_col   = next((c["name"] for c in all_cols if re.search(r"(patient_id|hcp_id|npi|provider_id|member_id)", c["name"], re.I)), "entity_id")
    date_col = next((c["name"] for c in all_cols if re.search(r"(_date|_dt|fill_date|rx_date|svc_date)", c["name"], re.I)), "event_date")

    # File inventory — dynamic based on output type and notes
    file_rows = [
        ("schema_reference.md", "Column-level metadata for all validated tables"),
        ("kpi_definitions.md",  "Business definitions and SQL logic for each KPI"),
        ("EXAMPLES.md",         "Query stubs for each KPI — the primary SQL reference for this project"),
        ("README.md",           "Project context, data sources, known caveats"),
    ]
    if visual_output:
        file_rows.append(("styling_guide.md", "Lilly brand colors, chart conventions, typography"))
    if s.get("stakeholder_notes", "").strip():
        file_rows.append(("stakeholder_notes.md", "Stakeholder context and communication notes"))
    file_rows.append(("session.json", "Input state snapshot — use for refresh path"))
    file_table = "\n".join(f"| `{f}` | {d} |" for f, d in file_rows)

    return f"""---
name: {re.sub(r'[^a-z0-9-]', '-', project_name.lower())}-analyst
description: "Primary context file for {project_name}. Auto-loaded by Claude Code at session start."
---

# {project_name} — Claude context
*Generated by File Primer on {today}*

---

## Role & project
**Project**: {project_name}  
**Visual deliverable**: {"Yes — styling_guide.md included" if visual_output else "No"}  
**Owner / DRI**: {owner or "—"}  
**Audience**: {audience_str}  

{description}
{kpi_block}

---

## Tools & stack

| Tool | Purpose |
|------|---------|
| **Amazon Redshift** | Primary data warehouse — all SQL runs here |
| **SQL** | Core language for extraction, transformation, analysis |
| **Tableau** | Visual analytics and interactive dashboards |
| **JupyterHub** | Exploratory analysis and pipeline development |
| **Claude Code** | AI assistant via terminal / MCP |

---

## Data environment

### Platform
- **Database**: Amazon Redshift (PostgreSQL-compatible)
- **SQL dialect**: Redshift SQL — always use Redshift-specific syntax

### Tables in scope
{table_list}

### Redshift syntax rules
- Use `LISTAGG` — not `STRING_AGG`
- Use `DATEADD`, `DATEDIFF`, `DATE_TRUNC` for all date operations
- Use `NVL()` or `COALESCE()` for null handling
- Window frame: `ROWS BETWEEN N PRECEDING AND CURRENT ROW` only — `RANGE BETWEEN` with date intervals not supported
- Use `APPROXIMATE COUNT(DISTINCT col)` for large cardinality estimates
- No `TOP N` — use `LIMIT`
- No `CROSS JOIN LATERAL` — use subqueries or temp tables

---

## SQL conventions (Lilly BI&A standard)

- **Persistent temp tables over CTEs** for multi-step logic — one logical step per table
- Naming: `temp_[project]_[step]` (e.g. `temp_{re.sub(r'[^a-z0-9]', '_', project_name.lower())[:20]}_base`)
- Always drop before creating: `DROP TABLE IF EXISTS temp_...;`
- Cast both sides of a `UNION` to matching types explicitly
- Use `PERCENTILE_CONT` carefully — limited window contexts in Redshift
- **Never `SELECT *`** in production — always specify columns

### Standard temp table pattern
```sql
DROP TABLE IF EXISTS temp_{re.sub(r'[^a-z0-9]', '_', project_name.lower())[:15]}_step1;
CREATE TABLE temp_{re.sub(r'[^a-z0-9]', '_', project_name.lower())[:15]}_step1 AS
SELECT
    {id_col},
    {date_col},
    -- ... specify all needed columns
FROM {next(iter(validated_tables), "schema.table_name")}
WHERE <filter_conditions>;

-- Always verify row counts before proceeding
-- SELECT COUNT(*), COUNT(DISTINCT {id_col}) FROM temp_...;
```

---

## Terms I use → what I mean

| Term | What it means in this project |
|------|-------------------------------|
| "Rolling N months" | `ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW` window frame |
| "Dedupe" | `ROW_NUMBER() OVER (PARTITION BY {id_col} ORDER BY {date_col} DESC)` keep rn=1 |
| "Cohort" | Group by first event date (`MIN({date_col})`) |
| "Funnel" | Sequential step flags multiplied to enforce ordering |
| "Flag / Tag" | `CASE WHEN` returning 1/0 or a label string |
| "TUA" | Trialist / User / Adopter — cumulative ever-flag classification |
| "SoM" | Share of Market — mutually exclusive segment via priority CASE waterfall |
| "Fan-out" | Row multiplication from a join — detect with COUNT(*) vs COUNT(DISTINCT {id_col}) |
| "Window truncation" | Most recent period has incomplete data — check before attributing trend changes |
| "YoY" | Same calendar period, current year vs prior year |
| "Date spine" | Continuous month/week sequence for gap-free time-series joins |
| "Index date" | First qualifying event date — anchor for cohort and DOT calculations |

---

## Relevant patterns for this project

---

## What to avoid
- Do **not** use CTEs for multi-step logic — use persistent temp tables
- Do **not** use MySQL syntax (`LIMIT x, y`, backtick identifiers)
- Do **not** use `STRING_AGG` — use `LISTAGG`
- Do **not** use `TOP N` — use `LIMIT`
- Do **not** `SELECT *` in production SQL
- Do **not** rename or invent column names — use exactly what `schema_reference.md` specifies
- Do **not** report the most recent month as complete without a window truncation check
- Do **not** join tables without checking for fan-out first

---

## How I want Claude to respond

### SQL
- Write clean SQL with 4-space indentation
- Add `-- comment` for non-obvious logic
- Give a 1–2 sentence explanation before the query: what it does, why this approach
- If multiple approaches exist, show the recommended one and briefly note the alternative
- Flag Redshift limitations relevant to the query
- Use exact column names from `schema_reference.md` — never invent them

### Standard query format
```sql
-- Brief description: what this query does and why this approach

DROP TABLE IF EXISTS temp_{re.sub(r'[^a-z0-9]', '_', project_name.lower())[:15]}_step;
CREATE TABLE temp_{re.sub(r'[^a-z0-9]', '_', project_name.lower())[:15]}_step AS
SELECT
    {id_col},                                         -- entity key
    DATE_TRUNC('month', {date_col})  AS month,        -- reporting period
    COUNT(DISTINCT {id_col})         AS metric_value  -- the measure
FROM {next(iter(validated_tables), "schema.table_name")}
WHERE {date_col} >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))
  AND <additional_filters>
GROUP BY 1, 2
ORDER BY 1, 2;

-- Always validate before proceeding to next step
-- SELECT COUNT(*), COUNT(DISTINCT {id_col}) FROM temp_...;
```

### Problem solving
- Translate the business question into SQL logic first, then write code
- Break complex problems into named steps before writing the first temp table
- Validate intermediate row counts at each step
- If something looks wrong in the output, check for fan-out and window truncation before concluding it's a data issue

---

## File inventory

| File | Purpose |
|------|---------|
{file_table}

---

*Generated by File Primer — edit as tables, KPIs, or workflow change.*
"""


def _gen_skills_md(s, validated_tables, detected):
    """
    Generates a lean SKILLS.md — gotcha/guidance reference per pattern.
    SQL lives in EXAMPLES.md; SKILLS.md tells Claude *when*, *why*, and *what to watch out for*.
    Cross-references EXAMPLES.md for every wired instance.
    """
    today        = datetime.now().strftime("%B %d, %Y")
    project_name = s.get("project_name", "Unnamed Project")
    kpis         = s.get("kpis", [])

    if not detected:
        return f"""# SKILLS.md — {project_name}
*Generated by File Primer on {today}*

> No patterns detected. Validate tables in File Primer and add KPIs to enable pattern detection.
"""

    # Build per-table col lists (used for column-level gotcha substitution)
    cols_by_table = {}
    for tname, meta in validated_tables.items():
        if meta.get("ok"):
            cols_by_table[tname] = meta.get("columns", [])

    tables_list  = list(cols_by_table.keys())
    primary      = tables_list[0] if tables_list else "schema.table_name"
    primary_cols = cols_by_table.get(primary, [])

    def first_col_matching(cols, pattern):
        rx = re.compile(pattern, re.IGNORECASE)
        for c in cols:
            if rx.search(c["name"]):
                return c["name"]
        return None

    id_col   = first_col_matching(primary_cols, r"(patient_id|hcp_id|npi|provider_id|member_id|customer_id)") or "entity_id"
    date_col = first_col_matching(primary_cols, r"(_date$|_dt$|fill_date|rx_date|svc_date|disp_date)") or "event_date"

    # Map each detected pattern to KPI names that reference it (for cross-links)
    def kpis_for_pattern(pattern_id):
        matches = []
        for k in kpis:
            if not isinstance(k, dict): continue
            kname = (k.get("name") or "").strip()
            if not kname: continue
            kstr = (kname + " " + (k.get("definition") or "")).lower()
            for kw in next((p["kpi_keywords"] for p in _pattern_library() if p["id"] == pattern_id), []):
                if kw.lower() in kstr:
                    matches.append(kname)
                    break
        return matches

    # Per-pattern gotcha + guidance content (no SQL — that lives in EXAMPLES.md)
    PATTERN_GUIDANCE = {
        "deduplication": {
            "when": "Any table that has one row per event and you need one row per entity — patient, HCP, fill. Required before almost every join.",
            "why": "Without deduplication, joins fan out silently. A single patient with 3 fills joined to a 2-row HCP table becomes 6 rows. Row counts look right; metrics are wrong.",
            "gotchas": [
                f"`ROW_NUMBER() OVER (PARTITION BY {id_col} ORDER BY {date_col} DESC)` — most reliable on Redshift. `DISTINCT ON` is Postgres syntax and not supported.",
                "Always validate after: `SELECT COUNT(*), COUNT(DISTINCT {id_col}) FROM temp_dedup` — these must match.",
                "Fan-out after joins is silent. Run the two-count check after every join, not just after dedup.",
                "If you're deduplicating to latest record, confirm which date column represents 'latest' — fill date vs claim date vs record update date can differ.",
            ],
            "redshift_note": "Use persistent temp tables (`DROP TABLE IF EXISTS; CREATE TABLE temp_... AS SELECT`) — never `WITH` CTEs on large tables due to WLM abort risk.",
        },
        "date_spine": {
            "when": "Time-series analysis where gaps in dates would silently drop months from output. Any trend chart or period-over-period metric.",
            "why": "If a brand had zero fills in one month, that month simply won't appear in a GROUP BY — making the trend line skip rather than show zero. The spine forces every month to appear.",
            "gotchas": [
                "Generate the spine from a fixed anchor date, not `MIN(date)` from the data — the data's minimum may shift between refreshes.",
                "Join metrics onto the spine with LEFT JOIN, not INNER JOIN — the whole point is to preserve months with no data.",
                "Use `DATE_TRUNC('month', ...)` consistently on both spine and metric table before joining.",
                f"After joining: `COALESCE(metric_value, 0)` — nulls from months with no activity should be zero, not null.",
            ],
            "redshift_note": "Generate spine rows using `ROW_NUMBER() OVER (ORDER BY 1) - 1` offset pattern — Redshift does not support `generate_series()`.",
        },
        "rolling_window": {
            "when": "Smoothing noisy monthly metrics, computing MAT (moving annual total), or inter-fill gap windows. NBRx rolling 3M or 12M.",
            "why": "Monthly metrics are noisy. A rolling window stabilises trend without losing granularity — better for stakeholder charts than raw monthly bars.",
            "gotchas": [
                "Redshift does NOT support `RANGE BETWEEN INTERVAL '3 months' PRECEDING`. Only `ROWS BETWEEN N PRECEDING AND CURRENT ROW` works.",
                "Pre-aggregate to monthly grain BEFORE applying the window function — applying a rolling window to row-level data gives wrong results.",
                f"Partition by `{id_col}` if computing per-entity rolling metrics; omit partition for portfolio-level rollups.",
                "Rolling window at the start of history is truncated (only 1–2 months of data in the window). Flag or exclude these from stakeholder output.",
            ],
            "redshift_note": "Pre-aggregate to target grain first, store as a persistent temp table, then apply the window function in a second step.",
        },
        "yoy": {
            "when": "Quarterly business reviews, brand performance decks, period-over-period growth reporting.",
            "why": "Absolute counts are context-free. YoY frames performance against a baseline the audience already holds in their head.",
            "gotchas": [
                "Most-recent-month is almost always truncated — never include it in YoY comparisons without a truncation check first.",
                "Align comparison periods: rolling 12M vs rolling 12M is cleaner than calendar year vs calendar year when the brand launched mid-year.",
                "Watch for launch year bias: if the brand launched in month 6 of prior year, CY count will outperform PY by construction.",
                "Use `EXTRACT(year FROM date_col)` not `DATE_TRUNC('year', ...)` when filtering — both work but `EXTRACT` is clearer in CASE statements.",
            ],
            "redshift_note": None,
        },
        "cohort": {
            "when": "Retention, persistence, or engagement tracked from a patient's or HCP's first event. First Rx as cohort anchor, refill behaviour by month offset.",
            "why": "Aggregated persistence metrics hide cohort effects. A brand launched 18 months ago will show worse 12M persistence than one launched 36 months ago, purely because early cohorts haven't had time to mature.",
            "gotchas": [
                f"Anchor on `MIN({date_col})` per `{id_col}` — store this as a persistent temp table before joining back to event history.",
                "Month offset = `DATEDIFF('month', first_event_date, activity_date)` — not calendar month subtraction.",
                "Cohort sizes shrink at longer offsets (only older cohorts contribute) — always show N alongside retention rates.",
                "If index date changes definition between refreshes (e.g. first fill vs first claim), cohort membership shifts. Document which definition is in use.",
            ],
            "redshift_note": None,
        },
        "funnel": {
            "when": "Patient journey funnels (identified → diagnosed → prescribed → filled → refilled), HCP targeting funnels (universe → segmented → called → written).",
            "why": "Funnel analysis surfaces where drop-off is largest — the biggest opportunity is rarely where stakeholders assume.",
            "gotchas": [
                "Enforce sequential ordering by multiplying step flags — step 3 flag should only be 1 if step 2 is also 1.",
                "LEFT JOIN from the widest universe table down — don't start from a filtered table or you'll undercount the top of funnel.",
                f"Always report N at each step alongside conversion rate — a 50% conversion from 10 patients is not the same as 50% from 10,000.",
                "Funnel is point-in-time unless specified otherwise. Document the as-of date in the query header.",
            ],
            "redshift_note": "Build step flags as a single wide persistent temp table — one row per entity, one column per step. Avoids repeated subquery scans.",
        },
        "waterfall": {
            "when": "Assigning patients or HCPs to mutually exclusive segments: share of market buckets, TUA classification, patient eligibility tiers.",
            "why": "Without a priority waterfall, an entity can qualify for multiple segments simultaneously. Overlap makes segment counts sum to more than the universe and is undetectable without explicit mutual exclusivity logic.",
            "gotchas": [
                "Priority order in the CASE statement determines segment assignment — document it explicitly; it is an analytical decision, not a technical one.",
                "Keep audit flags alongside the segment column (`flag_tier_1`, `flag_tier_2`, ...) so QA can verify the waterfall logic independently.",
                "Verify mutual exclusivity: `SELECT segment, COUNT(*) FROM temp_segmented GROUP BY 1` — every entity should appear in exactly one bucket.",
                "ELSE clause should be an explicit label ('Other', 'Unclassified') never NULL — null segments disappear silently in downstream GROUP BYs.",
            ],
            "redshift_note": None,
        },
        "ever_flag": {
            "when": "TUA (Trialist/User/Adopter) classification, ever-prescribed, ever-diagnosed flags, net new HCP tracking. Once an entity crosses a threshold it stays at that level.",
            "why": "TUA is cumulative by definition — a patient who wrote 4 scripts then stopped is still an Adopter. Point-in-time counts miss this; ever-flags capture it correctly.",
            "gotchas": [
                f"Cumulate BEFORE joining to other tables — fan-out from joins can inflate `COUNT(DISTINCT {date_col})` and push entities into higher TUA tiers incorrectly.",
                f"Use `COUNT(DISTINCT {date_col}::DATE)` not `COUNT(*)` — same-day duplicate records inflate event counts.",
                "lb=98 (inception-to-date) is the standard for cumulative TUA goal tracking views at Lilly BI&A — confirm lb_months with the analytics partner.",
                "Once-ever flags are monotonically non-decreasing — if a refresh shows a patient downgraded from Adopter to User, it is a data or logic error, not real behaviour.",
            ],
            "redshift_note": "Store cumulated event counts as a persistent temp table before the CASE classification step — do not nest the COUNT inside the CASE.",
        },
        "fill_gap": {
            "when": "Therapy persistence, gap analysis, days-on-therapy (DOT), patient adherence metrics. Classifies each fill interval as continuous, short gap, or lapse.",
            "why": "Persistence is not binary. A patient who refills every 60 days on a 30-day supply is lapsing; one on a 90-day supply is continuous. Gap classification makes this explicit.",
            "gotchas": [
                f"`LAG({date_col}) OVER (PARTITION BY {id_col} ORDER BY {date_col})` — always partition and order explicitly; Redshift does not guarantee row order without ORDER BY.",
                "Gap thresholds (e.g. ≤45 days = continuous) should match the drug's days-supply assumptions — align with the analytics partner before hardcoding.",
                "First fill has no prior fill — `days_since_prior_fill` is NULL for fill_number = 1. Handle with CASE or COALESCE.",
                "Multiple fills on the same date (duplicate records) will produce a gap of 0 days — dedup before running gap analysis.",
            ],
            "redshift_note": None,
        },
        "window_truncation": {
            "when": "Any period-over-period refresh, especially before presenting trend improvements to stakeholders. The most recent month in any claims-based dataset is almost always incomplete.",
            "why": "Claims data lags by 4–8 weeks. The most recent month looks better (lower counts) not because of real improvement but because claims haven't finished adjudicating. Presenting truncated data as a positive trend is a common and costly error.",
            "gotchas": [
                "Run this check BEFORE any trend analysis — not after. It should be the first query in every refresh.",
                "If the most recent month is < 80% of the prior month's row density, flag it as incomplete and exclude from the trend narrative.",
                "Stakeholder communication standard: 'Most recent month subject to data lag — reporting through [prior month] for this analysis.'",
                "Different data sources have different lag profiles: LAAD ~4–6 weeks, IQVIA MMIT ~2–4 weeks, Flatiron ~6–8 weeks. Know your source.",
            ],
            "redshift_note": None,
        },
        "full_outer": {
            "when": "Combining actuals with goals/targets when either side may have months the other doesn't. Goal tables often skip months or HCPs with zero activity.",
            "why": "INNER JOIN on actuals × goals silently drops months where either side has no row. FULL OUTER JOIN + COALESCE preserves all months and fills nulls with zero.",
            "gotchas": [
                "UNION type casting: if actuals use `INTEGER` and goals use `BIGINT`, the join key comparison will fail or produce unexpected results — cast both sides explicitly before joining.",
                "Goal table month format at Lilly: month values may encode the month number in the day position after `::date` cast — use a CASE-based remapping if month = Jan but day shows as '01' through '12'.",
                "COALESCE on the join key: `COALESCE(a.month, g.month)` — required because one side may be null for a given row.",
                "Validate: row count of the FULL OUTER JOIN result should be >= MAX(actuals rows, goals rows) and <= (actuals rows + goals rows).",
            ],
            "redshift_note": "Cast both sides of the join key to the same type before joining. Implicit casting in Redshift is less forgiving than in standard SQL.",
        },
        "topn": {
            "when": "HCP opportunity ranking, patient cost stratification, territory leaderboards, BTK decile scoring.",
            "why": "Ranking surfaces prioritisation signal. A sorted list of all 10,000 HCPs is not actionable; the top 100 by weighted opportunity score is.",
            "gotchas": [
                "`DENSE_RANK()` shares rank positions for ties (1, 1, 3); `ROW_NUMBER()` breaks ties arbitrarily (1, 2, 3). Use DENSE_RANK for leaderboards, ROW_NUMBER when you need exactly N rows.",
                "Decile scoring: `NTILE(10) OVER (ORDER BY metric DESC)` — decile 1 = highest. Confirm direction with stakeholder before labelling.",
                "Multi-metric composite scoring: normalise each metric to 0–1 range before weighting and summing — raw counts on different scales will dominate the composite.",
                "Rank stability: if two HCPs have identical scores, their rank order is non-deterministic across refreshes. Add a tiebreaker (e.g. NPI) to the ORDER BY.",
            ],
            "redshift_note": None,
        },
    }

    # Skill index table
    index_rows = "\n".join(
        f"| [{p['name']}](#{p['id']}) | {p['desc']} | {', '.join(reasons[:2])} |"
        for p, score, reasons in detected
    )

    # Skill blocks
    blocks = []
    for pattern, score, reasons in detected:
        pid    = pattern["id"]
        pname  = pattern["name"]
        pdesc  = pattern["desc"]
        g      = PATTERN_GUIDANCE.get(pid, {})

        when        = g.get("when", "*See pattern description.*")
        why         = g.get("why", "")
        gotchas     = g.get("gotchas", [])
        rs_note     = g.get("redshift_note")
        linked_kpis = kpis_for_pattern(pid)

        gotcha_lines = "\n".join(f"- {gt}" for gt in gotchas) if gotchas else "- *No specific gotchas recorded.*"
        rs_block     = f"\n**Redshift note:** {rs_note}\n" if rs_note else ""
        kpi_links    = (
            "\n**Wired instances in this project:** " +
            " · ".join(f"`EXAMPLES.md` → Example {i+1} ({kn})" for i, kn in enumerate(linked_kpis))
            if linked_kpis else
            "\n*No KPIs in this project matched to this pattern — see `EXAMPLES.md` for all query stubs.*"
        )
        reason_str   = ", ".join(reasons)

        blocks.append(f"""---

## {pname} {{#{pid}}}

*{pdesc}*  
*Detected because: {reason_str}*

**Use when:** {when}

**Why it matters:** {why}

**Watch out for:**
{gotcha_lines}
{rs_block}{kpi_links}

""")

    proj_slug = re.sub(r'[^a-z0-9-]', '-', project_name.lower())
    return f"""---
name: {proj_slug}-skills
description: "SQL pattern guidance for {project_name} — when to use each pattern, Redshift gotchas, and cross-references to EXAMPLES.md."
---

# SKILLS.md — {project_name}
*Generated by File Primer on {today}*

> **How to use this file**  
> `SKILLS.md` is a *guidance* reference — it tells Claude (and you) **when** to reach for a pattern,  
> **why** it matters, and **what to watch out for** on Redshift.  
> The actual SQL for this project lives in **`EXAMPLES.md`** — wired to your real tables and KPIs.  
> Start with `EXAMPLES.md`; come here when something doesn't look right or you need to understand the pattern shape.

---

## Pattern index

| Pattern | What it solves | Why it was detected |
|---------|---------------|---------------------|
{index_rows}

{''.join(blocks)}
---

*To add a new pattern to the File Primer library for all future projects, update `_pattern_library()` in `app.py`.*
"""


def _gen_examples_md(s, validated_tables, detected):
    """
    Generates EXAMPLES.md — one worked stub per KPI, using real table names.
    SQL logic source (playbook / paste / describe) feeds real content instead of stubs.
    """
    today        = datetime.now().strftime("%B %d, %Y")
    project_name = s.get("project_name", "Unnamed Project")
    kpis_raw     = s.get("kpis", [])
    cols_by_table = {}
    for tname, meta in validated_tables.items():
        if meta.get("ok"):
            cols_by_table[tname] = meta.get("columns", [])

    tables_list  = list(cols_by_table.keys())
    primary      = tables_list[0] if tables_list else "schema.table_name"
    primary_cols = cols_by_table.get(primary, [])

    def first_id(cols):
        return next((c["name"] for c in cols if re.search(
            r"(patient_id|hcp_id|npi|provider_id|member_id|customer_id)", c["name"], re.I
        )), "entity_id")

    def first_date(cols):
        return next((c["name"] for c in cols if re.search(
            r"(_date$|_dt$|fill_date|rx_date|svc_date|service_date|disp_date)", c["name"], re.I
        )), "event_date")

    id_col   = first_id(primary_cols)
    date_col = first_date(primary_cols)

    PLAYBOOK_LABELS = {
        "nbrx_rolling":     "NBRx rolling window",
        "tua":              "TUA classification",
        "som_waterfall":    "SoM priority waterfall",
        "btk_decile":       "BTK decile calculation",
        "hcp_funnel":       "HCP opportunity funnel",
        "cross_ta_overlap": "Cross-TA patient overlap",
        "moa":              "Mode of administration",
        "account_seg":      "Account segmentation",
        "dosing":           "Dosing schedule",
        "net_new":          "Net new HCPs",
        "concordance":      "Concordance by regimen",
        "pop_growth":       "Period-over-period growth",
    }

    def best_pattern_for_kpi(kpi_str):
        kpi_lower = kpi_str.lower()
        for pattern, score, _ in detected:
            for kw in pattern.get("kpi_keywords", []):
                if kw.lower() in kpi_lower:
                    return pattern
        return detected[0][0] if detected else None

    def sql_block_for_kpi(k):
        if not isinstance(k, dict):
            return None
        mode      = k.get("sql_source_mode", "")
        kname     = k.get("name", "this KPI")
        proj_slug = re.sub(r'[^a-z0-9]', '_', project_name.lower())[:12]
        kslug     = re.sub(r'[^a-z0-9]', '_', kname.lower())[:25]

        if mode == "paste":
            sql = (k.get("sql_pasted") or "").strip()
            if sql:
                return (
                    f"```sql\n"
                    f"-- {kname} — pasted SQL (exact logic preserved)\n"
                    f"-- grain: confirm before use — see kpi_definitions.md\n"
                    f"-- dependency: standalone unless noted above\n"
                    f"{sql}\n"
                    f"```"
                )

        if mode == "playbook":
            entry = k.get("sql_playbook_entry", "")
            label = PLAYBOOK_LABELS.get(entry, entry)
            if entry:
                return (
                    f"```sql\n"
                    f"-- {kname} — based on playbook: {label}\n"
                    f"-- Adapt the pattern below to this KPI's specific filters and thresholds.\n"
                    f"-- grain: one row per {id_col} × month (adjust per objective)\n"
                    f"-- threshold/config: see kpi_definitions.md for project-specific values\n\n"
                    f"DROP TABLE IF EXISTS temp_{proj_slug}_{kslug};\n"
                    f"CREATE TABLE temp_{proj_slug}_{kslug} AS\n"
                    f"SELECT\n"
                    f"    {id_col},\n"
                    f"    DATE_TRUNC('month', {date_col})   AS month,\n"
                    f"    -- TODO: apply {label} pattern logic here\n"
                    f"    COUNT(DISTINCT {id_col})           AS {kslug}\n"
                    f"FROM {primary}\n"
                    f"WHERE {date_col} >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))\n"
                    f"GROUP BY 1, 2;\n"
                    f"```"
                )

        if mode == "describe":
            rules      = k.get("sql_rules") or {}
            cols       = rules.get("columns", "")
            biz_rules  = rules.get("business_rules", "")
            exclusions = rules.get("exclusions", "")
            edge_cases = rules.get("edge_cases", "")
            parts = [f"-- {kname} — logic described by analyst"]
            parts.append(f"-- grain: one row per {id_col} × month (adjust per objective)")
            parts.append(f"-- dependency: standalone unless noted above")
            if cols:       parts.append(f"-- Columns used     : {cols}")
            if biz_rules:  parts.append(f"-- Business rules   : {biz_rules}")
            if exclusions: parts.append(f"-- Exclusions       : {exclusions}")
            if edge_cases: parts.append(f"-- Known edge cases : {edge_cases}")
            comment_block = "\n".join(parts)
            return (
                f"```sql\n"
                f"{comment_block}\n\n"
                f"DROP TABLE IF EXISTS temp_{proj_slug}_{kslug};\n"
                f"CREATE TABLE temp_{proj_slug}_{kslug} AS\n"
                f"SELECT\n"
                f"    {id_col},\n"
                f"    DATE_TRUNC('month', {date_col})   AS month,\n"
                f"    -- TODO: implement logic per business rules above\n"
                f"    COUNT(DISTINCT {id_col})           AS {kslug}\n"
                f"FROM {primary}\n"
                f"WHERE {date_col} >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))\n"
                f"  -- TODO: apply exclusions listed above\n"
                f"GROUP BY 1, 2;\n"
                f"```"
            )

        # No mode — generic stub
        return (
            f"```sql\n"
            f"-- {kname}\n"
            f"-- grain: one row per {id_col} × month (adjust per objective)\n"
            f"-- TODO: add SQL logic here\n\n"
            f"DROP TABLE IF EXISTS temp_{proj_slug}_{kslug};\n"
            f"CREATE TABLE temp_{proj_slug}_{kslug} AS\n"
            f"SELECT\n"
            f"    {id_col},\n"
            f"    DATE_TRUNC('month', {date_col})   AS month,\n"
            f"    COUNT(DISTINCT {id_col})           AS {kslug}\n"
            f"FROM {primary}\n"
            f"WHERE {date_col} >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))\n"
            f"GROUP BY 1, 2;\n"
            f"```"
        )

    active_kpis = [k for k in kpis_raw if isinstance(k, dict) and (k.get("name") or "").strip()]
    if not active_kpis:
        active_kpis = [{"name": "[KPI 1 — add in File Primer step 4]"}]

    second = tables_list[1] if len(tables_list) > 1 else primary

    examples = []
    for i, k in enumerate(active_kpis, 1):
        kpi_name_str = k.get("name", "").strip() if isinstance(k, dict) else str(k).strip()
        pat          = best_pattern_for_kpi(kpi_name_str)
        pattern_ref  = f"Pattern: **{pat['name']}** — see `kpi_definitions.md` for notes" if pat else "See `kpi_definitions.md` for pattern notes"
        sql          = sql_block_for_kpi(k)
        mode         = k.get("sql_source_mode", "") if isinstance(k, dict) else ""

        mode_note = {
            "playbook": "*SQL based on playbook entry — adapt filters and thresholds before running.*",
            "paste":    "*SQL pasted directly by analyst — verify grain and temp table naming.*",
            "describe": "*SQL stub generated from described rules — implement TODO sections before running.*",
            "":         "*SQL stub — add metric-specific logic before running.*",
        }.get(mode, "")

        second_ref = f", `{second}`" if second != primary else ""
        examples.append(
            f"---\n\n"
            f"## Example {i} — {kpi_name_str}\n\n"
            f"**Business question:** How do we measure **{kpi_name_str}** for this project?  \n"
            f"**Tables involved:** `{primary}`{second_ref}  \n"
            f"**Relevant pattern:** {pattern_ref}\n\n"
            f"{sql}\n\n"
            f"{mode_note}\n\n"
            f"**Known considerations:**  \n"
            f"- *[Document data quality caveats, lag windows, population exclusions here]*  \n"
            f"- *[Flag if window truncation check is needed for this metric]*  \n"
        )

    proj_slug_dash = re.sub(r'[^a-z0-9-]', '-', project_name.lower())
    return (
        f"---\n"
        f"name: {proj_slug_dash}-examples\n"
        f"description: \"Worked query stubs for {project_name}. Table and column names wired from Redshift metadata.\"\n"
        f"---\n\n"
        f"# EXAMPLES.md — {project_name}\n"
        f"*Generated by File Primer on {today} — one stub per KPI, using your actual table names*\n\n"
        f"> These are **starting frames**, not finished queries.  \n"
        f"> The table names, column names, and temp table naming convention are pre-filled from your metadata.  \n"
        f"> Fill in the business logic, filters, and metric definitions.\n\n"
        f"{''.join(examples)}\n\n"
        f"---\n\n"
        f"*Add completed queries here as the project matures.  \n"
        f"The more examples in this file, the better Claude understands your expected query style.*\n"
    )



def _gen_schema_reference(validated_tables, s):
    today = datetime.now().strftime("%B %d, %Y")
    sections = []

    if not validated_tables:
        return f"""# Schema reference
*Generated by File Primer on {today}*

> ⚠️ No tables were validated. Re-run with table validation to populate real column metadata.
"""

    for full_name, meta in validated_tables.items():
        schema, table = full_name.split(".", 1)
        if not meta.get("ok"):
            sections.append(f"## `{full_name}`\n> ❌ Validation failed: {meta.get('error', 'unknown error')}\n")
            continue

        cols = meta.get("columns", [])
        row_count = meta.get("row_count", -1)
        count_str = f"{row_count:,}" if row_count >= 0 else "unknown"

        dist_col = next((c["name"] for c in cols if c.get("distkey")), "none")
        sort_cols = sorted([(c["name"], c["sortkey"]) for c in cols if c.get("sortkey", 0) > 0], key=lambda x: x[1])
        sort_str = ", ".join(n for n, _ in sort_cols) if sort_cols else "none"

        col_rows = "\n".join(
            f"| `{c['name']}` | `{c['type']}` | {'Yes' if c['nullable'] else 'No'} | "
            f"{'`DISTKEY` ' if c.get('distkey') else ''}"
            f"{'`SORTKEY(' + str(c['sortkey']) + ')`' if c.get('sortkey', 0) > 0 else ''}|"
            for c in cols
        )

        sections.append(f"""## `{full_name}`
**Schema**: `{schema}` | **Table**: `{table}`
**Approx rows**: {count_str} | **DISTKEY**: `{dist_col}` | **SORTKEYS**: `{sort_str}`

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
{col_rows}
""")

    return f"""# Schema reference
*Generated by File Primer on {today} — live Redshift metadata*

{chr(10).join(sections)}
---
*Re-validate in File Primer after schema changes.*
"""


def _gen_kpi_definitions(kpis, s, validated_tables=None):
    today = datetime.now().strftime("%B %d, %Y")

    def kpi_name(k):  return k.get("name","")              if isinstance(k, dict) else str(k)
    def kpi_def(k):   return k.get("definition","").strip() if isinstance(k, dict) else ""
    def kpi_grain(k): return k.get("grain","").strip()      if isinstance(k, dict) else ""
    def kpi_tw(k):
        if not isinstance(k, dict): return ""
        tw = k.get("time_window","").strip()
        if tw == "Custom date range":
            custom = k.get("time_window_custom","").strip()
            return custom if custom else "Custom date range"
        return tw
    def kpi_cav(k):   return k.get("caveats","").strip()    if isinstance(k, dict) else ""
    def kpi_pnotes(k): return k.get("pattern_notes","").strip() if isinstance(k, dict) else ""

    active = [k for k in kpis if kpi_name(k).strip()]

    if not active:
        return (
            f"# KPI definitions\n"
            f"*Generated by File Primer on {today}*\n\n"
            f"> No KPIs specified. Add definitions here as the project matures.\n"
        )

    all_tables = list((validated_tables or {}).keys())
    table_str  = ", ".join(f"`{t}`" for t in all_tables) if all_tables else "*[specify]*"

    blocks = []
    for kpi in active:
        name       = kpi_name(kpi)
        definition = kpi_def(kpi)   or "*[What does this measure and why does it matter to the audience?]*"
        grain      = kpi_grain(kpi) or "*[patient / HCP / month — one row per what?]*"
        tw         = kpi_tw(kpi)    or "*[Rolling 12M? Point-in-time? Inception-to-date?]*"
        caveats    = kpi_cav(kpi)   or "*[Data lag, population exclusions, window truncation risk]*"
        pnotes     = kpi_pnotes(kpi)

        # Business rules section — only when describe mode was used
        rules_block = ""
        if isinstance(kpi, dict) and kpi.get("sql_source_mode") == "describe":
            rules      = kpi.get("sql_rules") or {}
            cols       = rules.get("columns", "").strip()
            biz_rules  = rules.get("business_rules", "").strip()
            exclusions = rules.get("exclusions", "").strip()
            edge_cases = rules.get("edge_cases", "").strip()
            if any([cols, biz_rules, exclusions, edge_cases]):
                parts = ["\n## Business rules\n"]
                if cols:       parts.append(f"**Columns used**: {cols}")
                if biz_rules:  parts.append(f"**Business rules**: {biz_rules}")
                if exclusions: parts.append(f"**Exclusions**: {exclusions}")
                if edge_cases: parts.append(f"**Known edge cases**: {edge_cases}")
                rules_block = "\n".join(parts) + "\n"

        blocks.append(
            f"### {name}\n\n"
            f"**Business definition**: {definition}\n"
            f"**Data source**: {table_str}\n"
            f"**Grain**: {grain}\n"
            f"**Time window**: {tw}\n\n"
            f"```sql\n"
            f"-- TODO: canonical SQL for {name}\n"
            f"-- Reference EXAMPLES.md for the query stub\n"
            f"```\n\n"
            f"**Known caveats**: {caveats}\n"
            + (f"**Pattern notes**: {pnotes}\n" if pnotes else "")
            + f"{rules_block}\n"
        )

    return (
        f"# KPI definitions\n"
        f"*Generated by File Primer on {today}*\n\n"
        f"> Keep this file in sync with the SQL in `EXAMPLES.md`.  \n"
        f"> If the definition changes, update both — they are the source of truth for this project.\n\n"
        f"{''.join(blocks)}\n"
    )



def _gen_readme(s):
    today         = datetime.now().strftime("%B %d, %Y")
    project_name  = s.get("project_name", "Unnamed Project")
    description   = s.get("description", "")
    owner         = s.get("owner", "")
    audience      = s.get("audience", [])
    tables_raw    = s.get("tables_raw", "")
    visual_output = s.get("visual_output", False) or s.get("output_type", "") in ("Dashboard", "Report")

    return f"""# {project_name}
*Generated by File Primer on {today}*

## What this is
{description or "*(not provided)*"}

**Visual deliverable**: {"Yes — styling_guide.md included" if visual_output else "No"}
**Owner / DRI**: {owner or "—"}
**Audience**: {", ".join(audience) if audience else "not specified"}

## File guide

| File | Start here when... |
|------|--------------------|
| `CLAUDE.md` | Starting a new Claude Code session |
| `schema_reference.md` | You need to know what columns a table has |
| `kpi_definitions.md` | You need the business definition of a metric |
| `EXAMPLES.md` | You need a query stub for a specific KPI |
| `session.json` | Re-running File Primer to refresh this scaffold |

## Data sources
```
{tables_raw or "*(not specified)*"}
```

## How to run
1. Open Claude Code from this project directory — `CLAUDE.md` loads automatically
2. Tell Claude: *"Read EXAMPLES.md and kpi_definitions.md, then help me with..."*
3. Run SQL steps sequentially — each persistent temp table is one named step
4. Validate row counts at each step: `SELECT COUNT(*), COUNT(DISTINCT entity_id) FROM temp_...;`
5. Check for window truncation before reporting the most recent period

## Known limitations
- *[Document data completeness issues, lag windows, population gaps here]*

## Revision history
| Date | Author | Change |
|------|--------|--------|
| {today} | {owner or "—"} | Initial scaffold generated by File Primer |
"""


def _gen_styling_guide(s):
    today    = datetime.now().strftime("%B %d, %Y")
    audience = s.get("audience", [])

    return f"""# Styling guide
*Generated by File Primer on {today}*

**Audience**: {", ".join(audience) if audience else "not specified"}

## Lilly brand colors

| Token | Hex | Usage |
|-------|-----|-------|
| Lilly Red | `#D52B1E` | Primary accent, CTA elements, key callouts |
| Lilly Dark | `#1A1A1A` | Body text |
| Lilly Light Gray | `#F5F5F5` | Background surfaces |
| Lilly Mid Gray | `#767676` | Secondary labels, axis text |
| Lilly Blue | `#0047BB` | Links, info states, secondary accent |
| White | `#FFFFFF` | Card backgrounds |

## Chart conventions
- Horizontal bars preferred for HCP / patient lists — easier label reading
- Line charts with markers for time series — mark key launch or event dates
- Always label axes — never rely on legend alone for single-series charts
- Suppress gridlines where data labels are present on bars
- KPI cards: 24px bold number, 13px muted label, unit in superscript
- Colour-blind safe: use shape/pattern as secondary differentiator alongside colour

## Typography
- Display / headers: Lilly Sans (fallback: `"Helvetica Neue", Arial, sans-serif`)
- Monospace (SQL snippets, table names): `"Courier New", monospace`
- Minimum font size in charts: 11px
- Dashboard section headers: 16px / 500 weight

## Layout
- Standard margin: 24px
- Card padding: 16px
- KPI card height: 80–100px
- Max content width: 1200px

## Accessibility
- Minimum contrast: 4.5:1 body text, 3:1 large text (WCAG AA)
- Never use colour as the only differentiator — add label or pattern
"""


def _gen_stakeholder_notes(s):
    today    = datetime.now().strftime("%B %d, %Y")
    notes    = s.get("stakeholder_notes", "")
    audience = s.get("audience", [])
    owner    = s.get("owner", "")

    return f"""# Stakeholder notes
*Generated by File Primer on {today}*

## Audience
{", ".join(audience) if audience else "Not specified"}

## Owner / DRI
{owner or "—"}

## Notes captured during project setup
{notes}

## Communication preferences
- *[Preferred cadence — weekly, monthly, ad hoc?]*
- *[Preferred format — email summary, slide deck, live walkthrough?]*
- *[Escalation path for data quality issues?]*

## Key decisions & open items
| Item | Status | Owner | Date |
|------|--------|-------|------|
| *[decision or open question]* | Open | — | {today} |

## Prior analyses to reference
- *[Link or describe any predecessor analysis this builds on]*
"""


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8500, debug=False)
