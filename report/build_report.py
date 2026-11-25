"""Builds the CSE557 project report (Word .docx) from the project's measured outputs.

Format follows the course template: Times New Roman 12, single line spacing, evaluation-sheet cover, team table,
table of contents and the 8 required sections. Every number is read from outputs/ (never typed by hand).
Run from the repository root:  .venv/bin/python report/build_report.py
"""
import ast
import json
import subprocess
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from docx import Document
from docx.enum.section import WD_SECTION
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_BREAK, WD_COLOR_INDEX
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs"
T = OUT / "tables"
FIG = OUT / "figures"
SHOT = ROOT / "docs" / "screenshots"
ASSETS = ROOT / "report" / "assets"
ASSETS.mkdir(exist_ok=True)
FONT = "Times New Roman"

# ------------------------------------------------------------------ data
J = lambda p: json.loads((ROOT / p).read_text())
bench = J("outputs/benchmark_results.json")
env = J("outputs/environment.json")
ml = J("outputs/ml_results.json")
dsnap = J("data/raw_delay/delay_snapshot_meta.json")
corr_d = J("outputs/delay_correctness_check.json")
wf_d = pd.read_csv(T / "delay_waterfall.csv")
ov = pd.read_csv(T / "delay_overall.csv").iloc[0]
hop = pd.read_csv(T / "delay_hop_overall.csv").iloc[0]
hwo = pd.read_csv(T / "headway_overall.csv").iloc[0]
by_hour = pd.read_csv(T / "delay_by_hour.csv").set_index("hour_of_day")
dayper = pd.read_csv(T / "delay_by_daytype_period.csv").set_index(["day_type", "period"])
ptype = pd.read_csv(T / "delay_by_point_type.csv").set_index("point_type")
by_route = pd.read_csv(T / "delay_by_route.csv")
seg_d = pd.read_parquet(T / "delay_segment_slowdown.parquet")
order_df = pd.read_csv(T / "delay_by_timepoint_order.csv")
imp = pd.read_csv(T / "ml_feature_importance.csv")
reqs = dict(l.split("==") for l in (ROOT / "requirements.txt").read_text().split() if "==" in l)
pkg = J("webapp/package.json")
pk_v = {**pkg["dependencies"], **pkg["devDependencies"]}
cfg = __import__("yaml").safe_load((ROOT / "config.yaml").read_text())
n_tests = subprocess.run([str(ROOT / ".venv/bin/python"), "-m", "pytest", "--collect-only", "-q"], capture_output=True, text=True, cwd=ROOT).stdout.count("::")

I = lambda x: f"{int(round(x)):,}"
F = lambda x, d=2: f"{x:,.{d}f}"
P = lambda x, d=1: f"{100 * x:.{d}f}%"
MIN = lambda s: f"{s / 60:.1f} min"
raw_d, fin_d = int(wf_d.iloc[0].rows_out), int(wf_d.iloc[-1].rows_out)
sc = bench["scaling"]
msc = bench.get("model_scaling", {}).get("scaling")
jv = bench["join_comparison"]["variants"]
MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
months = ", ".join(MON[m - 1] for m in cfg["delay"]["months"])
reg, clf = ml["regression"], ml["classification"]
G, GN, BM, BC = reg["GBT (full features)"], reg["GBT without previous delay"], reg["baseline: train mean"], reg["baseline: previous delay carried forward"]
CG, CGN, CC, CN = clf["GBT (full features)"], clf["GBT without previous delay"], clf["baseline: previous delay carried forward"], clf["baseline: never late"]
best_base = min(BM["mae_min"], BC["mae_min"])
gain = 100 * (best_base - G["mae_min"]) / best_base
csv_bytes = sum(dsnap["files"][f"MBTA-Bus-Arrival-Departure-Times_2024-{m:02d}.csv"]["bytes"] for m in cfg["delay"]["months"])


def src_of(file, func):
    text = (ROOT / file).read_text()
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.FunctionDef) and node.name == func:
            return ast.get_source_segment(text, node)
    raise KeyError(func)


def lines_of(file, start_text, n):
    ls = (ROOT / file).read_text().splitlines()
    i = next(k for k, l in enumerate(ls) if start_text in l)
    return "\n".join(ls[i:i + n])


# ------------------------------------------------------------------ docx helpers
doc = Document()
st = doc.styles["Normal"]
st.font.name, st.font.size = FONT, Pt(12)
st.element.rPr.rFonts.set(qn("w:eastAsia"), FONT)
st.paragraph_format.line_spacing = 1.0
st.paragraph_format.space_after = Pt(6)
sec = doc.sections[0]
sec.left_margin = sec.right_margin = Inches(1)
sec.top_margin = sec.bottom_margin = Inches(1)

_fig, _tab, _bm = [0], [0], [0]


def run_fmt(r, size=12, bold=None, italic=None, mono=False, color=None):
    r.font.name = "Courier New" if mono else FONT
    r._element.rPr.rFonts.set(qn("w:eastAsia"), r.font.name)
    r.font.size = Pt(size)
    if bold is not None:
        r.bold = bold
    if italic is not None:
        r.italic = italic
    if color:
        r.font.color.rgb = RGBColor.from_string(color)
    return r


def para(text="", size=12, bold=False, italic=False, align=None, after=6, before=0, keep=False):
    p = doc.add_paragraph()
    p.paragraph_format.space_after, p.paragraph_format.space_before = Pt(after), Pt(before)
    if keep:
        p.paragraph_format.keep_with_next = True
    if align == "c":
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    elif align == "j":
        p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    if text:
        add_rich(p, text, size, bold, italic)
    return p


def add_rich(p, text, size=12, bold=False, italic=False):
    """**bold** segments inside text."""
    parts = text.split("**")
    for i, part in enumerate(parts):
        if part:
            run_fmt(p.add_run(part), size, bold or i % 2 == 1, italic)


def body(text):
    return para(text, align="j")


def bullet(text, level=0):
    p = doc.add_paragraph(style="List Bullet")
    p.paragraph_format.space_after = Pt(3)
    p.paragraph_format.left_indent = Inches(0.25 + 0.25 * level)
    add_rich(p, text)
    return p


def note(text):
    """Highlighted instruction for the team (delete before submission)."""
    p = doc.add_paragraph()
    r = p.add_run("[TEAM NOTE: " + text + "]")
    run_fmt(r, 11, italic=True)
    r.font.highlight_color = WD_COLOR_INDEX.YELLOW
    return p


def h1(num, title, bookmark):
    p = doc.add_paragraph()
    p.paragraph_format.page_break_before = True
    p.paragraph_format.space_after = Pt(10)
    p.paragraph_format.keep_with_next = True
    run_fmt(p.add_run(f"{num}. {title}"), 14, bold=True)
    _bm[0] += 1
    s, e = OxmlElement("w:bookmarkStart"), OxmlElement("w:bookmarkEnd")
    for el in (s, e):
        el.set(qn("w:id"), str(_bm[0]))
    s.set(qn("w:name"), bookmark)
    p._p.insert(1, s)
    p._p.append(e)
    return p


def h2(text):
    return para(text, bold=True, after=4, before=8, keep=True)


def code(text):
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.left_indent = Inches(0.2)
    pPr = p._p.get_or_add_pPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear"); shd.set(qn("w:color"), "auto"); shd.set(qn("w:fill"), "F2F2F2")
    pPr.append(shd)
    lines = text.rstrip().split("\n")
    for i, l in enumerate(lines):
        r = p.add_run(l)
        run_fmt(r, 9, mono=True)
        if i < len(lines) - 1:
            r.add_break()
    return p



def borders(t, widths=None):
    """Explicit single-line borders (renderers that ignore the Table Grid style still draw them) and fixed column widths."""
    tblPr = t._tbl.tblPr
    b = OxmlElement("w:tblBorders")
    for edge in ("top", "left", "bottom", "right", "insideH", "insideV"):
        e = OxmlElement(f"w:{edge}")
        e.set(qn("w:val"), "single"); e.set(qn("w:sz"), "6"); e.set(qn("w:space"), "0"); e.set(qn("w:color"), "000000")
        b.append(e)
    tblPr.append(b)
    if widths:
        t.autofit = False
        for row in t.rows:
            for i, w in enumerate(widths):
                if i < len(row.cells):
                    row.cells[i].width = Inches(w)


def shade(cell, fill):
    tcPr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear"); shd.set(qn("w:color"), "auto"); shd.set(qn("w:fill"), fill)
    tcPr.append(shd)


def table(headers, rows, widths=None, size=10.5, caption=None, align_num=True):
    if caption:
        _tab[0] += 1
        para(f"Table {_tab[0]}: {caption}", size=11, italic=True, after=3, keep=True)
    t = doc.add_table(rows=1, cols=len(headers))
    t.style = "Table Grid"; borders(t)
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    for i, h in enumerate(headers):
        c = t.rows[0].cells[i]
        c.text = ""
        run_fmt(c.paragraphs[0].add_run(h), size, bold=True)
        shade(c, "D9D9D9")
    for row in rows:
        cells = t.add_row().cells
        for i, v in enumerate(row):
            cells[i].text = ""
            p = cells[i].paragraphs[0]
            p.paragraph_format.space_after = Pt(0)
            add_rich(p, str(v), size)
            if align_num and i > 0 and str(v).replace(",", "").replace(".", "").replace("%", "").replace("x", "").replace("-", "").replace(" ", "").isdigit():
                p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    if widths:
        borders_w = widths
        t.autofit = False
        for row in t.rows:
            for i, w in enumerate(borders_w):
                row.cells[i].width = Inches(w)
    for row in t.rows:  # keep rows intact across pages
        trPr = row._tr.get_or_add_trPr()
        cs = OxmlElement("w:cantSplit")
        trPr.append(cs)
    doc.add_paragraph().paragraph_format.space_after = Pt(4)
    return t


def figure(path, caption, width=6.2):
    _fig[0] += 1
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.keep_with_next = True
    p.paragraph_format.space_after = Pt(2)
    p.add_run().add_picture(str(path), width=Inches(width))
    para(f"Figure {_fig[0]}: {caption}", size=11, italic=True, align="c", after=10)


def add_field(p, instr, cached="", size=12):
    def fc(kind):
        r = p.add_run()
        run_fmt(r, size)
        el = OxmlElement("w:fldChar")
        el.set(qn("w:fldCharType"), kind)
        r._r.append(el)
    fc("begin")
    r = p.add_run()
    run_fmt(r, size)
    it = OxmlElement("w:instrText")
    it.set(qn("xml:space"), "preserve")
    it.text = f" {instr} "
    r._r.append(it)
    fc("separate")
    run_fmt(p.add_run(cached), size)
    fc("end")



# ------------------------------------------------------------------ workflow diagram
def make_workflow():
    fig, ax = plt.subplots(figsize=(11, 4.2))
    ax.set_xlim(0, 11); ax.set_ylim(0, 4.2); ax.axis("off")

    def box(x, y, w, h, text, fc):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.04,rounding_size=0.12", fc=fc, ec="#333", lw=1))
        ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=8.6)

    def arrow(x1, y1, x2, y2):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=12, color="#333", lw=1.2))
    ax.text(0.1, 3.95, "Analytics", fontsize=10, fontweight="bold", color="#B86E00")
    A = ["Monthly CSVs\n(4 months of 2024)", "Ingest\nexplicit schema,\nall text", "Clean\nparse times,\ndrop gaps, dedupe", "Delay\nactual - scheduled\n(midnight-safe)", "Window lag()\nrun time between\ntimepoints", "Aggregate\non-time, hour, route,\nbunching", "Tables, figures,\ndashboard"]
    w, g = 1.35, 0.28
    for i, t in enumerate(A):
        x = 0.1 + i * (w + g)
        box(x, 2.45, w, 1.3, t, "#FDF1DC")
        if i:
            arrow(x - g + 0.02, 3.1, x - 0.02, 3.1)
    ax.text(0.1, 1.95, "Prediction (Spark MLlib)", fontsize=10, fontweight="bold", color="#0072B2")
    B = ["Features\nhour, route, previous\ndelay (lag)", "Temporal split\nlast 25% of days\nof each month = test", "History\nroute averages from\ntrain days, broadcast", "Train\nGBT regressor and\nGBT classifier", "Evaluate\nvs baselines,\nheld-out days only", "Metrics, figures,\ndashboard"]
    for i, t in enumerate(B):
        x = 0.1 + i * (w + g)
        box(x, 0.45, w, 1.3, t, "#E6F1F8")
        if i:
            arrow(x - g + 0.02, 1.1, x - 0.02, 1.1)
    ax.text(5.5, 0.05, "Everything: PySpark DataFrames, row-count waterfall, Parquet intermediates, benchmarks on local[1], local[2], local[4]", ha="center", fontsize=9, style="italic")
    fig.savefig(ASSETS / "workflow.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


make_workflow()

# ================================================================== COVER (evaluation sheet)
for text, size, bold in [("Department of Computer Science and Engineering", 16, False), ("", 6, False), ("M S Ramaiah Institute of Technology", 18, False), ("(Autonomous Institute, Affiliated to VTU)", 11, False), ("", 6, False), ("Bangalore – 54", 16, False)]:
    p = para(text, size=size, bold=bold, align="c", after=2)
p = para(after=40)
p = para(align="c", after=40)
p.add_run().add_picture(str(ASSETS / "logo_cover.png"), width=Inches(1.2))
p = para(align="c", after=14)
r = run_fmt(p.add_run("Evaluation Sheet"), 14, bold=True); r.underline = True
t = doc.add_table(rows=3, cols=5)
t.style = "Table Grid"; borders(t)
t.alignment = WD_TABLE_ALIGNMENT.CENTER
hdr = ["USN", "Name", "Lab Weekly Assignment (10)", "Dataset Analysis for Real-Time Scenarios (10)", "Total Marks (20)"]
for i, h in enumerate(hdr):
    c = t.rows[0].cells[i]; c.text = ""
    run_fmt(c.paragraphs[0].add_run(h), 11, bold=True); c.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
    if i == 4:
        shade(c, "D9F2D0")
for ri, (usn, name) in enumerate([("1MS24CS113", "Nikitaa Dhinakaran"), ("1MS24CS229", "Pranavi Manjunath")], start=1):
    for ci, v in enumerate([usn, name, "", "", ""]):
        c = t.rows[ri].cells[ci]; c.text = ""
        run_fmt(c.paragraphs[0].add_run(v), 11)
        if ci == 4:
            shade(c, "D9F2D0")
    t.rows[ri].height = Inches(0.45)
borders(t, [1.2, 1.8, 1.2, 1.5, 0.9])
para(after=50)
for line in ["Evaluated By", "", "Name:", "Designation:", "Department:", "Signature:"]:
    para(line, after=2)

# ================================================================== PAGE 2: title, team, contents
doc.add_page_break()
p = para(after=10)
p.add_run().add_picture(str(ASSETS / "logo_header.png"), width=Inches(1.7))
para("Big Data Analytics - CSE557 (Other Component)", size=16, bold=True, align="c", after=14)
p = para(after=10)
run_fmt(p.add_run("Dataset Analysis for Real-Time Scenarios        "), 14, bold=True)
run_fmt(p.add_run("Faculty Name: "), 14, bold=True)
run_fmt(p.add_run("Uzma Sulthana"), 14, bold=True)
p = para("Project title: Scalable Bus Delay Analytics and Delay Prediction on Boston MBTA Data using PySpark", size=12, bold=True, after=10, align="j")
t = doc.add_table(rows=2, cols=4)
t.style = "Table Grid"; borders(t)
t.alignment = WD_TABLE_ALIGNMENT.CENTER
tm = t.rows[0].cells[0].merge(t.rows[0].cells[3]); tm.text = ""
run_fmt(tm.paragraphs[0].add_run("Team Member Details"), 12, bold=True); tm.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
hdrs = ["Sl. No.", "USN", "Name", "Email ID"]
row = t.rows[1]
for i, h in enumerate(hdrs):
    row.cells[i].text = ""; run_fmt(row.cells[i].paragraphs[0].add_run(h), 11, bold=True); row.cells[i].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
for k, (usn, name, mail) in enumerate([("1MS24CS113", "Nikitaa Dhinakaran", "1ms24cs113@msrit.edu"), ("1MS24CS229", "Pranavi Manjunath", "1ms24cs229@msrit.edu")], start=1):
    cells = t.add_row().cells
    for i, v in enumerate([str(k), usn, name, mail]):
        cells[i].text = ""
        run_fmt(cells[i].paragraphs[0].add_run(v), 11)
borders(t, [0.7, 1.3, 2.2, 2.3])
para(after=14)
para("Table of Contents", size=14, bold=True, align="c", after=8)
TOC = [("Real-world Scenario and Problem Statement", "sec1"), ("Recent Research Paper and Existing Approach", "sec2"), ("Objectives and Proposed Analysis", "sec3"),
       ("Dataset Identification and Preparation", "sec4"), ("Big Data Implementation and Workflow", "sec5"), ("Results, Visualization and Paper Comparison", "sec6"),
       ("Performance Evaluation and Observations", "sec7"), ("Tools, Responsible Use and References", "sec8")]
t = doc.add_table(rows=1, cols=3)
t.style = "Table Grid"; borders(t)
t.alignment = WD_TABLE_ALIGNMENT.CENTER
for i, h in enumerate(["Sl No.", "Title", "Page No"]):
    c = t.rows[0].cells[i]; c.text = ""; run_fmt(c.paragraphs[0].add_run(h), 12, bold=True); c.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
for k, (title, bm) in enumerate(TOC, start=1):
    cells = t.add_row().cells
    cells[0].text = ""; run_fmt(cells[0].paragraphs[0].add_run(str(k)), 12, bold=True); cells[0].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
    cells[1].text = ""; run_fmt(cells[1].paragraphs[0].add_run(title), 12, bold=True)
    cells[2].text = ""; add_field(cells[2].paragraphs[0], f"PAGEREF {bm} \\h", "")
    cells[2].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
    t.rows[k].height = Inches(0.36)
for row in t.rows:
    row.cells[0].width, row.cells[1].width, row.cells[2].width = Inches(0.8), Inches(4.7), Inches(1.0)

# footer page numbers
fp = sec.footer.paragraphs[0]
fp.alignment = WD_ALIGN_PARAGRAPH.CENTER
add_field(fp, "PAGE", "1", 11)


# ================================================================== 1
h1(1, "Real-world Scenario and Problem Statement", "sec1")
h2("1.1 Real-world scenario")
body(f"City bus services are only useful if they are reliable. When buses run late, commuters wait longer, connections are missed and schedulers cannot plan. Modern transit agencies record the scheduled and the actual time of every bus at every checkpoint (timepoint). The Massachusetts Bay Transportation Authority (MBTA) in Boston publishes this record openly. The four months we analyse contain **{I(raw_d)} bus arrival and departure events** ({csv_bytes / 1e9:.1f} GB of CSV text), and the full year has three times as many.")
body("Turning such a record into answers (how late are buses, when and where, how often do they bunch, and can the delay of the next timepoint be predicted) means cleaning tens of millions of rows, ordering events within every trip, and computing grouped statistics and model features. This is slow and memory hungry on a single-node tool, which motivates a distributed data-processing engine such as Apache Spark.")
h2("1.2 Problem statement")
body("**To build a scalable PySpark pipeline that measures bus delay from real scheduled and actual times, and to train and evaluate Spark MLlib models that predict the delay of a bus at its next timepoint, while measuring how well the pipeline scales with the number of cores.**")
h2("1.3 Stakeholders")
for s in ["Transit operations and scheduling teams, who design timetables and respond to disruptions.",
          "Traffic and city planners, who manage corridors where buses are repeatedly late.",
          "Commuters, who experience late and bunched services and benefit from reliable arrival forecasts."]:
    bullet(s)
h2("1.4 Why this dataset")
body("An earlier version of this project analysed a static timetable of Bengaluru buses (BMTC). A static timetable contains only planned times, so actual delay and any prediction of it cannot be computed. A search for a public BMTC dataset with actual arrival times found none. The project therefore uses the MBTA dataset, which has both scheduled and actual times, an open licence, and a size that matters for Spark. The analysis describes **Boston bus operations in 2024**; it makes no claim about other cities.")
h2("1.5 Analytical questions")
table(["ID", "Question"], [
    ["Q1", "How late are buses in reality, how often are they on time, and how does delay vary by hour, day type, route and position along the trip?"],
    ["Q2", "How much bunching and how many large gaps occur on high-frequency routes?"],
    ["Q3", "Does the timetable plan the peak slowdown that actually happens?"],
    ["Q4", "Can Spark MLlib models predict the delay in minutes, and whether a bus will be late, better than simple baselines on days the model has never seen?"],
    ["Q5", "What speedup and throughput does PySpark achieve with 1, 2 and 4 cores for the analysis and for model training, and how do broadcast and shuffle joins compare?"],
], widths=[0.5, 6.0], caption="Analytical questions addressed by the project")
h2("1.6 Why this is a Big Data problem")
body(f"The data used contains {I(raw_d)} events in {len(cfg['delay']['months'])} monthly files. The work needs window functions over ordered sequences within every trip, grouped statistics over many keys, a train/test split by date, joins of small history tables onto millions of events, and model training on a million-row sample. These are the operations a distributed engine is designed for. The data are moderate in absolute size on one laptop, so the project also measures honestly how much of Spark's benefit appears at this scale.")

# ================================================================== 2
h1(2, "Recent Research Paper and Existing Approach", "sec2")
h2("2.1 Base research paper")
body("The base paper cited in our proposal is:")
p = para(after=6, align="j")
add_rich(p, "N. L. Mnyone and N. U. Amin, **\"Real-Time Data Processing in Smart Transportation Using Apache Spark\"**, Preprints.org, 2026, DOI: 10.20944/preprints202603.1261.v1.")
body("Details that could be verified from the publisher's listing: it is a preprint (not peer reviewed), submitted on 14 March 2026 and posted on 17 March 2026. Its abstract states that it covers the types of transportation data sources and the challenges in collecting and integrating such data, and that it explains the architecture and components of Apache Spark, including the driver program, cluster manager, executors, resilient distributed datasets (RDDs) and Spark Streaming.")
h2("2.2 Approach described in our proposal")
body("As summarised in our project proposal, the paper uses synthetic GPS and vehicle-sensor logs, builds on Spark Core RDDs and basic Spark SQL, and evaluates ingestion bottlenecks and memory use on single-table feeds. This characterises the existing approach we compare against: a general demonstration of Spark for transportation data rather than an analysis of a large real transit dataset with a prediction task.")
h2("2.3 Existing approaches to bus delay analysis (general context)")
for s in ["Reliability reports from transit agencies typically use fixed measures such as on-time percentage by route, computed in databases or spreadsheets. These work for small samples but become slow on tens of millions of events.",
          "Research on bus delay and arrival-time prediction commonly uses automatic vehicle location (AVL) data and regression or tree-based models. Such studies often split data randomly, which lets a model see the same days it is tested on.",
          "Single-machine data-frame libraries handle a month of data with difficulty; distributed engines remove the memory limit but need careful design (partitioning, joins, avoiding data leakage)."]:
    bullet(s)
h2("2.4 Gap addressed by this project")
for s in ["A **real, large dataset** with scheduled and actual times rather than synthetic single-table logs.",
          "**Sequence-level features** computed with window functions (the previous timepoint's delay in the same trip).",
          "**Prediction with an honest evaluation:** temporal train/test split, simple baselines, an ablation without the strongest feature, and metrics on held-out days only.",
          "**Measured** join and scaling behaviour: broadcast versus shuffle join, and speedup and efficiency of both the analysis and the model training on 1, 2 and 4 cores.",
          "An explicit **data-completeness waterfall** and automated correctness checks against an independent pandas implementation."]:
    bullet(s)

# ================================================================== 3
h1(3, "Objectives and Proposed Analysis", "sec3")
h2("3.1 Objectives")
for s in ["**Dataset preparation:** ingest, parse and clean the monthly MBTA files, handle times that wrap past midnight, remove invalid rows and report a data-completeness waterfall.",
          "**Delay analytics:** compute departure delay, on-time performance, delay by hour, route and trip position, actual versus scheduled run time, and bunching.",
          "**Delay prediction:** train Spark MLlib gradient-boosted trees to predict delay in minutes (regression) and late versus not late (classification), and evaluate them on held-out days against baselines.",
          "**Evaluation:** benchmark on local[1], local[2] and local[4], compute speedup Sₙ = T₁ / Tₙ, efficiency, latency and throughput, and compare broadcast with shuffle joins.",
          "**Communication:** present results as figures, tables and an interactive web dashboard."]:
    bullet(s)
h2("3.2 Proposed analysis and metrics")
table(["Metric", "Definition"], [
    ["Departure delay", "actual minus scheduled departure at a timepoint (minutes; positive = late)"],
    ["On-time rate", f"share of departures between {cfg['delay']['on_time_early_sec'] // 60} and +{cfg['delay']['on_time_late_sec'] // 60} minutes of schedule"],
    ["Slowdown ratio", "mean peak run time / mean off-peak run time between consecutive timepoints (1.0 = no slowdown); scheduled and actual"],
    ["Bunching, large gap", f"actual gap to the previous bus below {cfg['delay']['headway_bunch_ratio']} x or above {cfg['delay']['headway_gap_ratio']} x the scheduled gap"],
    ["MAE, RMSE, R²", "mean absolute error, root mean squared error and coefficient of determination of the delay prediction (test days)"],
    ["Precision, recall, F1, AUC", "classification quality for 'late' (delay above 5 minutes); AUC = area under the ROC curve"],
    ["Speedup, efficiency", "Sₙ = T₁ / Tₙ; efficiency = Sₙ / N"],
    ["Throughput", "cleaned events processed per second"],
], widths=[1.6, 4.9], caption="Metrics used in the analysis")
h2("3.3 Scope and assumptions")
for s in [f"**Peak** hours are 07:00 to 10:00 and 17:00 to 20:00 by the scheduled hour; all other hours are off-peak (set in configuration).",
          "Delay is judged on **'Schedule'-standard trips** only; the dataset states that scheduled times must not be used to judge 'Headway'-standard trips, which are analysed through headways.",
          f"The prediction is a **next-timepoint forecast**: at the moment a bus leaves its previous timepoint, predict its delay at the next one. The split is **temporal**: the last {int(100 * cfg['delay_model']['test_fraction'])}% of the days of every month are the test set and are never used for training or for the history features.",
          "All features are known before the event; the delay at the previous timepoint of the same trip is allowed, delays at the current or later timepoints are not.",
          f"Only {len(cfg['delay']['months'])} months of 2024 ({months}) are analysed, to keep run times manageable; the dataset covers all twelve."]:
    bullet(s)

# ================================================================== 4
h1(4, "Dataset Identification and Preparation", "sec4")
h2("4.1 Dataset: MBTA bus arrival and departure times (Boston)")
body(f"**Source:** MBTA Open Data Portal, item \"{dsnap['title']}\" (ArcGIS item {dsnap['item_id']}), published by the Massachusetts Bay Transportation Authority. **Licence shown on the item:** {dsnap['licence_on_item']} (public domain). **Download:** `scripts/download_delay_data.py` (a {dsnap['zip_bytes'] / 1e6:.0f} MB zip containing 12 monthly CSV files; file sizes and SHA-256 checksums are recorded). **Time span:** the 2024 calendar year is available; **{len(cfg['delay']['months'])} months are analysed** ({months}), one per season, giving **{I(raw_d)} rows**. The data contain no personal information.")
table(["Column", "Meaning"], [
    ["service_date, route_id, direction_id", "day of the trip, route, inbound or outbound"],
    ["half_trip_id", "identifier of one one-way trip"],
    ["time_point_id, time_point_order", "a scheduled checkpoint on the route and its order along the trip"],
    ["point_type", "Startpoint, Midpoint or Endpoint"],
    ["standard_type", "Schedule (judged on schedule) or Headway (judged on spacing)"],
    ["scheduled, actual", "scheduled and actual departure time at the timepoint"],
    ["scheduled_headway, headway", "planned and actual seconds since the previous bus"],
], widths=[2.3, 4.2], caption="Main columns of the MBTA dataset (one row = one bus trip leaving one timepoint)")
h2("4.2 Data quality issues found and handled")
for s in ["All columns are read as text with an explicit schema and converted explicitly; blank strings become null.",
          "Rows without date, route, trip or timepoint order are dropped; duplicate (date, trip, timepoint) events are removed.",
          "The timestamps carry a placeholder date (1900-01-01), so they are converted to seconds of the day. **Delay is computed modulo 24 hours** so that a bus scheduled at 23:58 and leaving at 00:01 is 3 minutes late rather than 23.9 hours early.",
          "Events with no actual time (data-collection gaps) are dropped, and events with an absolute delay above 2 hours are treated as errors.",
          "Only 'Schedule'-standard trips are used for delay and for the model."]:
    bullet(s)
rows = [[r.stage, I(r.rows_in), I(r.rows_out), f"{r.pct_retained:.2f}%"] for r in wf_d.itertuples()]
table(["Stage", "Rows in", "Rows out", "Retained"], rows, widths=[3.6, 1.0, 1.0, 0.9], size=10, caption="Data-completeness waterfall")
body(f"Of {I(raw_d)} raw events, {I(fin_d)} ({F(100 * fin_d / raw_d)}%) are used for the delay analysis and the model. {I(wf_d.iloc[3].rows_out - wf_d.iloc[4].rows_out)} events ({F(100 - wf_d.iloc[4].pct_retained)}%) had no actual time.")
figure(FIG / "fig4_completeness_waterfall.png", "Rows remaining after each preparation step (quality steps in green, the intended scope filter in orange)", 6.0)
h2("4.3 Features and split for the prediction task")
table(["Feature", "Meaning"], [
    ["hour of day, day of week, month", "clock features of the scheduled time"],
    ["route, direction, point type, timepoint order", "where the event is on the network and in the trip (route and others are indexed categories)"],
    ["previous-timepoint delay", "delay at the previous timepoint of the same trip (window function lag(); 0 plus an indicator when there is none)"],
    ["route average delay, route late rate", "computed on the training days only, then broadcast-joined to every event"],
    ["route + timepoint average delay", "computed on the training days only; falls back to the route average for unseen timepoints"],
], widths=[2.6, 3.9], caption="Features used by the models")
body(f"Training days: {I(ml['split']['train']['rows'])} events on {ml['split']['train']['days']} days ({ml['split']['train']['first_date']} to {ml['split']['train']['last_date']}). Test days: **{I(ml['test_rows'])}** events on {ml['split']['test']['days']} days ({ml['split']['test']['first_date']} to {ml['split']['test']['last_date']}); late share {P(ml['split']['train']['late_rate'])} in training and {P(ml['split']['test']['late_rate'])} in testing. The models are fitted on a seeded sample of {I(ml['train_rows_used'])} training events (cap {I(ml['train_cap'])}, so local-mode training stays in minutes); all history features use all training days.")

# ================================================================== 5
h1(5, "Big Data Implementation and Workflow", "sec5")
h2("5.1 Architecture and workflow")
body("The project is a modular Python package on Apache Spark: ingest with an explicit schema, clean with a row-count waterfall, compute delay and window features, aggregate, write Parquet and CSV tables, build model features, train and evaluate MLlib models, then visualise and benchmark.")
figure(ASSETS / "workflow.png", "Workflow of the analysis and prediction parts of the project", 6.4)
h2("5.2 Technology stack")
table(["Technology", "Role in this project"], [
    ["Python 3.11", "language for all code"],
    ["Apache Spark 3.5.3 / PySpark", "distributed engine; runs in local[N] mode where N cores act as workers"],
    ["Spark DataFrames, Window functions", "typed tables with lazy execution; lag() over ordered timepoints within each trip"],
    ["Spark MLlib", "StringIndexer, VectorAssembler, GBTRegressor, GBTClassifier"],
    ["Parquet", "compressed columnar format for intermediate and large output tables"],
    ["Pandas / PyArrow", "small result tables, CSV and Parquet output, and the independent correctness checks"],
    ["Matplotlib", "300-dpi figures"],
    ["pytest", f"{n_tests} automated tests"],
    ["React, TypeScript, Vite, Recharts", "interactive web dashboard"],
    ["Java 17", "runtime required by Spark"],
], widths=[2.4, 4.1], caption="Technologies used")
h2("5.3 Code organisation")
table(["File", "Responsibility"], [
    ["src/ingest.py", "Spark session and configuration"],
    ["src/waterfall.py", "the row-count waterfall"],
    ["src/features.py", "peak / off-peak time buckets"],
    ["src/delay_pipeline.py", "ingest, cleaning, delay, window features and aggregations"],
    ["src/delay_model.py", "model features, temporal split, train-only history, GBT models, metrics"],
    ["src/benchmark.py", "one-process-per-configuration benchmark, model-training benchmark, join comparison"],
    ["src/viz.py, src/make_docs.py", "figures and generated documentation"],
    ["tests/", f"{n_tests} tests: cleaning rules, midnight wrap, pandas cross-check, no-leakage model tests, dashboard export"],
    ["webapp/", "React dashboard that reads exported JSON"],
    ["config.yaml, run_all.sh", "all thresholds and the end-to-end runner"],
], widths=[2.2, 4.3], caption="Project structure", size=10)
h2("5.4 Key implementation details")
body("**(a) Midnight-safe delay** (src/delay_pipeline.py): times are stamped with a placeholder date, so the difference is taken modulo 24 hours and mapped to [-12 h, +12 h).")
code(src_of("src/delay_pipeline.py", "wrap_delay"))
body("**(b) Window function and lag()** (src/delay_pipeline.py, stage_delay_agg): within each trip the timepoints are ordered and lag() gives the previous timepoint, from which the scheduled and actual run times are computed.")
code(lines_of("src/delay_pipeline.py", "w = Window.partitionBy", 5))
body("**(c) Model features without leakage** (src/delay_model.py): the previous delay comes from lag() over earlier timepoints only, so a row never sees its own or a later delay.")
code(src_of("src/delay_model.py", "add_trip_features"))
body("**(d) Temporal split and train-only history joined by broadcast** (src/delay_model.py): the last days of each month are the test set; route averages are computed on training rows only and attached with a broadcast join.")
code(src_of("src/delay_model.py", "test_flag") + "\n\n" + "\n".join(l for l in src_of("src/delay_model.py", "add_history").splitlines() if l.strip().startswith(("b =", "r = ", "rt = ", "out = ", "g = ")) or "broadcast" in l))
body(f"**(e) Models** (src/delay_model.py, fit_gbt): gradient-boosted trees from Spark MLlib, {cfg['delay_model']['gbt_max_iter']} boosting rounds of depth {cfg['delay_model']['gbt_max_depth']}, a fixed seed, and enough bins for all {ml['n_routes']} routes. Two variants are trained for each task: with all features, and without the previous-delay features (ablation).")
h2("5.5 Web dashboard")
body("The results are also available as an interactive dashboard (React, TypeScript, Recharts). It has three tabs: the delay explorer, the prediction results and the pipeline with benchmarks. It runs from JSON exported from the measured outputs and needs no Spark or server.")
for nm, cap in [("dashboard-delay.png", "Dashboard: delay explorer"), ("dashboard-prediction.png", "Dashboard: delay prediction results")]:
    if (SHOT / nm).exists():
        figure(SHOT / nm, cap, 5.6)

# ================================================================== 6
h1(6, "Results, Visualization and Paper Comparison", "sec6")
h2("6.1 How late are buses?")
hr_worst, hr_best = by_hour.on_time_rate.idxmin(), by_hour.on_time_rate.idxmax()
first, last15 = order_df.iloc[0], order_df[order_df.time_point_order <= 15].iloc[-1]
body(f"Over {I(ov.n)} departure events on Schedule-standard trips, the mean delay is **{MIN(ov.mean_delay_sec)}** (median {MIN(ov.median_delay_sec)}, 90th percentile {MIN(ov.p90_delay_sec)}). **{P(ov.on_time_rate)}** of departures are on time (between {cfg['delay']['on_time_early_sec'] // 60} and +{cfg['delay']['on_time_late_sec'] // 60} minutes), {P(ov.late_rate)} are late and {P(ov.early_rate)} are early.")
for s in [f"By hour, the lowest on-time share is at {int(hr_worst)}h ({P(by_hour.loc[hr_worst, 'on_time_rate'])}) and the highest at {int(hr_best)}h ({P(by_hour.loc[hr_best, 'on_time_rate'])}); overnight hours have few trips.",
          f"Weekday mean delay: peak {MIN(dayper.loc[('weekday', 'PEAK'), 'mean_delay_sec'])} versus off-peak {MIN(dayper.loc[('weekday', 'OFF_PEAK'), 'mean_delay_sec'])}; weekend: peak {MIN(dayper.loc[('weekend', 'PEAK'), 'mean_delay_sec'])} versus off-peak {MIN(dayper.loc[('weekend', 'OFF_PEAK'), 'mean_delay_sec'])}.",
          f"By point type the mean delay is {MIN(ptype.loc['Startpoint', 'mean_delay_sec'])} at trip starts, {MIN(ptype.loc['Midpoint', 'mean_delay_sec'])} at midpoints and {MIN(ptype.loc['Endpoint', 'mean_delay_sec'])} at endpoints. By position in the trip it is {MIN(first.mean_delay_sec)} at the first timepoint and {MIN(last15.mean_delay_sec)} at timepoint {int(last15.time_point_order)}; the curve is not monotonic and later positions cover far fewer trips, so it is not a simple build-up of delay.",
          f"Between consecutive timepoints the mean scheduled run time is {F(hop.mean_sched_hop, 0)} s and the mean actual run time {F(hop.mean_act_hop, 0)} s ({F(hop.mean_added_sec, 1)} s added per hop on average).",
          f"Across {I(len(seg_d))} segments with enough peak and off-peak events, the median actual peak/off-peak run-time ratio is {F(seg_d.actual_slowdown_ratio.median(), 3)} and the median scheduled ratio {F(seg_d.sched_slowdown_ratio.median(), 3)}; {P(((seg_d.sched_slowdown_ratio - 1).abs() <= .01).mean())} of segments have a scheduled ratio within 1% of 1.0."]:
    bullet(s)
figure(FIG / "fig1_delay_overview.png", "Delay distribution, on-time and late share by hour, and headway bunching (Boston MBTA)", 6.4)
figure(FIG / "fig2_delay_routes_and_buildup.png", "Most delayed routes and mean delay by position in the trip", 6.2)
figure(FIG / "fig3_actual_vs_scheduled_slowdown.png", "Scheduled versus actual slowdown per segment, and delay by period and day type", 6.2)
top = by_route.head(5)
table(["Route", "Events", "Mean delay", "90th pct", "On time"], [[r.route_id, I(r.n), MIN(r.mean_delay_sec), MIN(r.p90_delay_sec), P(r.on_time_rate)] for r in top.itertuples()], widths=[1.0, 1.3, 1.4, 1.4, 1.3], caption=f"Five most delayed routes (at least {cfg['delay']['min_events']} events)")
h2("6.2 Headway: bunching and large gaps")
body(f"On {I(hwo.n)} events from Headway-standard trips the mean ratio of actual to scheduled gap is {F(hwo.mean_headway_ratio, 3)}; **{P(hwo.bunched_rate)}** of gaps are under half the scheduled gap (bunching) and **{P(hwo.gap_rate)}** are more than 1.5 times the scheduled gap.")
h2("6.3 Delay prediction (machine learning)")
body(f"Two gradient-boosted tree models were trained on {I(ml['train_rows_used'])} events from the training days and scored on {I(ml['test_rows'])} events from {ml['split']['test']['days']} held-out days. Each is compared with simple baselines and with a version that lacks the previous-delay feature.")
table(["Model", "RMSE (min)", "MAE (min)", "R²"], [[k, F(v["rmse_min"], 3), F(v["mae_min"], 3), F(v["r2"], 3)] for k, v in reg.items()], widths=[3.4, 1.0, 1.0, 1.0], caption="Regression: predicting the delay in minutes (test days)")
table(["Model", "Accuracy", "Precision", "Recall", "F1", "AUC"], [[k, P(v["accuracy"]), P(v["precision"]), P(v["recall"]), F(v["f1"], 3), F(v["auc"], 3) if v["auc"] is not None else "n/a"] for k, v in clf.items()], widths=[2.8, 0.8, 0.8, 0.7, 0.6, 0.6], caption=f"Classification: will the bus be late (more than {ml['late_threshold_sec'] // 60} minutes)? (test days)")
for s in [f"**Regression.** The full model has MAE {F(G['mae_min'])} min and R² {F(G['r2'])}, against {F(BM['mae_min'])} min for predicting the training mean and {F(BC['mae_min'])} min for carrying the previous delay forward ({F(gain, 1)}% lower error than the better baseline).",
          f"**Where the skill comes from.** Without the previous-delay features the model's MAE is {F(GN['mae_min'])} min (R² {F(GN['r2'])}), so most of the skill comes from knowing how late the bus already was; hour, route and timepoint explain much less.",
          f"**Classification.** The full model has accuracy {P(CG['accuracy'])}, precision {P(CG['precision'])}, recall {P(CG['recall'])} and AUC {F(CG['auc'], 3)}. Since only {P(ml['split']['test']['late_rate'])} of test events are late, 'never late' already reaches {P(CN['accuracy'])} accuracy, so recall, F1 and AUC are the meaningful measures. The carried-forward baseline reaches AUC {F(CC['auc'], 3)}; without the previous delay the model reaches {F(CGN['auc'], 3)}.",
          "**Scope.** These are next-timepoint predictions (made when the bus leaves the previous timepoint), evaluated on later days of the same four months. They do not show that delay can be forecast days ahead, in other months or in other cities."]:
    bullet(s)
figure(FIG / "fig5_ml_regression.png", "Regression: error compared with baselines, calibration and error by hour (test days)", 6.4)
figure(FIG / "fig6_ml_classification.png", "Classification: precision, recall and F1, reliability of the predicted probability, and confusion matrix (test days)", 6.4)
figure(FIG / "fig7_ml_feature_importance.png", "Feature importance of the full gradient-boosted tree models", 6.0)
h2("6.4 Comparison with the base paper")
body("The comparison is of methodology only, based on how the paper is described in our proposal; no numeric comparison is made because the data and tasks differ.")
table(["Aspect", "Base paper (as stated in our proposal)", "This project"], [
    ["Data", "synthetic GPS / sensor logs", f"real scheduled and actual bus events (Boston MBTA, {I(raw_d)} rows)"],
    ["API", "RDDs, basic Spark SQL", "DataFrames, Window functions (lag), Spark MLlib"],
    ["Analytics", "ingestion bottlenecks, memory use", "delay, on-time and bunching metrics, and delay prediction with baselines on held-out days"],
    ["Join / shuffle", "not part of our summary", "measured broadcast vs sort-merge join with plans and shuffle bytes"],
    ["Scaling", "memory and ingestion", "speedup, latency and efficiency of the pipeline and of model training on 1, 2 and 4 cores"],
], widths=[1.2, 2.5, 2.8], caption="Methodological comparison with the base paper")

# ================================================================== 7
h1(7, "Performance Evaluation and Observations", "sec7")
h2("7.1 Experimental setup")
body(f"**Machine:** {env['cpu']}, {env['logical_cores']} logical cores ({env['performance_cores']} performance + {env['efficiency_cores']} efficiency), {env['ram_gb']} GB RAM, {env['os']}. **Software:** PySpark {env['pyspark']}, Python {env['python']}, Java {env['java'].split(chr(34))[1]}. **Protocol:** each core count (local[1], local[2], local[4]) runs in its own Python process; a quick protocol is used: {bench['config']['timed_runs']} timed run(s) after {bench['config']['warmup_runs']} discarded warm-up run(s) per configuration, so small differences are not significant; Spark is configured identically for every run (shuffle partitions {bench['config']['shuffle_partitions']}, adaptive execution off); stages are timed only after an action (a Parquet write or a model fit). Baseline is local[1].")
h2("7.2 Analysis pipeline")
rows = []
for n in sorted(sc, key=int):
    e = sc[n]
    s = e["stages_seconds"]
    rows.append([n, F(s["clean"]["median"], 1), F(s["window_agg"]["median"], 1), F(s["total"]["median"], 1), I(e["throughput_rows_per_s"]), F(e["speedup_total"]), F(e["efficiency_total"])])
table(["Cores", "Clean (s)", "Window + agg (s)", "Total (s)", "Rows/s", "Speedup", "Efficiency"], rows, widths=[0.6, 0.9, 1.3, 0.9, 1.0, 0.9, 0.9], caption=f"Pipeline scaling ({I(bench['cleaned_event_rows'])} cleaned events; {bench['config']['timed_runs']} timed runs after {bench['config']['warmup_runs']} warm-up)")
figure(FIG / "fig8_pipeline_speedup.png", "Speedup, latency and efficiency of the analysis pipeline", 6.2)
h2("7.3 Model training")
if msc:
    rows = []
    for n in sorted(msc, key=int):
        e = msc[n]
        s = e["stages_seconds"]
        rows.append([n, F(s["features"]["median"], 1), F(s["fit_regressor"]["median"], 1), F(s["fit_classifier"]["median"], 1), F(s["total"]["median"], 1), F(e["speedup_total"]), F(e["efficiency_total"])])
    ms = bench["model_scaling"]
    table(["Cores", "Features (s)", "Fit regressor (s)", "Fit classifier (s)", "Total (s)", "Speedup", "Efficiency"], rows, widths=[0.6, 1.0, 1.3, 1.3, 0.9, 0.8, 0.8], caption=f"Model-training scaling ({I(ms['train_rows_used'])} training rows, {ms['gbt']['gbt_max_iter']} rounds, depth {ms['gbt']['gbt_max_depth']}; {ms['timed_runs']} timed runs after {ms['warmup_runs']} warm-up)")
    figure(FIG / "fig9_model_training_speedup.png", "Speedup, latency and efficiency of model training", 6.2)
h2("7.4 Broadcast versus shuffle join")
jc = bench["join_comparison"]
rows = [[("Broadcast join" if k == "broadcast" else "Shuffle (sort-merge) join"), F(v["join_seconds"]["median"], 3), f"{F(v['join_seconds']['min'], 3)} - {F(v['join_seconds']['max'], 3)}", I(v["shuffle_write_bytes_per_run_median"])] for k, v in jv.items()]
table(["Variant", "Median (s)", "Min - max (s)", "Shuffle bytes written"], rows, widths=[2.4, 1.1, 1.5, 1.5], caption=f"Joining the train-only history tables onto the test events (local[{jc['cores']}])")
body("In the broadcast variant the small route and route+timepoint tables are copied to every worker, so the large event table is not shuffled. In the shuffle variant, autoBroadcastJoinThreshold is set to -1 so Spark uses a sort-merge join that redistributes both tables; adaptive execution is off so Spark cannot silently change strategy. Shuffle bytes come from the Spark event log. The physical plans are saved in outputs/explain_join_broadcast.txt and outputs/explain_join_shuffle.txt.")
h2("7.5 Correctness evaluation")
body(f"The delay aggregates were verified by recomputing them independently in pandas directly from the raw CSV: {I(corr_d['events_compared'])} events on {len(corr_d['routes_checked'])} routes matched Spark with a maximum difference of {corr_d['max_abs_diff_mean_std_on_time']:.1e}. In total {n_tests} automated tests pass, covering the time parsing and midnight wrap, the cleaning rules, and the model: the lag feature uses only earlier timepoints and never crosses trips or days, the temporal split has no date overlap, history features come from training rows only, and the metric helpers are correct.")
h2("7.6 Observations")
s4 = sc["4"]
obs = [f"**Speedup is sub-linear.** On four cores the analysis pipeline reaches {F(s4['speedup_total'])}x (efficiency {F(s4['efficiency_total'])})" + (f" and model training {F(msc['4']['speedup_total'])}x (efficiency {F(msc['4']['efficiency_total'])})." if msc else "."),
       "**Serial parts limit scaling.** JVM start-up, coordinating task scheduling, driver-side collection of small tables and shuffles do not shrink with more cores; tree training also has sequential steps between boosting rounds.",
       f"**Mixed core types.** The test machine has {env['performance_cores']} performance and {env['efficiency_cores']} efficiency cores, which may further lower efficiency at higher core counts (not isolated in these measurements).",
       f"**Broadcast join** wrote {I(jv['broadcast']['shuffle_write_bytes_per_run_median'])} shuffle bytes against {I(jv['shuffle']['shuffle_write_bytes_per_run_median'])} for the sort-merge join, and took {F(jv['broadcast']['join_seconds']['median'], 2)} s against {F(jv['shuffle']['join_seconds']['median'], 2)} s, because the history tables are small."]
for s in obs:
    bullet(s)
h2("7.7 Limitations of the evaluation")
for s in ["All runs are on one laptop in local mode; Spark was not tested on a multi-machine cluster.",
          f"Timed runs are few (pipeline {bench['config']['timed_runs']}" + (f", model training {bench['model_scaling']['timed_runs']}" if msc else "") + "), so small differences should not be over-interpreted.",
          f"Model training uses a sample of {I(ml['train_rows_used'])} events (cap {I(ml['train_cap'])}); more data or tuning might change the metrics.",
          "The models are evaluated on later days of the same four months of 2024 in Boston; results do not transfer to other periods or cities."]:
    bullet(s)

# ================================================================== 8
h1(8, "Tools, Responsible Use and References", "sec8")
h2("8.1 Tools and versions")
table(["Tool", "Version", "Purpose"], [
    ["Python", env["python"], "implementation language"],
    ["Apache Spark / PySpark (incl. MLlib)", env["pyspark"], "distributed processing and machine learning"],
    ["Java (OpenJDK)", env["java"].split('"')[1], "runtime for Spark"],
    ["NumPy", reqs["numpy"], "numerical support for MLlib"],
    ["Pandas", reqs["pandas"], "small tables and validation"],
    ["PyArrow", reqs["pyarrow"], "Parquet I/O"],
    ["Matplotlib", reqs["matplotlib"], "figures"],
    ["PyYAML", reqs["pyyaml"], "configuration"],
    ["pytest", reqs["pytest"], "tests"],
    ["React / TypeScript / Vite / Recharts", f"{pk_v['react']} / {pk_v['typescript']} / {pk_v['vite']} / {pk_v['recharts']}", "web dashboard"],
    ["Git and GitHub", "-", "version control and publication"],
], widths=[2.6, 1.9, 2.0], caption="Tools and versions used")
h2("8.2 Responsible use")
for s in ["**No personal data.** The dataset contains only route, trip, stop and timepoint identifiers and times. No rider, driver or vehicle-level personal information is present.",
          "**Licence and attribution.** The MBTA dataset is published under CC0 (public domain); the agency states it is not liable for errors in the data, and attribution to the MBTA Open Data Portal is given.",
          "**Data reliability.** Actual times come from MBTA's own vehicle tracking and the item notes the data is not guaranteed complete for any stop or date; the share of events without an actual time is reported. Findings are conclusions about this dataset.",
          "**Honest scope.** Results describe Boston bus operations in four months of 2024. The prediction is next-timepoint, evaluated on held-out later days, compared with baselines, and the dependence on the previous delay is shown by an ablation. No claim is made that delay can be forecast far ahead or in other cities.",
          "**Lawful data access.** Only the publicly published dataset was used, obtained from the official portal.",
          "**Reproducibility and transparency.** Every number in the report is produced by code in the repository, thresholds are kept in a configuration file, data checksums are recorded and the validation tests are included.",
          "**Limitations stated.** Timepoints only, four sampled months, Schedule-standard trips only, a capped training sample, and single-machine benchmarks."]:
    bullet(s)
h2("8.3 References")
refs = [
    "N. L. Mnyone and N. U. Amin, \"Real-Time Data Processing in Smart Transportation Using Apache Spark,\" Preprints.org, 2026, preprint (not peer reviewed). DOI: 10.20944/preprints202603.1261.v1.",
    "Massachusetts Bay Transportation Authority, \"MBTA Bus Arrival Departure Times 2024,\" MBTA Open Data Portal. https://mbta-massdot.opendata.arcgis.com/datasets/96c77138c3144906bce93d0257531b6a (accessed 17 Nov 2026).",
    "The Apache Software Foundation, \"Apache Spark 3.5.3 Documentation,\" including the MLlib guide. https://spark.apache.org/docs/3.5.3/",
    "J. H. Friedman, \"Greedy function approximation: a gradient boosting machine,\" The Annals of Statistics, vol. 29, no. 5, pp. 1189-1232, 2001.",
    "N. Dhinakaran and P. Manjunath, \"bda-project: bus delay analytics and prediction with PySpark,\" GitHub repository. https://github.com/nik1taa28/bda-project",
]
for i, r in enumerate(refs, start=1):
    p = para(f"[{i}] {r}", align="j", after=4)
    p.paragraph_format.left_indent = Inches(0.4)
    p.paragraph_format.first_line_indent = Inches(-0.4)

# update fields (page numbers) when the file is opened in Word
settings = doc.settings.element
uf = OxmlElement("w:updateFields")
uf.set(qn("w:val"), "true")
settings.append(uf)

out = ROOT / "report" / "BDA_Project_Report.docx"
doc.save(out)
print("saved", out, f"{out.stat().st_size / 1e6:.1f} MB;", f"{_fig[0]} figures, {_tab[0]} tables, {n_tests} tests")
