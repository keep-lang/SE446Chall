# app.py
# T4 - Streamlit dashboard. Reads the JSONL batches written by ingester.py
# directly with pandas. The streaming pipeline (readStream/writeStream into
# three memory sinks) runs in streaming_job.py - the dashboard just visualises
# the same JSONL data the streaming job is consuming.
#
# Why pandas instead of spark.sql here:
#   PySpark 3.5.x calls distutils.version.LooseVersion in its toPandas() path,
#   and distutils was removed in Python 3.12. Reading JSONL directly is
#   faster anyway for a dashboard that re-renders every 7 seconds.
import glob
import json
import re
import time
from collections import Counter

import pandas as pd
import streamlit as st

from llm_summary import summarise

STOP_WORDS = {
    "the","a","an","and","or","but","if","then","else","for","of","to","in","on",
    "at","by","with","from","is","are","was","were","be","been","being","this",
    "that","these","those","it","its","as","not","no","do","does","did","has",
    "have","had","will","would","can","could","should","may","might","says","say",
    "said","new","over","into","up","down","out","off","about","after","before",
    "you","your","we","our","i","he","she","they","them","his","her","their",
    "us","more","than","also","just","like","get","got","one","two","amid","via",
    "what","when","where","who","how","why","which","while","than","there","here",
}

WORD_RE = re.compile(r"[a-z]{3,}")


def load_df() -> pd.DataFrame:
    """Read every JSONL batch the ingester has dropped."""
    rows = []
    for path in sorted(glob.glob("data/incoming/batch_*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except OSError:
            continue
    if not rows:
        return pd.DataFrame(columns=["source", "title", "url", "ts"])
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    return df


st.set_page_config(page_title="News Pulse - live", layout="wide")
st.title("News Pulse - live")
st.caption("RSS -> Spark -> Streamlit. Page auto-refreshes every 7s. "
           "Streaming pipeline runs in streaming_job.py (memory sinks: by_source, by_window, top_words).")

df = load_df()
total = len(df)
st.metric("Headlines processed", f"{total:,}")

col1, col2 = st.columns(2)

# ---- by_source --------------------------------------------------------------
with col1:
    st.subheader("Headlines by source")
    if total == 0:
        st.info("No data yet. Make sure ingester.py is running.")
    else:
        by_src = df.groupby("source").size().sort_values(ascending=False)
        st.bar_chart(by_src)

# ---- by_window (1-minute tumbling) -----------------------------------------
with col2:
    st.subheader("Headlines per 1-minute window")
    if total == 0:
        st.info("No data yet.")
    else:
        ts_series = df["ts"].dropna()
        if ts_series.empty:
            st.info("No timestamped rows yet.")
        else:
            by_win = (df.dropna(subset=["ts"])
                        .assign(window=lambda x: x["ts"].dt.floor("1min"))
                        .groupby("window").size()
                        .sort_index())
            st.line_chart(by_win)

# ---- top_words --------------------------------------------------------------
st.subheader("Top words")
if total == 0:
    top_df = pd.DataFrame(columns=["word", "count"])
    st.info("No tokens yet.")
else:
    counter = Counter()
    for title in df["title"].dropna():
        for w in WORD_RE.findall(title.lower()):
            if w not in STOP_WORDS:
                counter[w] += 1
    top_df = pd.DataFrame(counter.most_common(15), columns=["word", "count"])
    st.dataframe(top_df, use_container_width=True, hide_index=True)

# ---- LLM summary ------------------------------------------------------------
st.subheader("Thematic summary (LLM)")
pairs = [] if top_df.empty else list(zip(top_df["word"].tolist(), top_df["count"].tolist()))
summary = summarise(pairs)
st.text_area("summary", value=summary, height=130, label_visibility="collapsed")

st.caption("Auto-refresh in 7s.")
time.sleep(7)
st.rerun()
