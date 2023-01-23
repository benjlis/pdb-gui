import streamlit as st
import plotly.express as px
import pandas as pd
import psycopg2


VERSION = 'Beta-v0.1'
TITLE = f'PDB Search'
SEARCH_PLACEHOLDER = 'Search the FOIArchive''s PDB collection'
SEARCH_HELP = 'Use double quotes for phrases, OR for logical or, and - for \
logical not.'

st.set_page_config(page_title=TITLE, layout="wide")
st.title(TITLE)
st.caption(VERSION)
st.image('assets/pdb.jpg', use_column_width='always')
with open("./assets/pdb.md", "r") as f:
    markdown_text = f.read()
st.markdown(markdown_text)


# Database functions
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])


# Perform query.
# Uses to only rerun when the query changes or after 3 hours
@st.experimental_memo(ttl=9800)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


# Database initialization
conn = init_connection()
doc_dist_qry = """
select extract(year from authored)::integer yr, corpus collection,
       count(*) doccnt
   from foiarchive.docs
   where full_text @@ websearch_to_tsquery('english', '{search}') and
         corpus='pdb'
   group by yr, corpus
   order by yr, doccnt
"""

doc_agg_qry = """
select count(*) total_docs, min(authored) from_date, max(authored) to_date
   from foiarchive.docs
   where full_text @@ websearch_to_tsquery('english', '{search}') and
         corpus='pdb'
"""


# Search
srchstr = st.text_input(label='',
                        placeholder=SEARCH_PLACEHOLDER,
                        help=SEARCH_HELP)
if srchstr:
    doc_dist_df = pd.read_sql_query(doc_dist_qry.format(search=srchstr), conn)
    if len(doc_dist_df):
        fig = px.bar(doc_dist_df, x='yr', y='doccnt', color='collection',
                     labels={'yr': 'Year',
                             'doccnt': 'Number of Documents',
                             'collection': 'Collections:'})
        fig.update_layout(legend={'orientation': 'h',
                                  'yanchor': 'bottom',
                                  'y': 1.02,
                                  'xanchor': 'right',
                                  'x': 1})
        st.plotly_chart(fig, use_container_width=True)
        aggs = run_query(doc_agg_qry.format(search=srchstr))
        doccnt = aggs[0][0]
        firstdt = aggs[0][1]
        lastdt = aggs[0][2]
        st.markdown(f"## {doccnt} FOIArchive docs contain {srchstr}")
        st.write(f"First Occurrence {firstdt}")
        st.write(f"Last Occurrence {lastdt}")
        st.table(doc_dist_df)
    else:
        st.markdown(f"Your search `{srchstr}` did not match any documents")
