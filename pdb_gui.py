import streamlit as st
import pandas as pd
import psycopg2


VERSION = 'Beta-v0.2'
TITLE = f'PDB Search'
SEARCH_PLACEHOLDER = "Search the FOIArchive's PDB collection"
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
select extract(year from authored)::integer "Year", count(*) "PDBs"
   from foiarchive.docs
   where full_text @@ websearch_to_tsquery('english', '{search}') and
         corpus='pdb'
   group by "Year"
   order by "Year"
"""

doc_agg_qry = """
select count(*) total_docs, min(authored) from_date, max(authored) to_date
   from foiarchive.docs
   where full_text @@ websearch_to_tsquery('english', '{search}') and
         corpus='pdb'
"""

doc_qry = """
select to_char(authored,'YYYY-MM-DD') date, 
       '[' || f.title || '](' || source || ')' pdb,
       d.page_count pages, d.redactions  
    from foiarchive.docs f join declassification_pdb.docs d 
                                on (f.doc_id = d.id) 
    where full_text @@ websearch_to_tsquery('english', '{search}') and
          corpus = 'pdb'
    order by authored;
"""

@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

# Search
srchstr = st.text_input(label='',
                        placeholder=SEARCH_PLACEHOLDER,
                        help=SEARCH_HELP)
if srchstr:
    doc_dist_df = pd.read_sql_query(doc_dist_qry.format(search=srchstr), conn)
    if len(doc_dist_df):
        aggs = run_query(doc_agg_qry.format(search=srchstr))
        doccnt = aggs[0][0]
        firstdt = aggs[0][1]
        lastdt = aggs[0][2]
        st.subheader(f"{doccnt} PDBs mention `{srchstr}`")
        # st.write(f"First Occurrence {firstdt}")
        # st.write(f"Last Occurrence {lastdt}")
        st.bar_chart(doc_dist_df, x="Year", y="PDBs")
        doc_df = pd.read_sql_query(doc_qry.format(search=srchstr), conn)
        csv = convert_df(doc_df)
        st.download_button(label='CSV Download', data=csv,
                           file_name='pdb.csv', mime='text/csv')
        st.markdown(doc_df.to_markdown(index=False))
    else:
        st.markdown(f"Your search `{srchstr}` did not match any documents")
