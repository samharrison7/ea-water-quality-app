import streamlit as st


pg = st.navigation([
    st.Page('pages/home.py', title='Home', icon="🏠"),
    st.Page('pages/determinand.py', title='Determinand', icon="⚛️"),
])
pg.run()
