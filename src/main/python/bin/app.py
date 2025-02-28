import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import seaborn as sns

# st.set_page_config(
#     page_title="Dashboard",
#     page_icon="🏂",
#     layout="wide",
#     initial_sidebar_state="expanded")

from urllib.request import urlopen
import json
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

df_city = pd.read_csv('/home/anmol/PycharmProjects/pipeline/pipeline/src/main/python/final/city/part-00000-3681d02f-af5f-4bc9-a23c-485b145d55af-c000.csv')
df_presc = pd.read_csv('/home/anmol/PycharmProjects/pipeline/pipeline/src/main/python/final/presc/part-00000-911e399a-4286-4bca-985f-4793343ae79a-c000.csv')

# st.write(df_city)
# st.write(df_presc)
#
#
# # fig = px.choropleth(df_presc, geojson=counties, locations='presc_state', color='total_drug_cost', featureidkey="properties.code", color_continuous_scale="Viridis",
# #                        range_color=(0, 12),
# #                        scope="usa",
# #                        labels={'total_drug_cost':'Total Drug Cost'})
# # st.plotly_chart(fig)
#
# geojson = px.data.election_geojson()
# df_presc['presc_state'] = df_presc['presc_state'].astype(str)
#
# # Creating the choropleth
# fig = px.choropleth(
#     df_presc,
#     geojson=geojson,
#     locations='presc_state',
#     color='total_drug_cost',
#     featureidkey="properties.district",
#     locationmode = 'USA-states',
#     color_continuous_scale="Viridis",
#     range_color=(0, 12),
#     scope="usa",
#     labels={'total_drug_cost': 'Total Drug Cost'}
# )
#
# # Display the figure in Streamlit
# st.plotly_chart(fig)

# Page Title
st.set_page_config(
        page_title="Dashboard",
)

# Streamlit App Title
st.title('City and Prescription Dashboard')

# Create a sidebar for navigation
st.sidebar.title("Navigation")
options = st.sidebar.radio("Select a page", ("City Data", "Prescription Data", "Drug Cost Insights"))

if options == "City Data":
    # Plot: Population by City (Bar Chart)
    st.subheader("City Population")
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x='city', y='population', data=df_city, ax=ax)
    plt.xticks(rotation=90)
    ax.set_title("Population by City")
    st.pyplot(fig)

    # Plot: Population Distribution (Histogram)
    st.subheader("Population Distribution Across Cities")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(df_city['population'], bins=20, color='blue', alpha=0.7)
    ax.set_xlabel('Population')
    ax.set_ylabel('Frequency')
    ax.set_title('Population Distribution Across Cities')
    st.pyplot(fig)

    # Plot: Top 10 Cities by Population
    st.subheader("Top 10 Cities by Population")
    top_10_cities = df_city.nlargest(10, 'population')
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x='city', y='population', data=top_10_cities, ax=ax)
    plt.xticks(rotation=45)
    ax.set_title("Top 10 Cities by Population")
    st.pyplot(fig)

elif options == "Prescription Data":
    # Plot: Prescription Count by State (Bar Chart)
    st.subheader("Prescription Count by State")
    presc_state_counts = df_presc['presc_state'].value_counts().reset_index()
    presc_state_counts.columns = ['State', 'Prescription Count']
    fig = px.bar(presc_state_counts, x='State', y='Prescription Count', title="Prescription Count by State")
    st.plotly_chart(fig)

    # Plot: Prescription Count vs Drug Cost (Scatter Plot)
    st.subheader("Prescription Count vs Drug Cost")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(df_presc['trx_cnt'], df_presc['total_drug_cost'])
    ax.set_xlabel("Prescription Count (trx_cnt)")
    ax.set_ylabel("Total Drug Cost")
    ax.set_title("Prescription Count vs Drug Cost")
    st.pyplot(fig)

    # Plot: Prescription Count by Year (Bar Chart)
    st.subheader("Prescription Count by Year")
    df_presc['year_exp'] = df_presc['year_exp'].astype(str)  # Ensure 'year_exp' is treated as a string for better visualization
    presc_year_counts = df_presc['year_exp'].value_counts().reset_index()
    presc_year_counts.columns = ['Year', 'Prescription Count']
    fig = px.bar(presc_year_counts, x='Year', y='Prescription Count', title="Prescription Count by Year")
    st.plotly_chart(fig)

    # Plot: Top 10 Cities by Prescription Count
    st.subheader("Top 10 Cities by Prescription Count")
    top_10_cities_presc = df_presc.groupby('presc_state').agg({'trx_cnt': 'sum'}).reset_index()
    top_10_cities_presc = top_10_cities_presc.nlargest(10, 'trx_cnt')
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x='presc_state', y='trx_cnt', data=top_10_cities_presc, ax=ax)
    plt.xticks(rotation=45)
    ax.set_title("Top 10 Cities by Prescription Count")
    st.pyplot(fig)

elif options == "Drug Cost Insights":
    # Plot: Drug Cost vs Prescription Count (Scatter Plot)
    st.subheader("Drug Cost vs Prescription Count")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(df_presc['trx_cnt'], df_presc['total_drug_cost'])
    ax.set_xlabel("Prescription Count (trx_cnt)")
    ax.set_ylabel("Total Drug Cost")
    ax.set_title("Drug Cost vs Prescription Count")
    st.pyplot(fig)

    # Plot: Total Drug Cost by State (Bar Chart)
    st.subheader("Total Drug Cost by State")
    drug_cost_state = df_presc.groupby('presc_state')['total_drug_cost'].sum().reset_index()
    fig = px.bar(drug_cost_state, x='presc_state', y='total_drug_cost', title="Total Drug Cost by State")
    st.plotly_chart(fig)

    # Plot: Total Drug Cost vs Prescription Count for Top 10 Prescribers (Scatter Plot)
    st.subheader("Total Drug Cost vs Prescription Count for Top 10 Prescribers")
    top_10_prescribers = df_presc.groupby('presc_fullname').agg({'trx_cnt': 'sum', 'total_drug_cost': 'sum'}).reset_index()
    top_10_prescribers = top_10_prescribers.nlargest(10, 'trx_cnt')
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(top_10_prescribers['trx_cnt'], top_10_prescribers['total_drug_cost'])
    ax.set_xlabel("Prescription Count (trx_cnt)")
    ax.set_ylabel("Total Drug Cost")
    ax.set_title("Top 10 Prescribers: Drug Cost vs Prescription Count")
    st.pyplot(fig)