import polars as pl
import streamlit as st


storage_options = {
    'key': st.secrets['storage']['aws_access_key_id'],
    'secret': st.secrets['storage']['aws_secret_access_key'],
    'endpoint_url': st.secrets['storage']['endpoint_url']
}


@st.cache_data
def get_determinands():
    df_determinands = pl.read_csv(
        's3://ea-water-quality/EA_WQA_determinands_by-sampleMaterialType.csv',
        storage_options=storage_options
    )
    # Get the determinands (by notation and prefLabel)
    df_ = (
        df_determinands
        .select(['determinand.notation', 'determinand.prefLabel', 'unit'])
        .unique()
        .drop_nulls('determinand.prefLabel')
        .sort('determinand.prefLabel')
    )
    determinand_lookup = {f'{row["determinand.prefLabel"]} ({row["determinand.notation"]})': row['determinand.notation']
                          for row in df_.iter_rows(named=True)}
    # Get a unique list of sample material types
    sample_material_types = df_determinands['sampleMaterialType'].unique().to_list()
    return determinand_lookup, sample_material_types


determinand_lookup, sample_material_types = get_determinands()
determinands = st.multiselect('Select determinands',
                              options=list(determinand_lookup.keys()))
determinand_notations = [determinand_lookup[d] for d in determinands]

st.write('## Detects vs determinations')
st.write("""For the selected determinands, this shows the total number
of measurements (determinations) compared to the number of measurements
where the determinand(s) was detected (i.e. the value was above the limit
of detection). You can use the following filter to only filter to specific
sample material types.""")

# Create a dataframe for the given determinands
if len(determinand_notations) > 0:
    dfs = []
    for notation in determinand_notations:
        df_ = pl.scan_parquet(f's3://ea-water-quality/determinand_{notation}.parquet',
                              storage_options=dict(st.secrets['storage']))
        dfs.append(df_)

    df = pl.concat(dfs)
    sample_material = st.multiselect('Filter by sample material type',
                                     options=sample_material_types,
                                     default=['FINAL SEWAGE EFFLUENT',
                                              'ANY TRADE EFFLUENT'])

    # Fitler the dataframe to the selected sample material types
    # and calculate the relevant stats
    df = df.filter(pl.col('sampleMaterialType').is_in(sample_material)).collect()
    total_determinations = len(df)
    df_detects = df.filter(~pl.col('result').str.starts_with('<'))
    total_detects = len(df_detects)
    n_sampling_points = df_detects['samplingPoint.prefLabel'].n_unique()
    proportion_detects = float(total_detects) / float(total_determinations) \
        if total_determinations > 0 else 0

    # Display the stats
    c1, c2 = st.columns(2)
    c1.metric('Number of measurements', f'{total_determinations:,}', border=True)
    c1.metric('Number of detects', f'{total_detects:,}', border=True)
    c2.metric('Detection proportion', f'{proportion_detects:.1%}', border=True)
    c2.metric('Number of sampling points', f'{n_sampling_points:,}', border=True)
else:
    st.warning('Please select at least one determinand to see the data.')
