import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from scipy import stats


storage_options = {
    'key': st.secrets['storage']['aws_access_key_id'],
    'secret': st.secrets['storage']['aws_secret_access_key'],
    'endpoint_url': st.secrets['storage']['endpoint_url']
}


# Each entry: original_unit → (canonical_family, factor_to_canonical, pretty_label).
# Rows whose unit is not in this table are passed through as their own canonical
# family with factor 1.0 — so single-unit datasets still work and any unrecognised
# unit only triggers an "incompatible units" error when mixed with something else.
UNIT_CONVERSIONS = {
    'GRAM PER LITRE':            ('g/L',  1.0,    'g/L'),
    'MILLIGRAM PER LITRE':       ('g/L',  1e-3,   'mg/L'),
    'MICROGRAM PER LITRE':       ('g/L',  1e-6,   'μg/L'),
    'NANOGRAM PER LITRE':        ('g/L',  1e-9,   'ng/L'),
    'Picograms per litre':       ('g/L',  1e-12,  'pg/L'),
    'GRAM PER KILOGRAM':         ('g/kg', 1.0,    'g/kg'),
    'MILLIGRAM PER KILOGRAM':    ('g/kg', 1e-3,   'mg/kg'),
    'MICROGRAM PER KILOGRAM':    ('g/kg', 1e-6,   'μg/kg'),
    'NANOGRAM PER KILOGRAM':     ('g/kg', 1e-9,   'ng/kg'),
    'Milligrams per cubic metre':('g/m³', 1e-3,   'mg/m³'),
    'Micrograms per cubic metre':('g/m³', 1e-6,   'μg/m³'),
    'Picograms per cubic metre': ('g/m³', 1e-12,  'pg/m³'),
}


def ros_impute(values: np.ndarray, is_censored: np.ndarray) -> np.ndarray | None:
    """Impute left-censored values via OLS regression of log(detects) on
    standard-normal quantiles of plotting positions. Returns a new array
    where censored entries are replaced by the regression prediction
    (capped at the original detection limit), or None if the regression
    cannot be fit (too few detects, zero variance, or non-positive values).
    """
    n = len(values)
    if n == 0 or np.any(values <= 0):
        return None

    # Sort: non-detects sort just below detects of the same magnitude.
    order = np.lexsort((is_censored.astype(int) * -1, values))
    ranks = np.empty(n, dtype=np.int64)
    ranks[order] = np.arange(n)

    p = (ranks + 1) / (n + 1)
    z = stats.norm.ppf(p)

    detect_mask = ~is_censored
    if detect_mask.sum() < 2:
        return None
    z_det = z[detect_mask]
    if z_det.std() == 0:
        return None

    fit = stats.linregress(z_det, np.log(values[detect_mask]))

    imputed = values.copy().astype(float)
    cens_idx = np.where(is_censored)[0]
    predicted = np.exp(fit.intercept + fit.slope * z[cens_idx])
    imputed[cens_idx] = np.minimum(predicted, values[cens_idx])
    return imputed


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
    # For each determinand, which sample material types actually have measurements.
    # Used to restrict the sample-material dropdown to relevant options.
    sample_materials_by_determinand = {
        row['determinand.notation']: row['materials']
        for row in df_determinands
        .group_by('determinand.notation')
        .agg(pl.col('sampleMaterialType').unique().alias('materials'))
        .iter_rows(named=True)
    }
    return determinand_lookup, sample_materials_by_determinand


determinand_lookup, sample_materials_by_determinand = get_determinands()
determinands = st.multiselect('Select determinands',
                              options=list(determinand_lookup.keys()))
determinand_notations = [determinand_lookup[d] for d in determinands]
sample_material_types = sorted({
    sm
    for n in determinand_notations
    for sm in sample_materials_by_determinand.get(n, [])
})

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
                                     options=sample_material_types)

    # Filter to the selected sample material types and parse `result` into
    # a censored flag + numeric value. Rows that fail to parse are dropped.
    canonical_map = {k: v[0] for k, v in UNIT_CONVERSIONS.items()}
    factor_map = {k: v[1] for k, v in UNIT_CONVERSIONS.items()}
    df = (
        df.filter(pl.col('sampleMaterialType').is_in(sample_material))
        .collect()
        .with_columns(
            is_censored=pl.col('result').str.starts_with('<'),
            result_value=pl.col('result').str.strip_prefix('<').cast(pl.Float64, strict=False),
            unit_canonical=pl.col('unit').replace(canonical_map),
            unit_factor=pl.col('unit').replace_strict(
                list(factor_map.keys()),
                list(factor_map.values()),
                default=1.0,
                return_dtype=pl.Float64,
            ),
        )
        .drop_nulls('result_value')
    )

    total_determinations = len(df)
    df_detects = df.filter(~pl.col('is_censored'))
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

    st.write('## Distribution')
    st.write("""Distribution of the measurement values for the selected
    determinand(s) and sample material type(s). Use the radio buttons to
    choose how non-detects (results reported as `<value`) are handled.""")

    if total_determinations == 0:
        st.info('No measurements for this selection.')
    else:
        canonicals = df['unit_canonical'].unique().to_list()
        if len(canonicals) > 1:
            group_lines = [
                f'**{r["unit_canonical"]}**: {", ".join(r["units"])}'
                for r in df.group_by('unit_canonical')
                .agg(pl.col('unit').unique().alias('units'))
                .sort('unit_canonical')
                .to_dicts()
            ]
            st.error(
                'Cannot plot distribution — selected determinands use incompatible '
                'units (no simple conversion between these families):\n\n'
                + '\n\n'.join(group_lines)
            )
        else:
            # Pick the most common original unit as the display unit and convert
            # every row's value into it. For single-unit selections this is a
            # no-op (factor cancels out).
            unit_counts = df.group_by('unit').len().sort('len', descending=True)
            display_unit = unit_counts['unit'][0]
            display_factor = UNIT_CONVERSIONS.get(display_unit, (display_unit, 1.0))[1]
            display_label = UNIT_CONVERSIONS.get(display_unit, (None, None, display_unit))[2]

            df = df.with_columns(
                result_value=pl.col('result_value') * pl.col('unit_factor') / display_factor
            )

            converted = unit_counts.filter(pl.col('unit') != display_unit)
            if converted.height > 0:
                msg = ', '.join(
                    f'{r["unit"]} ({r["len"]:,} rows)' for r in converted.to_dicts()
                )
                st.caption(f'Converted to {display_label}: {msg}.')

            strategy = st.radio(
                'Non-detect handling',
                ['Linear regression (ROS)', 'Substitute (LoD/2)', 'Exclude'],
                horizontal=True,
            )
            log_axis = st.toggle('Log-scale x-axis', value=True)

            values_np = df['result_value'].to_numpy()
            censored_np = df['is_censored'].to_numpy()

            if strategy == 'Exclude':
                values = values_np[~censored_np]
            elif strategy == 'Substitute (LoD/2)':
                values = np.where(censored_np, values_np / 2, values_np)
            else:  # Linear regression (ROS)
                imputed = ros_impute(values_np, censored_np)
                if imputed is None:
                    st.warning(
                        'Could not fit ROS regression (need ≥ 2 positive detects '
                        'with variance). Falling back to LoD/2 substitution.'
                    )
                    values = np.where(censored_np, values_np / 2, values_np)
                else:
                    values = imputed

            if len(values) == 0:
                st.info('No measurements available after applying the chosen strategy.')
            else:
                if log_axis and np.any(values <= 0):
                    st.warning('Log-scale x-axis disabled: data contains values ≤ 0.')
                    log_axis = False

                geomean = float(np.exp(np.mean(np.log(values)))) if np.all(values > 0) else None
                category = '<br>'.join([
                    ', '.join(determinands),
                    ', '.join(sample_material),
                ])

                pdf = pl.DataFrame({
                    'result_value': values,
                    'category': [category] * len(values),
                }).to_pandas()

                fig = px.box(
                    pdf,
                    x='result_value',
                    y='category',
                    orientation='h',
                    points='outliers',
                    log_x=log_axis,
                )
                fig.update_traces(showlegend=False, selector=dict(type='box'))
                fig.update_layout(
                    xaxis_title=f'Value ({display_label})',
                    yaxis_title=None,
                    showlegend=False,
                    height=320,
                    margin=dict(l=10, r=10, t=10, b=10),
                )
                fig.update_yaxes(visible=False)

                if geomean is not None:
                    fig.add_trace(go.Scatter(
                        x=[geomean],
                        y=[category],
                        mode='markers',
                        marker=dict(symbol='diamond', color='red', size=12,
                                    line=dict(color='black', width=0.5)),
                        name='Geomean',
                        hovertemplate=f'Geomean: %{{x:.4g}} {display_label}<extra></extra>',
                    ))

                st.caption(f'n = {len(values):,}' + (
                    f' · geomean (red diamond) = {geomean:.4g} {display_label}'
                    if geomean is not None else ' · geomean: n/a (data contains values ≤ 0)'
                ))
                st.plotly_chart(fig, use_container_width=True)
else:
    st.warning('Please select at least one determinand to see the data.')
