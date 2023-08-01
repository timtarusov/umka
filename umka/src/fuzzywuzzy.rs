use fuzzywuzzy::fuzz::{partial_ratio, ratio, token_set_ratio, token_sort_ratio};
use polars::export::num::ToPrimitive;
use polars_core::prelude::*;
use polars_core::utils::accumulate_dataframes_vertical;
use rayon::prelude::*;

fn split_offsets(len: usize, n: usize) -> Vec<(usize, usize)> {
    if n == 1 {
        return vec![(0, len)];
    } else {
        let chunk_size = len / n;

        (0..n)
            .map(|partition| {
                let offset = partition * chunk_size;
                let len = if partition == n - 1 {
                    len - offset
                } else {
                    chunk_size
                };
                (partition * chunk_size, len)
            })
            .collect()
    }
}

fn fuzz_ratio(sa: &Series, sb: &Series) -> PolarsResult<Series> {
    let sa = sa.utf8()?;
    let sb = sb.utf8()?;
    let ca = sa
        .into_iter()
        .zip(sb.into_iter())
        .map(|(a, b)| match (a, b) {
            (Some(a), Some(b)) => {
                let score = ratio(&a, &b).to_f64().unwrap();
                Ok(Some(score))
            }
            _ => Ok(None),
        })
        .collect::<PolarsResult<Float64Chunked>>()?;
    Ok(ca.into_series())
}

fn fuzz_partial_ratio(sa: &Series, sb: &Series) -> PolarsResult<Series> {
    let sa = sa.utf8()?;
    let sb = sb.utf8()?;
    let ca = sa
        .into_iter()
        .zip(sb.into_iter())
        .map(|(a, b)| match (a, b) {
            (Some(a), Some(b)) => {
                let score = partial_ratio(&a, &b).to_f64().unwrap();
                Ok(Some(score))
            }
            _ => Ok(None),
        })
        .collect::<PolarsResult<Float64Chunked>>()?;
    Ok(ca.into_series())
}

fn fuzz_token_set_ratio(sa: &Series, sb: &Series) -> PolarsResult<Series> {
    let sa = sa.utf8()?;
    let sb = sb.utf8()?;
    let ca = sa
        .into_iter()
        .zip(sb.into_iter())
        .map(|(a, b)| match (a, b) {
            (Some(a), Some(b)) => {
                let score = token_set_ratio(&a, &b, true, true).to_f64().unwrap();
                Ok(Some(score))
            }
            _ => Ok(None),
        })
        .collect::<PolarsResult<Float64Chunked>>()?;
    Ok(ca.into_series())
}

fn fuzz_token_sort_ratio(sa: &Series, sb: &Series) -> PolarsResult<Series> {
    let sa = sa.utf8()?;
    let sb = sb.utf8()?;
    let ca = sa
        .into_iter()
        .zip(sb.into_iter())
        .map(|(a, b)| match (a, b) {
            (Some(a), Some(b)) => {
                let score = token_sort_ratio(&a, &b, true, true).to_f64().unwrap();
                Ok(Some(score))
            }
            _ => Ok(None),
        })
        .collect::<PolarsResult<Float64Chunked>>()?;
    Ok(ca.into_series())
}

fn parallellize(
    df: DataFrame,
    cola: &str,
    colb: &str,
    name: &str,
    func: fn(&Series, &Series) -> PolarsResult<Series>,
) -> PolarsResult<DataFrame> {
    let offsets = split_offsets(df.height(), rayon::current_num_threads());
    let dfs = offsets
        .par_iter()
        .map(|(offset, len)| {
            let sub_df = df.slice(*offset as i64, *len);
            let ca = sub_df.column(cola).unwrap();
            let cb = sub_df.column(colb).unwrap();
            let out = func(ca, cb)?;
            df! {
               name => out
            }
        })
        .collect::<PolarsResult<Vec<_>>>()?;
    accumulate_dataframes_vertical(dfs)
}

pub(super) fn pl_ratio(df: DataFrame, cola: &str, colb: &str) -> PolarsResult<DataFrame> {
    parallellize(df, cola, colb, "ratio", fuzz_ratio)
}

pub(super) fn pl_partial_ratio(df: DataFrame, cola: &str, colb: &str) -> PolarsResult<DataFrame> {
    parallellize(df, cola, colb, "partial_ratio", fuzz_partial_ratio)
}

pub(super) fn pl_token_set_ratio(df: DataFrame, cola: &str, colb: &str) -> PolarsResult<DataFrame> {
    parallellize(df, cola, colb, "token_set_ratio", fuzz_token_set_ratio)
}

pub(super) fn pl_token_sort_ratio(
    df: DataFrame,
    cola: &str,
    colb: &str,
) -> PolarsResult<DataFrame> {
    parallellize(df, cola, colb, "token_sort_ratio", fuzz_token_sort_ratio)
}
