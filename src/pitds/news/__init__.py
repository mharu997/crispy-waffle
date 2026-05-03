"""Tiingo News API ingester.

Pulls news articles from Tiingo's REST endpoint and writes them to a
hive-partitioned Parquet dataset at ``data/processed/news/`` keyed by
``crawl_year``. The PIT contract: ``crawl_date`` is the as-of gate
(when Tiingo first indexed the article), not ``published_date``.
"""
