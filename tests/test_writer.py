import json
import os
import pytest
from scripts.writer import write_watchlist


_SAMPLE_BUCKETS = [
    {"name": "21EMA", "tickers": ["AAPL", "MSFT"]},
    {"name": "Vol Up", "tickers": ["GOOG"]},
]


def test_write_creates_watchlist_json(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.writer._DATA_DIR", str(tmp_path))
    monkeypatch.setattr("scripts.writer._WATCHLIST_PATH", str(tmp_path / "watchlist.json"))
    monkeypatch.setattr("scripts.writer._META_PATH", str(tmp_path / "meta.json"))

    write_watchlist(_SAMPLE_BUCKETS, date_str="2026-03-19")

    wl = json.loads((tmp_path / "watchlist.json").read_text())
    assert wl["date"] == "2026-03-19"
    assert len(wl["buckets"]) == 2
    assert wl["buckets"][0]["name"] == "21EMA"
    assert "AAPL" in wl["buckets"][0]["tickers"]


def test_write_creates_meta_json(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.writer._DATA_DIR", str(tmp_path))
    monkeypatch.setattr("scripts.writer._WATCHLIST_PATH", str(tmp_path / "watchlist.json"))
    monkeypatch.setattr("scripts.writer._META_PATH", str(tmp_path / "meta.json"))

    write_watchlist(_SAMPLE_BUCKETS, date_str="2026-03-19")

    meta = json.loads((tmp_path / "meta.json").read_text())
    assert meta["date"] == "2026-03-19"
    assert meta["total_tickers"] == 3  # AAPL + MSFT + GOOG
    assert "updated" in meta


def test_write_creates_data_dir_if_missing(tmp_path, monkeypatch):
    new_dir = tmp_path / "new_data"
    monkeypatch.setattr("scripts.writer._DATA_DIR", str(new_dir))
    monkeypatch.setattr("scripts.writer._WATCHLIST_PATH", str(new_dir / "watchlist.json"))
    monkeypatch.setattr("scripts.writer._META_PATH", str(new_dir / "meta.json"))

    write_watchlist(_SAMPLE_BUCKETS, date_str="2026-03-19")

    assert new_dir.exists()
    assert (new_dir / "watchlist.json").exists()


def test_empty_buckets(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.writer._DATA_DIR", str(tmp_path))
    monkeypatch.setattr("scripts.writer._WATCHLIST_PATH", str(tmp_path / "watchlist.json"))
    monkeypatch.setattr("scripts.writer._META_PATH", str(tmp_path / "meta.json"))

    write_watchlist([], date_str="2026-03-19")

    wl = json.loads((tmp_path / "watchlist.json").read_text())
    assert wl["buckets"] == []
    meta = json.loads((tmp_path / "meta.json").read_text())
    assert meta["total_tickers"] == 0
