"""Tests for the pure torrent layout helpers."""

from __future__ import annotations

from pathlib import Path

from racing_sync.torrent_layout import (
    blob_common_top,
    blob_files,
    blob_is_wrapped,
    common_top,
    is_flat,
    join_remote,
    looks_like_single_file,
)
from racing_sync.watchdir import _bencode


def _flat_blob(name: bytes = b"Movie.2024.1080p.mkv", size: int = 100) -> bytes:
    return _bencode({b"info": {b"name": name, b"length": size}})


def _wrapped_blob(top: bytes = b"Show.S01") -> bytes:
    paths = [(b"Show.S01E01.mkv", 1000), (b"Season 1/Show.S01E02.mkv", 2000)]
    return _bencode(
        {
            b"info": {
                b"name": top,
                b"files": [{b"length": size, b"path": path.split(b"/")} for path, size in paths],
            }
        }
    )


def test_common_top_wrapped_single_file_inside_folder():
    assert common_top(["Top/file.mkv"]) == "Top"


def test_common_top_multi_file_nested():
    names = ["Top/file.mkv", "Top/Season 1/other.mkv", "Top/Sub/Deep/third.nfo"]
    assert common_top(names) == "Top"


def test_common_top_normalizes_backslashes_and_leading_slashes():
    assert common_top(["/Top\\file.mkv", " Top/other.mkv "]) == "Top"


def test_common_top_multi_top_is_none():
    assert common_top(["A/file.mkv", "B/file.mkv"]) is None


def test_common_top_flat_is_none():
    assert common_top(["file.mkv"]) is None
    assert common_top(["Top/file.mkv", "file.mkv"]) is None


def test_common_top_empty_is_none():
    assert common_top([]) is None
    assert common_top([""]) is None
    assert common_top(["", "   ", "/"]) is None


def test_common_top_unsafe_tops_are_none():
    assert common_top(["../evil/file.mkv"]) is None
    assert common_top(["./file.mkv"]) is None
    assert common_top(["/../file.mkv"]) is None


def test_is_flat():
    assert is_flat(["file.mkv"]) is True
    assert is_flat(["file.mkv", "", "other.nfo"]) is True
    assert is_flat(["Top/file.mkv"]) is False
    assert is_flat(["file.mkv", "Top/other.mkv"]) is False
    assert is_flat([]) is False
    assert is_flat(["", "   "]) is False


def test_blob_files_rejects_bad_blobs():
    assert blob_files(None) == []
    assert blob_files(b"") == []
    assert blob_files(b"not a torrent") == []
    assert blob_files(bytearray(b"d4:infod")) == []  # type: ignore[arg-type]


def test_blob_helpers_single_file_is_flat():
    blob = _flat_blob()
    files = blob_files(blob)
    assert [f.name for f in files] == ["Movie.2024.1080p.mkv"]
    assert blob_common_top(blob) is None
    assert blob_is_wrapped(blob) is False


def test_blob_helpers_multi_file_is_wrapped():
    blob = _wrapped_blob()
    files = blob_files(blob)
    assert [f.name for f in files] == [
        "Show.S01/Show.S01E01.mkv",
        "Show.S01/Season 1/Show.S01E02.mkv",
    ]
    assert blob_common_top(blob) == "Show.S01"
    assert blob_is_wrapped(blob) is True


def test_blob_helpers_bad_blob():
    assert blob_common_top(b"not a torrent") is None
    assert blob_is_wrapped(b"not a torrent") is False
    assert blob_common_top(None) is None
    assert blob_is_wrapped(None) is False


def test_looks_like_single_file():
    folder = "Example.Show.S24E32.1080p.WEB.H264-GROUP"
    assert looks_like_single_file(folder) is False
    assert looks_like_single_file(folder + ".mkv") is True
    assert looks_like_single_file("Movie.MKV") is True
    assert looks_like_single_file("Top/Movie.mkv") is True
    assert looks_like_single_file("Top\\Movie.rar") is True
    assert looks_like_single_file("subtitles.srt") is True
    assert looks_like_single_file("Movie") is False
    assert looks_like_single_file("") is False
    assert looks_like_single_file(None) is False  # type: ignore[arg-type]


def test_join_remote_rclone_paths():
    assert join_remote("remote:qbittorrent/unsorted/", "Top") == ("remote:qbittorrent/unsorted/Top")
    assert join_remote("remote:qbittorrent/unsorted", "Top") == ("remote:qbittorrent/unsorted/Top")
    assert join_remote("remote:qbittorrent/unsorted/", " Top ") == (
        "remote:qbittorrent/unsorted/Top"
    )


def test_join_remote_local_path():
    expected = str(Path("/mnt/fuse/seed") / "Top")
    assert join_remote("/mnt/fuse/seed", "Top") == expected
    # Single-letter names are Windows drives, not rclone remotes.
    assert join_remote("C:\\seed", "Top") == str(Path("C:\\seed") / "Top")


def test_join_remote_rejects_unsafe_segments():
    remote = "remote:qbittorrent/unsorted/"
    assert join_remote(remote, "") == remote
    assert join_remote(remote, "   ") == remote
    assert join_remote(remote, ".") == remote
    assert join_remote(remote, "..") == remote
    assert join_remote(remote, "../evil") == remote
    assert join_remote(remote, "a/b") == remote
    assert join_remote(remote, "a\\b") == remote
    assert join_remote("/mnt/fuse/seed", "../evil") == "/mnt/fuse/seed"
