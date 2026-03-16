from importlib import import_module


def test_package_importable():
    module = import_module("music_ingest")
    assert module is not None
