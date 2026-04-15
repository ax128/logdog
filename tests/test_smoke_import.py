def test_import_main():
    import logdog.main as m

    assert hasattr(m, "create_app")
