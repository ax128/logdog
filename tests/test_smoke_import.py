def test_import_main():
    import logwatch.main as m

    assert hasattr(m, "create_app")
