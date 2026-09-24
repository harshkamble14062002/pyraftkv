import pyraftkv


def test_package_import():
    assert pyraftkv is not None
    assert pyraftkv.__version__ == "1.0.0"
