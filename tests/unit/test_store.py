from pyraftkv.storage.store import KVStore


def test_put_and_get():
    store = KVStore()

    store.put("name", "harsha")

    assert store.get("name") == "harsha"


def test_get_missing_key():
    store = KVStore()

    assert store.get("missing") is None


def test_put_overwrites_existing_value():
    store = KVStore()

    store.put("language", "java")
    store.put("language", "python")

    assert store.get("language") == "python"


def test_delete_existing_key():
    store = KVStore()

    store.put("name", "harsha")

    result = store.delete("name")

    assert result is True
    assert store.get("name") is None


def test_delete_missing_key():
    store = KVStore()

    assert store.delete("missing") is False

def test_clear():
    store = KVStore()

    store.put("name", "harsha")
    store.put("language", "python")

    store.clear()

    assert store.get("name") is None
    assert store.get("language") is None