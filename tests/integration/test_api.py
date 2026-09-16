from fastapi.testclient import TestClient

from pyraftkv.api.app import app, store

client = TestClient(app)


def setup_function() -> None:
    store.clear()


def test_put_value():
    response = client.put(
        "/kv/name",
        json={"value": "harsha"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "key": "name",
        "value": "harsha",
    }


def test_get_value():
    client.put(
        "/kv/language",
        json={"value": "python"},
    )

    response = client.get("/kv/language")

    assert response.status_code == 200
    assert response.json()["value"] == "python"


def test_get_missing_key():
    response = client.get("/kv/missing")

    assert response.status_code == 404


def test_delete_value():
    client.put(
        "/kv/name",
        json={"value": "harsha"},
    )

    response = client.delete("/kv/name")

    assert response.status_code == 200

    response = client.get("/kv/name")

    assert response.status_code == 404


def test_delete_missing_key():
    response = client.delete("/kv/missing")

    assert response.status_code == 404
