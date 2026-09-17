from fastapi.testclient import TestClient

from pyraftkv.api.app import create_app


def test_put_and_get(tmp_path):
    app = create_app(tmp_path / "wal.log")
    client = TestClient(app)

    response = client.put(
        "/kv/name",
        json={"value": "harsha"},
    )

    assert response.status_code == 200

    response = client.get("/kv/name")

    assert response.status_code == 200
    assert response.json() == {
        "key": "name",
        "value": "harsha",
    }


def test_missing_key_returns_404(tmp_path):
    app = create_app(tmp_path / "wal.log")
    client = TestClient(app)

    response = client.get("/kv/missing")

    assert response.status_code == 404


def test_delete_value(tmp_path):
    app = create_app(tmp_path / "wal.log")
    client = TestClient(app)

    client.put(
        "/kv/name",
        json={"value": "harsha"},
    )

    response = client.delete("/kv/name")

    assert response.status_code == 200

    response = client.get("/kv/name")

    assert response.status_code == 404


def test_api_data_survives_restart(tmp_path):
    wal_path = tmp_path / "wal.log"

    app1 = create_app(wal_path)
    client1 = TestClient(app1)

    client1.put(
        "/kv/language",
        json={"value": "python"},
    )

    # Simulate application restart.
    app2 = create_app(wal_path)
    client2 = TestClient(app2)

    response = client2.get("/kv/language")

    assert response.status_code == 200
    assert response.json()["value"] == "python"


def test_api_delete_survives_restart(tmp_path):
    wal_path = tmp_path / "wal.log"

    app1 = create_app(wal_path)
    client1 = TestClient(app1)
    client1.put("/kv/language", json={"value": "python"})
    client1.delete("/kv/language")

    app2 = create_app(wal_path)
    client2 = TestClient(app2)

    response = client2.get("/kv/language")

    assert response.status_code == 404
