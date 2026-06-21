"""API tests using FastAPI's TestClient (Python lessons only, fast)."""
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_healthz():
    body = client.get("/healthz").json()
    assert body["status"] == "ok"
    assert body["challenges"] >= 50


def test_tracks_endpoint():
    tracks = client.get("/api/tracks").json()["tracks"]
    assert {"python", "pyspark", "performance", "streaming", "capstone"} <= {t["id"] for t in tracks}


def test_index_served():
    resp = client.get("/")
    assert resp.status_code == 200
    assert "SparkQuest" in resp.text


def test_run_python():
    body = client.post("/api/run", json={"challenge_id": "py-01-hello", "code": "print('Hello')"}).json()
    assert body["ran"] is True
    assert "Hello" in body["stdout"]


def test_submit_awards_xp_and_badge():
    body = client.post(
        "/api/submit",
        json={
            "challenge_id": "py-02-variables",
            "code": "price = 189.50\nquantity = 100\ncost = price * quantity",
            "user_id": "pytest-user",
        },
    ).json()
    assert body["passed"] is True
    assert body["xp_awarded"] == 50
    assert "first_blood" in body["new_badges"]


def test_tutor_rule_based_fallback():
    body = client.post(
        "/api/tutor", json={"challenge_id": "py-01-hello", "question": "give me a hint"}
    ).json()
    assert body["provider"] == "rule-based"
    assert len(body["reply"]) > 0


def test_unknown_challenge_returns_404():
    assert client.get("/api/challenge/does-not-exist").status_code == 404


def test_handbook_toc_and_chapter():
    chapters = client.get("/api/handbook").json()["chapters"]
    assert len(chapters) >= 8
    first = client.get(f"/api/handbook/{chapters[0]['slug']}").json()
    assert len(first["markdown"]) > 100


def test_interview_bank():
    body = client.get("/api/interview").json()
    assert body["total"] >= 90
    assert len(body["categories"]) >= 8
    assert all("q" in q and "a" in q for q in body["questions"])
