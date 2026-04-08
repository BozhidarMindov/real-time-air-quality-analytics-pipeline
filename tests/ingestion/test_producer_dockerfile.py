from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_producer_dockerfile_uses_run_producer_script():
    dockerfile = (ROOT / "src" / "ingestion" / "Dockerfile").read_text(encoding="utf-8")

    assert "COPY scripts/run_producer.py ./scripts/run_producer.py" in dockerfile
    assert 'CMD ["uv", "run", "python", "scripts/run_producer.py"]' in dockerfile
    assert 'CMD ["uv", "run", "python", "-m", "src.ingestion.producer"]' not in dockerfile
