from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_analytics_dockerfile_installs_java_and_runs_batch_script():
    dockerfile = (ROOT / "src" / "analytics" / "Dockerfile").read_text(encoding="utf-8")

    assert "default-jre-headless" in dockerfile
    assert "COPY scripts/run_analytics.py ./scripts/run_analytics.py" in dockerfile
    assert "COPY notebooks ./notebooks" in dockerfile
    assert 'CMD ["uv", "run", "python", "scripts/run_analytics.py"]' in dockerfile


def test_docker_compose_includes_analytics_and_jupyter_services():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "analytics:" in compose
    assert "dockerfile: src/analytics/Dockerfile" in compose
    assert "AQI_SPIKE_THRESHOLD: 90" in compose
    assert "AQI_JUMP_THRESHOLD: 30" in compose
    assert "jupyter:" in compose
    assert '- "8888:8888"' in compose
    assert '--ip=0.0.0.0' in compose
    assert "./notebooks:/app/notebooks" in compose
