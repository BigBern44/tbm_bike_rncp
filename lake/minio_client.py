"""Upload des Parquet bronze vers MinIO (S3-compatible).

Les credentials viennent exclusivement de l'environnement (voir .env.example).
La clé objet reprend le chemin partitionné local :
bronze/<feed>/date=AAAA-MM-JJ/<horodatage>.parquet — un re-upload écrase le
même objet (idempotence).
"""

from __future__ import annotations

import os
from pathlib import Path

import boto3


def _env(name: str, default: str | None = None) -> str:
    """Lit une variable d'environnement obligatoire (sauf défaut fourni)."""
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"{name} absente de l'environnement (voir .env.example).")
    return value


def get_s3_client():
    """Construit un client S3 pointé sur MinIO depuis l'environnement."""
    use_ssl = _env("MINIO_USE_SSL", "false").lower() == "true"
    scheme = "https" if use_ssl else "http"
    return boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{_env('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=_env("MINIO_ACCESS_KEY"),
        aws_secret_access_key=_env("MINIO_SECRET_KEY"),
    )


def build_object_key(local_path: Path, layer: str = "bronze") -> str:
    """Dérive la clé objet S3 du chemin local partitionné.

    data/bronze/station_status/date=2026-06-12/x.parquet
    → bronze/station_status/date=2026-06-12/x.parquet
    """
    feed_dir = local_path.parent.parent  # <feed>/date=…/fichier
    return f"{layer}/{feed_dir.name}/{local_path.parent.name}/{local_path.name}"


def ensure_bucket(client, bucket: str) -> None:
    """Crée le bucket s'il n'existe pas encore (no-op sinon)."""
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)


def upload_parquet(local_path: Path, client=None, bucket: str | None = None) -> str:
    """Téléverse un Parquet local vers MinIO et retourne l'URI s3:// écrite."""
    client = client or get_s3_client()
    bucket = bucket or _env("MINIO_BUCKET", "lake")
    ensure_bucket(client, bucket)

    key = build_object_key(local_path)
    client.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"
