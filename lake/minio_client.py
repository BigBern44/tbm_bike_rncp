"""Upload des Parquet bronze vers MinIO (S3-compatible).

Les credentials viennent exclusivement de l'environnement (voir .env.example).
Le Parquet est téléversé depuis la mémoire (aucun fichier local) sous une clé
partitionnée bronze/<feed>/date=AAAA-MM-JJ/<horodatage>.parquet — un re-upload
sur la même clé écrase l'objet (idempotence).
"""

from __future__ import annotations

import os

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


def ensure_bucket(client, bucket: str) -> None:
    """Crée le bucket s'il n'existe pas encore (no-op sinon)."""
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)


def upload_parquet_bytes(
    object_key: str, data: bytes, client=None, bucket: str | None = None
) -> str:
    """Téléverse un Parquet sérialisé en mémoire vers MinIO ; retourne l'URI s3://.

    `object_key` est la clé S3 complète et partitionnée
    (bronze/<feed>/date=AAAA-MM-JJ/<horodatage>.parquet). Un re-upload sur la même
    clé écrase l'objet : idempotence, jamais de doublon, aucun fichier local.
    """
    client = client or get_s3_client()
    bucket = bucket or _env("MINIO_BUCKET", "lake")
    ensure_bucket(client, bucket)

    client.put_object(Bucket=bucket, Key=object_key, Body=data)
    return f"s3://{bucket}/{object_key}"
