from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable


class KafkaIntegrationError(RuntimeError):
    pass


def kafka_configured() -> bool:
    return bool(
        os.environ.get("ODS_KAFKA_BOOTSTRAP_SERVERS")
        or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_request(
    schema: str,
    message_type: str,
    producer: str,
    target: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "message_type": message_type,
        "message_id": str(uuid.uuid4()),
        "schema": schema,
        "schema_version": "1.0",
        "producer": producer,
        "target": target,
        "published_at": utc_now(),
        "payload": payload,
    }


def new_event(
    schema: str,
    producer: str,
    source_id: str,
    session_id: str,
    sequence: int,
    artifact_version: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    now = utc_now()
    return {
        "message_type": "event",
        "message_id": str(uuid.uuid4()),
        "schema": schema,
        "schema_version": "1.0",
        "producer": producer,
        "published_at": now,
        "captured_at": now,
        "source_id": source_id,
        "session_id": session_id,
        "sequence": sequence,
        "artifact_version": artifact_version,
        "payload": payload,
    }


def _client_config() -> dict[str, str]:
    bootstrap = (
        os.environ.get("ODS_KAFKA_BOOTSTRAP_SERVERS")
        or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or ""
    ).strip()
    if not bootstrap:
        raise KafkaIntegrationError(
            "Defina ODS_KAFKA_BOOTSTRAP_SERVERS com o endereço do broker Kafka de testes."
        )

    config = {
        "bootstrap.servers": bootstrap,
        "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    }
    for env_name, config_name in (
        ("KAFKA_SASL_MECHANISM", "sasl.mechanism"),
        ("KAFKA_SASL_USERNAME", "sasl.username"),
        ("KAFKA_SASL_PASSWORD", "sasl.password"),
        ("KAFKA_SSL_CA_LOCATION", "ssl.ca.location"),
    ):
        value = os.environ.get(env_name)
        if value:
            config[config_name] = value
    return config


def publish_and_wait(
    input_topic: str,
    request: dict[str, Any],
    output_topic: str,
    matches: Callable[[dict[str, Any]], bool],
    *,
    timeout: float | None = None,
) -> dict[str, Any]:
    try:
        from confluent_kafka import Consumer, KafkaError, Producer
    except ImportError as error:
        raise KafkaIntegrationError(
            "Cliente Kafka ausente. Instale as dependências com "
            "`python -m pip install -r requirements.txt`."
        ) from error

    config = _client_config()
    reply_timeout = timeout or float(os.environ.get("ODS_KAFKA_REPLY_TIMEOUT_SECONDS", "45"))
    ready_timeout = float(os.environ.get("ODS_KAFKA_READY_TIMEOUT_SECONDS", "30"))
    consumer = Consumer(
        {
            **config,
            "group.id": f"b4-integration-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )

    try:
        consumer.subscribe([output_topic])
        ready_deadline = time.monotonic() + ready_timeout
        while not consumer.assignment() and time.monotonic() < ready_deadline:
            consumer.poll(0.25)
        if not consumer.assignment():
            raise KafkaIntegrationError(
                f"Sem partição atribuída para {output_topic}; confira tópico, ACLs e broker."
            )

        delivery_errors: list[str] = []
        producer = Producer({**config, "client.id": "b4-integration-producer"})
        producer.produce(
            input_topic,
            key=request["message_id"].encode("utf-8"),
            value=json.dumps(request, ensure_ascii=False).encode("utf-8"),
            callback=lambda error, _: delivery_errors.append(str(error)) if error else None,
        )
        pending = producer.flush(15)
        if pending:
            raise KafkaIntegrationError(
                f"Timeout ao publicar em {input_topic}; {pending} mensagem(ns) pendente(s)."
            )
        if delivery_errors:
            raise KafkaIntegrationError(
                f"Falha ao publicar em {input_topic}: {'; '.join(delivery_errors)}"
            )

        deadline = time.monotonic() + reply_timeout
        while time.monotonic() < deadline:
            record = consumer.poll(min(0.5, max(0.0, deadline - time.monotonic())))
            if record is None:
                continue
            error = record.error()
            if error:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaIntegrationError(f"Erro ao consumir {output_topic}: {error}")
            try:
                message = json.loads(record.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(message, dict) and matches(message):
                return message

        raise KafkaIntegrationError(
            f"Sem resposta correspondente em {output_topic} após {reply_timeout:g}s "
            f"para message_id={request['message_id']}."
        )
    finally:
        consumer.close()
