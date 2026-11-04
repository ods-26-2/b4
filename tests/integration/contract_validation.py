from __future__ import annotations

from datetime import datetime, timedelta
import math
import re
from uuid import UUID


_UUID_V4 = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _is_uuid_v4(value: object) -> bool:
    if not isinstance(value, str) or not _UUID_V4.fullmatch(value):
        return False
    try:
        return UUID(value).version == 4
    except ValueError:
        return False


def _is_datetime(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.tzinfo is not None and parsed.utcoffset() == timedelta(0)
    except ValueError:
        return False


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _is_pair(value: object) -> bool:
    return isinstance(value, list) and len(value) == 2 and all(_is_number(item) for item in value)


def validate_message(
    message: object,
    *,
    schema: str,
    message_type: str,
    producer: str,
    payload_fields: tuple[str, ...],
    context_fields: tuple[str, ...] = (),
) -> list[str]:
    """Validate the common envelope and the required fields used by a test."""
    errors: list[str] = []
    if not isinstance(message, dict):
        return ["message must be a JSON object"]

    required = (
        "message_type",
        "message_id",
        "schema",
        "schema_version",
        "producer",
        "published_at",
        "payload",
        *context_fields,
    )
    for field in required:
        if field not in message:
            errors.append(f"missing envelope field: {field}")

    if message.get("message_type") != message_type:
        errors.append(f"message_type must be {message_type!r}")
    if message.get("schema") != schema:
        errors.append(f"schema must be {schema!r}; got {message.get('schema')!r}")
    if message.get("producer") != producer:
        errors.append(f"producer must be {producer!r}")
    if not _is_uuid_v4(message.get("message_id")):
        errors.append("message_id must be a UUID v4")

    version = message.get("schema_version")
    if not isinstance(version, str) or not re.fullmatch(r"\d+\.\d+", version):
        errors.append("schema_version must use MAJOR.MINOR")
    if not _is_datetime(message.get("published_at")):
        errors.append("published_at must be an ISO 8601 timestamp")

    if "captured_at" in context_fields and not _is_datetime(message.get("captured_at")):
        errors.append("captured_at must be an ISO 8601 timestamp")
    if "source_id" in context_fields and not isinstance(message.get("source_id"), str):
        errors.append("source_id must be a string")
    if "session_id" in context_fields and not _is_uuid_v4(message.get("session_id")):
        errors.append("session_id must be a UUID v4")
    if "artifact_version" in context_fields and not isinstance(message.get("artifact_version"), str):
        errors.append("artifact_version must be a string")
    if "sequence" in context_fields and (
        isinstance(message.get("sequence"), bool)
        or not isinstance(message.get("sequence"), int)
        or message["sequence"] < 0
    ):
        errors.append("sequence must be a non-negative integer")
    if "correlation_id" in context_fields and not _is_uuid_v4(message.get("correlation_id")):
        errors.append("correlation_id must be a UUID v4")

    payload = message.get("payload")
    if not isinstance(payload, dict):
        errors.append("payload must be a JSON object")
        return errors

    for field in payload_fields:
        if field not in payload:
            errors.append(f"missing payload field: {field}")

    if schema == "ods.i2.tracks" and isinstance(payload.get("tracks"), list):
        if isinstance(payload.get("frame_index"), bool) or not isinstance(payload.get("frame_index"), int):
            errors.append("frame_index must be an integer")
        for index, track in enumerate(payload["tracks"]):
            if not isinstance(track, dict):
                errors.append(f"tracks[{index}] must be an object")
                continue
            for field in ("track_id", "class", "state", "predicted", "confidence", "point_px", "bbox"):
                if field not in track:
                    errors.append(f"tracks[{index}] is missing {field}")
            if track.get("state") not in {"CONFIRMED", "LOST"}:
                errors.append(f"tracks[{index}].state must be CONFIRMED or LOST")
            if not isinstance(track.get("predicted"), bool):
                errors.append(f"tracks[{index}].predicted must be boolean")
            if not isinstance(track.get("track_id"), str) or not isinstance(track.get("class"), str):
                errors.append(f"tracks[{index}] must contain string track_id and class")
            if not _is_number(track.get("confidence")) or not 0 <= track["confidence"] <= 1:
                errors.append(f"tracks[{index}].confidence must be between 0 and 1")
            if not isinstance(track.get("point_px"), list) or len(track["point_px"]) != 2:
                errors.append(f"tracks[{index}].point_px must contain two values")
            if not isinstance(track.get("bbox"), list) or len(track["bbox"]) != 4:
                errors.append(f"tracks[{index}].bbox must contain four values")
            if isinstance(track.get("point_px"), list) and not all(_is_number(value) for value in track["point_px"]):
                errors.append(f"tracks[{index}].point_px values must be numbers")
            if isinstance(track.get("bbox"), list) and not all(
                isinstance(value, int) and not isinstance(value, bool) for value in track["bbox"]
            ):
                errors.append(f"tracks[{index}].bbox values must be integers")

    if schema == "ods.i3.calibration":
        if not _is_datetime(payload.get("valid_from")):
            errors.append("valid_from must be an ISO 8601 timestamp")
        for field in ("image_width", "image_height", "holdout_points"):
            if isinstance(payload.get(field), bool) or not isinstance(payload.get(field), int):
                errors.append(f"{field} must be an integer")
            elif payload[field] < 0 or (field in {"image_width", "image_height"} and payload[field] == 0):
                errors.append(f"{field} is outside its valid range")
        if not _is_number(payload.get("reprojection_rms_cm")):
            errors.append("reprojection_rms_cm must be a number")
        elif payload["reprojection_rms_cm"] < 0:
            errors.append("reprojection_rms_cm must be non-negative")
        intrinsics = payload.get("intrinsics")
        if not isinstance(intrinsics, dict) or any(
            not _is_number(intrinsics.get(field)) for field in ("fx", "fy", "cx", "cy")
        ):
            errors.append("intrinsics must contain numeric fx, fy, cx, and cy")
        distortion = payload.get("distortion")
        if not isinstance(distortion, list) or len(distortion) != 5 or not all(
            _is_number(value) for value in distortion
        ):
            errors.append("distortion must contain five numbers")
        homography = payload.get("homography_px_to_m")
        if not isinstance(homography, list) or len(homography) != 3 or any(
            not isinstance(row, list) or len(row) != 3 or not all(_is_number(value) for value in row)
            for row in homography
        ):
            errors.append("homography_px_to_m must be a 3x3 matrix of numbers")
        reference = payload.get("reference_frame")
        if not isinstance(reference, dict) or any(
            not isinstance(reference.get(field), str) for field in ("space_id", "origin", "axes")
        ):
            errors.append("reference_frame must contain space_id, origin, and axes strings")

    if schema == "ods.s1.position" and isinstance(payload.get("positions"), list):
        if isinstance(payload.get("frame_index"), bool) or not isinstance(payload.get("frame_index"), int):
            errors.append("frame_index must be an integer")
        for index, position in enumerate(payload["positions"]):
            if not isinstance(position, dict):
                errors.append(f"positions[{index}] must be an object")
                continue
            for field in ("track_id", "class", "point_px", "zone_ids"):
                if field not in position:
                    errors.append(f"positions[{index}] is missing {field}")
            if not _is_pair(position.get("point_px")):
                errors.append(f"positions[{index}].point_px must contain two numbers")
            if not isinstance(position.get("zone_ids"), list) or not all(
                isinstance(zone_id, str) for zone_id in position.get("zone_ids", [])
            ):
                errors.append(f"positions[{index}].zone_ids must be an array of strings")
            if "position_m" in position and not _is_pair(position["position_m"]):
                errors.append(f"positions[{index}].position_m must contain two numbers")
            if "position_m" in position and not isinstance(position.get("space_id"), str):
                errors.append(f"positions[{index}].space_id is required with position_m")

    if schema == "ods.s1.zone_transition":
        if payload.get("transition") not in {"ENTER", "EXIT"}:
            errors.append("transition must be ENTER or EXIT")
        if not _is_datetime(payload.get("entered_at")):
            errors.append("entered_at must be an ISO 8601 timestamp")
        if payload.get("transition") == "EXIT" and (
            isinstance(payload.get("dwell_s"), bool) or not isinstance(payload.get("dwell_s"), int)
        ):
            errors.append("dwell_s must be an integer for EXIT")
        if isinstance(payload.get("dwell_s"), int) and payload["dwell_s"] < 0:
            errors.append("dwell_s must be non-negative")

    if schema == "ods.s1.zone_occupancy":
        if isinstance(payload.get("count"), bool) or not isinstance(payload.get("count"), int):
            errors.append("count must be an integer")
        if not isinstance(payload.get("count_by_class"), dict):
            errors.append("count_by_class must be an object")
        elif any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in payload["count_by_class"].values()
        ):
            errors.append("count_by_class values must be non-negative integers")
        if not isinstance(payload.get("track_ids"), list):
            errors.append("track_ids must be an array")
        if isinstance(payload.get("count"), int) and isinstance(payload.get("track_ids"), list):
            if payload["count"] != len(payload["track_ids"]):
                errors.append("count must equal the number of track_ids")

    if schema in {"ods.s3.alert.reply", "ods.s3.alert_action.reply"}:
        if payload.get("status") not in {"OK", "ERROR"}:
            errors.append("reply payload status must be OK or ERROR")
        if payload.get("status") == "OK" and not _is_uuid_v4(payload.get("alert_id")):
            errors.append("successful alert reply must contain a UUID v4 alert_id")

    if schema == "ods.s5.metrics.reply":
        if payload.get("status") not in {"OK", "ERROR"}:
            errors.append("reply payload status must be OK or ERROR")
        if payload.get("status") == "OK" and not isinstance(payload.get("series"), list):
            errors.append("successful metrics reply must contain a series array")

    if schema == "ods.s5.zone_metrics":
        for field in ("window_start_at", "window_end_at"):
            if not _is_datetime(payload.get(field)):
                errors.append(f"{field} must be an ISO 8601 timestamp")
        if payload.get("granularity") not in {"1m", "15m", "1h", "1d"}:
            errors.append("granularity is not supported by the contract")
        for field in ("total_count", "occupancy_peak", "dwell_avg_s"):
            if isinstance(payload.get(field), bool) or not isinstance(payload.get(field), int):
                errors.append(f"{field} must be an integer")
        if not _is_number(payload.get("occupancy_avg")) or not 0 <= payload["occupancy_avg"] <= 1:
            errors.append("occupancy_avg must be between 0 and 1")

    return errors
