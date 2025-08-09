def snapshot_stats(video_ids: list[str]) -> list[dict]:
    """
    Re-hydrate a list of IDs and store a dated snapshot
    (e.g., in a table raw_metrics_snapshots with fetched_at).
    """

def compute_view_velocity(conn, horizon_hours: int) -> None:
    """
    From snapshots: compute (views@H hours / max(subscribers, 1)) per video,
    and store in a label table used by modeling.
    """