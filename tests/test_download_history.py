"""Persistence tests for completed download history."""

import os

from app.database import DB_NAME, db_connect, init_db
from app.services.cart_service import CartService


def test_record_download_history_is_idempotent(tmp_path):
    db_path = os.path.join(tmp_path, DB_NAME)
    init_db(db_path)
    cart = CartService.__new__(CartService)
    cart.db_path = db_path
    item = {
        "id": "cart-1",
        "stream_id": "movie-1",
        "source_id": "src1",
        "content_type": "vod",
        "name": "A Movie",
        "file_path": "/downloads/A Movie/A Movie.mp4",
        "file_size": 123,
    }

    cart.record_download_history(item)
    cart.record_download_history(item)

    conn = db_connect(db_path)
    try:
        rows = conn.execute("SELECT * FROM download_history").fetchall()
        assert len(rows) == 1
        assert rows[0]["cart_item_id"] == "cart-1"
        assert rows[0]["file_path"] == item["file_path"]
    finally:
        conn.close()


def test_init_db_backfills_completed_cart_items_and_history_survives_cart_cleanup(tmp_path):
    db_path = os.path.join(tmp_path, DB_NAME)
    init_db(db_path)

    conn = db_connect(db_path)
    try:
        conn.execute(
            """INSERT INTO cart_items
               (id, stream_id, source_id, content_type, name, added_at, status,
                file_path, file_size)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "cart-completed",
                "movie-2",
                "src1",
                "vod",
                "Old Movie",
                "2026-01-01T00:00:00",
                "completed",
                "/downloads/Old Movie/Old Movie.mp4",
                456,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    init_db(db_path)

    conn = db_connect(db_path)
    try:
        conn.execute("DELETE FROM cart_items WHERE id = ?", ("cart-completed",))
        conn.commit()
        row = conn.execute(
            "SELECT name, file_size FROM download_history WHERE cart_item_id = ?",
            ("cart-completed",),
        ).fetchone()
        assert row["name"] == "Old Movie"
        assert row["file_size"] == 456
    finally:
        conn.close()
