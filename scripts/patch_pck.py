#!/usr/bin/env python3
"""
Patch Godot mod scripts inside public/chunks/index.pck.part00..14 cleanly.
Updates web_save_sync.gd (host migration resume + fast action polling) and
lan_transport.gd (host reconnect support) while keeping manifest and index.html in sync.

Usage:
  python scripts/patch_pck.py
  python scripts/patch_pck.py --verify
"""

import argparse
import hashlib
import json
import os
import re
import struct
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PUBLIC_DIR = os.path.join(ROOT, "public")
CHUNKS_DIR = os.path.join(PUBLIC_DIR, "chunks")
MANIFEST_PATH = os.path.join(CHUNKS_DIR, "pck.manifest.json")
INDEX_HTML_PATH = os.path.join(PUBLIC_DIR, "index.html")

TARGET_WEB_SAVE_SYNC = "res://mods-unpacked/six666-BrotatoOnline/scripts/web_save_sync.gd"
TARGET_LAN_TRANSPORT = "res://mods-unpacked/six666-BrotatoOnline/scripts/lan_transport.gd"


def load_pck_bytes():
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    buf = bytearray()
    for rel in manifest["chunks"]:
        chunk_path = os.path.join(PUBLIC_DIR, rel.lstrip("/"))
        with open(chunk_path, "rb") as cf:
            buf.extend(cf.read())
    return manifest, buf


def parse_pck_directory(buf):
    if buf[:4] != b"GDPC":
        raise ValueError(f"Invalid PCK magic: {buf[:4]!r}")
    pck_ver, maj, minr, pat = struct.unpack_from("<IIII", buf, 4)
    file_count = struct.unpack_from("<I", buf, 84)[0]
    p = 88
    entries = {}
    for _ in range(file_count):
        plen = struct.unpack_from("<I", buf, p)[0]
        p += 4
        path = buf[p : p + plen].rstrip(b"\x00").decode("utf-8", "replace")
        p += plen
        meta_pos = p
        off, sz = struct.unpack_from("<QQ", buf, p)
        md5 = bytes(buf[p + 16 : p + 32])
        p += 32
        entries[path] = {
            "meta_pos": meta_pos,
            "offset": off,
            "size": sz,
            "md5": md5,
        }
    return (pck_ver, maj, minr, pat, file_count, entries)


def patch_web_save_sync(src: str) -> str:
    if 'action == "migrate_to_host"' in src and "_prepare_migrated_run_json" in src:
        return src

    # 1. Add migration polling state vars
    old_vars = "const POLL_INTERVAL_MSEC = 750\nvar _last_poll_msec = 0\nvar _last_snapshot_hash = 0"
    new_vars = (
        "const POLL_INTERVAL_MSEC = 750\n"
        "const ACTION_POLL_INTERVAL_MSEC = 100\n"
        "const MIGRATION_SAVE_ARRAYS = [\"players_data\", \"locked_shop_items\", \"tracked_item_effects\", \"shop_items\", \"reroll_count\", \"paid_reroll_count\", \"initial_free_rerolls\", \"free_rerolls\", \"item_steals\"]\n"
        "var _last_poll_msec = 0\n"
        "var _last_action_poll_msec = 0\n"
        "var _last_snapshot_hash = 0\n"
        "var _pending_auto_continue_migration = false\n"
        "var _pending_auto_continue_deadline_msec = 0"
    )
    if old_vars not in src:
        raise RuntimeError("Could not locate poll constants in web_save_sync.gd")
    src = src.replace(old_vars, new_vars, 1)

    # 2. Update _process for fast action polling + auto-ContinueButton trigger
    old_process = (
        "func _process(_delta: float) -> void:\n"
        "\tif not OS.has_feature(\"JavaScript\"):\n"
        "\t\treturn\n"
        "\tvar now = OS.get_ticks_msec()\n"
        "\tif now - _last_poll_msec < POLL_INTERVAL_MSEC:\n"
        "\t\treturn\n"
        "\t_last_poll_msec = now\n"
        "\t_check_pending_browser_actions()\n"
        "\tsync_to_browser_storage()"
    )
    new_process = (
        "func _process(_delta: float) -> void:\n"
        "\tif not OS.has_feature(\"JavaScript\"):\n"
        "\t\treturn\n"
        "\tvar now = OS.get_ticks_msec()\n"
        "\tif _pending_auto_continue_migration:\n"
        "\t\tif now > _pending_auto_continue_deadline_msec:\n"
        "\t\t\t_pending_auto_continue_migration = false\n"
        "\t\telse:\n"
        "\t\t\tvar tree = get_tree()\n"
        "\t\t\tvar scene = tree.current_scene if tree != null else null\n"
        "\t\t\tif scene != null and is_instance_valid(scene) and str(scene.filename).find(\"title_screen\") != -1:\n"
        "\t\t\t\tvar btn = scene.find_node(\"ContinueButton\", true, false)\n"
        "\t\t\t\tif btn != null and is_instance_valid(btn) and btn.visible and not btn.disabled:\n"
        "\t\t\t\t\t_pending_auto_continue_migration = false\n"
        "\t\t\t\t\tprint(\"[BO_WEB_SYNC] Auto-triggered ContinueButton for host migration!\")\n"
        "\t\t\t\t\tbtn.emit_signal(\"pressed\")\n"
        "\tif now - _last_action_poll_msec >= ACTION_POLL_INTERVAL_MSEC:\n"
        "\t\t_last_action_poll_msec = now\n"
        "\t\t_check_pending_browser_actions()\n"
        "\tif now - _last_poll_msec >= POLL_INTERVAL_MSEC:\n"
        "\t\t_last_poll_msec = now\n"
        "\t\tsync_to_browser_storage()"
    )
    if old_process not in src:
        raise RuntimeError("Could not locate _process in web_save_sync.gd")
    src = src.replace(old_process, new_process, 1)

    # 3. Add migrate_to_host and auto_rejoin_migrated_room handlers + helpers
    anchor = (
        "\t\t\tif peer_key != \"\" and rm != null and rm.has_method(\"kick_peer\"):\n"
        "\t\t\t\trm.call(\"kick_peer\", peer_key)"
    )
    migration_handlers = (
        anchor
        + "\n"
        "\telif action == \"migrate_to_host\":\n"
        "\t\tvar parent = get_parent()\n"
        "\t\tvar sm = parent.get_node_or_null(\"BrotatoOnlineSessionManager\") if parent != null else null\n"
        "\t\tvar slots_mgr = parent.get_node_or_null(\"BrotatoOnlineOnlinePlayerSlotManager\") if parent != null else null\n"
        "\t\tvar my_index = -1\n"
        "\t\tif slots_mgr != null and slots_mgr.has_method(\"get_local_mirrored_player_index\"):\n"
        "\t\t\tmy_index = int(slots_mgr.call(\"get_local_mirrored_player_index\"))\n"
        "\t\tvar tree = get_tree()\n"
        "\t\tif tree != null:\n"
        "\t\t\ttree.paused = false\n"
        "\t\tif sm != null:\n"
        "\t\t\tsm.set(\"_host_disconnect_notice_pending\", false)\n"
        "\t\t\tif sm.has_method(\"_is_in_active_online_run_scene\") and sm.call(\"_is_in_active_online_run_scene\") and sm.has_method(\"_freeze_current_run_for_host_disconnect\"):\n"
        "\t\t\t\tsm.call(\"_freeze_current_run_for_host_disconnect\")\n"
        "\t\t\tif sm.has_method(\"leave_lobby\"):\n"
        "\t\t\t\tsm.call(\"leave_lobby\")\n"
        "\t\tvar dir = Directory.new()\n"
        "\t\tdir.make_dir_recursive(\"user://user\")\n"
        "\t\tdir.make_dir_recursive(\"user://mod_data/six666-BrotatoOnline\")\n"
        "\t\tvar profile_id = int(ProgressData.current_profile_id) if ProgressData != null else 0\n"
        "\t\tvar raw_run_json = str(cmd.get(\"run_json\", \"\"))\n"
        "\t\tvar remaining_peers = int(cmd.get(\"remaining_peer_count\", 0))\n"
        "\t\tvar prepared_run_json = _prepare_migrated_run_json(raw_run_json, my_index, remaining_peers)\n"
        "\t\tvar target_run_path = \"user://user/run_v3_\" + str(profile_id) + \".json\"\n"
        "\t\tvar f = File.new()\n"
        "\t\tif prepared_run_json != \"\" and f.open(target_run_path, File.WRITE) == OK:\n"
        "\t\t\tf.store_string(prepared_run_json)\n"
        "\t\t\tf.close()\n"
        "\t\t_erase_checkpoint_identity(target_run_path)\n"
        "\t\tif ProgressData != null and ProgressData.has_method(\"load_profile_save\"):\n"
        "\t\t\tProgressData.call(\"load_profile_save\", profile_id)\n"
        "\t\t_last_snapshot_hash = 0\n"
        "\t\tsync_to_browser_storage()\n"
        "\t\t_pending_auto_continue_migration = true\n"
        "\t\t_pending_auto_continue_deadline_msec = OS.get_ticks_msec() + 10000\n"
        "\t\tif tree != null and MenuData != null:\n"
        "\t\t\ttree.change_scene(MenuData.title_screen_scene)\n"
        "\telif action == \"auto_rejoin_migrated_room\":\n"
        "\t\tvar parent = get_parent()\n"
        "\t\tvar sm = parent.get_node_or_null(\"BrotatoOnlineSessionManager\") if parent != null else null\n"
        "\t\tvar tree = get_tree()\n"
        "\t\tif tree != null:\n"
        "\t\t\ttree.paused = false\n"
        "\t\tif sm != null:\n"
        "\t\t\tsm.set(\"_host_disconnect_notice_pending\", false)\n"
        "\t\t\tvar was_in_run = sm.has_method(\"_is_in_active_online_run_scene\") and bool(sm.call(\"_is_in_active_online_run_scene\"))\n"
        "\t\t\tif was_in_run and sm.has_method(\"_freeze_current_run_for_host_disconnect\"):\n"
        "\t\t\t\tsm.call(\"_freeze_current_run_for_host_disconnect\")\n"
        "\t\t\tif sm.has_method(\"leave_lobby\"):\n"
        "\t\t\t\tsm.call(\"leave_lobby\")\n"
        "\t\t\tif was_in_run and tree != null and MenuData != null:\n"
        "\t\t\t\ttree.change_scene(MenuData.title_screen_scene)\n"
        "\t\t\tif sm.has_method(\"join_lan\"):\n"
        "\t\t\t\tvar port = int(cmd.get(\"port\", 27462))\n"
        "\t\t\t\tsm.join_lan(\"relay\", port if port > 0 else 27462)"
    )
    if anchor not in src:
        raise RuntimeError("Could not locate kick_peer anchor in web_save_sync.gd")
    src = src.replace(anchor, migration_handlers, 1)

    helpers = (
        "\n\nfunc _prepare_migrated_run_json(run_json: String, my_index: int, remaining_peers: int) -> String:\n"
        "\tvar parsed = parse_json(run_json)\n"
        "\tif typeof(parsed) != TYPE_DICTIONARY:\n"
        "\t\treturn run_json\n"
        "\tvar players = parsed.get(\"players_data\", null)\n"
        "\tif typeof(players) != TYPE_ARRAY or players.size() <= 1:\n"
        "\t\treturn run_json\n"
        "\tvar my_pos = my_index\n"
        "\tif parsed.has(\"bo_roster_slots\") and typeof(parsed[\"bo_roster_slots\"]) == TYPE_ARRAY:\n"
        "\t\tvar roster_pos = parsed[\"bo_roster_slots\"].find(my_index)\n"
        "\t\tif roster_pos >= 0:\n"
        "\t\t\tmy_pos = roster_pos\n"
        "\tif my_pos <= 0 or my_pos >= players.size():\n"
        "\t\tmy_pos = 1\n"
        "\tvar kept = [my_pos]\n"
        "\tif remaining_peers > 0:\n"
        "\t\tfor idx in range(1, players.size()):\n"
        "\t\t\tif idx != my_pos:\n"
        "\t\t\t\tkept.append(idx)\n"
        "\tfor key in MIGRATION_SAVE_ARRAYS:\n"
        "\t\tvar src_arr = parsed.get(key, null)\n"
        "\t\tif typeof(src_arr) != TYPE_ARRAY:\n"
        "\t\t\tcontinue\n"
        "\t\tvar next_arr = []\n"
        "\t\tfor pos in kept:\n"
        "\t\t\tif pos >= 0 and pos < src_arr.size():\n"
        "\t\t\t\tnext_arr.append(src_arr[pos])\n"
        "\t\tif key != \"players_data\" and not src_arr.empty():\n"
        "\t\t\tvar sample = src_arr[0]\n"
        "\t\t\twhile next_arr.size() < src_arr.size():\n"
        "\t\t\t\tif typeof(sample) == TYPE_ARRAY:\n"
        "\t\t\t\t\tnext_arr.append([])\n"
        "\t\t\t\telif typeof(sample) == TYPE_DICTIONARY:\n"
        "\t\t\t\t\tnext_arr.append({})\n"
        "\t\t\t\telif typeof(sample) == TYPE_BOOL:\n"
        "\t\t\t\t\tnext_arr.append(false)\n"
        "\t\t\t\telif typeof(sample) == TYPE_INT or typeof(sample) == TYPE_REAL:\n"
        "\t\t\t\t\tnext_arr.append(0)\n"
        "\t\t\t\telse:\n"
        "\t\t\t\t\tnext_arr.append(null)\n"
        "\t\tparsed[key] = next_arr\n"
        "\tparsed.erase(\"bo_roster_slots\")\n"
        "\tparsed.erase(\"bo_roster_revision\")\n"
        "\tif parsed.get(\"players_data\", []).size() == 1 and parsed.has(\"is_coop_run\"):\n"
        "\t\tparsed[\"is_coop_run\"] = false\n"
        "\treturn to_json(parsed)\n"
        "\n\nfunc _erase_checkpoint_identity(run_path: String) -> void:\n"
        "\tif not File.new().file_exists(run_path):\n"
        "\t\treturn\n"
        "\tvar store_path = \"user://mod_data/six666-BrotatoOnline/saved_identities.cfg\"\n"
        "\tvar store = ConfigFile.new()\n"
        "\tif store.load(store_path) != OK:\n"
        "\t\treturn\n"
        "\tvar key = run_path.sha256_text() + \":\" + File.new().get_sha256(run_path)\n"
        "\tif store.has_section_key(\"checkpoints\", key):\n"
        "\t\tstore.erase_section_key(\"checkpoints\", key)\n"
        "\t\tstore.save(store_path)\n"
    )
    src = src.rstrip() + helpers
    return src


def patch_lan_transport(src: str) -> str:
    if "_begin_host_reconnect" in src and "_try_host_reconnect" in src:
        return src

    # 1. Update _process reconnect branch to support both host and client
    old_proc = (
        "\tif _reconnect_active:\n"
        "\t\tif now >= _reconnect_deadline_msec:\n"
        "\t\t\t_finish_client_reconnect_failure(\"grace_timeout\")\n"
        "\t\t\treturn\n"
        "\t\tif _ws == null and now >= _reconnect_next_attempt_msec:\n"
        "\t\t\t_try_client_reconnect(now)"
    )
    new_proc = (
        "\tif _reconnect_active:\n"
        "\t\tif now >= _reconnect_deadline_msec:\n"
        "\t\t\tif _is_host:\n"
        "\t\t\t\t_finish_host_reconnect_failure(\"grace_timeout\")\n"
        "\t\t\telse:\n"
        "\t\t\t\t_finish_client_reconnect_failure(\"grace_timeout\")\n"
        "\t\t\treturn\n"
        "\t\tif _ws == null and now >= _reconnect_next_attempt_msec:\n"
        "\t\t\tif _is_host:\n"
        "\t\t\t\t_try_host_reconnect(now)\n"
        "\t\t\telse:\n"
        "\t\t\t\t_try_client_reconnect(now)"
    )
    if old_proc not in src:
        raise RuntimeError("Could not locate _reconnect_active block in lan_transport.gd")
    src = src.replace(old_proc, new_proc, 1)

    # 2. Clear host reconnect flags on _on_ws_connected
    old_connected = (
        "\tif _is_host:\n"
        "\t\t_last_room_info_sync_msec = 0\n"
        "\t\t_push_host_room_info()"
    )
    new_connected = (
        "\tif _is_host:\n"
        "\t\t_reconnect_active = false\n"
        "\t\t_reconnect_deadline_msec = 0\n"
        "\t\t_reconnect_next_attempt_msec = 0\n"
        "\t\t_last_room_info_sync_msec = 0\n"
        "\t\t_push_host_room_info()"
    )
    if old_connected not in src:
        raise RuntimeError("Could not locate _on_ws_connected host branch in lan_transport.gd")
    src = src.replace(old_connected, new_connected, 1)

    # 3. Update _on_ws_closed and _on_ws_error for host reconnect
    old_closed = (
        "func _on_ws_closed(_was_clean: bool = false) -> void:\n"
        "\t_ws_connected = false\n"
        "\tif not _active:\n"
        "\t\treturn\n"
        "\tif _is_host:\n"
        "\t\treturn\n"
        "\t_begin_client_reconnect(\"ws_closed\")"
    )
    new_closed = (
        "func _on_ws_closed(_was_clean: bool = false) -> void:\n"
        "\t_ws_connected = false\n"
        "\tif not _active:\n"
        "\t\treturn\n"
        "\tif _is_host:\n"
        "\t\t_begin_host_reconnect(\"ws_closed\")\n"
        "\t\treturn\n"
        "\t_begin_client_reconnect(\"ws_closed\")"
    )
    if old_closed not in src:
        raise RuntimeError("Could not locate _on_ws_closed in lan_transport.gd")
    src = src.replace(old_closed, new_closed, 1)

    old_error = (
        "func _on_ws_error() -> void:\n"
        "\t_ws_connected = false\n"
        "\tif not _active:\n"
        "\t\treturn\n"
        "\tif _is_host:\n"
        "\t\treturn"
    )
    new_error = (
        "func _on_ws_error() -> void:\n"
        "\t_ws_connected = false\n"
        "\tif not _active:\n"
        "\t\treturn\n"
        "\tif _is_host:\n"
        "\t\tif _reconnect_active:\n"
        "\t\t\t_discard_current_ws()\n"
        "\t\t\t_reconnect_next_attempt_msec = OS.get_ticks_msec() + RECONNECT_RETRY_INTERVAL_MSEC\n"
        "\t\t\treturn\n"
        "\t\t_begin_host_reconnect(\"ws_error\")\n"
        "\t\treturn"
    )
    if old_error not in src:
        raise RuntimeError("Could not locate _on_ws_error in lan_transport.gd")
    src = src.replace(old_error, new_error, 1)

    # 4. Append host reconnect helpers
    host_helpers = (
        "\n\nfunc _begin_host_reconnect(reason: String) -> void:\n"
        "\tif not _is_host or not _active:\n"
        "\t\treturn\n"
        "\tif _reconnect_active:\n"
        "\t\treturn\n"
        "\tvar now = OS.get_ticks_msec()\n"
        "\t_reconnect_active = true\n"
        "\t_reconnect_started_msec = now\n"
        "\t_reconnect_deadline_msec = now + 15000\n"
        "\t_reconnect_next_attempt_msec = now\n"
        "\t_reconnect_attempts = 0\n"
        "\t_diag_log(\"host_disconnect_grace_started\", \"reason=\" + reason)\n"
        "\t_discard_current_ws()\n"
        "\n\nfunc _try_host_reconnect(now: int) -> void:\n"
        "\tif not _reconnect_active or not _is_host:\n"
        "\t\treturn\n"
        "\t_reconnect_attempts += 1\n"
        "\tvar url = _resolve_ws_url(\"host\", \"\", _host_port)\n"
        "\tvar err = _connect_ws(url)\n"
        "\tif err != OK:\n"
        "\t\t_reconnect_next_attempt_msec = now + RECONNECT_RETRY_INTERVAL_MSEC\n"
        "\n\nfunc _finish_host_reconnect_failure(reason: String) -> void:\n"
        "\tif not _reconnect_active:\n"
        "\t\treturn\n"
        "\t_reconnect_active = false\n"
        "\t_diag_log(\"host_reconnect_failed\", \"reason=\" + reason)\n"
        "\temit_signal(\"connection_failed\", self, reason)\n"
        "\tstop()\n"
    )
    src = src.rstrip() + host_helpers
    return src


def append_patched_file(buf: bytearray, entry: dict, new_bytes: bytes):
    while len(buf) % 16 != 0:
        buf.append(0)
    new_offset = len(buf)
    new_size = len(new_bytes)
    new_md5 = hashlib.md5(new_bytes).digest()
    buf.extend(new_bytes)
    struct.pack_into("<QQ", buf, entry["meta_pos"], new_offset, new_size)
    buf[entry["meta_pos"] + 16 : entry["meta_pos"] + 32] = new_md5


def write_chunks_and_manifest(manifest: dict, buf: bytearray):
    chunk_size = int(manifest["chunkSize"])
    total = len(buf)
    num_chunks = (total + chunk_size - 1) // chunk_size
    new_chunks = []
    for idx in range(num_chunks):
        rel = f"chunks/index.pck.part{idx:02d}"
        out_path = os.path.join(PUBLIC_DIR, rel)
        chunk_bytes = buf[idx * chunk_size : (idx + 1) * chunk_size]
        with open(out_path, "wb") as f:
            f.write(chunk_bytes)
        new_chunks.append(rel)

    version_hash = hashlib.sha256(
        f"{total}:{num_chunks}:".encode("utf-8") + bytes(buf[-8192:])
    ).hexdigest()[:12]

    manifest["version"] = version_hash
    manifest["totalBytes"] = total
    manifest["chunks"] = new_chunks
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    with open(INDEX_HTML_PATH, "r", encoding="utf-8") as f:
        html = f.read()
    updated_html, count = re.subn(r'("index\.pck":\s*)\d+', rf"\g<1>{total}", html)
    if count != 1:
        raise RuntimeError(f"Expected 1 fileSizes index.pck replacement in index.html, got {count}")
    with open(INDEX_HTML_PATH, "w", encoding="utf-8") as f:
        f.write(updated_html)


def verify_pck():
    manifest, buf = load_pck_bytes()
    total = len(buf)
    if total != manifest["totalBytes"]:
        raise AssertionError(f"PCK chunk total {total} != manifest.totalBytes {manifest['totalBytes']}")

    with open(INDEX_HTML_PATH, "r", encoding="utf-8") as f:
        html = f.read()
    m = re.search(r'"index\.pck":\s*(\d+)', html)
    if not m or int(m.group(1)) != total:
        raise AssertionError(f"index.html fileSizes index.pck mismatch: {m.group(1) if m else None} != {total}")

    _, maj, minr, pat, file_count, entries = parse_pck_directory(buf)
    for target, required_marker in [
        (TARGET_WEB_SAVE_SYNC, 'action == "migrate_to_host"'),
        (TARGET_LAN_TRANSPORT, "_begin_host_reconnect"),
    ]:
        e = entries[target]
        data = bytes(buf[e["offset"] : e["offset"] + e["size"]])
        if hashlib.md5(data).digest() != e["md5"]:
            raise AssertionError(f"MD5 mismatch for {target}")
        text = data.decode("utf-8")
        if required_marker not in text:
            raise AssertionError(f"Missing marker {required_marker!r} in {target}")

    print(
        f"[OK] Verified PCK: Godot {maj}.{minr}.{pat}, files={file_count}, "
        f"totalBytes={total}, version={manifest['version']}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true", help="Verify PCK integrity without modifying")
    args = parser.parse_args()

    if args.verify:
        verify_pck()
        return

    manifest, buf = load_pck_bytes()
    _, _, _, _, _, entries = parse_pck_directory(buf)

    ws_entry = entries[TARGET_WEB_SAVE_SYNC]
    lt_entry = entries[TARGET_LAN_TRANSPORT]

    ws_src = bytes(buf[ws_entry["offset"] : ws_entry["offset"] + ws_entry["size"]]).decode("utf-8")
    lt_src = bytes(buf[lt_entry["offset"] : lt_entry["offset"] + lt_entry["size"]]).decode("utf-8")

    new_ws_src = patch_web_save_sync(ws_src)
    new_lt_src = patch_lan_transport(lt_src)

    changed = False
    if new_ws_src != ws_src:
        append_patched_file(buf, ws_entry, new_ws_src.encode("utf-8"))
        changed = True
        print(f"Patched {TARGET_WEB_SAVE_SYNC} ({len(ws_src)} -> {len(new_ws_src)} chars)")
    else:
        print(f"Already patched: {TARGET_WEB_SAVE_SYNC}")

    if new_lt_src != lt_src:
        append_patched_file(buf, lt_entry, new_lt_src.encode("utf-8"))
        changed = True
        print(f"Patched {TARGET_LAN_TRANSPORT} ({len(lt_src)} -> {len(new_lt_src)} chars)")
    else:
        print(f"Already patched: {TARGET_LAN_TRANSPORT}")

    if changed:
        write_chunks_and_manifest(manifest, buf)
        print("Updated PCK chunks, pck.manifest.json, and public/index.html")

    verify_pck()


if __name__ == "__main__":
    main()
