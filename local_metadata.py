from __future__ import annotations

import asyncio
import re
import tempfile
import time
from pathlib import Path
from typing import Any

from astrbot.api import logger

try:
    import libtorrent as lt
except ImportError:  # pragma: no cover - handled at runtime for graceful fallback
    lt = None


_INFO_HASH_RE = re.compile(r"urn:btih:([A-Fa-f0-9]{40})", re.IGNORECASE)

_FILE_TYPES = {
    "video": {".3gp", ".avi", ".flv", ".m2ts", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg", ".ts", ".webm", ".wmv"},
    "image": {".avif", ".bmp", ".gif", ".heic", ".jpeg", ".jpg", ".png", ".tif", ".tiff", ".webp"},
    "audio": {".aac", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".wav", ".wma"},
    "archive": {".7z", ".bz2", ".gz", ".iso", ".rar", ".tar", ".tgz", ".xz", ".zip"},
    "document": {".doc", ".docx", ".epub", ".mobi", ".odf", ".ods", ".odt", ".pdf", ".ppt", ".pptx", ".xls", ".xlsx"},
    "text": {".ass", ".csv", ".json", ".log", ".md", ".nfo", ".srt", ".ssa", ".sub", ".txt", ".xml", ".yaml", ".yml"},
}


class LocalMetadataResolver:
    """Fetches authenticated torrent metadata without downloading payload pieces."""

    def __init__(self, timeout: float = 25.0, cache_ttl: float = 86400.0, cache_size: int = 256):
        self.timeout = max(5.0, min(float(timeout), 120.0))
        self.cache_ttl = max(60.0, float(cache_ttl))
        self.cache_size = max(1, int(cache_size))
        self._session: Any = None
        self._lock = asyncio.Lock()
        self._cache: dict[str, tuple[float, dict]] = {}

    @property
    def available(self) -> bool:
        return lt is not None

    def start(self) -> None:
        if lt is None:
            return
        try:
            self._get_session()
            logger.info("本地磁链元数据解析器已启动，DHT 正在预热")
        except Exception as exc:
            logger.warning(f"本地磁链元数据解析器启动失败: {type(exc).__name__}: {exc}")

    async def resolve(self, magnet_link: str) -> dict | None:
        if lt is None:
            logger.warning("本地磁链元数据解析不可用：未安装 libtorrent")
            return None

        info_hash = self._info_hash(magnet_link)
        if not info_hash:
            return None

        cached = self._cache.get(info_hash)
        if cached and time.monotonic() - cached[0] < self.cache_ttl:
            return dict(cached[1])

        async with self._lock:
            cached = self._cache.get(info_hash)
            if cached and time.monotonic() - cached[0] < self.cache_ttl:
                return dict(cached[1])

            try:
                result = await asyncio.to_thread(self._resolve_sync, magnet_link)
            except Exception as exc:
                logger.warning(f"本地 DHT 元数据解析失败 hash={info_hash}: {type(exc).__name__}: {exc}")
                return None

            if result:
                self._cache[info_hash] = (time.monotonic(), result)
                self._trim_cache()
                logger.info(
                    f"本地 DHT 元数据解析成功 hash={info_hash} "
                    f"files={result.get('count', 0)} size={result.get('size', 0)}"
                )
                return dict(result)
            logger.info(f"本地 DHT 元数据解析超时 hash={info_hash} timeout={self.timeout:.0f}s")
            return None

    def close(self) -> None:
        self._cache.clear()
        self._session = None

    def _resolve_sync(self, magnet_link: str) -> dict | None:
        session = self._get_session()
        params = lt.parse_magnet_uri(magnet_link)
        params.save_path = tempfile.gettempdir()
        params.storage_mode = lt.storage_mode_t.storage_mode_sparse
        params.flags |= lt.torrent_flags.upload_mode
        handle = session.add_torrent(params)

        try:
            deadline = time.monotonic() + self.timeout
            while time.monotonic() < deadline:
                if handle.has_metadata():
                    torrent_info = handle.torrent_file()
                    if torrent_info is not None:
                        return self._metadata_from_torrent_info(torrent_info)
                time.sleep(0.25)
            return None
        finally:
            try:
                session.remove_torrent(handle, lt.options_t.delete_files)
            except Exception as exc:
                logger.debug(f"移除本地元数据任务失败: {exc}")

    def _get_session(self):
        if self._session is not None:
            return self._session

        self._session = lt.session({
            "listen_interfaces": "0.0.0.0:6881",
            "enable_dht": True,
            "enable_lsd": False,
            "enable_natpmp": False,
            "enable_upnp": False,
            "active_downloads": 1,
            "active_seeds": 0,
        })
        for host, port in (
            ("router.bittorrent.com", 6881),
            ("router.utorrent.com", 6881),
            ("dht.transmissionbt.com", 6881),
        ):
            self._session.add_dht_router(host, port)
        return self._session

    @staticmethod
    def _metadata_from_torrent_info(torrent_info) -> dict:
        files = torrent_info.files()
        entries = []
        total_size = 0
        type_sizes: dict[str, int] = {}

        for index in range(files.num_files()):
            if hasattr(files, "pad_file_at") and files.pad_file_at(index):
                continue
            path = LocalMetadataResolver._clean_text(files.file_path(index))
            size = int(files.file_size(index))
            total_size += size
            file_type = LocalMetadataResolver._classify_path(path)
            type_sizes[file_type] = type_sizes.get(file_type, 0) + size
            entries.append({"path": path, "size": size})

        entries.sort(key=lambda item: item["size"], reverse=True)
        dominant_type = max(type_sizes, key=type_sizes.get) if type_sizes else "unknown"
        name = LocalMetadataResolver._clean_text(torrent_info.name()) or "未知"

        return {
            "name": name,
            "size": total_size,
            "count": len(entries),
            "file_type": dominant_type,
            "files": entries,
            "metadata_source": "local_dht",
        }

    @staticmethod
    def _classify_path(path: str) -> str:
        suffix = Path(path).suffix.lower()
        for file_type, extensions in _FILE_TYPES.items():
            if suffix in extensions:
                return file_type
        return "unknown"

    @staticmethod
    def _clean_text(value: object) -> str:
        return str(value or "").replace("\r", " ").replace("\n", " ").strip()

    @staticmethod
    def _info_hash(magnet_link: str) -> str | None:
        match = _INFO_HASH_RE.search(magnet_link or "")
        return match.group(1).upper() if match else None

    def _trim_cache(self) -> None:
        now = time.monotonic()
        self._cache = {
            key: value for key, value in self._cache.items()
            if now - value[0] < self.cache_ttl
        }
        if len(self._cache) <= self.cache_size:
            return
        oldest = sorted(self._cache, key=lambda key: self._cache[key][0])
        for key in oldest[:len(self._cache) - self.cache_size]:
            self._cache.pop(key, None)
