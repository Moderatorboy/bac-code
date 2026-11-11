import asyncio
import hashlib
import hmac
import json
import os
import re
import sqlite3
import shutil
import subprocess
import tempfile
import time
import unicodedata
from contextlib import asynccontextmanager
from urllib.parse import quote

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from telethon import TelegramClient, errors as tgerr, events
from telethon.tl.custom import Button

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SIGN_SECRET = os.environ["SIGN_SECRET"].encode()
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "*")
ALLOWED_ORIGINS = [origin.strip() for origin in ALLOWED_ORIGIN.split(",") if origin.strip()]
if not ALLOWED_ORIGINS:
    ALLOWED_ORIGINS = ["*"]
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
LINK_TTL = int(os.environ.get("LINK_TTL", "21600"))

# Upload helper: sirf ye Telegram user IDs bot ko video bhej kar add kar sakte hain
ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()}
# Private channel jahan videos store hongi (e.g. -1001234567890)
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "0"))

CHUNK = 512 * 1024

# ---------- multi-bot pool: ek bot pe load badhe to agla bot ----------
MAX_STREAMS = int(os.environ.get("MAX_STREAMS_PER_BOT", "5"))  # ek bot par itni streams ek saath
SLOT_TTL = 2 * 3600  # atki hui slot 2 ghante baad apne aap hat jati hai


class Bot:
    def __init__(self, idx: int, token: str, tg_client):
        self.idx, self.token, self.client = idx, token, tg_client
        self.username = "?"
        self.ok = False        # start hua ya nahi
        self.down_until = 0.0  # FloodWait / connection error ke baad aaram
        self.bad_chats = {}    # chat_id -> is waqt tak skip (bot us channel mein nahi hai)
        self.slots = {}        # chal rahi streams

    @property
    def active(self) -> int:
        cutoff = time.time() - SLOT_TTL
        for k in [k for k, t in self.slots.items() if t < cutoff]:
            del self.slots[k]
        return len(self.slots)

    def acquire(self):
        slot = object()
        self.slots[slot] = time.time()
        return slot

    def release(self, slot):
        self.slots.pop(slot, None)

    def available(self, chat_id: int, now: float) -> bool:
        return self.ok and self.down_until <= now and self.bad_chats.get(chat_id, 0) <= now

    def mark_failed(self, e: Exception, chat_id: int):
        now = time.time()
        if isinstance(e, tgerr.FloodWaitError):
            secs = getattr(e, "seconds", 30)
            self.down_until = now + min(secs, 600) + 1
            why = f"FloodWait {secs}s"
        elif isinstance(e, (ValueError, tgerr.ChannelPrivateError)):
            self.bad_chats[chat_id] = now + 300
            why = f"channel {chat_id} ka access nahi"
        else:
            self.down_until = now + 20
            why = repr(e)
        print(f"[bots] bot {self.idx}: {why} -> agla bot try hoga")


class Pool:
    def __init__(self, bots):
        self.bots = bots

    def pick(self, chat_id: int, exclude=()):
        now = time.time()
        cands = [b for b in self.bots if b.idx not in exclude and b.available(chat_id, now)]
        if not cands:
            return None
        return min(cands, key=lambda b: (b.active, b.idx))


client = TelegramClient("bot_session", API_ID, API_HASH)  # bot 1: commands/upload bhi yahi sambhalta hai
_bots = [Bot(1, BOT_TOKEN, client)]
for _i in range(2, 6):  # BOT_TOKEN_2 ... BOT_TOKEN_5 (session file env ke number se judi rehti hai)
    _t = os.environ.get(f"BOT_TOKEN_{_i}", "").strip()
    if _t and _t not in {b.token for b in _bots}:
        _bots.append(Bot(_i, _t, TelegramClient(f"bot_session_{_i}", API_ID, API_HASH)))
POOL = Pool(_bots)

# videos.json load: id -> {chat_id, message_id, title}
VIDEOS = {}
if os.path.exists("videos.json"):
    with open("videos.json", encoding="utf-8") as f:
        DATA = json.load(f)
else:
    DATA = {"courses": []}


def _num(x):
    if x is None or isinstance(x, bool):
        return None
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _slug(x) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(x).lower()).strip("-") or "batch"


def _clean_lecture_title(title) -> str:
    title = str(title).splitlines()[0].strip() if str(title).splitlines() else ""
    title = title.split("\u27ad", 1)[0].split("\u2728", 1)[0].strip()
    title = re.sub(r"\s+", " ", title)
    return re.sub(r"\s+FREE(?=\.[a-z0-9]+$)|\s+FREE$", "", title, flags=re.IGNORECASE).strip()


def parse_batch(b: dict, cid: str):
    """Ek batch (alag-alag JSON formats) -> (videos, notes). Kharab entries chupchap skip."""
    chat = int(b["channel_id"])
    vids, notes, seen_v, seen_n = [], [], set(), set()
    campus_batch = str(b.get("display_name") or b.get("batch_name") or "").strip().casefold() == "campus x dsmp 2.0"

    def add_note(mid, title, chapter, url=None, lesson_id=None, kind="notes"):
        if url and str(url).startswith(("http://", "https://")):
            key = ("u", str(url))
        elif mid is not None:
            url, key = None, ("m", mid)
        else:
            return
        if key in seen_n:
            return
        seen_n.add(key)
        title = re.sub(r"\s+", " ", str(title)).strip()[:120] or "Notes"
        if url:
            notes.append({"id": f"{cid}-u{len(notes) + 1}", "title": title, "chapter": chapter,
                          "url": str(url), "lesson_id": lesson_id, "kind": kind})
        else:
            notes.append({"id": f"{cid}-n{mid}", "title": title, "chapter": chapter,
                          "chat_id": chat, "message_id": mid, "lesson_id": lesson_id, "kind": kind})

    def add_sheet(s, chapter, prefix="", kind="sheets"):
        if not isinstance(s, dict):
            return
        mid = next((m for m in (_num(s.get(k)) for k in ("notes_id", "file_id", "id", "sheet_id")) if m is not None), None)
        title = prefix + str(s.get("title") or s.get("name") or s.get("notes_title") or "Notes")
        add_note(mid, title, chapter, url=s.get("link"), kind=kind)

    image_note_count = 0
    for ch in b.get("chapters") or []:
        if not isinstance(ch, dict):
            continue
        cname = str(ch.get("chapter_name") or "").strip()
        last_video_id = None
        for l in ch.get("lectures") or []:
            if not isinstance(l, dict):
                continue
            if l.get("type") == "chapter":
                cname = str(l.get("chapter_name") or cname).strip()
                continue
            vid = _num(l.get("video_id"))
            raw_title = l.get("title") or l.get("lec_no") or ""
            title = _clean_lecture_title(raw_title) if campus_batch else str(raw_title).strip()
            if vid is None:
                add_sheet(l, cname)  # sirf notes wali entry
                continue
            if campus_batch and re.match(r"https?://", title, flags=re.IGNORECASE):
                add_note(None, "Study Material Link", cname, url=title, lesson_id=last_video_id, kind="sheets")
                continue
            document = re.search(
                r"\.(jpe?g|png|webp|pdf|html?|ipynb|xlsx?|csv|zip|pptx?|docx?)$",
                title,
                flags=re.IGNORECASE,
            ) if campus_batch else None
            if document:
                image_note_count += 1
                note_title = f"Notes {image_note_count}" if document.group(1).lower() in {"jpg", "jpeg", "png", "webp"} else title
                kind = "sheets" if document.group(1).lower() in {"html", "htm", "ipynb", "xlsx", "xls", "csv", "zip", "ppt", "pptx", "doc", "docx"} else "notes"
                add_note(vid, note_title, cname, lesson_id=last_video_id, kind=kind)
                continue
            source_cid = str(l.get("_source_course_id") or cid)
            video_key = (source_cid, vid)
            lesson_id = f"{source_cid}-v{vid}"
            if video_key not in seen_v:
                seen_v.add(video_key)
                vids.append({"id": lesson_id, "title": title or f"Lecture {vid}", "chapter": cname,
                            "unit": str(l.get("_source_unit") or ""),
                            "unit_id": str(l.get("_source_unit_id") or ""),
                            "chat_id": chat, "message_id": vid})
                last_video_id = lesson_id
            for k, tag in (("notes_id", "Notes"), ("sheet_id", "Sheet")):
                kind = "sheets" if k == "sheet_id" else "notes"
                add_note(_num(l.get(k)), f"{title} ({tag})", cname, lesson_id=lesson_id, kind=kind)
        for s2 in ch.get("sheets") or []:
            add_sheet(s2, cname, kind="sheets")
        for s2 in ch.get("dpps") or []:
            add_sheet(s2, cname, "DPP - ", kind="sheets")
    for s2 in b.get("study_material") or []:
        add_sheet(s2, "Study Material", kind="notes")
    return vids, notes


BATCH_THUMBNAILS = {
    "adca": "adca.jpg",
    "aktu3rd": "gw.jpg",
    "campus": "college.jpg",
    "chaicode": "chai.jpg",
    "codewithharry": "harry.jpg",
    "devops": "devops.png",
    "drop": "dropshipping.jpg",
    "dsaapna": "college.jpg",
    "earners": "EARNERS.jpg",
    "harkirat": "cohort.jpg",
    "hhld": "keerti.jpg",
    "ineuron": "ineuron.jpg",
    "jasonfedin": "jasonfedin.jpg",
    "love": "supreme.jpg",
    "modeling": "fmf.jpg",
    "otherbatch": "gw.jpg",
    "photographics": "photographics.jpg",
    "poh": "poh.png",
    "pwskill": "pwskill.jpg",
    "shreyansh": "Shreyansh.webp",
    "trading": "trading.jpg",
    "udemy": "udemy.jpg",
    "webdevinhindi": "college.jpg",
    "wedding": "rajaa.jpg",
}


def import_batches(path: str):
    """JSON file -> courses. Dict (group -> [batches]) aur seedhi list [batches], dono chalte hain."""
    with open(path, encoding="utf-8-sig") as f:
        raw = json.load(f)
    stem = _slug(os.path.splitext(os.path.basename(path))[0])
    thumbnail = BATCH_THUMBNAILS.get(stem, "college.jpg")
    thumbnail_url = f"{PUBLIC_BASE_URL}/image/{quote(thumbnail)}" if PUBLIC_BASE_URL else f"/image/{quote(thumbnail)}"
    groups = list(raw.items()) if isinstance(raw, dict) else [("", raw)]

    reserved_ids = set()

    def allocate_id(group, name):
        base = f"{group}-{_slug(name)}" if group else f"{stem}-{_slug(name)}"
        cid, k = base, 1
        while cid in reserved_ids or any(c["id"] == cid for c in DATA["courses"]):
            cid = f"{base}-{stem}" if k == 1 else f"{base}-{stem}-{k}"
            k += 1
        reserved_ids.add(cid)
        return cid

    def import_one(batch, name, legacy_ids=None, subject_batch=False, direct_subjects=None, subject_chapters=None,
                   display_name=None):
        cid = allocate_id(group, name)
        try:
            vids, notes = parse_batch(batch, cid)
            if not vids and not notes:
                print(f"[batches] {os.path.basename(path)}: '{name}' khaali hai, skip")
                return
            DATA["courses"].append({
                "id": cid, "title": display_name or name or cid, "thumb": batch.get("thumb") or thumbnail_url,
                "videos": vids, "notes": notes, "imported": True,
                "legacy_ids": legacy_ids or [], "subject_batch": subject_batch,
                "direct_subjects": direct_subjects or [],
                "subject_chapters": subject_chapters or [],
            })
        except Exception as e:  # ek kharab batch se baaki na ruke
            print(f"[batches] {os.path.basename(path)}: '{name}' skip ({e!r})")

    for group, batches in groups:
        if not isinstance(batches, list):
            continue
        parent_batches = {}
        for b in batches:
            if isinstance(b, dict) and b.get("parent_batch"):
                parent_batches.setdefault(str(b["parent_batch"]).strip(), []).append(b)
                continue
            name = b.get("batch_name", "") if isinstance(b, dict) else ""
            display_name = b.get("display_name") if isinstance(b, dict) else None
            import_one(b, name, display_name=display_name)

        for parent_name, subjects in parent_batches.items():
            chapters, legacy_ids, study_material, subject_chapters = [], [], [], []
            for subject in subjects:
                subject_name = str(subject.get("batch_name") or "").strip()
                source_cid = allocate_id(group, subject_name)
                legacy_ids.append(source_cid)
                lectures, sheets, dpps = [], [], []
                subject_units = []
                for section_index, section in enumerate(subject.get("chapters") or [], start=1):
                    if not isinstance(section, dict):
                        continue
                    unit_name = str(section.get("chapter_name") or "").strip()
                    unit_id = f"{source_cid}-unit{section_index}"
                    subject_units.append({"id": unit_id, "title": unit_name})
                    for lecture in section.get("lectures") or []:
                        if isinstance(lecture, dict):
                            lecture = dict(lecture)
                            lecture["_source_unit_id"] = unit_id
                            if _num(lecture.get("video_id")) is not None:
                                lecture["_source_course_id"] = source_cid
                                lecture["_source_unit"] = unit_name
                            lectures.append(lecture)
                    sheets.extend(section.get("sheets") or [])
                    dpps.extend(section.get("dpps") or [])
                chapters.append({
                    "chapter_name": subject_name,
                    "lectures": lectures,
                    "sheets": sheets,
                    "dpps": dpps,
                })
                subject_chapters.append({"subject": subject_name, "chapters": subject_units})
                study_material.extend(subject.get("study_material") or [])
            merged = {
                "batch_name": parent_name,
                "channel_id": subjects[0]["channel_id"],
                "thumb": next((subject.get("thumb") for subject in subjects if subject.get("thumb")), None),
                "chapters": chapters,
                "study_material": study_material,
            }
            direct_subjects = {
                str(subject.get("direct_subject") or "").strip()
                for subject in subjects
                if subject.get("direct_subject")
            }
            import_one(
                merged,
                parent_name,
                legacy_ids,
                subject_batch=True,
                direct_subjects=direct_subjects,
                subject_chapters=subject_chapters,
            )


def _find(name: str, want_dir: bool):
    """Windows/Linux dono pe chale: 'batches' ko 'Batches' folder bhi mil jaye."""
    ok = os.path.isdir if want_dir else os.path.isfile
    if ok(name):
        return name
    return next((n for n in os.listdir(".") if n.lower() == name.lower() and ok(n)), None)


def disabled_batch_names():
    folder = _find(os.environ.get("BATCHES_DIR", "batches"), True)
    if not folder:
        return set()
    names = set()
    for filename in os.listdir(folder):
        if not filename.lower().endswith(".json.disabled"):
            continue
        path = os.path.join(folder, filename)
        try:
            with open(path, encoding="utf-8-sig") as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            print(f"[batches] {path} disabled list read nahi hui: {e!r}")
            continue
        groups = raw.values() if isinstance(raw, dict) else [raw]
        for batches in groups:
            if not isinstance(batches, list):
                continue
            for batch in batches:
                if not isinstance(batch, dict):
                    continue
                name = batch.get("parent_batch") or batch.get("display_name") or batch.get("batch_name")
                if name:
                    names.add(str(name).strip().casefold())
    return names


def format_course_list(courses):
    ordered = sorted(courses, key=lambda course: str(course.get("id", "")).casefold())
    video_count = sum(len(course.get("videos") or []) for course in ordered)
    note_count = sum(len(course.get("notes") or []) for course in ordered)
    if not ordered:
        return "Koi course nahi"
    entries = [
        f"{index}. {course.get('title') or course.get('id')}\n"
        f"   🆔 ID: {course.get('id', '-')}\n"
        f"   🎬 Videos: {len(course.get('videos') or [])} | 📄 Notes: {len(course.get('notes') or [])}"
        for index, course in enumerate(ordered, start=1)
    ]
    heading = f"📚 Total Batches: {len(ordered)}\n🎬 Total Videos: {video_count} | 📄 Total Notes: {note_count}"
    return f"{heading}\n{'─' * 30}\n" + "\n\n".join(entries)


def load_all_batches():
    """batches.json + batches/ folder ki saari .json files load karta hai."""
    paths = []
    single = _find(os.environ.get("BATCHES_FILE", "batches.json"), False)
    if single:
        paths.append(single)
    folder = _find(os.environ.get("BATCHES_DIR", "batches"), True)
    if folder:
        nat = lambda n: [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", n.lower())]  # devops2 < devops10
        paths += [os.path.join(folder, n) for n in sorted(os.listdir(folder), key=nat) if n.lower().endswith(".json")]
    before = len(DATA["courses"])
    for p in paths:
        try:
            import_batches(p)
        except Exception as e:  # ek kharab file se baaki na ruke
            print(f"[batches] {p} load nahi hui: {e!r}")
    print(f"[batches] {len(paths)} file(s) se {len(DATA['courses']) - before} batch load hue")


load_all_batches()
for course in DATA["courses"]:
    for v in course["videos"]:
        VIDEOS[v["id"]] = v


def save_data():
    manual = {"courses": [c for c in DATA["courses"] if not c.get("imported")]}
    with open("videos.json", "w", encoding="utf-8") as f:
        json.dump(manual, f, ensure_ascii=False, indent=2)

# chhota cache taaki har Range request pe Telegram se message dubara na mangna pade
_msg_cache = {}  # video_id -> (expires_at, message)
MSG_TTL = 600
_duration_cache = {}
_duration_semaphore = asyncio.Semaphore(2)


# ---------- bot: video bhejo -> Stream/Download buttons, /add se course mein ----------
HELP_TEXT = (
    "Video bhejo to Stream aur Download ke buttons milenge.\n\n"
    "Course mein add karne ke liye:\n"
    "1) Video ko reply karke likho: /add course1 | Lesson title\n"
    "2) Ya video ke caption mein hi: /add course1 | Lesson title\n\n"
    "/addbatch - interactive batch creator\n"
    "/list - courses aur granted members\n"
    "/grant /denied /id /notify /append /delvideo /backup /bots"
)

# (sender_id, user ka message id) -> channel mein bani copy ka message id
UPLOADED = {}


def is_file_msg(m) -> bool:
    return bool(m and m.media and (m.video or m.document))


def file_links(message_id: int):
    exp = int(time.time()) + LINK_TTL
    sig = sign(f"file:{message_id}", exp)
    q = f"exp={exp}&sig={sig}"
    watch = f"{PUBLIC_BASE_URL}/watch/{message_id}?{q}"
    download = f"{PUBLIC_BASE_URL}/f/{message_id}?dl=1&{q}"
    return watch, download


def link_buttons(message_id: int):
    watch, download = file_links(message_id)
    return [[Button.url("Stream", watch), Button.url("Download", download)]]


def parse_add_args(arg: str, default_title: str):
    """'course1 | Title' / 'Title' / 'course1' -> (course ya None, title)"""
    manual = [c for c in DATA["courses"] if not c.get("imported")]
    course_ids = [c["id"] for c in manual]
    course_id, title = None, arg
    if "|" in arg:
        course_id, title = [p.strip() for p in arg.split("|", 1)]
    elif arg in course_ids:
        course_id, title = arg, ""
    if course_id:
        course = next((c for c in manual if c["id"] == course_id), None)
    else:
        course = manual[0] if manual else None
    return course, (title or default_title)


def add_entry(course: dict, title: str, channel_msg_id: int) -> dict:
    n = len(course["videos"]) + 1
    while f"{course['id']}-v{n}" in VIDEOS:
        n += 1
    entry = {
        "id": f"{course['id']}-v{n}",
        "title": title,
        "chat_id": CHANNEL_ID,
        "message_id": channel_msg_id,
    }
    course["videos"].append(entry)
    VIDEOS[entry["id"]] = entry
    save_data()
    return entry


def next_course_video_id(course):
    index = 1
    while f"{course['id']}-v{index}" in VIDEOS:
        index += 1
    return f"{course['id']}-v{index}"


def _course_overrides_path():
    return os.environ.get("COURSE_OVERRIDES_FILE", "course_overrides.json")


def save_course_override(course):
    path = _course_overrides_path()
    overrides = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            overrides = json.load(f)
    overrides[course["id"]] = {"videos": course["videos"], "notes": course.get("notes", [])}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(overrides, f, ensure_ascii=False, indent=2)


def apply_course_overrides():
    path = _course_overrides_path()
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        overrides = json.load(f)
    for course in DATA["courses"]:
        override = overrides.get(course["id"])
        if override:
            course["videos"] = override.get("videos", course["videos"])
            course["notes"] = override.get("notes", course.get("notes", []))


apply_course_overrides()
VIDEOS.clear()
for _course in DATA["courses"]:
    for _video in _course["videos"]:
        VIDEOS[_video["id"]] = _video


def has_media(message):
    return bool(message and message.media)


def clean_upload_caption(text):
    text = unicodedata.normalize("NFKC", str(text or ""))
    if "\u27ad" in text:
        text = text.split("\u27ad", 1)[0]
    text = re.sub(r"(?<!\w)[@#][\w.]+", " ", text)
    text = re.sub(r"(?i)\b(?:downloaded|uploaded|shared)\s+by\s*:?.*$", " ", text)
    text = re.sub(r"(?i)(?:^|\s)\bby\s*:?.*$", " ", text)
    return re.sub(r"\s+", " ", text).strip(" -|•")


def parse_internal_message_link(value):
    links = re.findall(r"https?://t\.me/c/(\d+)/(\d+)", str(value).strip())
    if not links:
        return None
    if len(links) > 2 or (len(links) == 2 and links[0][0] != links[1][0]):
        raise ValueError("Range ke dono Telegram links ek hi channel ke hone chahiye")
    raw_chat, start = links[0]
    last = int(links[1][1]) if len(links) == 2 else int(start)
    first = int(start)
    if first < 1 or last < first or last - first > 1000:
        raise ValueError("Telegram range 1 se 1001 messages ke andar hona chahiye")
    return int(f"-100{raw_chat}"), first, last


async def send_file_after_floodwait(chat_id, media, **kwargs):
    while True:
        try:
            return await client.send_file(chat_id, media, **kwargs)
        except tgerr.FloodWaitError as e:
            seconds = max(1, int(getattr(e, "seconds", 1)))
            print(f"[upload] FloodWait: {seconds}s; sequence pause karke retry hoga")
            await asyncio.sleep(seconds + 1)


async def prompt_batch_root(event, state):
    state["stage"] = "root"
    await event.reply(
        f"Batch: {state['title']}\nPehle Subject add karein, phir usmein Chapter aur Lecture.",
        buttons=[[Button.inline("➕ Subject", b"batch:subject"), Button.inline("✅ Publish", b"batch:publish")]],
        parse_mode=None,
    )


async def publish_user_batch(event, state):
    course = state["course"]
    if not course["videos"]:
        await event.reply("Batch mein kam se kam ek video add karna zaroori hai.")
        return
    save_data()
    custom_notification(
        f"Naya batch available: {course['title']}",
        title="Naya Batch",
        url=f"player.html?id={quote(course['id'])}",
    )
    pending_batches.pop(event.sender_id, None)
    await event.respond(f"✅ Batch publish ho gaya: {course['title']}\nID: {course['id']}")


pending_batches = {}
pending_notifications = set()
_outage_alerts = set()


def is_granted_admin(user_id):
    return DB.execute("SELECT 1 FROM bot_admins WHERE user_id=?", (user_id,)).fetchone() is not None


def new_user_course(title, thumb=None):
    cid = f"user-{_slug(title)}"
    suffix = 2
    while any(course["id"] == cid for course in DATA["courses"]):
        cid = f"user-{_slug(title)}-{suffix}"
        suffix += 1
    course = {
        "id": cid,
        "title": title,
        "thumb": thumb or "",
        "videos": [],
        "notes": [],
        "imported": False,
        "subject_batch": True,
        "direct_subjects": [],
        "subject_chapters": [],
    }
    DATA["courses"].append(course)
    return course


def sync_user_course_structure(course):
    subjects = {}
    for video in course["videos"]:
        subject = video.get("chapter", "")
        subjects.setdefault(subject, {})
        if video.get("unit") and video.get("unit") != "Other Chapters":
            subjects[subject][video.get("unit_id", video["unit"])] = {
                "id": video.get("unit_id", ""),
                "title": video["unit"],
            }
    course["direct_subjects"] = sorted(subject for subject, units in subjects.items() if not units)
    course["subject_chapters"] = [
        {"subject": subject, "chapters": list(units.values())}
        for subject, units in subjects.items()
    ]


def custom_notification(body, title="Notification", url=None, media_path=None):
    DB.execute(
        "INSERT INTO notifications (title, body, url, media_path, created_at) VALUES (?, ?, ?, ?, ?)",
        (title[:120], body[:2000], url, media_path, int(time.time())),
    )
    DB.commit()


async def notify_admins(text):
    for user_id in ADMIN_IDS:
        try:
            await client.send_message(user_id, text)
        except Exception as e:
            print(f"[alert] admin {user_id} ko message nahi ja saka: {e!r}")


async def alert_admins_once(key, text):
    if key not in _outage_alerts:
        _outage_alerts.add(key)
        await notify_admins(text)


async def mark_bot_failed(bot, error, chat_id):
    bot.mark_failed(error, chat_id)
    if isinstance(error, (ValueError, tgerr.ChannelPrivateError)):
        await alert_admins_once(
            f"channel:{chat_id}:bot:{bot.idx}",
            f"⚠️ Bot {bot.idx} channel {chat_id} access nahi kar paa raha. Bot ko source channel mein add/admin karein.",
        )


async def store_notification_message(message, title="Update", body_override=None):
    body = str((message.message if body_override is None else body_override) or "").strip()
    media_path = None
    if message.media:
        directory = os.environ.get("NOTIFICATION_MEDIA_DIR", "notification_media")
        os.makedirs(directory, exist_ok=True)
        extension = getattr(message.file, "ext", None) or ".bin"
        filename = f"{int(time.time())}-{message.id}{extension}"
        saved = await message.download_media(file=os.path.join(directory, filename))
        if not saved:
            raise RuntimeError("Notification media download nahi ho paya")
        media_path = filename
    if not body and not media_path:
        raise ValueError("Text, link ya media message bhejein")
    custom_notification(body or title, title=title, media_path=media_path)


async def start_add_batch(event, title, source=None):
    thumb = ""
    if source and getattr(source, "photo", None):
        thumb = await save_batch_thumbnail(source)
    state = {"stage": "batch_name", "title": "", "thumb": thumb}
    pending_batches[event.sender_id] = state
    if title:
        state["title"] = clean_upload_caption(title) or title[:120]
        state["course"] = new_user_course(state["title"], thumb)
        await prompt_batch_root(event, state)
    else:
        await event.reply("Batch ka naam bhejo. Cancel ke liye /cancel.")


async def save_batch_thumbnail(source):
    directory = os.path.join(os.path.dirname(__file__), "image")
    os.makedirs(directory, exist_ok=True)
    filename = f"custom-{int(time.time())}-{source.id}.jpg"
    saved = await source.download_media(file=os.path.join(directory, filename))
    if not saved:
        raise RuntimeError("Batch thumbnail save nahi ho paya")
    return f"{PUBLIC_BASE_URL}/image/{quote(filename)}" if PUBLIC_BASE_URL else f"/image/{quote(filename)}"


async def resolve_target_id(event, argument):
    if argument.strip().isdigit():
        user_id = int(argument.strip())
        try:
            sender = await client.get_entity(user_id)
            name = " ".join(filter(None, (getattr(sender, "first_name", None), getattr(sender, "last_name", None))))
            return user_id, name or argument.strip()
        except (ValueError, tgerr.RPCError):
            return user_id, argument.strip()
    replied = await event.get_reply_message()
    if replied:
        sender = await replied.get_sender()
        if sender:
            return sender.id, getattr(sender, "first_name", None) or str(sender.id)
    return None, None


async def send_uploads_to_channel(chat_id, uploads):
    sent = []
    for item in uploads:
        message = await send_file_after_floodwait(
            chat_id,
            item["source_message"].media,
            caption=item["caption"],
            force_document=False,
        )
        sent.append(message)
    return sent


async def commit_pending_videos(event, state):
    course = state["course"]
    chapter = state.get("chapter") or {
        "id": f"{course['id']}-{_slug(state['subject'])}-unit0",
        "title": "Other Chapters",
    }
    uploads = list(state["videos"])
    upload_count = len(uploads)
    if not uploads:
        await event.reply("Pehle 1 se 5 video bhejo ya Telegram channel link/range paste karo.")
        return
    new_items = []
    seen_sources = {(v["chat_id"], v["message_id"]) for v in course["videos"]}
    for index, item in enumerate(uploads, start=1):
        if item.get("source_message") is None and (item["chat_id"], item["message_id"]) in seen_sources:
            state["videos"].remove(item)
            continue
        item_title = item.get("caption") if item.get("source_message") is None else ""
        title = item_title or state["lecture"] + (f" - Part {index}" if upload_count > 1 else "")
        lesson_id = next_course_video_id(course)
        chat_id, message_id = item["chat_id"], item["message_id"]
        caption = item.get("caption") or title
        if item.get("source_message") is not None:
            sent = await send_file_after_floodwait(
                CHANNEL_ID, item["source_message"].media, caption=caption, force_document=False
            )
            chat_id, message_id = CHANNEL_ID, sent.id
        video = {
            "id": lesson_id,
            "title": title,
            "chapter": state["subject"],
            "unit": chapter["title"],
            "unit_id": chapter["id"],
            "chat_id": chat_id,
            "message_id": message_id,
        }
        new_items.append(video)
        course["videos"].append(video)
        VIDEOS[lesson_id] = video
        seen_sources.add((chat_id, message_id))
        state["videos"].remove(item)
    if not new_items:
        state["stage"] = "chapter_menu"
        await event.reply("Is chapter mein ye video messages pehle se hain; duplicate skip kiye.")
        return
    state["current_lessons"] = [v for v in course["videos"] if v["id"] in {n["id"] for n in new_items}]
    state["stage"] = "notes"
    state["resources"] = []
    await event.reply(
        f"✅ {len(new_items)} video(s) add ho gaye. Notes bhejo, phir /done.\n"
        "Notes nahi hain to Cancel button dabao.",
        buttons=[[Button.inline("Cancel notes", b"batch:skip-notes"), Button.inline("Cancel batch", b"batch:cancel")]],
        parse_mode=None,
    )


async def attach_pending_resources(event, state, kind):
    course = state["course"]
    resources = list(state.get("resources", []))
    for index, resource in enumerate(resources, start=1):
        sent = await send_file_after_floodwait(
            CHANNEL_ID,
            resource["source_message"].media,
            caption=clean_upload_caption(resource.get("caption")) or f"{kind.title()} {index}",
        )
        for lesson in state["current_lessons"]:
            course["notes"].append({
                "id": f"{course['id']}-{lesson['id']}-n{sent.id}",
                "title": clean_upload_caption(resource.get("caption")) or f"{kind.title()} {index}",
                "kind": kind,
                "chapter": lesson.get("chapter", ""),
                "lesson_id": lesson["id"],
                "chat_id": CHANNEL_ID,
                "message_id": sent.id,
            })
        state["resources"].remove(resource)
    if kind == "notes":
        state["stage"] = "sheets"
        await event.reply(
            "Ab Sheets bhejo, phir /done. Sheets nahi hain to Cancel button dabao.",
            buttons=[[Button.inline("Cancel sheets", b"batch:skip-sheets"), Button.inline("Cancel batch", b"batch:cancel")]],
            parse_mode=None,
        )
    else:
        sync_user_course_structure(course)
        save_data()
        state["stage"] = "chapter_menu"
        await event.reply(
            "Lecture aur attachments save ho gaye.",
            buttons=[[Button.inline("➕ Lecture", b"batch:lecture"), Button.inline("✅ Publish", b"batch:publish")]],
            parse_mode=None,
        )


@client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
async def on_private_message(event):
    msg = event.message
    text = (msg.raw_text or "").strip()
    sender_id = event.sender_id

    if text == "/id":
        await event.reply(f"Telegram ID: `{sender_id}`", parse_mode="md")
        return
    if sender_id not in ADMIN_IDS and not is_granted_admin(sender_id):
        return

    if text == "/cancel" and sender_id in pending_batches:
        pending_batches.pop(sender_id, None)
        await event.reply("Batch creation cancel ho gaya.")
        return

    if sender_id in pending_notifications:
        if text == "/cancel":
            pending_notifications.discard(sender_id)
            await event.reply("Notification cancel ho gayi.")
        else:
            await store_notification_message(msg, body_override=text)
            pending_notifications.discard(sender_id)
            await event.reply("✅ Notification sab users ke liye website par publish ho gayi.")
        return

    state = pending_batches.get(sender_id)
    if state:
        stage = state.get("stage")
        if stage in {"batch_name", "subject_name", "chapter_name", "lecture_name"} and text and not text.startswith("/"):
            if stage == "batch_name":
                if getattr(msg, "photo", None):
                    state["thumb"] = await save_batch_thumbnail(msg)
                state["title"] = clean_upload_caption(text) or text[:120]
                state["course"] = new_user_course(state["title"], state.get("thumb"))
                await prompt_batch_root(event, state)
            elif stage == "subject_name":
                state["subject"] = text[:120].strip()
                state["chapter"] = None
                state["chapters"] = []
                state["stage"] = "subject_menu"
                await event.reply(
                    f"Subject: {state['subject']}. Ab add Chapter ya direct Lecture karein.",
                    buttons=[[Button.inline("➕ Chapter", b"batch:chapter"), Button.inline("➕ Lecture", b"batch:lecture")],
                             [Button.inline("➕ Subject", b"batch:subject"), Button.inline("✅ Publish", b"batch:publish")]],
                    parse_mode=None,
                )
            elif stage == "chapter_name":
                chapter = {
                    "id": f"{state['course']['id']}-{_slug(state['subject'])}-unit{len(state['chapters']) + 1}",
                    "title": text[:120].strip(),
                }
                state["chapters"].append(chapter)
                state["chapter"] = chapter
                state["stage"] = "chapter_menu"
                await event.reply(
                    f"Chapter: {chapter['title']}. Ab Lecture add karein.",
                    buttons=[[Button.inline("➕ Lecture", b"batch:lecture"), Button.inline("✅ Publish", b"batch:publish")]],
                    parse_mode=None,
                )
            else:
                state["lecture"] = text[:160].strip()
                state["videos"] = []
                state["stage"] = "videos"
                await event.reply(
                    f"Lecture: {state['lecture']}\n1-5 videos bhejo, ya t.me/c/CHANNEL/START END range paste karo. Phir /done.",
                    buttons=[[Button.inline("Cancel lecture", b"batch:cancel-lecture"), Button.inline("Cancel batch", b"batch:cancel")]],
                    parse_mode=None,
                )
            return
        if stage in {"videos", "notes", "sheets"}:
            if text == "/done":
                try:
                    if stage == "videos":
                        await commit_pending_videos(event, state)
                    else:
                        await attach_pending_resources(event, state, stage)
                except (tgerr.RPCError, OSError, RuntimeError) as e:
                    print(f"[addbatch] {stage} upload failed for {sender_id}: {e!r}")
                    await event.reply(f"Upload nahi ho paya. Files pending rakhe hain; /done se retry karein. Error: {e}")
                return
            if stage == "videos" and text:
                try:
                    link = parse_internal_message_link(text)
                except ValueError as e:
                    await event.reply(str(e))
                    return
                if link:
                    chat_id, first, last = link
                    try:
                        messages = await client.get_messages(chat_id, ids=list(range(first, last + 1)))
                    except (tgerr.RPCError, OSError) as e:
                        await event.reply(f"Channel messages nahi padh paya. Bot ko source channel admin karein. Error: {e}")
                        return
                    for source in messages:
                        if source and source.media and not any(
                            item["chat_id"] == chat_id and item["message_id"] == source.id
                            for item in state["videos"]
                        ):
                            caption = clean_upload_caption(source.message or source.file.name or "")
                            state["videos"].append({
                                "chat_id": chat_id, "message_id": source.id, "caption": caption,
                                "source_message": None,
                            })
                    await event.reply(f"Range se {len(state['videos'])} videos collect hue. /done bhejein.")
                    return
            if has_media(msg):
                bucket = state["videos"] if stage == "videos" else state["resources"]
                if stage == "videos" and len(bucket) >= 5:
                    await event.reply("Ek lecture mein maximum 5 videos bhej sakte hain.")
                    return
                bucket.append({
                    "source_message": msg,
                    "chat_id": msg.chat_id or sender_id,
                    "message_id": msg.id,
                    "caption": clean_upload_caption(msg.message or msg.file.name or ""),
                })
                await event.reply(f"{len(bucket)} file(s) receive hui. Aur bhejo ya /done likho.")
                return

    if text.startswith("/addbatch"):
        source = msg if getattr(msg, "photo", None) else await event.get_reply_message()
        argument = text[len("/addbatch"):].strip()
        if "|" in argument:
                argument = argument.split("|", 1)[1].strip()
        try:
            await start_add_batch(event, argument, source)
        except (OSError, RuntimeError) as e:
            print(f"[addbatch] setup failed for {sender_id}: {e!r}")
            await event.reply(f"Batch start nahi ho paya: {e}")
        return

    if text.startswith("/grant") or text.startswith("/denied"):
        command = text.split(maxsplit=1)[0]
        if sender_id not in ADMIN_IDS:
                await event.reply("Ye command sirf owner chala sakta hai.")
                return
        argument = text[len(command):].strip()
        target_id, display_name = await resolve_target_id(event, argument)
        if not target_id:
                await event.reply(f"Use: {command} <Telegram user ID> ya us user ke message par reply karein.")
                return
        if target_id in ADMIN_IDS:
                await event.reply("Owner ka access change nahi kar sakte.")
                return
        if command == "/grant":
                DB.execute(
                    "INSERT INTO bot_admins (user_id, display_name, granted_by, granted_at) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, granted_by=excluded.granted_by",
                    (target_id, display_name, sender_id, int(time.time())),
                )
                DB.commit()
                await event.reply(f"✅ {display_name} ({target_id}) ko batch-management access mil gaya.")
        else:
                DB.execute("DELETE FROM bot_admins WHERE user_id=?", (target_id,))
                DB.commit()
                pending_batches.pop(target_id, None)
                pending_notifications.discard(target_id)
                await event.reply(f"Access revoke kar diya: {display_name} ({target_id}).")
        return

    if text == "/notify" or text.startswith("/notify "):
        body = text[len("/notify"):].strip()
        replied = await event.get_reply_message()
        source = msg if msg.media else replied
        if body or (source and source.media):
                body_override = body if body or source is msg else None
                try:
                    await store_notification_message(source or msg, body_override=body_override)
                    await event.reply("✅ Notification sab users ke liye website par publish ho gayi.")
                except (OSError, RuntimeError, ValueError) as e:
                    await event.reply(f"Notification save nahi ho payi: {e}")
        else:
                pending_notifications.add(sender_id)
                await event.reply("Ab ek text, link, photo, GIF ya video bhejo. Cancel ke liye /cancel.")
        return

    if text.startswith("/append"):
        argument = text[len("/append"):].strip()
        if "|" not in argument:
                await event.reply("Use: /append <batch_id> | Lecture title, aur video ya t.me/c link attach/reply karein.")
                return
        course_id, title = [part.strip() for part in argument.split("|", 1)]
        course = course_by_id(course_id)
        if not course:
                await event.reply("Batch ID nahi mila. /list se check karein.")
                return
        source = msg if has_media(msg) else await event.get_reply_message()
        if source and has_media(source):
                entry = {
                    "id": next_course_video_id(course),
                    "title": clean_upload_caption(title) or "Lecture",
                    "chapter": "Added Lectures",
                    "unit": "Added Lectures",
                    "unit_id": f"{course_id}-added",
                    "chat_id": CHANNEL_ID,
                    "message_id": source.id,
                }
                sent = await send_file_after_floodwait(
                    CHANNEL_ID, source.media, caption=clean_upload_caption(source.message) or entry["title"]
                )
                entry["message_id"] = sent.id
        else:
                link_source = source.raw_text if source else argument
                link = parse_internal_message_link(link_source) or parse_internal_message_link(argument)
                if not link:
                    await event.reply("Video attach ya reply karein, ya t.me/c/channel/start se end link bhejein.")
                    return
                chat_id, first, last = link
                new_messages = await client.get_messages(chat_id, ids=list(range(first, last + 1)))
                existing = {(video["chat_id"], video["message_id"]) for video in course["videos"]}
                added = []
                for source_message in new_messages:
                    if not source_message or not source_message.media or (chat_id, source_message.id) in existing:
                        continue
                    entry = {
                        "id": next_course_video_id(course),
                        "title": clean_upload_caption(source_message.message) or f"{title} {len(added) + 1}",
                        "chapter": "Added Lectures", "unit": "Added Lectures", "unit_id": f"{course_id}-added",
                        "chat_id": chat_id, "message_id": source_message.id,
                    }
                    added.append(entry)
                    course["videos"].append(entry)
                    VIDEOS[entry["id"]] = entry
                if course.get("imported"):
                    save_course_override(course)
                else:
                    save_data()
                await event.reply(f"✅ {len(added)} nayi videos append hui; duplicates skip kiye.")
                return
        course["videos"].append(entry)
        VIDEOS[entry["id"]] = entry
        if course.get("imported"):
                save_course_override(course)
        else:
                save_data()
        await event.reply(f"✅ Video append hui: {entry['id']}")
        return

    if text.startswith("/delvideo"):
        video_id = text[len("/delvideo"):].strip()
        course = course_of(video_id)
        if not course:
                await event.reply("Video ID nahi mila.")
                return
        course["videos"] = [video for video in course["videos"] if video["id"] != video_id]
        course["notes"] = [note for note in course.get("notes", []) if note.get("lesson_id") != video_id]
        VIDEOS.pop(video_id, None)
        DB.execute("DELETE FROM done WHERE video_id=?", (video_id,))
        DB.execute("DELETE FROM watch_progress WHERE video_id=?", (video_id,))
        DB.commit()
        if course.get("imported"):
            save_course_override(course)
        else:
            save_data()
        await event.reply(f"✅ Video aur uske notes delete hue: {video_id}")
        return

    if text == "/backup":
        paths = [path for path in ("videos.json", _course_overrides_path()) if os.path.isfile(path)]
        if not os.path.isfile(DB_PATH):
            await event.reply("Backup ke liye enrollment database nahi mili.")
            return
        backup_file = tempfile.NamedTemporaryFile(prefix="codextrms-backup-", suffix=".db", delete=False)
        backup_file.close()
        backup_db = sqlite3.connect(backup_file.name)
        try:
            DB.backup(backup_db)
            paths.append(backup_file.name)
            await event.reply("Backup files bhej raha hoon...")
            for path in paths:
                caption = "enrollments.db" if path == backup_file.name else os.path.basename(path)
                await client.send_file(event.chat_id, path, caption=caption)
        finally:
            backup_db.close()
            if os.path.isfile(backup_file.name):
                os.remove(backup_file.name)
        return

    # ---- /bots: load aur status ----
    if text == "/bots" and not is_file_msg(msg):
        now = time.time()
        lines = []
        for b in POOL.bots:
            st = "band" if not b.ok else (f"rest {int(b.down_until - now)}s" if b.down_until > now else "ok")
            lines.append(f"Bot {b.idx} @{b.username}: {b.active}/{MAX_STREAMS} streams, {st}")
        await event.reply("\n".join(lines), parse_mode=None)
        return

    # ---- /list ----
    if text == "/list" and not is_file_msg(msg):
        disabled_names = disabled_batch_names()
        courses = [c for c in DATA["courses"] if str(c.get("title", "")).strip().casefold() not in disabled_names]
        members = DB.execute("SELECT user_id, display_name FROM bot_admins ORDER BY display_name COLLATE NOCASE").fetchall()
        suffix = "\n\n👥 Granted members:\n" + "\n".join(f"{name or 'Unknown'} — {uid}" for uid, name in members) if members else ""
        await event.reply(format_course_list(courses) + suffix, parse_mode=None)
        return

    # ---- /add: course mein jodo ----
    if text.startswith("/add"):
        arg = text[4:].strip()
        source = msg if is_file_msg(msg) else await event.get_reply_message()
        if not is_file_msg(source):
            await event.reply(
                "Video ke caption mein ya video ko reply karke likho:\n/add course1 | Lesson title",
                parse_mode=None,
            )
            return
        default_title = source.file.name if source.file and source.file.name else "Untitled"
        course, title = parse_add_args(arg, default_title)
        if not course:
            await event.reply("Course id nahi mila. /list se check karo.", parse_mode=None)
            return

        key = (event.sender_id, source.id)
        channel_msg_id = UPLOADED.get(key)
        if channel_msg_id is None:  # pehle se channel mein nahi hai to ab daalo
            sent = await client.send_file(CHANNEL_ID, source.media, caption=title)
            channel_msg_id = sent.id
            UPLOADED[key] = channel_msg_id

        entry = add_entry(course, title, channel_msg_id)
        await event.reply(
            f"Course mein add ho gaya\n"
            f"Course: {course['title']}\n"
            f"Video ID: {entry['id']}\n"
            f"Title: {title}",
            buttons=link_buttons(channel_msg_id),
            parse_mode=None,
        )
        return

    # ---- sirf video: Stream / Download buttons ----
    if is_file_msg(msg):
        name = msg.file.name if msg.file and msg.file.name else "video"
        key = (event.sender_id, msg.id)
        channel_msg_id = UPLOADED.get(key)
        if channel_msg_id is None:
            sent = await client.send_file(CHANNEL_ID, msg.media, caption=name)
            channel_msg_id = sent.id
            UPLOADED[key] = channel_msg_id
        size_mb = (msg.file.size or 0) / (1024 * 1024)
        await event.reply(
            f"{name}\nSize: {size_mb:.1f} MB\n\n"
            f"Course mein add karna ho to is video ko reply karke:\n/add course1 | Title",
            buttons=link_buttons(channel_msg_id),
            parse_mode=None,
        )
        return

    await event.reply(HELP_TEXT, parse_mode=None)


@client.on(events.CallbackQuery)
async def on_batch_callback(event):
    sender_id = event.sender_id
    if sender_id not in ADMIN_IDS and not is_granted_admin(sender_id):
        await event.answer("Permission nahi hai.", alert=True)
        return
    state = pending_batches.get(sender_id)
    if not state:
        await event.answer("Ye batch session expire ho gaya. /addbatch phir se chalao.", alert=True)
        return
    action = event.data.decode("utf-8").split(":", 1)[-1]
    if action == "subject":
        state["stage"] = "subject_name"
        await event.answer()
        await event.respond("Subject ka naam bhejo.")
    elif action == "chapter":
        if not state.get("subject"):
            await event.answer("Pehle subject add karein.", alert=True)
            return
        state["stage"] = "chapter_name"
        await event.answer()
        await event.respond(f"'{state['subject']}' ka chapter name bhejo.")
    elif action == "lecture":
        if not state.get("subject"):
            await event.answer("Pehle subject add karein.", alert=True)
            return
        if not state.get("chapter"):
            state["chapter"] = {
                "id": f"{state['course']['id']}-{_slug(state['subject'])}-unit0",
                "title": "Other Chapters",
            }
        state["stage"] = "lecture_name"
        await event.answer()
        await event.respond("Lecture ka naam bhejo.")
    elif action == "publish":
        await event.answer()
        await publish_user_batch(event, state)
    elif action == "skip-notes":
        state["resources"] = []
        state["stage"] = "sheets"
        await event.answer()
        await event.respond(
            "Ab Sheets bhejo, phir /done. Sheets nahi hain to Cancel sheets dabao.",
            buttons=[[Button.inline("Cancel sheets", b"batch:skip-sheets"), Button.inline("Cancel batch", b"batch:cancel")]],
        )
    elif action == "skip-sheets":
        sync_user_course_structure(state["course"])
        save_data()
        state["stage"] = "chapter_menu"
        await event.answer()
        await event.respond(
            "Lecture aur attachments save ho gaye.",
            buttons=[[Button.inline("➕ Lecture", b"batch:lecture"), Button.inline("✅ Publish", b"batch:publish")]],
        )
    elif action == "cancel-lecture":
        state["videos"] = []
        state["stage"] = "chapter_menu"
        await event.answer("Lecture cancel ho gaya.")
    elif action == "cancel":
        course = state.get("course")
        if course:
            for video in course["videos"]:
                VIDEOS.pop(video["id"], None)
            DATA["courses"] = [item for item in DATA["courses"] if item["id"] != course["id"]]
            save_data()
        pending_batches.pop(sender_id, None)
        await event.answer("Batch cancel ho gaya.")


async def start_bots():
    for b in POOL.bots:
        try:
            await b.client.start(bot_token=b.token)
            me = await b.client.get_me()
            if str(me.id) != b.token.split(":")[0]:
                name = "bot_session" if b.idx == 1 else f"bot_session_{b.idx}"
                print(f"[bots] bot {b.idx}: {name}.session kisi aur bot ka hai. Use delete karke dobara chalao")
                continue
            b.username, b.ok = me.username or str(me.id), True
            print(f"[bots] bot {b.idx} ready: @{b.username}")
        except Exception as e:
            print(f"[bots] bot {b.idx} start nahi hua: {e!r}")
    if not POOL.bots[0].ok:
        raise RuntimeError("Bot 1 (BOT_TOKEN) start nahi hua")


async def check_channels():
    """Har bot ko har channel ka access hai ya nahi, startup par hi bata deta hai."""
    sample = {}
    for v in VIDEOS.values():
        sample.setdefault(v["chat_id"], v["message_id"])
    for c in DATA["courses"]:
        for n in c.get("notes", []):
            if "chat_id" in n:
                sample.setdefault(n["chat_id"], n["message_id"])
    if CHANNEL_ID:
        sample.setdefault(CHANNEL_ID, None)
    accessible = {chat: False for chat in sample}
    for b in POOL.bots:
        if not b.ok:
            continue
        bad = []
        for chat, mid in sample.items():
            try:
                if mid is None:
                    await b.client.get_input_entity(chat)
                else:
                    await b.client.get_messages(chat, ids=mid)
                accessible[chat] = True
            except Exception:
                bad.append(chat)
                b.bad_chats[chat] = time.time() + 300
        print(f"[bots] bot {b.idx}: " + (f"{len(sample)} channel OK" if not bad else f"ACCESS NAHI: {bad}"))
    for chat_id, has_access in accessible.items():
        key = f"channel:{chat_id}"
        if has_access:
            _outage_alerts.discard(key)
        else:
            await alert_admins_once(key, f"⚠️ Koi configured bot channel {chat_id} access nahi kar paa raha. Bot ko channel mein add/admin karein.")


async def monitor_bot_health():
    while True:
        await asyncio.sleep(30)
        now = time.time()
        if any(bot.ok and bot.down_until <= now for bot in POOL.bots):
            _outage_alerts.discard("all-bots")
        else:
            await alert_admins_once("all-bots", "🚨 Saare streaming bots offline ya FloodWait mein hain.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_bots()
    tasks = [
        asyncio.create_task(check_channels()),
        asyncio.create_task(monitor_bot_health()),
        asyncio.create_task(notification_cleanup_loop()),
    ]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for b in POOL.bots:
        if b.ok:
            await b.client.disconnect()


app = FastAPI(lifespan=lifespan)
app.mount("/image", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "image")), name="image")
NOTIFICATION_MEDIA_DIR = os.environ.get("NOTIFICATION_MEDIA_DIR", "notification_media")
os.makedirs(NOTIFICATION_MEDIA_DIR, exist_ok=True)
app.mount("/notification-media", StaticFiles(directory=NOTIFICATION_MEDIA_DIR), name="notification-media")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?|null",
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=True,
)


# ---------- signed link ----------
def sign(video_id: str, exp: int) -> str:
    return hmac.new(SIGN_SECRET, f"{video_id}:{exp}".encode(), hashlib.sha256).hexdigest()


# ---------- free enrollment ----------
TOKEN_TTL = 365 * 24 * 3600  # 1 saal (anonymous account hai, expire hone par progress chala jata)

DB_PATH = os.environ.get("DATABASE_FILE", "enrollments.db")
DB = sqlite3.connect(DB_PATH, check_same_thread=False)
DB.execute(
    "CREATE TABLE IF NOT EXISTS users ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, "
    "email TEXT UNIQUE, created_at INTEGER)"
)
DB.executescript("""
CREATE TABLE IF NOT EXISTS bot_admins (
    user_id INTEGER PRIMARY KEY,
    display_name TEXT,
    granted_by INTEGER,
    granted_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    url TEXT,
    media_path TEXT,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS watch_progress (
    user_id INTEGER NOT NULL,
    video_id TEXT NOT NULL,
    position REAL NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (user_id, video_id)
);
""")
DB.executemany(
    "INSERT OR IGNORE INTO bot_admins (user_id, display_name, granted_by, granted_at) VALUES (?, ?, ?, ?)",
    ((user_id, "Owner", user_id, int(time.time())) for user_id in ADMIN_IDS),
)
DB.commit()


def make_token(user_id: int) -> str:
    exp = int(time.time()) + TOKEN_TTL
    sig = hmac.new(SIGN_SECRET, f"user:{user_id}:{exp}".encode(), hashlib.sha256).hexdigest()
    return f"{user_id}.{exp}.{sig}"


def require_enrolled(request: Request) -> int:
    """Authorization: Bearer <token> check karta hai. Valid ho to user_id return."""
    auth = request.headers.get("authorization", "")
    token = auth[7:] if auth.lower().startswith("bearer ") else ""
    try:
        uid, exp, sig = token.split(".")
        expected = hmac.new(SIGN_SECRET, f"user:{uid}:{exp}".encode(), hashlib.sha256).hexdigest()
        if int(exp) < time.time() or not hmac.compare_digest(sig, expected):
            raise ValueError
        return int(uid)
    except ValueError:
        raise HTTPException(401, "Please enroll first")


ENROLL_LIMIT = 100     # ek IP se
ENROLL_WINDOW = 3600   # ek ghante mein
_enroll_hits = {}      # ip -> [timestamps]


def client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


@app.post("/api/enroll")
async def enroll(request: Request):
    # naam/email nahi lete; spam rokne ke liye IP limit
    ip = client_ip(request)
    now = time.time()
    hits = [t for t in _enroll_hits.get(ip, []) if now - t < ENROLL_WINDOW]
    if len(hits) >= ENROLL_LIMIT:
        raise HTTPException(429, "Bahut zyada try ho gaye. Thodi der baad aao.")
    hits.append(now)
    _enroll_hits[ip] = hits

    cur = DB.execute(
        "INSERT INTO users (name, email, created_at) VALUES (NULL, NULL, ?)",
        (int(now),),
    )
    DB.commit()
    return {"token": make_token(cur.lastrowid)}


# ---------- batches, progress, profile ----------
TZ_OFFSET = 19800  # IST (streak/aaj ka din India time se)
DB.executescript("""
CREATE TABLE IF NOT EXISTS enrollments (user_id INTEGER, course_id TEXT, ts INTEGER, PRIMARY KEY (user_id, course_id));
CREATE TABLE IF NOT EXISTS done (user_id INTEGER, video_id TEXT, ts INTEGER, PRIMARY KEY (user_id, video_id));
CREATE TABLE IF NOT EXISTS activity (user_id INTEGER, day TEXT, seconds INTEGER DEFAULT 0, PRIMARY KEY (user_id, day));
""")

for course in DATA["courses"]:
    for legacy_id in course.get("legacy_ids", []):
        DB.execute(
            "INSERT OR IGNORE INTO enrollments (user_id, course_id, ts) "
            "SELECT user_id, ?, ts FROM enrollments WHERE course_id=?",
            (course["id"], legacy_id),
        )
        DB.execute("DELETE FROM enrollments WHERE course_id=?", (legacy_id,))
DB.commit()


def day_str(offset_days: int = 0) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(time.time() + TZ_OFFSET + offset_days * 86400))


def course_by_id(cid: str):
    return next((c for c in DATA["courses"] if c["id"] == cid), None)


def course_of(video_id: str):
    return next((c for c in DATA["courses"] if any(v["id"] == video_id for v in c["videos"])), None)


def is_enrolled(uid: int, cid: str) -> bool:
    return DB.execute("SELECT 1 FROM enrollments WHERE user_id=? AND course_id=?", (uid, cid)).fetchone() is not None


def done_ids(uid: int) -> set:
    return {r[0] for r in DB.execute("SELECT video_id FROM done WHERE user_id=?", (uid,))}


@app.get("/api/config")
async def public_config():
    # .env se Telegram usernames (bina @ ke)
    clean = lambda k: os.environ.get(k, "").strip().lstrip("@")
    contact = clean("TG_CONTACT_USERNAME")
    return {"contact": contact, "socials": clean("TG_SOCIALS_USERNAME") or contact, "bot": clean("TG_BOT_USERNAME")}


@app.get("/api/notifications")
async def list_notifications():
    purge_expired_notifications()
    cutoff = int(time.time()) - 7 * 86400
    rows = DB.execute(
        "SELECT id, title, body, url, media_path, created_at FROM notifications "
        "WHERE created_at >= ? ORDER BY created_at DESC LIMIT 50",
        (cutoff,),
    ).fetchall()
    return [
        {
            "id": row[0],
            "title": row[1],
            "body": row[2],
            "url": row[3],
            "media": f"/notification-media/{quote(row[4])}" if row[4] else None,
            "createdAt": row[5],
        }
        for row in rows
    ]


def purge_expired_notifications():
    cutoff = int(time.time()) - 7 * 86400
    expired = DB.execute(
        "SELECT media_path FROM notifications WHERE created_at < ? AND media_path IS NOT NULL", (cutoff,)
    ).fetchall()
    DB.execute("DELETE FROM notifications WHERE created_at < ?", (cutoff,))
    DB.commit()
    for (filename,) in expired:
        path = os.path.abspath(os.path.join(NOTIFICATION_MEDIA_DIR, filename))
        if os.path.dirname(path) == os.path.abspath(NOTIFICATION_MEDIA_DIR) and os.path.isfile(path):
            os.remove(path)


async def notification_cleanup_loop():
    while True:
        purge_expired_notifications()
        await asyncio.sleep(3600)


@app.get("/api/courses")
async def list_courses(request: Request):
    uid = require_enrolled(request)
    done = done_ids(uid)
    mine = {r[0] for r in DB.execute("SELECT course_id FROM enrollments WHERE user_id=?", (uid,))}
    return [
        {"id": c["id"], "title": c["title"], "thumb": c.get("thumb", ""), "total": len(c["videos"]),
         "done": sum(v["id"] in done for v in c["videos"]), "enrolled": c["id"] in mine,
         "searchText": " ".join(
             [c["title"]]
             + [part for video in c["videos"] for part in (video.get("title", ""), video.get("chapter", ""), video.get("unit", ""))]
         )}
        for c in DATA["courses"]
    ]


@app.post("/api/courses/{cid}/enroll")
async def enroll_course(cid: str, request: Request):
    uid = require_enrolled(request)
    if not course_by_id(cid):
        raise HTTPException(404, "Batch nahi mila")
    DB.execute("INSERT OR IGNORE INTO enrollments VALUES (?, ?, ?)", (uid, cid, int(time.time())))
    DB.commit()
    return {"ok": True}


@app.delete("/api/courses/{cid}/enroll")
async def unenroll_course(cid: str, request: Request):
    uid = require_enrolled(request)
    if not course_by_id(cid):
        raise HTTPException(404, "Batch nahi mila")
    DB.execute("DELETE FROM enrollments WHERE user_id=? AND course_id=?", (uid, cid))
    DB.commit()
    return {"ok": True}


@app.get("/api/courses/{cid}")
async def course_detail(cid: str, request: Request):
    uid = require_enrolled(request)
    c = course_by_id(cid)
    if not c:
        raise HTTPException(404, "Batch nahi mila")
    if not is_enrolled(uid, cid):
        raise HTTPException(403, "Pehle batch enroll karo")
    done = done_ids(uid)
    positions = dict(DB.execute(
        "SELECT video_id, position FROM watch_progress WHERE user_id=?", (uid,)
    ).fetchall())
    return {"id": c["id"], "title": c["title"], "subjectBatch": c.get("subject_batch", False),
            "directSubjects": sorted(c.get("direct_subjects", [])),
            "subjectChapters": c.get("subject_chapters", []),
            "lessons": [{"id": v["id"], "title": v["title"], "chapter": v.get("chapter", ""),
                         "unit": v.get("unit", ""), "unitId": v.get("unit_id", ""),
                         "done": v["id"] in done, "position": positions.get(v["id"], 0),
                         "notes": [{"id": n["id"], "title": n["title"], "kind": n.get("kind", "notes")}
                                   for n in c.get("notes", [])
                                   if n.get("lesson_id") == v["id"]
                                   or (not n.get("lesson_id")
                                       and n.get("chapter", "") == v.get("chapter", "")
                                       and n.get("title") in {f"{v['title']} (Notes)", f"{v['title']} (Sheet)"})]}
                        for v in c["videos"]],
            "notes": [{"id": n["id"], "title": n["title"], "chapter": n.get("chapter", ""),
                       "kind": n.get("kind", "notes"), "lessonId": n.get("lesson_id", "")}
                      for n in c.get("notes", [])]}


class LessonDurations(BaseModel):
    video_ids: list[str]


async def lesson_duration(video_id: str, video: dict):
    async with _duration_semaphore:
        tried = set()
        while True:
            bot = POOL.pick(video["chat_id"], tried)
            if bot is None:
                raise HTTPException(503, "Abhi video duration load nahi ho pa rahi. Thodi der baad try karo.")
            tried.add(bot.idx)
            slot = bot.acquire()
            try:
                message = await fetch_message(bot, video_id, video["chat_id"], video["message_id"])
                attributes = getattr(getattr(message, "document", None), "attributes", [])
                return next(
                    (float(attribute.duration) for attribute in attributes
                     if getattr(attribute, "duration", None) is not None),
                    None,
                )
            except HTTPException:
                raise
            except Exception as error:
                await mark_bot_failed(bot, error, video["chat_id"])
            finally:
                bot.release(slot)


@app.post("/api/courses/{cid}/durations")
async def course_lesson_durations(cid: str, body: LessonDurations, request: Request):
    uid = require_enrolled(request)
    course = course_by_id(cid)
    if not course:
        raise HTTPException(404, "Batch nahi mila")
    if not is_enrolled(uid, cid):
        raise HTTPException(403, "Pehle batch enroll karo")
    if len(body.video_ids) > 50:
        raise HTTPException(400, "Ek baar mein 50 videos tak ki duration mang sakte hain")
    lessons = {video["id"]: video for video in course["videos"]}
    if any(video_id not in lessons for video_id in body.video_ids):
        raise HTTPException(400, "Invalid video list")

    async def get_duration(video_id):
        if video_id not in _duration_cache:
            _duration_cache[video_id] = await lesson_duration(video_id, lessons[video_id])
        return video_id, _duration_cache[video_id]

    durations = dict(await asyncio.gather(*(get_duration(video_id) for video_id in dict.fromkeys(body.video_ids))))
    return {"durations": durations}


def note_of(note_id: str):
    for c in DATA["courses"]:
        for n in c.get("notes", []):
            if n["id"] == note_id:
                return c, n
    return None, None


@app.get("/api/note/{note_id}")
async def note_url(note_id: str, request: Request):
    uid = require_enrolled(request)
    c, n = note_of(note_id)
    if not n or not is_enrolled(uid, c["id"]):
        raise HTTPException(403, "Pehle batch enroll karo")
    if n.get("url"):  # bahari link (Notion/GitHub wagairah)
        return {"url": n["url"]}
    exp = int(time.time()) + LINK_TTL
    return {"url": f"{PUBLIC_BASE_URL}/n/{note_id}?exp={exp}&sig={sign(f'note:{note_id}', exp)}"}


class Progress(BaseModel):
    video_id: str
    seconds: int = 0
    done: bool | None = None
    position: float | None = None


class BulkProgress(BaseModel):
    video_ids: list[str]
    done: bool


@app.post("/api/progress")
async def save_progress(body: Progress, request: Request):
    uid = require_enrolled(request)
    c = course_of(body.video_id)
    if not c or not is_enrolled(uid, c["id"]):
        raise HTTPException(403, "Pehle batch enroll karo")
    secs = max(0, min(body.seconds, 120))
    if secs:
        DB.execute(
            "INSERT INTO activity (user_id, day, seconds) VALUES (?, ?, ?) "
            "ON CONFLICT(user_id, day) DO UPDATE SET seconds = seconds + excluded.seconds",
            (uid, day_str(), secs),
        )
    if body.done is True:
        DB.execute("INSERT OR IGNORE INTO done VALUES (?, ?, ?)", (uid, body.video_id, int(time.time())))
    elif body.done is False:
        DB.execute("DELETE FROM done WHERE user_id=? AND video_id=?", (uid, body.video_id))
    if body.position is not None:
        position = max(0.0, min(float(body.position), 86400.0))
        DB.execute(
            "INSERT INTO watch_progress (user_id, video_id, position, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(user_id, video_id) DO UPDATE SET position=excluded.position, updated_at=excluded.updated_at",
            (uid, body.video_id, position, int(time.time())),
        )
    DB.commit()
    return {"ok": True}


@app.post("/api/progress/bulk")
async def save_bulk_progress(body: BulkProgress, request: Request):
    uid = require_enrolled(request)
    if not body.video_ids or len(body.video_ids) > 1000:
        raise HTTPException(400, "1 se 1000 lessons select karo")
    video_ids = list(dict.fromkeys(body.video_ids))
    courses = {course_of(video_id)["id"] for video_id in video_ids if course_of(video_id)}
    if len(courses) != 1 or any(not course_of(video_id) for video_id in video_ids):
        raise HTTPException(400, "Selected lessons ek hi batch ke hone chahiye")
    cid = next(iter(courses))
    if not is_enrolled(uid, cid):
        raise HTTPException(403, "Pehle batch enroll karo")
    if body.done:
        DB.executemany(
            "INSERT OR IGNORE INTO done VALUES (?, ?, ?)",
            ((uid, video_id, int(time.time())) for video_id in video_ids),
        )
    else:
        DB.executemany(
            "DELETE FROM done WHERE user_id=? AND video_id=?",
            ((uid, video_id) for video_id in video_ids),
        )
    DB.commit()
    return {"ok": True, "updated": len(video_ids), "done": body.done}


@app.get("/api/me")
async def me(request: Request):
    uid = require_enrolled(request)
    joined = DB.execute("SELECT created_at FROM users WHERE id=?", (uid,)).fetchone()[0]
    done = done_ids(uid)
    mine = [course_by_id(r[0]) for r in DB.execute("SELECT course_id FROM enrollments WHERE user_id=?", (uid,))]
    mine = [c for c in mine if c]
    total = sum(len(c["videos"]) for c in mine)
    dn = sum(v["id"] in done for c in mine for v in c["videos"])
    completed = sum(1 for c in mine if c["videos"] and all(v["id"] in done for v in c["videos"]))
    days = dict(DB.execute("SELECT day, seconds FROM activity WHERE user_id=?", (uid,)).fetchall())
    off = 0 if day_str() in days else -1
    streak = 0
    while day_str(off) in days:
        streak += 1
        off -= 1
    xp = len(done) * 10
    return {"joined": joined, "streak": streak, "level": xp // 500 + 1, "xp": xp % 500,
            "today": days.get(day_str(), 0), "total_seconds": sum(days.values()),
            "done": dn, "total": total, "completed": completed}


@app.get("/api/play/{video_id}")
async def get_play_url(video_id: str, request: Request):
    uid = require_enrolled(request)
    if video_id not in VIDEOS:
        raise HTTPException(404, "Video not found")
    c = course_of(video_id)
    if not c or not is_enrolled(uid, c["id"]):
        raise HTTPException(403, "Pehle batch enroll karo")
    exp = int(time.time()) + LINK_TTL
    sig = sign(video_id, exp)
    return {"url": f"{PUBLIC_BASE_URL}/stream/{video_id}?exp={exp}&sig={sig}"}


@app.get("/api/download/{video_id}")
async def get_download_url(video_id: str, request: Request):
    uid = require_enrolled(request)
    if video_id not in VIDEOS:
        raise HTTPException(404, "Video not found")
    c = course_of(video_id)
    if not c or not is_enrolled(uid, c["id"]):
        raise HTTPException(403, "Pehle batch enroll karo")
    exp = int(time.time()) + LINK_TTL
    sig = sign(video_id, exp)
    return {"url": f"{PUBLIC_BASE_URL}/download/{video_id}?exp={exp}&sig={sig}"}


# ---------- streaming ----------
def check_sig(key: str, exp: int, sig: str):
    if exp < time.time() or not hmac.compare_digest(sig.encode(), sign(key, exp).encode()):
        raise HTTPException(403, "Invalid or expired link")


async def fetch_message(bot, cache_key: str, chat_id: int, message_id: int):
    # message har bot ke liye alag cache hota hai (file reference bot-specific hota hai)
    ck = f"{bot.idx}:{cache_key}"
    now = time.time()
    cached = _msg_cache.get(ck)
    if cached and cached[0] > now:
        return cached[1]
    msg = await bot.client.get_messages(chat_id, ids=message_id)
    if not msg or not msg.media:
        raise HTTPException(404, "Telegram message/media not found")
    if len(_msg_cache) > 2000:
        for k in [k for k, (t, _) in _msg_cache.items() if t < now]:
            del _msg_cache[k]
    _msg_cache[ck] = (now + MSG_TTL, msg)
    return msg


async def stream_iter(bot, slot, msg, cache_key, chat_id, message_id, start: int, end: int):
    """start..end (inclusive) bytes. Bot atke/FloodWait aaye to bich mein hi agle bot par chala jata hai."""
    pos, remaining, tried = start, end - start + 1, {bot.idx}
    try:
        while remaining > 0:
            aligned = pos - (pos % CHUNK)
            skip = pos - aligned
            try:
                async for chunk in bot.client.iter_download(msg.media, offset=aligned, request_size=CHUNK):
                    if skip:
                        chunk, skip = chunk[skip:], 0
                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]
                    remaining -= len(chunk)
                    pos += len(chunk)
                    yield chunk
                    if remaining <= 0:
                        return
                return  # file khatam
            except Exception as e:
                await mark_bot_failed(bot, e, chat_id)
                while True:
                    nxt = POOL.pick(chat_id, tried)
                    if nxt is None:
                        raise
                    tried.add(nxt.idx)
                    bot.release(slot)
                    bot, slot = nxt, nxt.acquire()
                    try:
                        msg = await fetch_message(bot, cache_key, chat_id, message_id)
                        break
                    except Exception as e2:
                        await mark_bot_failed(bot, e2, chat_id)
    finally:
        bot.release(slot)


async def transcode_iter(bot, slot, msg, chat_id: int):
    """MKV ko fragmented MP4 mein badal kar browser playback ke liye stream karta hai."""
    ffmpeg = shutil.which(os.environ.get("FFMPEG_PATH", "ffmpeg"))
    if not ffmpeg:
        bot.release(slot)
        raise RuntimeError("MKV playback ke liye FFmpeg install karein ya FFMPEG_PATH set karein")

    proc = await asyncio.to_thread(
        subprocess.Popen,
        [
            ffmpeg, "-hide_banner", "-loglevel", "error", "-i", "pipe:0",
            "-map", "0:v:0", "-map", "0:a:0?", "-c:v", "libx264",
            "-preset", "veryfast", "-pix_fmt", "yuv420p", "-c:a", "aac",
            "-b:a", "128k", "-movflags", "frag_keyframe+empty_moov+default_base_moof",
            "-f", "mp4", "pipe:1",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    async def feed_ffmpeg():
        try:
            async for chunk in bot.client.iter_download(msg.media, request_size=CHUNK):
                await asyncio.to_thread(proc.stdin.write, chunk)
                await asyncio.to_thread(proc.stdin.flush)
        finally:
            if proc.stdin and not proc.stdin.closed:
                await asyncio.to_thread(proc.stdin.close)

    feeder = asyncio.create_task(feed_ffmpeg())
    stderr_task = asyncio.create_task(asyncio.to_thread(proc.stderr.read))
    try:
        while chunk := await asyncio.to_thread(proc.stdout.read1, CHUNK):
            yield chunk
        await feeder
        return_code = await asyncio.to_thread(proc.wait)
        stderr = (await stderr_task).decode(errors="replace").strip()
        if return_code:
            raise RuntimeError(f"FFmpeg video conversion failed: {stderr or return_code}")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        await mark_bot_failed(bot, e, chat_id)
        print(f"[transcode] message {getattr(msg, 'id', 'unknown')} failed: {e!r}")
        raise
    finally:
        if proc.poll() is None:
            proc.kill()
        if not feeder.done():
            feeder.cancel()
            await asyncio.gather(feeder, return_exceptions=True)
        await asyncio.to_thread(proc.wait)
        if not stderr_task.done():
            await asyncio.gather(stderr_task, return_exceptions=True)
        for pipe in (proc.stdin, proc.stdout, proc.stderr):
            if pipe and not pipe.closed:
                pipe.close()
        bot.release(slot)


async def build_response(msg, request: Request, bot, slot, cache_key, chat_id, message_id, download: bool = False):
    size = msg.file.size
    mime = msg.file.mime_type or "video/mp4"
    if not download and mime.lower() in {"video/x-matroska", "application/x-matroska"}:
        converted = transcode_iter(bot, slot, msg, chat_id)
        startup = bytearray()
        try:
            while b"moof" not in startup and len(startup) < 2 * CHUNK:
                startup.extend(await anext(converted))
        except StopAsyncIteration:
            raise HTTPException(502, "FFmpeg produced no playable video data")
        except Exception as e:
            raise HTTPException(502, f"Video conversion failed: {e!r}") from e

        async def converted_body():
            try:
                yield bytes(startup)
                async for chunk in converted:
                    yield chunk
            finally:
                await converted.aclose()

        return StreamingResponse(
            converted_body(),
            headers={"Content-Type": "video/mp4", "Cache-Control": "no-store", "Accept-Ranges": "none"},
        )

    start, end = 0, size - 1
    status = 200
    range_header = request.headers.get("range")
    if range_header and range_header.startswith("bytes="):
        try:
            s, e = range_header[6:].split("-", 1)
            if s == "":  # suffix range: bytes=-500
                start = max(size - int(e), 0)
            else:
                start = int(s)
                if e:
                    end = min(int(e), size - 1)
        except ValueError:
            raise HTTPException(416, "Bad range")
        if start > end or start >= size:
            raise HTTPException(416, "Range not satisfiable")
        status = 206

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime,
        "Content-Length": str(end - start + 1),
    }
    if download:
        name = msg.file.name or f"file_{msg.id}{msg.file.ext or ''}"
        headers["Content-Disposition"] = "attachment; filename*=UTF-8''" + quote(name)
    else:
        headers["Cache-Control"] = "private, max-age=3600"
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    gen = stream_iter(bot, slot, msg, cache_key, chat_id, message_id, start, end)
    return StreamingResponse(gen, status_code=status, headers=headers)


async def serve_pooled(request: Request, cache_key: str, chat_id: int, message_id: int, download: bool = False):
    """Sabse sahi bot chunke, message laye, stream shuru kare. Bot fail ho to agla bot."""
    tried = set()
    while True:
        bot = POOL.pick(chat_id, tried)
        if bot is None:
            await alert_admins_once("all-bots", f"🚨 Video channel {chat_id}: saare bots unavailable hain.")
            raise HTTPException(503, "Abhi saare bots busy ya unavailable hain. Thodi der baad try karo.")
        tried.add(bot.idx)
        slot = bot.acquire()  # pehle hi slot le lo, taaki ek saath aayi requests ek hi bot par na pile hon
        try:
            msg = await fetch_message(bot, cache_key, chat_id, message_id)
            return await build_response(msg, request, bot, slot, cache_key, chat_id, message_id, download)
        except HTTPException:
            bot.release(slot)
            raise
        except Exception as e:
            bot.release(slot)
            await mark_bot_failed(bot, e, chat_id)
        except BaseException:
            bot.release(slot)
            raise


# Course videos (videos.json wali)
@app.get("/stream/{video_id}")
async def stream(video_id: str, request: Request, exp: int = 0, sig: str = ""):
    if video_id not in VIDEOS:
        raise HTTPException(404, "Video not found")
    check_sig(video_id, exp, sig)
    v = VIDEOS[video_id]
    return await serve_pooled(request, video_id, v["chat_id"], v["message_id"])


@app.get("/download/{video_id}")
async def download_video(video_id: str, request: Request, exp: int = 0, sig: str = ""):
    if video_id not in VIDEOS:
        raise HTTPException(404, "Video not found")
    check_sig(video_id, exp, sig)
    v = VIDEOS[video_id]
    return await serve_pooled(request, video_id, v["chat_id"], v["message_id"], download=True)


# Bot ke Stream/Download buttons wali direct files (channel message id se)
@app.get("/f/{message_id}")
async def file_direct(message_id: int, request: Request, exp: int = 0, sig: str = "", dl: int = 0):
    check_sig(f"file:{message_id}", exp, sig)
    return await serve_pooled(request, f"ch:{message_id}", CHANNEL_ID, message_id, download=bool(dl))


# Chhota player page (Stream button yahi kholta hai)
@app.get("/watch/{message_id}")
async def watch(message_id: int, exp: int = 0, sig: str = ""):
    check_sig(f"file:{message_id}", exp, sig)  # verify ke baad exp/sig safe hain
    src = f"/f/{message_id}?exp={exp}&sig={sig}"
    html = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<title>CODExTRMS | Video</title></head>"
        "<body style='margin:0;background:#000;display:flex;align-items:center;"
        "justify-content:center;min-height:100vh'>"
        f"<video src='{src}' controls autoplay playsinline "
        "style='width:100%;max-height:100vh'></video></body></html>"
    )
    return HTMLResponse(html)


# Sheets / notes (PDF etc.) - signed link
@app.get("/n/{note_id}")
async def note_stream(note_id: str, request: Request, exp: int = 0, sig: str = ""):
    check_sig(f"note:{note_id}", exp, sig)
    c, n = note_of(note_id)
    if not n or n.get("url"):
        raise HTTPException(404, "Note not found")
    return await serve_pooled(request, f"n:{note_id}", n["chat_id"], n["message_id"])
