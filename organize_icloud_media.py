"""Organize direct iCloud Drive media files into Images and Videos folders.

By default this is a dry run: it prints the proposed moves but changes nothing.
Run with --apply to move the files. Files are moved inside the iCloud folder,
not deleted. A Live Photo is detected when an image and a .MOV file have the
same filename stem (for example IMG_1001.HEIC and IMG_1001.MOV); both members
of that pair are placed in Images.
"""

import argparse
import shutil
import sys
from pathlib import Path
from typing import List, Optional, Set, Tuple


# Keep this in sync with the uploader unless you want to organize another
# iCloud folder. The script considers direct files only; it never recurses.
ICLOUD_FOLDER = Path(r"C:\Users\dell\iCloudDrive\Ganesh Chaturthi")
IMAGES_FOLDER_NAME = "Images"
VIDEOS_FOLDER_NAME = "Videos"

IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif", ".gif", ".webp", ".bmp",
    ".tif", ".tiff", ".dng", ".raw", ".arw", ".cr2", ".nef",
}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".3gp", ".webm"}


def media_kind(path: Path) -> Optional[str]:
    """Return image/video for a recognized media file, otherwise None."""
    suffix = path.suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        return "image"
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    return None


def find_live_photo_members(files: List[Path]) -> Set[Path]:
    """Return same-stem image/.MOV pairs, including both files of each pair."""
    by_stem = {}
    for file_path in files:
        by_stem.setdefault(file_path.stem.casefold(), []).append(file_path)

    live_photo_members: Set[Path] = set()
    for candidates in by_stem.values():
        has_image = any(candidate.suffix.lower() in IMAGE_EXTENSIONS for candidate in candidates)
        has_mov = any(candidate.suffix.lower() == ".mov" for candidate in candidates)
        if has_image and has_mov:
            live_photo_members.update(candidates)
    return live_photo_members


def plan_moves(source: Path) -> Tuple[List[Tuple[Path, Path, str]], int]:
    """Build a non-destructive move plan and count unrecognized files."""
    files = [path for path in source.iterdir() if path.is_file()]
    live_photo_members = find_live_photo_members(files)
    images_folder = source / IMAGES_FOLDER_NAME
    videos_folder = source / VIDEOS_FOLDER_NAME
    moves: List[Tuple[Path, Path, str]] = []
    ignored = 0

    for file_path in sorted(files, key=lambda path: path.name.casefold()):
        kind = media_kind(file_path)
        if kind is None:
            ignored += 1
            continue
        is_live_photo = file_path in live_photo_members
        destination_folder = images_folder if kind == "image" or is_live_photo else videos_folder
        label = "Live Photo" if is_live_photo else kind.title()
        moves.append((file_path, destination_folder / file_path.name, label))
    return moves, ignored


def execute_moves(moves: List[Tuple[Path, Path, str]], apply: bool) -> int:
    """Print or perform moves, never overwriting an existing destination."""
    moved = 0
    skipped_conflicts = 0
    for source, destination, label in moves:
        if destination.exists():
            print(f"SKIP (destination exists): {source.name} -> {destination}")
            skipped_conflicts += 1
            continue
        action = "MOVE" if apply else "WOULD MOVE"
        print(f"{action} [{label}]: {source.name} -> {destination.parent.name}\\")
        if apply:
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(source), str(destination))
                moved += 1
            except OSError as exc:
                print(f"ERROR moving {source.name}: {exc}", file=sys.stderr)

    if skipped_conflicts:
        print(f"Skipped {skipped_conflicts} file(s) because a same-named destination exists.")
    return moved


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="move files after showing the plan (default is preview only)",
    )
    parser.add_argument(
        "--folder", type=Path, default=ICLOUD_FOLDER,
        help=f"iCloud folder to organize (default: {ICLOUD_FOLDER})",
    )
    args = parser.parse_args()
    source = args.folder.expanduser().resolve()
    if not source.is_dir():
        print(f"Folder does not exist: {source}", file=sys.stderr)
        return 1

    moves, ignored = plan_moves(source)
    print(f"Folder: {source}")
    print(f"Recognized media files: {len(moves)}; unrecognized direct files: {ignored}")
    if not args.apply:
        print("Preview only. No files are being moved. Re-run with --apply to organize.")
    moved = execute_moves(moves, args.apply)
    if args.apply:
        print(f"Completed: moved {moved} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
