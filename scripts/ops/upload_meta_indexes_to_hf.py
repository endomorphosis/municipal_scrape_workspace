#!/usr/bin/env python3
"""
Upload CC meta indexes (pointer and rowgroup indices) to HuggingFace dataset.

Usage:
    python upload_meta_indexes_to_hf.py --source ~/common_crawl_meta_indexes/2025 \
        --repo Publicus/common_crawl_meta_indexes [--private] [--workers 4]
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

try:
    from huggingface_hub import HfApi, login
    from tqdm import tqdm
except ImportError as e:
    print(f"Error: Missing dependency. Install with: pip install huggingface-hub tqdm", file=sys.stderr)
    sys.exit(1)


def authenticate() -> Optional[str]:
    """Authenticate with HuggingFace using stored credentials or token."""
    try:
        api = HfApi()
        # Try to get existing token from cache
        try:
            me = api.whoami()
            print(f"✓ Already authenticated as {me.get('name', 'user')}")
            return None
        except Exception:
            # Not authenticated yet, try to use HF_TOKEN env var
            token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
            if token:
                print("✓ Using HF_TOKEN from environment")
                return token
            # Otherwise prompt for login
            print("⚠ Not authenticated. Please log in:")
            login()
            return None
    except Exception as e:
        print(f"Error during authentication: {e}", file=sys.stderr)
        return None


def upload_directory(
    source_dir: Path,
    repo_id: str,
    token: Optional[str] = None,
    private: bool = False,
    workers: int = 4,
    ignore_patterns: Optional[list] = None,
) -> bool:
    """Upload a local directory to HuggingFace dataset."""
    
    if not source_dir.is_dir():
        print(f"Error: Source directory not found: {source_dir}", file=sys.stderr)
        return False
    
    # Count files and total size
    total_size = 0
    file_count = 0
    for file_path in source_dir.rglob("*"):
        if file_path.is_file():
            file_count += 1
            total_size += file_path.stat().st_size
    
    size_gb = total_size / (1024**3)
    print(f"\nUpload summary:")
    print(f"  Source: {source_dir}")
    print(f"  Target: https://huggingface.co/datasets/{repo_id}")
    print(f"  Files: {file_count}")
    print(f"  Size: {size_gb:.2f} GB")
    print(f"  Workers: {workers}")
    print(f"  Private: {private}")
    
    try:
        api = HfApi(token=token)
        
        # Check if dataset exists, create if needed
        try:
            repo_info = api.dataset_info(repo_id)
            print(f"✓ Dataset exists: {repo_id}")
        except Exception:
            print(f"Creating dataset: {repo_id}")
            api.create_repo(
                repo_id=repo_id,
                repo_type="dataset",
                private=private,
                exist_ok=True,
            )
        
        print(f"\n⏳ Starting upload...")
        api.upload_large_folder(
            repo_id=repo_id,
            folder_path=str(source_dir),
            repo_type="dataset",
            private=private,
            num_workers=workers,
        )
        
        print(f"\n✓ Upload completed successfully!")
        print(f"  Dataset: https://huggingface.co/datasets/{repo_id}")
        return True
        
    except Exception as e:
        print(f"\nError during upload: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Upload CC meta indexes to HuggingFace dataset"
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path.home() / "common_crawl_meta_indexes" / "2025",
        help="Source directory with CC meta indexes (default: ~/common_crawl_meta_indexes/2025)",
    )
    parser.add_argument(
        "--repo",
        type=str,
        default="Publicus/common_crawl_meta_indexes",
        help="Target HuggingFace dataset repo ID (default: Publicus/common_crawl_meta_indexes)",
    )
    parser.add_argument(
        "--token",
        type=str,
        default=os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN"),
        help="HuggingFace API token (uses HF_TOKEN env var if not provided)",
    )
    parser.add_argument(
        "--private",
        action="store_true",
        help="Create as private dataset (default: public)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel upload workers (default: 4)",
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("HuggingFace Meta Index Uploader")
    print("=" * 70)
    
    # Authenticate
    token = authenticate()
    if token:
        args.token = token
    
    # Validate source
    source = Path(args.source).expanduser()
    if not source.is_dir():
        print(f"\nError: Source directory not found: {source}", file=sys.stderr)
        print(f"Expected CC meta indexes at: {source}", file=sys.stderr)
        sys.exit(1)
    
    # Upload
    success = upload_directory(
        source_dir=source,
        repo_id=args.repo,
        token=args.token,
        private=args.private,
        workers=args.workers,
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
