#!/usr/bin/env python3
"""Replace plain \\texttt{...} with \\id{...} outside codeblock; keep complex TeX inside texttt."""

from __future__ import annotations

import re
import sys
from pathlib import Path

# If argument contains these substrings, keep \\texttt (regex literals, special TeX, etc.).
SKIP_IF_CONTAINS = (
    "\\^{",
    "\\$",
    "\\%",
    "\\#",
    "\\&",
    "\\char",
    "\\verb",
    "\\lstinline",
)


def transform_segment(seg: str) -> str:
    out: list[str] = []
    i = 0
    n = len(seg)
    while i < n:
        if seg.startswith("\\texttt{", i):
            j = i + 8
            depth = 1
            start = j
            while j < n and depth:
                c = seg[j]
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                j += 1
            inner = seg[start : j - 1]
            if any(s in inner for s in SKIP_IF_CONTAINS):
                out.append(seg[i:j])
            else:
                inner2 = inner.replace("\\_", "_")
                out.append("\\id{" + inner2 + "}")
            i = j
            continue
        out.append(seg[i])
        i += 1
    return "".join(out)


def transform_file(text: str) -> str:
    parts = re.split(r"(\\begin\{codeblock\}.*?\\end\{codeblock\})", text, flags=re.S)
    buf: list[str] = []
    for part in parts:
        if part.startswith("\\begin{codeblock}"):
            buf.append(part)
        else:
            buf.append(transform_segment(part))
    return "".join(buf)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    targets = [
        root / "contents" / "ch02_related.tex",
        root / "contents" / "ch03_requirements.tex",
        root / "contents" / "ch04_design.tex",
        root / "contents" / "ch05_experiment.tex",
        root / "contents" / "summary.tex",
        root / "contents" / "digest.tex",
        root / "contents" / "intro.tex",
        root / "contents" / "appendix.tex",
    ]
    for path in targets:
        if not path.is_file():
            print(f"skip missing {path}", file=sys.stderr)
            continue
        old = path.read_text(encoding="utf-8")
        new = transform_file(old)
        if new != old:
            path.write_text(new, encoding="utf-8")
            print(f"updated {path.relative_to(root)}")
        else:
            print(f"unchanged {path.relative_to(root)}")


if __name__ == "__main__":
    main()
